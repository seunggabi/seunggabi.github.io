---
title: "Iceberg의 행 단위 변경은 쓰기 비용을 읽기로 미룬다"
date: 2026-11-20 06:01:00 +0900
categories: [Lakehouse & Storage, Iceberg]
tags: [iceberg, merge-on-read, compaction, spark, cdc, concept]
description: "Iceberg의 merge-on-read 행 단위 변경이 쓰기 비용을 읽기 쪽으로 미루는 방식과, delete 파일이 쌓일 때 조회가 느려지는 이유를 설명한다."
---

upsert가 계속 들어오는 Iceberg 테이블은 커밋이 쌓일수록 조회가 느려진다. 행 하나를 바꿀 때 데이터 파일을 다시 쓰지 않고 삭제 표시만 따로 남기면, 그 표시를 맞춰 보는 비용은 읽을 때마다 든다. delete 파일이 데이터 파일보다 많아질 만큼 쌓이면, 읽기 계획 단계에서 드라이버 메모리가 모자라 잡이 끝나기도 한다.

메타데이터 트리와 낙관적 커밋은 [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에 적었다. 그 글 끝에서 짧게 다룬 delete 파일의 비용을 여기서 풀어 쓴다.

버전은 Iceberg 1.12.0과 같은 태그의 테이블 스펙 기준이다.

## copy-on-write와 merge-on-read는 명령마다 고른다

Spark의 `DELETE`, `UPDATE`, `MERGE INTO`는 각각 `write.delete.mode`, `write.update.mode`, `write.merge.mode` 테이블 속성으로 방식을 정한다. 기본값은 셋 다 `copy-on-write`이고, `merge-on-read`는 v2 이상 테이블에서 고를 수 있다.

copy-on-write는 바뀐 행이 하나라도 든 데이터 파일을 통째로 다시 쓴다. 안 바뀐 행도 같이 복사되고, 그 수가 Spark 4.2부터 스냅샷 요약의 `spark.delete.num-copied-rows`, `spark.update.num-copied-rows`에 남는다. 읽는 쪽은 아무것도 맞춰 볼 필요가 없다.

merge-on-read는 데이터 파일을 그대로 두고 "이 행은 지워졌다"는 delete 파일만 더한다. 같은 지표가 0으로 찍힌다. 쓰기는 가볍고, 읽는 엔진이 데이터 파일과 delete 파일을 합쳐서 결과를 낸다. 어느 방식이든 `DELETE` 조건이 파티션 전체와 맞으면 파일을 쓰지 않고 메타데이터만 바꾼다.

```sql
ALTER TABLE prod.db.orders SET TBLPROPERTIES (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);
```

| | copy-on-write | merge-on-read |
|---|---|---|
| 쓰기 비용 | 바뀐 행이 든 파일 전체 | 지운 행만큼의 delete 파일 |
| 읽기 비용 | 추가 없음 | delete 파일을 맞춰 보는 만큼 |
| 맞는 경우 | 가끔 크게 고치고 자주 읽는 테이블 | 작은 변경이 자주 들어오는 테이블 |

## 삭제를 기록하는 방식은 세 가지다

| 방식 | 기록하는 것 | 스펙 버전 |
|---|---|---|
| position delete 파일 | 데이터 파일 경로와 행 위치 | v2, v3에서는 새로 추가할 수 없음 |
| equality delete 파일 | 하나 이상 컬럼의 값 (`id = 3`) | v2 이상 |
| deletion vector | 데이터 파일 하나의 지운 행 위치를 담은 Roaring 비트맵 | v3, Puffin `deletion-vector-v1` 블롭 |

Spark의 merge-on-read는 지울 행을 먼저 찾아서 위치를 안다. 그래서 v2 테이블에는 position delete를, v3 테이블에는 deletion vector를 쓴다. Flink upsert는 키 값으로 equality delete를 쓴다. 이전 행이 어느 파일 몇 번째에 있는지 찾지 않고 키만 적으면 되니, 쓰는 쪽이 읽기 없이 계속 흘려보낼 수 있는 방식이다. 파티션 테이블에서 Flink upsert를 쓰려면 파티션 원본 컬럼이 equality 필드에 들어 있어야 한다.

읽을 때 적용 범위는 스펙의 sequence number 규칙으로 정해진다.

- position delete와 deletion vector는 같은 경로의 데이터 파일 중 sequence number가 같거나 작은 것에만 붙는다.
- equality delete는 같은 파티션에서 sequence number가 더 작은 모든 데이터 파일에 붙는다. delete 파일이 파티션 없는 스펙으로 쓰였으면 모든 파티션에 붙는다.

deletion vector는 데이터 파일 하나에 스냅샷당 하나만 허용된다. 새 삭제가 생기면 writer가 기존 벡터와 position delete 파일을 합쳐 새 벡터로 바꾼다. 데이터 파일 하나를 읽을 때 맞춰 볼 삭제가 비트맵 하나로 끝나는 구조다. 대신 v3 테이블은 읽는 엔진이 v3를 지원해야 하고, `format-version`의 기본값은 1.12에서도 2다.

## 스트리밍 upsert는 커밋마다 equality delete를 더한다

CDC sink가 upsert로 쓰면 커밋 주기마다 파티션별로 equality delete 파일이 생긴다. 그 파일 하나는 같은 파티션의 더 오래된 데이터 파일 전부에 적용될 후보다. 읽는 엔진은 계획 단계에서 데이터 파일마다 붙을 delete 파일을 고르고, 실행 단계에서 delete 파일의 키를 읽어 행마다 대조한다. 커밋이 쌓일수록 양쪽이 같이 늘어난다.

delete 파일에도 컬럼 lower/upper bound가 있어서 스캔 조건과 겹칠 수 없으면 건너뛴다. 원리는 [컬럼 파일과 pruning 글](/posts/columnar-files-and-pruning/)의 min/max와 같지만, 키가 흩어진 upsert의 delete 파일은 범위가 넓어 잘 걸러지지 않을 것이다(추론).

CDC upsert sink에서 같은 PK가 여러 행으로 남는 중복과 그 정리 방식은 [CDC 개념 글](/posts/change-data-capture/)에 있다. 그런 정리 잡이라면 테이블을 읽기 전에 최신 스냅샷 요약의 `total-data-files`와 `total-delete-files` 합을 보고, 기준을 넘는 테이블은 건너뛰게 할 수 있다. 같은 숫자는 SQL로 바로 볼 수 있다.

```sql
SELECT committed_at, operation,
       summary['total-data-files']       AS data_files,
       summary['total-delete-files']     AS delete_files,
       summary['total-equality-deletes'] AS eq_delete_rows
FROM prod.db.orders.snapshots
ORDER BY committed_at DESC LIMIT 5;

-- content 1: position delete와 deletion vector, 2: equality delete
SELECT content, file_format, count(*) AS files, sum(record_count) AS rows
FROM prod.db.orders.delete_files
GROUP BY content, file_format;
```

## compaction이 delete를 녹이고, 스냅샷 만료가 파일을 지운다

`rewrite_data_files`는 데이터 파일을 다시 쓰면서 붙어 있던 삭제를 반영한다. 다만 기본값으로는 delete 때문에 파일을 고르지 않을 수 있다.

| 옵션 | 기본값 | 1.12 소스에서의 동작 |
|---|---|---|
| `delete-file-threshold` | 2147483647 | 데이터 파일에 붙은 delete 파일 수가 이 값 이상이면 대상 |
| `delete-ratio-threshold` | 0.3 | 지운 행 비율로 판단하되, 데이터 파일 하나만 가리키는 delete(DV, 파일 단위 position delete)만 센다 |
| `remove-dangling-deletes` | false | 다시 쓴 뒤 어떤 데이터 파일에도 적용되지 않는 delete 파일을 커밋 하나를 더해 지운다 |

equality delete는 비율 계산에 들어가지 않으니, upsert 테이블이 파일 크기는 적당한데 delete만 많다면 기본값으로는 손대지 않는다. `delete-file-threshold`를 낮춰야 delete가 많은 파일이 대상에 들어간다. equality delete는 다른 데이터 파일에도 걸릴 수 있어서 compaction 뒤에도 남는다. `remove-dangling-deletes`는 전체 스캔 계획에서 어떤 데이터 파일에도 붙지 않은 delete 파일을 지운다.

```sql
CALL prod.system.rewrite_data_files(
  table => 'db.orders',
  options => map('delete-file-threshold', '10', 'remove-dangling-deletes', 'true'));

-- 작은 position delete 파일을 합치고, 사라진 데이터 파일을 가리키는 행을 버린다
CALL prod.system.rewrite_position_delete_files('db.orders');

CALL prod.system.expire_snapshots(
  table => 'db.orders', older_than => TIMESTAMP '2026-11-15 00:00:00', retain_last => 10);
```

compaction만으로는 저장 공간이 줄지 않는다. 옛 데이터 파일과 delete 파일은 그걸 참조하는 스냅샷이 남아 있는 동안 살아 있고, `expire_snapshots`가 그 스냅샷만 쓰던 파일을 지울 때 사라진다. S3 Tables는 compaction, 스냅샷 만료, 미참조 파일 삭제를 관리형 작업으로 돌리지만, delete 파일이 지나치게 쌓인 테이블에서는 그 compaction마저 실패할 수 있다.

## 동시에 쓰는 writer는 격리 수준만큼 충돌을 검사한다

커밋은 포인터 교체가 실패하면 새 상태 위에서 다시 시도한다. 이때 그냥 다시 붙여도 되는지는 작업이 세운 가정이 아직 맞는지로 정한다. 행 단위 명령의 가정은 `write.delete.isolation-level`, `write.update.isolation-level`, `write.merge.isolation-level`이 정한다. 셋 다 기본값은 `serializable`이고, `snapshot`으로 낮출 수 있다. Iceberg 1.12의 Spark 4.1 모듈 기준으로, 읽은 스냅샷 이후의 커밋에서 검사하는 항목은 이렇다.

| 방식 | 항상 | `snapshot` | `serializable` |
|---|---|---|---|
| copy-on-write | 없음 | 조건에 걸리는 동시 삭제 | 조건에 걸리는 동시 삭제와 동시 추가 데이터 |
| merge-on-read | 지울 행이 든 데이터 파일이 아직 있는지. `UPDATE`, `MERGE`는 동시 삭제와 delete 파일까지 | 추가 검사 없음 | 조건에 걸리는 동시 추가 데이터 |

compaction과 merge-on-read가 부딪히는 자리가 여기다. merge-on-read `DELETE`가 가리킨 데이터 파일을 그사이 compaction이 다른 파일로 바꿔 쓰면, 그 데이터 파일이 사라졌으니 `DELETE` 커밋이 실패한다. 반대로 compaction도 입력 파일이 동시 커밋으로 지워졌거나 그 파일에 position delete나 deletion vector가 더해졌으면 실패한다. `partial-progress.enabled`를 켜면 compaction이 파일 그룹 단위로 나눠 커밋한다.

예를 들어 sink가 쓰는 테이블을 덮어쓰는 중복 제거 잡이라면 DataFrame 쓰기에 `isolation-level=serializable`과 `validate-from-snapshot-id`를 줘 이 검사를 직접 걸 수 있다. DataFrame overwrite의 `isolation-level` 기본값은 `null`이고, 이때는 아무 충돌도 검사하지 않는다. sink가 계속 커밋하는 테이블에서 배치가 sink의 새 행까지 덮어 지우지 않도록, 재시도 비용을 받아들이고 검사를 켜는 설계다.

## 참고 자료

- [Apache Iceberg Table Spec (Row-level Deletes, Scan Planning, Delete Formats, Version 3)](https://iceberg.apache.org/spec/)
- [Iceberg Configuration (write.*.mode, isolation-level, format-version)](https://iceberg.apache.org/docs/latest/configuration/)
- [Iceberg Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Iceberg Spark Procedures (rewrite_data_files, rewrite_position_delete_files, expire_snapshots)](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Iceberg Spark Queries (snapshots, delete_files)](https://iceberg.apache.org/docs/latest/spark-queries/)
- [Iceberg Spark Configuration (isolation-level, validate-from-snapshot-id)](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Iceberg Flink Writes (UPSERT)](https://iceberg.apache.org/docs/latest/flink-writes/)
- [Iceberg Reliability (concurrent write operations)](https://iceberg.apache.org/docs/latest/reliability/)
- [apache/iceberg 1.12.0 BinPackRewriteFilePlanner.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/actions/BinPackRewriteFilePlanner.java)
- [apache/iceberg 1.12.0 SparkPositionDeltaWrite.java (Spark 4.1)](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaWrite.java)
- [apache/iceberg 1.12.0 SparkWrite.java (Spark 4.1)](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java)

## English summary

Iceberg can apply row-level changes by rewriting whole data files (copy-on-write, the default) or by adding delete files that readers merge later (merge-on-read). Deletes are stored as position deletes, equality deletes, or, in format v3, one deletion vector per data file. Streaming upserts add equality deletes on every commit, and each one applies to all older data files in its partition, so reads slow down until `rewrite_data_files` folds them in and `expire_snapshots` removes the old files. Tables whose delete files explode may not even be planned, and a batch that overwrites a table under a streaming sink should use a serializable check against the snapshot it read. Merge-on-read buys cheap writes at the cost of scheduled maintenance and retries when concurrent writers conflict.
