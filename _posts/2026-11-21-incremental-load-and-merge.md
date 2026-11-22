---
title: "CDC 없이 바뀐 행만 가져오는 증분 적재와 MERGE"
date: 2026-11-21 05:28:00 +0900
categories: [Ingestion & CDC, Batch Ingestion]
tags: [incremental-load, merge-into, spark, iceberg, mysql, batch, concept]
description: "updated_at 워터마크만 쓰는 증분 적재가 늦게 커밋된 행을 놓치는 이유와, lookback으로 겹쳐 읽고 MERGE INTO로 키 기준 한 벌로 반영하는 방법을 정리한다."
---

원천 MySQL 테이블에서 한 시간마다 `updated_at`이 지난 실행보다 큰 행만 읽어 레이크 테이블에 반영하는 배치가 있다. 어느 날 주문 몇 건이 레이크에만 없다. 그 주문들은 긴 트랜잭션 안에서 10시 59분에 쓰였고 11시 2분에 커밋됐다. 11시 배치가 읽은 행 중 가장 늦은 `updated_at`은 10시 59분 58초였고, 그 값이 다음 실행의 하한이 됐다. 10시 59분이 찍힌 행은 커밋된 뒤에도 그 하한 아래에 있다.

폴링 방식이 놓치는 것의 목록은 [CDC 글](/posts/change-data-capture/)에 있다. binlog를 읽을 권한이 없거나 테이블 몇 개 때문에 CDC 파이프라인을 둘 수 없으면 이 방식을 쓴다. 그때 읽는 쪽은 겹쳐 읽고, 쓰는 쪽은 겹친 행을 키로 한 벌로 만들어야 한다.

버전은 Spark 4.1.4, Iceberg 1.12.0, MySQL 8.4, PostgreSQL 19 기준이다. Spark 4.3.0이 나와 있지만 Iceberg 1.12.0의 Spark 런타임은 4.1용까지 배포돼 있다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 전체 적재 | 매번 원천 전체를 읽어 대상을 통째로 바꾼다. 삭제까지 맞고 비용은 테이블 크기에 비례한다 |
| 하이 워터마크 | 지금까지 반영한 `updated_at`이나 ID의 최댓값. 다음 실행의 하한이 된다 |
| lookback 구간 | 워터마크보다 일정 시간 앞에서부터 다시 읽는 겹침 |
| 하드 삭제 | 행이 사라져 어떤 증분 쿼리에도 안 나오는 삭제 |
| soft delete | 삭제를 `deleted_at` 같은 컬럼 갱신으로 남기는 방식 |
| `MERGE INTO` | 원천과 대상을 ON 조건으로 맞춰 UPDATE, DELETE, INSERT를 한 커밋으로 한다 |
| `WHEN NOT MATCHED BY SOURCE` | 원천에 없는 대상 행을 갱신하거나 지우는 절 |
| `MERGE_CARDINALITY_VIOLATION` | 대상 한 행에 원천 여러 행이 맞을 때 Spark가 내는 에러 |
| copy-on-write, merge-on-read | 바뀐 행이 든 파일을 다시 쓰는지, 바뀐 행과 delete 파일만 더하는지 |
| 동적 파티션 덮어쓰기 | 결과에 행이 있는 파티션만 바꾸는 `INSERT OVERWRITE` 모드 |

## 전체 적재는 비싸지만 삭제까지 맞춘다

전체 적재는 원천을 다 읽어 대상을 한 번에 바꾼다. Iceberg 테이블에 `PARTITION` 절 없는 `INSERT OVERWRITE`를 static 모드로 쓰면 테이블 전체가 한 커밋으로 바뀌고, 원천에서 지운 행도 같이 사라진다. 워터마크도 필요 없고 다시 돌리면 그대로 같은 결과다. 대신 읽는 양과 원천 부하가 테이블 크기를 따라간다. 증분 적재는 믿을 수 있는 `updated_at`이나 증가하는 ID, 그 컬럼의 인덱스가 원천에 있어야 하고, 하드 삭제를 못 보며, 대상에 키 기준으로 반영해야 한다. 코드 테이블처럼 작거나 삭제가 잦은 테이블은 전체 적재가 단순하고, 크고 바뀌는 비율이 작은 테이블에서 증분 적재가 의미를 갖는다.

## 워터마크는 읽은 행에서 얻는다

워터마크는 배치가 돈 시각이 아니고, 실제로 읽어 반영한 행의 `updated_at` 최댓값이다. 배치 서버 시계로 "지금"을 저장하면 배치 서버와 DB 서버의 시계 차이만큼 구간이 비거나 겹친다. 애플리케이션 서버가 `updated_at`을 채우는 테이블이면 서버마다 시계가 다르다는 문제가 더 붙는다. 워터마크는 대상 커밋이 끝난 뒤에 저장한다. 가장 단순하게는 대상 테이블의 `max(updated_at)`을 하한으로 쓰고, Iceberg라면 쓰기 옵션 `snapshot-property.<키>`나 Java의 `CommitMetadata.withCommitProperties`로 데이터와 같은 커밋의 스냅샷 요약에 남길 수도 있다.

같은 시각의 행은 생각보다 많다. MySQL `TIMESTAMP`와 `DATETIME`은 정밀도를 안 적으면 초 단위다. `NOW()`는 문장이 시작된 시각 하나를 돌려주므로 `UPDATE` 한 문장이 바꾼 10만 행은 모두 같은 `updated_at`을 갖는다. `updated_at > :wm`으로 읽고 `LIMIT`으로 끊다가 페이지가 같은 초 중간에서 끝나면 그 초의 나머지는 다음 페이지에서 빠지고, `>=`로 바꾸면 같은 행을 또 읽는다. `(updated_at, id)` 쌍을 커서로 쓰면 순서가 하나로 정해진다.

```sql
-- MySQL 8.4, 한 실행 안의 두 번째 페이지부터 (첫 페이지는 마지막 줄 없이)
SELECT id, status, updated_at, deleted_at
FROM orders
WHERE updated_at >= :run_lower   -- 워터마크 - lookback
  AND updated_at <  :run_upper   -- 이번 실행의 상한
  AND (updated_at > :cur_ts OR (updated_at = :cur_ts AND id > :cur_id))
ORDER BY updated_at, id
LIMIT 50000;
```

상한을 두면 같은 실행을 다시 돌려도 같은 구간을 읽는다. auto-increment ID를 워터마크로 쓰면 insert만 보이므로 append만 하는 로그성 테이블에만 맞는다. 거기서도 ID 순서와 커밋 순서는 다르다. InnoDB는 행을 넣는 순간 번호를 할당하고, MySQL 8.4 기본값인 interleaved 모드(`innodb_autoinc_lock_mode=2`)에서는 동시에 도는 문장들이 번호를 섞어 받는다. 101을 받은 트랜잭션이 102보다 늦게 커밋되면, 102까지 읽은 배치 뒤로 101은 하한 아래에 남는다. 롤백된 번호는 재사용되지 않아 구멍이 원래 생기므로 구멍만 보고 늦은 행을 찾을 수도 없다.

## 늦게 커밋되는 행은 겹쳐 읽어서 잡는다

타임스탬프가 찍히는 시각과 다른 세션에 보이는 시각 사이에 간격이 있다. MySQL `CURRENT_TIMESTAMP`는 `NOW()`와 같아서 `ON UPDATE CURRENT_TIMESTAMP` 컬럼에도 문장 시작 시각이 찍히고, PostgreSQL `now()`와 `CURRENT_TIMESTAMP`는 트랜잭션 시작 시각이다. PostgreSQL에는 갱신 때 시각을 바꿔 주는 컬럼 옵션이 없어 트리거나 애플리케이션이 채운다. PostgreSQL에서 30분 열려 있던 트랜잭션이 넣은 행은 30분 전 시각을 달고 커밋된다. 그래서 하한을 워터마크에서 lookback만큼 당겨 읽는다. lookback은 시각이 찍힌 뒤 커밋까지 걸리는 가장 긴 시간에, 애플리케이션이 시각을 채운다면 서버 간 시계 차이를 더한 것보다 길어야 한다. 그보다 오래 열린 트랜잭션은 여전히 빠지므로 MySQL `information_schema.innodb_trx`의 `trx_started`나 PostgreSQL `pg_stat_activity`의 `xact_start`로 따로 지켜본다. 겹쳐 읽은 만큼 같은 행이 다시 오니, 대상은 키로 덮어쓰고 이미 반영한 새 값을 옛 값으로 되돌리지 않게 `updated_at`을 비교한다.

## 지워진 행은 따로 찾아야 한다

하드 삭제된 행은 어떤 증분 쿼리에도 나오지 않는다. 원천에서 지우는 대신 `deleted_at`을 채우는 soft delete로 바꾸면 삭제도 `updated_at`이 바뀐 행으로 읽힌다. 대가는 원천에 있다. 원천 애플리케이션의 모든 조회가 삭제 표시를 걸러야 하고, 지운 행이 테이블에 계속 남는다. 원천 스키마를 못 바꾸면 PK 목록 전체를 주기적으로 읽어 대상과 비교한다. 원천을 PK 전체로 두고 `WHEN NOT MATCHED BY SOURCE THEN DELETE`를 쓰면 MERGE 한 문장이다. 이 절은 원천이 그 범위의 전체일 때만 맞다. 증분으로 읽은 행을 원천으로 두면 이번에 안 바뀐 대상 행이 모두 지워진다. 또 이 절이 있으면 Spark가 ON 조건을 대상 테이블 필터로 내리지 않아 대상 전체를 읽고, copy-on-write면 읽은 파일을 모두 다시 쓴다(Spark 4.1 `RewriteMergeIntoTable` 소스를 읽고 정리한 추론이다). 증분 MERGE와 떼어 드물게 돌리는 이유다.

## MERGE는 대상 한 행에 원천 한 행만 허용한다

MERGE는 ON 조건으로 대상과 원천을 조인하고 행마다 절 하나만 적용한다. 양쪽에 다 있으면 `WHEN MATCHED` 절을 위에서부터 보고 처음 맞는 것 하나를, 원천에만 있으면 `WHEN NOT MATCHED`를, 대상에만 있으면 `WHEN NOT MATCHED BY SOURCE`를 본다. 맞는 절이 없으면 그 행은 그대로다. 대상 한 행이 원천 여러 행과 맞으면 Spark는 `MERGE_CARDINALITY_VIOLATION`(SQLSTATE 23K01)으로 실패한다. 이 검사는 절의 조건을 보기 전에 매치된 행마다 하므로, `s.updated_at > t.updated_at` 조건으로 한쪽이 걸러질 거라 기대해도 실패한다. 예외는 MATCHED 절이 조건 없는 `WHEN MATCHED THEN DELETE` 하나뿐인 MERGE로, 어느 원천 행이 맞든 결과가 같아서 검사하지 않는다. lookback으로 겹쳐 읽거나 페이지 사이에 갱신된 행이 뒤 페이지에 또 나오면 같은 키가 원천에 두 번 들어 있다. 원천을 키마다 한 행으로 먼저 줄인다.

```sql
-- Spark 4.1.4 + Iceberg 1.12.0: spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0
--   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--   --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
--   --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=/tmp/wh
CREATE TABLE local.shop.orders (id BIGINT, status STRING, updated_at TIMESTAMP) USING iceberg;
INSERT INTO local.shop.orders VALUES
  (1, 'paid', TIMESTAMP '2026-11-20 10:00:00'), (2, 'paid', TIMESTAMP '2026-11-20 10:05:00');
-- lookback으로 겹쳐 읽은 결과: 1은 두 번, 2는 원천에서 soft delete됨
CREATE TEMPORARY VIEW extracted AS SELECT * FROM VALUES
  (1, 'paid',     TIMESTAMP '2026-11-20 10:00:00', false),
  (1, 'refunded', TIMESTAMP '2026-11-20 11:00:00', false),
  (2, 'paid',     TIMESTAMP '2026-11-20 11:03:00', true),
  (3, 'paid',     TIMESTAMP '2026-11-20 11:02:00', false)
  AS v(id, status, updated_at, deleted);
MERGE INTO local.shop.orders t
USING (
  SELECT id, status, updated_at, deleted FROM (
    SELECT *, row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM extracted) x
  WHERE rn = 1
) s
ON t.id = s.id
WHEN MATCHED AND s.updated_at > t.updated_at AND s.deleted THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET status = s.status, updated_at = s.updated_at
WHEN NOT MATCHED AND NOT s.deleted THEN
  INSERT (id, status, updated_at) VALUES (s.id, s.status, s.updated_at);
SELECT * FROM local.shop.orders ORDER BY id;
-- 1  refunded  2026-11-20 11:00:00
-- 3  paid      2026-11-20 11:02:00
```

`USING` 안의 `row_number()`를 빼면 1번 키 때문에 위 에러가 난다. 같은 MERGE를 다시 돌려도 `updated_at` 비교에 걸려 행 내용은 그대로다. MERGE 한 번으로 SCD2 행을 닫고 새 행을 넣는 모양은 [SCD 글](/posts/dimensional-modeling-scd/)에 있다.

## Iceberg는 MERGE를 파일 단위로 반영한다

Spark가 MERGE를 조인과 행 단위 쓰기 계획으로 바꾸고, Iceberg는 테이블 속성 `write.merge.mode`로 방식을 고른다. 기본값 copy-on-write는 Spark의 `ReplaceData` 계획이 되어, 바뀔 행이 있을 수 있는 파일을 읽고 안 바뀐 행까지 담아 새로 쓴 뒤 `overwrite` 스냅샷으로 커밋한다. merge-on-read는 `WriteDelta` 계획이 되어 바뀐 행과 지울 행의 위치만 쓴다. delete 파일이 쌓이고 compaction으로 녹는 과정은 [Iceberg 행 단위 변경 글](/posts/iceberg-row-level-deletes/)에 있다.

| 절 조합 | 대상 읽기 | 쓰기 |
|---|---|---|
| `WHEN NOT MATCHED`만 | 조인 상대로만 읽는다 | anti join으로 이미 있는 키를 빼고 append |
| `MATCHED` 포함, `NOT MATCHED BY SOURCE` 없음 | ON 조건을 필터로 내린다. copy-on-write는 런타임에 매치된 행이 든 파일만 고른다 | copy-on-write는 그 파일 전체, merge-on-read는 바뀐 행만 |
| `NOT MATCHED BY SOURCE` 포함 | 테이블 전체 | copy-on-write는 읽은 파일 전부 |

첫 줄은 append만 하는 로그 테이블을 겹쳐 읽을 때 쓸모가 있다. 이미 넣은 키는 건너뛰고 기존 파일은 건드리지 않는다. 둘째 줄의 런타임 파일 고르기는 `spark.sql.optimizer.runtime.rowLevelOperationGroupFilter.enabled`가 켜는 기능이다.

## 파티션을 다시 계산할 수 있으면 통째로 덮어쓴다

원천 이벤트가 날짜로 나뉘어 있고 그날 결과를 전부 다시 계산할 수 있으면, 키 단위 MERGE 대신 날짜 파티션을 바꾼다. 일 집계 테이블이 그렇다. `spark.sql.sources.partitionOverwriteMode`의 기본값은 `static`이다. static 모드는 `PARTITION` 절에 적은 파티션을 먼저 지우고 쓰고, 절이 없으면 전부 지운다. `dynamic`으로 바꾸면 `INSERT OVERWRITE`가 결과에 행이 있는 파티션만 바꾸고, DataFrame에서는 `writeTo(...).overwritePartitions()`가 같은 일을 한다. Hive serde 테이블은 이 설정과 상관없이 항상 dynamic으로 덮어쓴다. dynamic 모드에는 함정이 셋 있다. 다시 계산한 결과에 행이 하나도 없는 날짜는 바뀌지 않아 옛 집계가 남는다. Iceberg에서 static 모드의 `PARTITION` 절은 테이블 컬럼만 받으므로 `days(ts)` 같은 숨은 파티션은 dynamic 모드로만 골라 바꿀 수 있다. 파티션 스펙이 바뀌면 dynamic 모드가 갈아 끼우는 범위도 같이 바뀐다. Iceberg 테이블에서 MERGE가 더 예측하기 쉬운 이유다.

[서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 배치는 워터마크 대신 시작일과 종료일을 받아 그 범위의 지표를 다시 만들고, MySQL unique 키로 겹치는 행을 지운 뒤 넣었다. 원천 로그가 늦게 들어오면 범위를 넓혀 같은 job을 다시 돌렸다. [Spark Streaming 적재 글](/posts/spark-streaming-to-hive/)의 일 배치는 날짜 폴더를 `_TMP` 경로에 다시 쓰고 바꿔 끼웠다. 둘 다 키를 추적하는 대신 구간을 통째로 다시 만드는 쪽이다. retry와 백필이 같은 날짜를 다시 부르는 과정은 [Airflow 멱등 DAG 글](/posts/airflow-idempotent-dags/)에 있다.

## 테이블마다 고르는 기준

| 원천 테이블 | 방식 |
|---|---|
| 작고 삭제가 잦다 | 전체 적재 |
| 크고 `updated_at`이 믿을 만하다 | 워터마크, lookback, 키별 중복 제거 뒤 MERGE, 가끔 PK 비교 |
| append만 하고 ID가 증가한다 | ID 워터마크와 lookback, `WHEN NOT MATCHED`만 있는 MERGE |
| 결과를 날짜 단위로 다시 계산할 수 있다 | dynamic 파티션 덮어쓰기 |
| 중간 상태와 삭제 순서가 필요하다 | 로그 기반 CDC |

lookback을 늘려도 그보다 오래 열린 트랜잭션은 빠지고, 폴링 사이에 두 번 바뀐 행의 중간 값은 어떤 lookback으로도 돌아오지 않는다.

## 참고 자료

- [Apache Iceberg, Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/), [Spark Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Apache Spark 4.1.4, RewriteMergeIntoTable.scala](https://github.com/apache/spark/blob/v4.1.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RewriteMergeIntoTable.scala), [MergeRowsExec.scala](https://github.com/apache/spark/blob/v4.1.4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/MergeRowsExec.scala), [error-conditions.json](https://github.com/apache/spark/blob/v4.1.4/common/utils/src/main/resources/error/error-conditions.json)
- [Apache Spark 4.1.4, Configuration](https://spark.apache.org/docs/4.1.4/configuration.html)
- [MySQL 8.4, Date and Time Functions](https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html), [Fractional Seconds](https://dev.mysql.com/doc/refman/8.4/en/fractional-seconds.html), [AUTO_INCREMENT Handling in InnoDB](https://dev.mysql.com/doc/refman/8.4/en/innodb-auto-increment-handling.html)
- [PostgreSQL 19, Date/Time Functions and Operators](https://www.postgresql.org/docs/19/functions-datetime.html)

## English summary

Without CDC, an incremental batch reads rows whose `updated_at` is past a high watermark taken from the rows it already loaded. MySQL and PostgreSQL stamp a row before it commits, so the batch rereads a lookback window, pages by `(updated_at, id)`, and dedupes each key before Spark `MERGE INTO`, which fails when one target row matches several source rows. Hard deletes need soft deletes or a periodic key comparison with `WHEN NOT MATCHED BY SOURCE`, which reads the whole target. In my metric batches at a music service, I recomputed whole date ranges instead, trading extra reads for reruns that need no watermark.
