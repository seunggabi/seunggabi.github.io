---
title: "Hive에서 Iceberg로 갈 때 데이터 파일은 두고 메타데이터부터 만든다"
date: 2026-11-21 11:30:00 +0900
categories: [Lakehouse & Storage, Iceberg]
tags: [iceberg, hive, migration, spark, athena, partitioning, concept]
description: "Hive 테이블을 데이터 재작성 없이 Iceberg로 옮기는 migrate, snapshot, add_files 프로시저의 차이와 파티션 재설계 시점을 설명한다."
---

매일 `MSCK REPAIR TABLE`로 새 날짜를 등록하는 Hive 테이블이 있다. 이걸 Iceberg로 옮기는 가장 단순한 방법은 CTAS로 새 테이블에 전부 다시 쓰는 것이다. 하루 100GB씩 3년 쌓였다면 약 110TB를 읽고 다시 쓴다. 다시 쓰지 않고 옮기는 길도 있다. Iceberg의 Spark 프로시저 `migrate`, `snapshot`, `add_files`는 데이터 파일은 그대로 두고 메타데이터만 만든다. 그다음 정할 건 둘이다. 파티션 설계를 언제 바꾸는가, 그리고 옛 데이터를 어디까지 다시 쓰는가.

버전은 Iceberg 1.12.0, Spark 4.0, Athena 엔진 버전 3 기준이다.

## 세 프로시저는 파일을 누가 갖는지가 다르다

| | `migrate` | `snapshot` | `add_files` |
|---|---|---|---|
| 원본 Hive 테이블 | `<이름>_BACKUP_`으로 남는다 (`drop_backup` 기본 false) | 그대로 둔다 | 그대로 둔다 |
| 만드는 것 | 같은 이름의 Iceberg 테이블 | 다른 이름의 Iceberg 테이블 | 이미 만든 Iceberg 테이블에 파일을 등록 |
| 새로 쓰는 파일 위치 | 원래 테이블 위치 | snapshot 테이블 위치 | 대상 테이블 위치 |
| `expire_snapshots` | 된다 | 금지된다. 원본이 쓰는 파일을 지울 수 있어서다 | 되고, 등록한 원본 파일도 지울 수 있다 |
| 쓰임 | 실제 전환 | 전환 전 리허설 | 위치나 이름을 바꾸며 옮길 때 |

`migrate`는 Spark 세션 카탈로그(`spark_catalog`)가 `org.apache.iceberg.spark.SparkSessionCatalog`로 설정돼 있어야 하고, 아니면 바로 실패한다. 아래 예제의 `spark_catalog`가 그 카탈로그다. `migrate`와 `snapshot`은 원본의 스키마, 파티션, 속성을 그대로 가져오고 파일은 다시 쓰지 않는다. Hive 파일에는 Iceberg 필드 ID가 없어서, 프로시저가 원본 스키마로 컬럼 이름과 ID를 짝지은 매핑을 테이블에 넣어 두고 읽을 때 그걸 쓴다. Avro, Parquet, ORC만 받고, 버킷 테이블은 실패한다. 기본 `parallelism`이 1이라 파티션이 많으면 올려 준다.

`add_files`는 파일 스키마를 검사하지 않는다. 다른 스키마의 파일이 섞여 들어가면 쿼리에서 실패한다. 같은 파일을 두 번 등록하는 건 `check_duplicate_files`(기본 true)로 막는다. Iceberg 문서도 `migrate`나 `snapshot`을 쓸 수 있으면 `add_files`는 쓰지 말라고 적는다.

## 순서

### 1. 목표 파티션을 정해 두고, 바꾸는 건 나중에 한다

`migrate`는 Hive의 `dt` 파티션을 identity 파티션으로 그대로 가져온다. 전환과 설계 변경을 한 번에 하면 무엇 때문에 숫자가 틀렸는지 가르기 어렵다. 목표 스펙은 이 단계에서 적어만 둔다. 보통 `days(event_ts)` 같은 hidden partitioning과 자주 거르는 컬럼의 정렬 순서다. 변환을 고르는 기준은 [스키마와 파티션 변경 글](/posts/iceberg-schema-and-partition-evolution/)에 정리했다.

### 2. snapshot으로 리허설한다

```sql
CALL spark_catalog.system.snapshot(
  source_table => 'db.logs',
  table        => 'db.logs_ice_test',
  parallelism  => 16);
```

새 테이블에서 행 수와 날짜별 건수가 원본과 같은지, Athena와 Trino가 읽는지 확인한다. 이 테이블은 데이터 파일의 유일한 주인이 아니다. 그래서 `gc.enabled=false`로 만들어져 `expire_snapshots`가 막혀 있고, 원본 Hive 쪽에서 파티션을 지우면 이 테이블이 참조하는 파일도 사라진다. 리허설이 끝나면 `DROP TABLE`로 지운다. 원본 파일은 남는다.

### 3. 쓰기를 잠시 멈추고 migrate 한다

`migrate`는 메타스토어에 등록된 파티션의 파일만 가져온다. MSCK가 한 번 빠져 등록이 안 된 날짜가 있으면 그날은 Iceberg에 없고, 2단계의 건수 비교도 양쪽에 같이 없으니 잡히지 않는다. 쓰기를 멈춘 뒤 `MSCK REPAIR TABLE`을 한 번 돌리고 migrate 한다.

```sql
CALL spark_catalog.system.migrate(
  table       => 'db.logs',
  parallelism => 16);
```

메타데이터만 만드니 시간은 데이터 크기가 아니라 파일 수를 따라간다. 3년치면 파티션이 1,095개이고, 파티션마다 파일이 수십 개면 매니페스트에 적을 파일이 수만 개다. 이름이 같으니 읽는 쿼리는 테이블 이름을 바꿀 필요가 없다. 바꿔야 하는 건 적재 작업이다. 경로에 파일만 떨구는 방식은 이제 테이블에 잡히지 않으니, 쓰기를 다시 열기 전에 Iceberg 테이블에 `INSERT`나 `MERGE INTO`로 쓰게 바꾼다. 그러면 매일 돌던 `MSCK REPAIR TABLE` 단계도 필요 없다. 파티션 정보가 이제 매니페스트에 있어서, 디렉터리는 있는데 메타스토어에 파티션이 없어 0건이 나오던 문제([Hive 파티션 글](/posts/hive-partitions-and-metastore/))도 더는 생기지 않는다.

### 4. 덮어쓰는 배치를 먼저 고치고, 새 스펙으로 바꾼다

스펙을 바꾸기 전에 `dt` 단위로 덮어쓰는 배치부터 바꾼다. Hive의 `dt`는 보통 적재일이고, `event_ts`의 날짜는 발생일이다. dynamic 덮어쓰기(`spark.sql.sources.partitionOverwriteMode=dynamic`)는 결과에 행이 있는 파티션을 갈아 끼우는데, 이 파티션을 스펙마다 따로 맞춘다. 스펙을 `days(event_ts)`로 바꾼 뒤 `dt=2026-11-21` 배치를 다시 돌리면 두 가지가 같이 생긴다. 옛 스펙의 `dt=2026-11-21` 파일은 새 스펙 파티션과 맞지 않아 지워지지 않으니, 그날 행이 두 번 들어간다. 새 스펙으로 이미 쓴 발생일 파티션은 통째로 바뀌니, 다른 날 적재된 같은 발생일 행이 사라진다. Iceberg 문서도 파티션이 바뀌는 테이블에는 `INSERT OVERWRITE` 대신 `MERGE INTO`를 권한다. 이런 배치는 `MERGE INTO`로 바꿔 둔다. 다시 돌린 결과에 없는 옛 행까지 지우려면 `WHEN NOT MATCHED BY SOURCE AND t.dt = '2026-11-21' THEN DELETE`를 같이 둔다. `dt` 조건의 `DELETE` 뒤 `INSERT`도 되지만, 커밋이 둘이라 그사이에 읽으면 그날이 비어 보인다.

그다음 스펙과 정렬 순서를 바꾼다.

```sql
ALTER TABLE db.logs REPLACE PARTITION FIELD dt WITH days(event_ts);
ALTER TABLE db.logs WRITE ORDERED BY event_type, event_ts;
```

스펙 변경은 메타데이터 커밋 하나다. 옛 파일은 옛 스펙에 남고, 쿼리 계획은 스펙마다 따로 조건을 만들어 거른다. 새 스펙 파일은 `event_ts` 조건으로 파티션이 걸러진다. `dt` 조건만 거는 쿼리는 새 구간에서 파일 단위 min/max 통계에만 기대고, 그것도 `dt`와 `event_ts`의 날짜가 거의 같을 때만 잘 걸러진다. 대시보드와 배치 쿼리의 조건을 `event_ts`로 옮겨야 새 파티션으로 파일이 걸러진다. 다만 조건을 옮기면 적재일 기준이 발생일 기준으로 바뀌어 숫자도 달라진다. 지표 정의를 먼저 맞추고 옮긴다.

### 5. 옛 데이터는 범위를 잘라 일부만 다시 쓴다

`rewrite_data_files`는 결과를 현재 스펙(`output-spec-id`)으로 쓴다. 옛 스펙 파일을 제자리에서 새 레이아웃으로 옮기는 가장 직접적인 수단이다. 다만 기본값은 크기가 적당한 파일을 건너뛰어서, 옛 파일이 다 옮겨지지 않는다.

| 옵션 | 기본값 | 옮길 구간에 줄 값 |
|---|---|---|
| `strategy` | `binpack` | 정렬까지 맞추려면 `sort` |
| `rewrite-all` | false | true. 크기와 상관없이 구간 안 파일을 모두 다시 쓴다 |
| `partial-progress.enabled` | false | true. 파일 그룹을 묶어 최대 `partial-progress.max-commits`(기본 10)번으로 나눠 커밋하니, 중간에 실패해도 앞 작업이 남는다 |
| `max-concurrent-file-group-rewrites` | 5 | executor 수에 맞춰 올린다 |

```sql
CALL spark_catalog.system.rewrite_data_files(
  table    => 'db.logs',
  strategy => 'sort',
  where    => "dt >= '2026-08-01' AND dt < '2026-09-01'",
  options  => map('rewrite-all', 'true',
                  'partial-progress.enabled', 'true'));
```

범위는 읽히는 양으로 정한다. Athena 쿼리 기록에서 최근 몇 달이 스캔의 대부분이면 그 구간만 옮긴다. 옛 스펙 파일도 `dt`와 `event_ts`의 날짜가 거의 같으면 min/max 통계로 걸러지니, 거의 안 읽는 2년 전 데이터는 그대로 둔다. 한 달이 3TB라면 다시 쓰기 한 번에 3TB를 읽고 3TB를 쓴다.

한 달씩 잘라 돌리면 실패해도 그 달만 다시 하면 된다. 다시 쓰는 파일을 다른 작업이 지우거나 그 파일에 행 삭제를 붙이면 커밋이 충돌한다. 새 파일을 더하기만 하는 append는 충돌하지 않는다. 최근 달은 재처리 배치의 `MERGE INTO`가 같은 파일을 건드리기 쉬우니, 고른 구간 안에서도 재처리가 끝난 오래된 달부터 한다.

Athena에도 `OPTIMIZE ... REWRITE DATA USING BIN_PACK`이 있다. 다만 `WHERE`에 파티션 컬럼만 쓸 수 있고 정렬은 없다. 스펙을 옮기고 정렬까지 맞추는 일은 Spark 프로시저로 하고, Athena `OPTIMIZE`는 그 뒤 작은 파일 정리에 쓴다. 작은 파일이 쌓이는 이유는 [작은 파일과 컴팩션 글](/posts/small-files-and-compaction/)에 있다.

### 6. 정리는 백업 테이블을 버릴 때 같이 한다

rewrite가 끝나도 옛 파일은 이전 스냅샷이 참조하고 있어서 지워지지 않는다. `expire_snapshots`(기본 5일보다 오래된 스냅샷, 최소 1개 유지)를 돌려야 실제로 지워진다. 그런데 `<이름>_BACKUP_` Hive 테이블도 같은 옛 파일을 가리킨다. expire가 그 파일을 지우면 백업 테이블은 다시 쓴 구간의 파티션 정보만 남고 안의 파일이 사라진다. 그래서 백업으로 되돌릴 수 있게 둘 기간(예: 2주) 동안은 expire를 미루고, 백업을 버리기로 한 뒤에 돌린다. 백업이 managed 테이블이면 `DROP TABLE`이 데이터 파일까지 지우니, 백업을 지우기 전에 external인지 먼저 본다.

`remove_orphan_files`는 테이블 위치 아래에서 기본 3일보다 오래되고 어느 메타데이터도 참조하지 않는 파일을 지운다. migrate한 테이블의 위치는 옛 Hive 디렉터리라서, 메타스토어에 등록되지 않았던 날짜의 파일이나 적재 작업이 남긴 임시 파일도 여기에 걸린다. 보존 기간을 가장 긴 쓰기 작업보다 짧게 주면 아직 커밋 전인 파일을 지운다. 메타데이터에 적힌 경로와 실제 목록의 경로가 다르면(HDFS 주소가 바뀐 경우 등) 기본값은 에러로 멈춘다. 이걸 `prefix_mismatch_mode => 'DELETE'`로 넘기면 살아 있는 파일을 지울 수 있다. 처음에는 `dry_run => true`로 목록부터 본다. Athena의 `VACUUM`은 이 둘을 한 번에 하고, 기본값은 5일과 스냅샷 1개다.

## 옮긴 뒤 보는 숫자

| 확인 | 방법 |
|---|---|
| 데이터가 같은가 | 날짜별 행 수와 주요 컬럼 합계를 백업 테이블과 비교 |
| 새 파티션이 쓰이는가 | 같은 쿼리의 Athena 스캔 바이트를 전환 전후로 비교 |
| 파일이 정리됐는가 | `db.logs.files` 메타데이터 테이블에서 구간별 파일 수와 평균 크기 |
| 스냅샷이 쌓이지 않는가 | `db.logs.snapshots` 수와 가장 오래된 스냅샷 시각 |

## 묻고 답하기

Q. 그냥 CTAS로 다시 쓰면 안 되나?

쓰기를 멈출 수 있는 시간 안에 다시 쓸 수 있는 크기면 그게 낫다. 한 번에 새 스펙과 정렬로 쓰고, 파일 주인도 처음부터 Iceberg 하나다. 110TB라면 CTAS의 큰 비용은 계산보다, 다시 쓰는 동안 들어오는 데이터를 어디에 쌓았다가 어떻게 합칠지다.

Q. `dt` identity 파티션을 그대로 두면 안 되나?

조회가 거의 `dt` 조건이고 덮어쓰기도 `dt` 단위라면 그대로 두는 게 맞다. hidden partitioning의 이점은 쿼리가 `event_ts`로 걸러질 때 생긴다. 쿼리가 바뀌지 않으면 스펙만 바꿔도 얻는 게 없다.

## 참고 자료

- [Apache Iceberg 1.12 Spark Procedures (migrate, snapshot, add_files, rewrite_data_files, expire_snapshots, remove_orphan_files)](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Apache Iceberg 1.12 Spark DDL (partition field, write order)](https://iceberg.apache.org/docs/latest/spark-ddl/)
- [Apache Iceberg 1.12 Spark Writes (dynamic overwrite)](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Apache Iceberg 1.12 Evolution (partition evolution, split planning)](https://iceberg.apache.org/docs/latest/evolution/)
- [Apache Iceberg 1.12 Maintenance (orphan files)](https://iceberg.apache.org/docs/latest/maintenance/)
- [Apache Iceberg 1.12 Configuration (history.expire, gc.enabled)](https://iceberg.apache.org/docs/latest/configuration/)
- [Amazon Athena OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html)
- [Amazon Athena VACUUM](https://docs.aws.amazon.com/athena/latest/ug/vacuum-statement.html)

## English summary

Say a Hive table adds partitions with MSCK REPAIR every day. To move it to Iceberg, I would not rewrite the data first. I would test with the snapshot procedure, then run migrate during a short write freeze, so only metadata is created and the original table stays as a backup. Load jobs must write through Iceberg instead of dropping files into folders. New data goes to a new spec such as days(event_ts). Before the spec change, batch jobs that overwrite by load date must move to MERGE INTO. Old data is rewritten month by month, and only for the ranges people actually read. Snapshot expiration waits until the backup table is no longer needed, because the backup still points to the old files.
