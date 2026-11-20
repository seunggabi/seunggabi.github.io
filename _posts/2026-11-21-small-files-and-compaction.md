---
title: "작은 파일은 쓰는 쪽에서 막고 남은 것만 합친다"
date: 2026-11-21 05:22:00 +0900
categories: [Lakehouse & Storage, Iceberg]
tags: [small-files, compaction, iceberg, spark, hive, s3-tables, concept]
---

일 단위 파티션 테이블에 Spark 배치가 태스크 200개로 한 달치를 쓰면, 태스크마다 30개 날짜에 파일을 하나씩 열어 최대 6,000개 파일이 생긴다. 데이터 양은 같은데, 그 뒤의 모든 쿼리가 파일 목록을 받고 파일을 열고 메타데이터를 읽는 일을 6,000번 한다. 작은 파일의 비용은 바이트보다 파일 수를 따라간다.

[Spark Streaming으로 HDFS에 쌓은 로그](/posts/spark-streaming-to-hive/)도 20초 마이크로배치의 대가로 작은 파일이 생겼고, 파티션을 돌며 합치는 잡을 따로 뒀다. 같은 일을 엔진별로 펼치면 두 갈래가 된다. 쓰는 쪽에서 파일 수를 정하는 방법과, 이미 생긴 파일을 합치는 방법이다.

버전은 Spark 4.3.0, Iceberg 1.12.0, Hive 4.2.1, 2026년 11월 AWS 문서 기준이다. Iceberg 예제는 1.12.0 런타임이 배포된 Spark 4.1에서 돈다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 작은 파일 | 목표 크기(Iceberg 기본 512MB, HDFS 블록 128MB)보다 훨씬 작은 데이터 파일 |
| 파일 열기 비용 | Spark가 파일 하나를 열 때 4MB를 더 읽는 것으로 치는 값(`openCostInBytes`) |
| 태스크 × 파티션 | 쓰기 태스크 하나가 건드린 파티션마다 파일이 최소 하나 생긴다 |
| `write.distribution-mode` | Iceberg가 쓰기 전에 행을 섞는 방식. `none`, `hash`, `range` |
| `write.target-file-size-bytes` | 파일이 이 크기에 닿으면 새 파일로 넘기는 상한. 작은 파일을 합치지는 않는다 |
| advisory partition size | AQE가 셔플 파티션을 합치거나 나눌 때 맞추는 목표 크기 |
| `REBALANCE` 힌트 | 출력 파티션을 너무 작지도 크지도 않게 맞추는 Spark 힌트. AQE가 필요하다 |
| binpack, sort, zorder | `rewrite_data_files` 전략. 크기만 맞추기, 정렬하며 쓰기, 여러 컬럼을 섞은 정렬 |
| `rewrite_manifests` | 매니페스트를 파티션 기준으로 다시 묶어 계획 시간을 줄인다 |
| `expire_snapshots`, `remove_orphan_files` | 옛 스냅샷과 그것만 쓰던 파일 삭제, 어떤 메타데이터에도 없는 파일 삭제 |
| `CONCATENATE` | Hive ORC·RCFile 파티션을 디코딩 없이 stripe·block 단위로 이어 붙이는 DDL |

## 파일 수에 비례하는 비용은 네 층에 붙는다

| 층 | 파일이 하나 늘 때 붙는 일 |
|---|---|
| HDFS NameNode | 네임스페이스와 블록 맵 전체를 메모리에 두므로 파일과 블록마다 힙을 쓴다 |
| S3 | LIST 한 번에 키 1,000개, 파일마다 GET. 분할된 prefix당 초당 GET·HEAD 11,000건, 쓰기 3,500건 이상을 받고, 그 이상으로 늘리는 동안 503 Slow Down |
| 읽기 태스크 | Spark는 파일마다 4MB를 더해 계산하고 태스크 하나에 `maxPartitionBytes`(128MB)까지 담는다 |
| 메타데이터와 통계 | Iceberg 매니페스트는 데이터 파일마다 항목 하나와 컬럼별 min/max, null 수를 둔다. Parquet·ORC는 파일마다 footer를 읽는다 |

Athena의 파티션 나열과 footer 비용은 [컬럼 파일 글](/posts/columnar-files-and-pruning/)에 있으니, 여기서는 태스크 수만 계산한다. 10GB가 128MB 파일 80개면 읽기 태스크도 80개 남짓이다. 같은 10GB가 1MB 파일 10,000개면 파일마다 5MB로 계산되어 태스크 하나에 25개 정도만 들어가고, 태스크는 400개 가까이 된다. 코어 수가 수백 개 이하라서 분할 상한이 128MB로 정해질 때의 계산이다. Iceberg에서는 계획 단계도 무거워진다. 엔진은 매니페스트 항목의 통계를 필터와 하나씩 비교해 읽을 파일을 고르는데, 그 항목 수가 파일 수와 같다.

## 작은 파일이 생기는 네 자리

파일은 쓰기 태스크 하나와 파티션 하나의 경계를 넘지 못한다. 그래서 한 번의 쓰기가 만드는 파일 수는 대략 "쓰기 태스크 수 × 태스크마다 건드린 파티션 수"이고, 파일 크기는 태스크 크기를 넘지 못한다.

| 원인 | 파일이 늘어나는 이유 |
|---|---|
| 스트리밍 마이크로배치 | 트리거나 체크포인트마다 열린 파일을 닫고 커밋한다. 주기가 짧을수록 파일이 작고 많다 |
| 높은 파티션 카디널리티 | 시간이나 ID처럼 값이 많은 컬럼으로 나누면 파티션마다 데이터가 적어 파일도 작다 |
| 많은 셔플 파티션 | `spark.sql.shuffle.partitions` 기본 200. 결과가 10MB여도 리듀스 태스크마다 파일이 생긴다 |
| 동적 파티션 insert | 파티션 값으로 섞지 않고 쓰면 태스크마다 거의 모든 값을 만나 태스크 × 파티션만큼 파일이 생긴다 |

Iceberg Spark 스트리밍은 커밋마다 매니페스트를 합치지 않는 fast append를 쓴다. 데이터 파일과 함께 작은 매니페스트도 쌓이고, Iceberg는 트리거 간격을 최소 1분으로 두라고 권한다. Flink에서는 체크포인트 주기가 같은 역할을 하며, 그 계산은 [Flink Iceberg sink 글](/posts/flink-iceberg-sink/)에 있다. Hive는 동적 파티션 수에 상한을 둔다. 한 쓰기에서 `hive.exec.max.dynamic.partitions`(기본 1000), 노드마다 `...pernode`(기본 100)를 넘으면 실패한다.

## 쓰기 전에 막으려면 같은 파티션의 행을 한 태스크로 모은다

| 수단 | 하는 일 | 한계 |
|---|---|---|
| `repartition(col)`, `DISTRIBUTE BY` | 파티션 컬럼으로 셔플해 값 하나가 태스크 하나로 간다 | 쏠린 값이 있으면 그 태스크가 커진다 |
| `coalesce(n)` | 셔플 없이 태스크 수를 줄인다 | 앞 단계 계산도 n개로 줄 수 있다. `coalesce(1)`이면 노드 하나에서 돈다 |
| `spark.sql.files.maxRecordsPerFile` | 파일 하나의 행 수 상한. 기본 0(무제한) | 큰 파일을 쪼갤 뿐 작은 파일을 합치지 않는다 |
| AQE 파티션 합치기 | 셔플 뒤 붙어 있는 작은 파티션을 한 태스크로 묶는다 | 기본값에서는 목표 크기 대신 병렬도를 우선한다 |
| `REBALANCE` 힌트 | 출력 파티션 크기를 맞추고 쏠린 파티션은 나눈다 | AQE가 꺼져 있으면 무시된다 |

AQE 합치기의 `parallelismFirst` 기본값은 [셔플과 skew 글](/posts/spark-shuffle-and-skew/)에 있다. Spark Streaming 글의 일 배치는 날짜 폴더 하나를 `coalesce(1)`로 파일 하나에 써서 바꿔 끼웠다. 날짜 하나가 파일 하나에 들어갈 크기라면 가장 단순한 선택이다.

Iceberg는 이 분배를 `write.distribution-mode`로 쓰기 계획에 넣는다. Spark에서 따로 정하지 않으면 정렬 순서가 있는 테이블은 `range`, 파티션 테이블은 `hash`, 그 밖은 `none`이다. `hash`나 `range`면 Spark가 쓰기 전에 셔플하고 AQE가 그 태스크를 advisory partition size에 맞춘다. Iceberg 1.12 Spark 모듈은 이 크기를 목표 파일 크기에 셔플 압축 비율을 곱해 잡고, Spark의 `advisoryPartitionSizeInBytes`보다 작게 두지 않는다. 행 기반 셔플 데이터가 같은 행의 Parquet 파일보다 크기 때문이다. 반대로 `target-file-size-bytes`는 태스크가 작으면 쓰일 일이 없다.

```sql
-- Spark 4.1 spark-sql, --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0
-- IcebergSparkSessionExtensions, spark.sql.catalog.local=SparkCatalog(type=hadoop, warehouse=/tmp/warehouse)
CREATE TABLE local.db.events (id BIGINT, ts TIMESTAMP, category STRING)
USING iceberg PARTITIONED BY (days(ts))
TBLPROPERTIES ('write.spark.fanout.enabled'='true');
CREATE TEMP VIEW src AS SELECT id, timestamp_seconds(1767225600 + (id % 30) * 86400) AS ts,
  CAST(id % 5 AS STRING) AS category FROM range(1000000);

SET spark.sql.iceberg.distribution-mode=none;   -- 태스크 200개가 30일 전부에 쓴다
INSERT INTO local.db.events SELECT /*+ REPARTITION(200) */ * FROM src;
SET spark.sql.iceberg.distribution-mode=hash;   -- 날짜별로 섞은 뒤 쓴다
INSERT INTO local.db.events SELECT /*+ REPARTITION(200) */ * FROM src;

SELECT snapshot_id, summary['added-data-files'] FROM local.db.events.snapshots;
```

계산상 첫 커밋은 6,000개 가까운 파일을, 두 번째 커밋은 날짜 수인 30개 안팎을 더한다. 행과 바이트는 같고 셔플이 한 번 더 들었다.

## 이미 생긴 파일은 엔진마다 합치는 도구가 다르다

```sql
-- Hive
ALTER TABLE logs PARTITION (dt='2026-11-20') CONCATENATE;

INSERT OVERWRITE TABLE logs PARTITION (dt='2026-11-20')
SELECT col1, col2 FROM logs WHERE dt='2026-11-20';
```

`CONCATENATE`는 ORC를 stripe 단위로, RCFile을 block 단위로 이어 붙여 압축 해제와 디코딩을 하지 않으니 빠르다. stripe를 그대로 옮기는 방식이라 작은 stripe는 작은 채로 남을 것으로 본다(문서에 명시된 내용은 아니다). Parquet이나 텍스트 테이블은 `INSERT OVERWRITE`로 엔진을 거쳐 다시 쓰고, 그 김에 정렬이나 포맷 변경도 할 수 있다. 쓰는 쪽에서는 작업 끝 평균 출력 파일이 `hive.merge.smallfiles.avgsize`(기본 16MB)보다 작으면 합치기 작업을 하나 더 붙이는 설정이 있다. ACID가 아닌 Hive 테이블에는 스냅샷이 없어서, 이 정리는 같은 파티션을 읽는 쿼리와 격리되지 않는다.

Iceberg는 `rewrite_data_files`가 맡는다.

| 전략 | 하는 일 | 맞는 경우 |
|---|---|---|
| `binpack` (기본) | 크기만 보고 묶어 목표 크기로 다시 쓴다 | 파일 수만 줄이면 될 때. 가장 싸다 |
| `sort` | 정렬 순서대로 셔플·정렬해 쓴다 | 한두 컬럼 필터가 잦을 때. 파일별 min/max 범위가 좁아진다 |
| `sort` + `zorder(c1,c2)` | 여러 컬럼 값을 섞은 값으로 정렬한다 | 여러 컬럼 필터가 섞일 때. 1.12에는 `hilbert()`도 있다 |

후보는 목표 크기의 75%(`min-file-size-bytes`)보다 작거나 180%(`max-file-size-bytes`)보다 큰 파일이다. 파티션 안에서 파일 그룹으로 묶고, 파일이 `min-input-files`(기본 5)개 이상이거나 합이 목표 크기를 넘는 그룹을 다시 쓴다. 파일이 하나뿐인 그룹은 너무 크거나 delete가 많지 않으면 건너뛰니, 이미 합친 파티션을 정렬만 다시 하려면 `rewrite-all`을 켠다. `where`로 최근 파티션만 고를 수 있고, `partial-progress.enabled`를 켜면 그룹 단위로 나눠 커밋한다.

```sql
CALL local.system.rewrite_data_files(table => 'db.events');   -- 날짜마다 파일 하나, 30개
CALL local.system.rewrite_data_files(table => 'db.events', strategy => 'sort',
  sort_order => 'zorder(category, id)', options => map('rewrite-all', 'true'));
```

delete 파일 때문에 다시 쓸 파일을 고르는 기준은 [행 단위 삭제 글](/posts/iceberg-row-level-deletes/)에, 스트리밍 커밋과 compaction이 부딪힐 때 누가 지는지는 [카탈로그와 커밋 글](/posts/iceberg-catalogs-and-commits/)에 있다. 데이터 파일 밖에도 정리할 것이 셋 있다.

| 프로시저 | 줄이는 것 | 기본값과 주의 |
|---|---|---|
| `rewrite_manifests` | 작은 매니페스트, 쿼리 필터와 어긋나게 묶인 매니페스트 | 파티션 필드 순으로 다시 묶고 `sort_by`로 자주 거르는 변환을 고른다. 일반 append는 매니페스트가 100개 쌓이면 8MB 목표로 알아서 합친다 |
| `expire_snapshots` | 옛 스냅샷과 그것만 쓰던 파일 | `older_than` 기본 5일 전, `retain_last` 기본 1. compaction이 대체한 파일은 이때 지워진다 |
| `remove_orphan_files` | 어떤 메타데이터도 가리키지 않는 파일 | `older_than` 기본 3일 전. 진행 중인 쓰기보다 짧게 잡으면 그 쓰기의 파일을 지워 테이블이 깨질 수 있다 |

S3 Tables는 이 정리를 관리형 작업으로 돌린다. compaction은 모든 테이블에 기본으로 켜져 있고, 목표 크기는 기본 512MB이며 64MB에서 512MB 사이로 바꾼다. 전략 기본값 `auto`는 정렬 순서가 있는 테이블에 `sort`를, 없는 테이블에 `binpack`을 쓰고, `z-order`는 `sort_order` 테이블 속성이 있어야 한다. 스냅샷 관리는 최소 1개, 최대 120시간을 남긴다. 버킷 단위 미참조 파일 삭제는 3일 지난 파일을 noncurrent로 표시하고 10일 뒤 영구 삭제한다. 사용자 브랜치나 태그가 있으면 스냅샷 관리가 실패한다는 제약은 [스냅샷과 브랜치 글](/posts/iceberg-snapshots-and-branches/)에 있다. 설정은 `PutTableMaintenanceConfiguration`의 `icebergCompaction` 항목(`targetFileSizeMB`, `strategy`)으로 바꾼다. compaction은 추가 요금이 붙고 `sort`, `z-order`가 `binpack`보다 비싸다. 그 돈을 쓸 만한 테이블을 고르는 기준은 [비용 글](/posts/data-platform-cost-levers/)에 있다.

## 배치와 스트리밍은 분담이 다르다

배치는 쓰는 쪽에서 거의 다 막을 수 있다. 분배 모드나 `repartition`이 셔플 한 번을 더 내는 대신 파일 수를 파티션 수 근처로 맞춘다. 스트리밍은 지연을 줄이려 주기를 짧게 잡는 순간 파일 수가 따라 늘고, 쓰는 쪽에서 할 수 있는 일은 커밋 하나의 파일을 파티션 수 근처로 묶는 데서 멈춘다. 나머지는 compaction 몫이다. compaction은 파일 수를 줄이는 대가로 같은 바이트를 한 번 더 읽고 쓰고, 스트리밍 커밋과 같은 테이블을 두고 경합한다.

## 참고 자료

- Spark 4.3.0: [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), [Hints](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html), [Configuration](https://spark.apache.org/docs/latest/configuration.html), [FilePartition.scala](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala)
- Iceberg 1.12.0: [Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/), [Maintenance](https://iceberg.apache.org/docs/latest/maintenance/), [Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/), [Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/), [Configuration](https://iceberg.apache.org/docs/latest/configuration/), [SparkWriteConf.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkWriteConf.java), [SizeBasedFileRewritePlanner.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/actions/SizeBasedFileRewritePlanner.java)
- Hive: [DDL (CONCATENATE)](https://hive.apache.org/docs/latest/language/languagemanual-ddl/), [Configuration Properties](https://hive.apache.org/docs/latest/user/configuration-properties/), Hadoop [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- AWS: [S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html), [S3 Tables table maintenance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance.html), [S3 Tables table bucket maintenance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-table-buckets-maintenance.html)

## English summary

Small files cost per file, not per byte: NameNode memory, S3 LIST and GET requests, Spark's 4 MB open cost per file, and one manifest entry per file in Iceberg. They come from short streaming commits, fine-grained partitions, too many shuffle partitions and dynamic partition writes, because each write task writes at least one file per partition it touches. At write time the fix is to send one partition's rows to one task with repartitioning, AQE, the REBALANCE hint or Iceberg's hash and range distribution, while afterwards Hive uses CONCATENATE or INSERT OVERWRITE and Iceberg uses rewrite_data_files, rewrite_manifests, expire_snapshots and remove_orphan_files, which S3 Tables runs as managed jobs. In my Spark Streaming log pipeline I wrote each daily folder with coalesce(1) and merged leftover small files in a separate job. Batch jobs can avoid most small files for the price of one extra shuffle, but streaming jobs trade latency for file count and leave the rest to compaction, which rewrites the same bytes again.
