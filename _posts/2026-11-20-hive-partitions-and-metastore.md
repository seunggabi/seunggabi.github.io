---
title: "Hive 파티션은 메타스토어와 어긋나면 에러 없이 빈 결과를 낸다"
date: 2026-11-20 05:12:00 +0900
categories: [Lakehouse & Storage, Partitioning]
tags: [hive, hive-metastore, partitioning, athena, aws-glue, spark, concept]
description: "S3에 파티션 폴더와 파일이 있어도 메타스토어에 등록되지 않으면 Athena가 에러 없이 0건을 내는 이유와 MSCK REPAIR 등 대처를 정리한다."
---

S3에 `dt=2026-11-19/` 폴더가 생겼고 파일도 들어 있는데, Athena에서 그날을 조회하면 0건이 나온다. 에러는 없다. Hive 스타일 테이블의 파티션은 저장소의 디렉터리와 메타스토어의 파티션 항목, 두 곳에 따로 있고, 쿼리 엔진은 메타스토어에 등록된 파티션만 읽는다. 두 곳을 맞추는 일은 쓰는 쪽이 따로 해야 한다.

광고 리포트 배치의 S3 계정 간 복사 잡에서는 복사를 마친 뒤 `MSCK REPAIR TABLE`을 돌리는 단계가 이 일을 맡았다.

버전은 Hive 4.0, Spark 4.3, Athena 엔진 버전 3 기준이다.

## 파티션 하나는 디렉터리 하나와 메타스토어 항목 하나다

`PARTITIONED BY (dt string)` 테이블에 `dt='2026-11-19'` 파티션이 있다는 말은, 테이블 위치 아래 `dt=2026-11-19/` 디렉터리에 파일이 있고 메타스토어(Hive Metastore나 AWS Glue Data Catalog)에도 그 값과 위치가 등록돼 있다는 뜻이다.

쿼리는 메타스토어에서 파티션 목록을 받아 WHERE 조건으로 거르고, 남은 파티션 위치의 파일을 나열해서 읽는다. [컬럼 파일과 프루닝 글](/posts/columnar-files-and-pruning/)에서 다룬 Athena의 파티션 위치 나열과 Glue 파티션 필터 pushdown이 이 단계에서 일어난다. 두 곳이 어긋나면 결과는 이렇게 갈린다.

| 디렉터리와 파일 | 메타스토어 항목 | 쿼리 결과 |
|---|---|---|
| 있음 | 있음 | 정상 |
| 있음 | 없음 | 그 파티션은 0건, 에러 없음 |
| 지워짐 | 있음 | 읽을 파일이 없어 0건. Athena의 MSCK는 이 항목을 지우지 않는다 |
| 쓰는 중 | 있음 | 그 순간 디렉터리에 있는 파일만 읽는다 |
| 다른 경로에 있음 | 있음 | 등록된 위치만 읽는다 |

넷째 줄은 Hive 스타일 테이블에 테이블 단위 커밋이 없어서 생긴다. 읽는 쪽은 쿼리할 때 디렉터리를 나열하고 쓰는 쪽은 파일을 하나씩 놓으니, 등록을 먼저 하면 반쯤 쓴 상태가 보일 수 있다. 파일을 먼저 놓으면 등록할 때까지 둘째 줄 상태가 된다.

Hive나 Spark가 `INSERT`로 직접 쓰면 파일과 메타스토어 항목이 같이 생기고, 남는 위험은 덮어쓰는 범위다. Spark 데이터 소스 테이블은 `spark.sql.sources.partitionOverwriteMode` 기본값이 `STATIC`이라 `INSERT OVERWRITE ... PARTITION (dt)`처럼 값을 하나도 고정하지 않으면 테이블 전체를 지우니, 날짜 파티션만 바꾸는 쓰기는 [멱등 DAG 글](/posts/airflow-idempotent-dags/)과 [지표 테이블 grain 글](/posts/metric-table-grain/)에 적었다.

Athena의 Hive 스타일 테이블처럼 외부(external) 테이블로 만들면 `DROP TABLE`은 메타데이터만 지우고 데이터는 남긴다. 데이터는 다른 잡이 S3나 HDFS에 직접 쓰고 메타스토어는 그 잡이 무엇을 썼는지 모르니, 등록 단계가 따로 필요하다.

## 등록 방법마다 언제, 얼마나 훑는지가 다르다

```sql
-- 외부 테이블
CREATE EXTERNAL TABLE logs (user_id string, event string)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://bucket/logs/';

-- 쓴 파티션을 알 때: 그 파티션만 등록한다
ALTER TABLE logs ADD IF NOT EXISTS PARTITION (dt='2026-11-19');

-- 모를 때: 테이블 위치 아래를 전부 훑는다 (Hive, Spark 문법)
MSCK REPAIR TABLE logs SYNC PARTITIONS;
```

| 방법 | 등록 시점 | 훑는 범위 | 못 잡는 것 |
|---|---|---|---|
| `ALTER TABLE ADD PARTITION` | 쓴 잡이 끝난 직후 | 없음, 값을 직접 준다 | 잡이 빠뜨린 파티션 |
| `MSCK REPAIR TABLE` | 실행할 때 | 테이블 위치 전체 | Hive 스타일이 아닌 경로 |
| Glue 크롤러 | 스케줄마다 | 크롤 대상 경로 | 증분 크롤은 기존 파티션의 변경과 삭제 |
| Glue ETL `enableUpdateCatalog` | 쓰는 동안 | 없음 | 그 잡 밖에서 쓴 파일 |
| Athena 파티션 프로젝션 | 등록하지 않는다 | 없음 | Athena 밖의 엔진 |

`ADD PARTITION`은 `IF NOT EXISTS`를 붙이면 다시 돌려도 에러가 나지 않고, `key=value` 모양이 아닌 경로는 `LOCATION`을 직접 준다.

`MSCK REPAIR TABLE`의 기본 동작은 ADD다. 디렉터리는 있는데 메타스토어에 없는 파티션을 추가한다. DROP은 디렉터리가 사라진 항목을 지우고, SYNC는 둘을 같이 한다. DROP과 SYNC는 HIVE-17824로 Hive 3.0.0에 들어왔고(2.4.0에도 백포트), Spark SQL도 `[MSCK] REPAIR TABLE ... [{ADD|DROP|SYNC} PARTITIONS]`와 `ALTER TABLE RECOVER PARTITIONS`를 받는다. Athena의 MSCK는 추가만 한다. 지운 파티션은 `ALTER TABLE DROP PARTITION`으로 따로 빼야 한다.

비용은 나열에서 나온다. MSCK는 테이블 위치와 그 하위 폴더를 전부 훑으므로 파티션이 쌓일수록 느려진다. Hive는 `hive.msck.repair.batch.size`로 나눠 처리하고, 4.0 기본값은 3000개씩이다. Athena에서는 MSCK가 시간 초과로 끊기면 일부 파티션만 추가된 채 남고, 다 들어갈 때까지 다시 돌려야 한다. 매일 파티션을 더하는 용도라면 `ADD PARTITION`이 낫다.

광고 리포트 배치의 S3 get/put 잡은 날짜마다 객체를 다른 계정으로 복사한 뒤 `MSCK REPAIR TABLE`로 새 날짜 디렉터리를 등록했다. 테이블 이름은 enum에 있는 값만 받았다. 글에 적힌 구조로 추론하면, 이 방식에서는 잡이 대상 테이블의 경로 규칙을 몰라도 되고 복사에 성공한 날짜는 디렉터리가 생겼으니 모두 등록된다. 대가는 실행마다 테이블 위치 전체를 나열한다는 점이다. 잡이 날짜 범위를 인자로 받으니, 파티션이 많아지면 그 범위만 `ADD PARTITION IF NOT EXISTS`로 등록하는 쪽으로 옮길 수 있다.

Glue 크롤러는 스키마 추론과 파티션 등록을 같이 한다. 한 폴더 수준에서 스키마가 대부분 비슷하면 한 테이블의 파티션으로 묶고, 아니면 테이블을 따로 만든다. 증분 크롤(`CRAWL_NEW_FOLDERS_ONLY`)은 새 폴더만 추가하고, 기존 파티션의 수정과 삭제는 잡지 못하며, 스키마 변경과 삭제 처리를 `LOG`로 고정한다. 크롤러 스케줄 사이에는 표의 둘째 줄, 파일은 있는데 항목이 없는 상태가 이어진다. Glue ETL 잡에서 `enableUpdateCatalog`와 `partitionKeys`를 주면 쓰면서 파티션을 등록하므로 크롤러를 다시 돌릴 필요가 없다.

## 파티션 프로젝션은 메타스토어 조회를 계산으로 바꾼다

Athena는 보통 Glue에 `GetPartitions`를 호출한 다음 프루닝한다. 파티션 프로젝션을 켜면 테이블 속성에 적은 범위와 형식으로 파티션 값과 위치를 직접 계산한다. 등록 단계가 없어진다.

```sql
ALTER TABLE logs SET TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2025-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://bucket/logs/dt=${dt}/'
);
```

켜는 순간 Athena는 Glue에 등록된 파티션 항목을 무시한다.

- date 타입 값은 쿼리 시점에 UTC 기준으로 만든다.
- S3에 없는 파티션도 만들어 내고, 에러 없이 0건을 돌려준다. 범위 밖의 값을 물어도 0건이다. 계산한 파티션의 절반 넘게 비어 있으면 일반 파티션이 낫다.
- Athena에서만 동작한다. 같은 테이블을 EMR의 Spark, Redshift Spectrum, Athena for Spark로 읽으면 Glue의 파티션 항목을 쓴다.
- `SHOW PARTITIONS`는 계산한 파티션을 보여 주지 않는다.

셋째 항목 때문에 프로젝션만 믿고 등록을 멈추면, Athena에서는 보이는 데이터가 Spark에서는 안 보인다. 여러 엔진이 같은 테이블을 읽는다면 등록은 계속하고, 프로젝션은 Athena의 조회 속도를 위해 얹는 쪽이 된다.

## 원본 폴더와 테이블 사이에도 같은 간격이 있다

[Spark Streaming 글](/posts/spark-streaming-to-hive/)의 스트리밍 잡은 이벤트 시각으로 HDFS 날짜 폴더에 텍스트를 붙여 썼고, 일 배치가 그 폴더를 읽어 Hive 테이블의 날짜 파티션으로 올렸다. 7일까지 늦게 오는 로그는 일 배치가 돈 뒤에 원본 폴더로 들어올 수 있다. 구조로 보면 이 로그는 파일은 있는데 테이블에는 아직 없는 상태가 되고, 적재 잡이 시작일과 종료일을 받으니 그 범위를 다시 돌리면 들어갔을 것이다. 이벤트 시각으로 폴더를 나눈 덕에 다시 돌릴 범위가 날짜 폴더 몇 개로 정해진다.

이 간격 자체를 없앤 것이 테이블 포맷이다. [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에서 본 것처럼 Iceberg는 커밋마다 매니페스트에 데이터 파일 목록을 남기므로, 읽는 쪽이 디렉터리를 나열하지 않고 파티션을 따로 등록할 단계도 없다.

## 참고 자료

- [Apache Hive: LanguageManual DDL (MSCK, ALTER TABLE ADD/DROP PARTITION, DROP TABLE)](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL)
- [Apache Hive: Managed vs. External Tables](https://cwiki.apache.org/confluence/display/Hive/Managed+vs.+External+Tables)
- [HIVE-17824: msck repair table should drop the missing partitions from metastore](https://issues.apache.org/jira/browse/HIVE-17824)
- [Spark SQL: REPAIR TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-repair-table.html)
- [Spark: Configuration (spark.sql.sources.partitionOverwriteMode)](https://spark.apache.org/docs/latest/configuration.html)
- [Amazon Athena: Partition your data](https://docs.aws.amazon.com/athena/latest/ug/partitions.html)
- [Amazon Athena: MSCK REPAIR TABLE](https://docs.aws.amazon.com/athena/latest/ug/msck-repair-table.html)
- [Amazon Athena: Partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
- [Amazon Athena: Supported types for partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html)
- [AWS Glue: Using crawlers to populate the Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
- [AWS Glue: Scheduling incremental crawls](https://docs.aws.amazon.com/glue/latest/dg/incremental-crawls.html)
- [AWS Glue: Updating the schema and adding partitions from ETL jobs](https://docs.aws.amazon.com/glue/latest/dg/update-from-job.html)

## English summary

A Hive-style partition exists in two places, a directory of files and an entry in the metastore, and queries only read partitions that are registered, so a mismatch returns zero rows instead of an error. You can register partitions with ALTER TABLE ADD PARTITION, scan for them with MSCK REPAIR TABLE (Hive 3 added DROP and SYNC, while Athena only adds), let a Glue crawler or Glue ETL job do it, or skip registration in Athena with partition projection, which other engines ignore. Engines that write with INSERT register partitions themselves, but Spark's default static overwrite mode can delete every partition when no value is fixed. In my ad report batch, a cross-account S3 copy job ran MSCK REPAIR after each copy, so it did not need to know the path layout, at the cost of listing the whole table location on every run.
