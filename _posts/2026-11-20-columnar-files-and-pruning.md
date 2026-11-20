---
title: "컬럼에 함수를 씌우면 왜 Parquet·ORC가 건너뛰지 못할까"
date: 2026-11-20 04:42:00 +0900
categories: [Lakehouse & Storage, Parquet]
tags: [concept, parquet, orc, predicate-pushdown, partitioning, small-files, athena, spark]
mermaid: true
---

같은 테이블, 같은 기간을 조회하는데 WHERE 절 모양만 다르면 스캔량이 크게 달라질 수 있다. [Superset epoch_us 글](/posts/superset-epoch-us/)에서 계산 컬럼을 쓰면 Athena로 가는 필터가 `from_unixtime(created_at / 1000000) >= TIMESTAMP '...'`로 나가고, 원하는 모양은 `created_at >= 1789344000000000`이었다. 둘의 차이는 엔진이 데이터를 읽기 전에 건너뛸 수 있느냐에 있다. 버전은 Spark 4.3, Athena 엔진 버전 3 기준이다.

## 건너뛰기는 네 단계에서 일어난다

컬럼 포맷 파일을 읽는 엔진은 안쪽으로 들어갈수록 더 작은 단위로 데이터를 걸러 낸다.

```mermaid
flowchart LR
  P["파티션 디렉터리<br/>dt=2026-11-19"] --> F["footer / row group 목록<br/>(ORC는 파일·stripe 통계)"]
  F --> R["row group / stripe<br/>min, max, null 수"]
  R --> G["page / ORC row group<br/>더 잘게 나눈 통계"]
```

| 단계 | Parquet | ORC |
|---|---|---|
| 파일 끝 메타데이터 | `FileMetaData`가 row group 목록을 가진다 | Footer에 파일 전체의 컬럼 통계가 있다 |
| 큰 묶음 | row group, 그 안에 컬럼마다 column chunk | stripe, 각 stripe에 index, data, footer |
| 묶음 단위 통계 | `ColumnMetaData.statistics`의 `min_value`, `max_value`, `null_count` | Metadata 영역에 stripe 단위 통계 |
| 더 잘게 | page index(`ColumnIndex`)에 page별 min/max | row index 항목마다 통계, 기본 10,000행 단위 |

Parquet은 메타데이터를 데이터 뒤에 쓴다. 한 번에 쭉 쓰면서 파일을 만들 수 있게 하려는 설계다. 읽는 쪽은 파일 끝의 길이 값으로 footer를 찾고, 거기서 필요한 컬럼의 column chunk 위치만 골라 읽는다.

ORC도 비슷하다. stripe는 자기 바이트와 파일 Footer, Postscript만으로 읽을 수 있다. row index 스트림은 stripe 앞쪽에 있어서, predicate pushdown이나 seek를 할 때만 읽는다. bloom filter를 켜면 min/max 검사를 통과한 row group에 한해 한 번 더 걸러 낸다.

Athena에서 기본 크기는 Parquet row group 128MB, ORC stripe 64MB이고, 이보다 줄이지 않는 편이 낫다.

## predicate pushdown과 partition pruning은 다른 층이다

partition pruning은 파일을 열기 전, 디렉터리 단위에서 일어난다. 파티션 키는 가상 컬럼이고, 필터가 파티션 키에 걸리면 맞는 파티션의 데이터만 읽는다.

predicate pushdown은 파일 안으로 내려가는 필터다. 리더가 row group이나 stripe의 min/max를 보고 조건에 맞을 수 없는 묶음을 통째로 건너뛴다. Spark는 `spark.sql.parquet.filterPushdown`과 `spark.sql.orc.filterPushdown`을 둘 다 기본값 `true`로 둔다.

min/max 통계는 값이 모여 있을 때 힘을 쓴다. `created_at`으로 정렬해 두면 짧은 시간 범위를 찾는 쿼리가 파일 메타데이터의 min/max로 필요 없는 부분을 건너뛴다. 정렬하지 않은 데이터라면 모든 row group의 min과 max가 거의 전체 범위를 덮어서, 통계가 있어도 건너뛸 게 없다.

Spark Streaming 잡이 쌓은 로그는 HDFS 텍스트 파일이었고, 텍스트 테이블을 ORC로 바꾸는 잡이 따로 있었다. 텍스트 파일에는 이런 통계가 없다. ORC로 바꾼 뒤에야 위의 stripe, row group 단위 건너뛰기를 쓸 수 있다.

## 컬럼에 함수를 씌우면 통계와 비교할 수 없다

통계는 원본 컬럼 값의 min/max다. 리더가 받는 필터도 "어떤 컬럼이 어떤 값보다 크다" 같은 모양이어야 한다. Spark를 예로 들면, 데이터 소스 필터 API가 이 모양을 그대로 보여 준다.

```scala
// org.apache.spark.sql.sources (Spark 소스)
case class GreaterThan(attribute: String, value: Any) extends Filter
```

컬럼 이름 하나와 리터럴 값 하나다. Spark의 `from_unixtime`은 문자열을 돌려주므로, 같은 조건을 Spark로 쓰면 `timestamp_seconds(created_at / 1000000) >= TIMESTAMP '...'`가 된다. 이 식은 위 모양으로 옮길 수 없다. 그래서 리더까지 내려가지 못하고, 그 컬럼을 전부 읽은 뒤 Spark가 행마다 계산해서 거른다.

Athena가 쓰는 Trino도 같은 구조다. 커넥터가 받는 필터의 기본 형태는 컬럼별 값 목록이나 범위(`TupleDomain`)이고, 함수 호출은 별도 식으로 넘어간다. 커넥터가 처리하지 못한 식은 Trino가 데이터를 읽은 뒤 직접 평가한다.

Spark에도 예외가 하나 있다. `UnwrapCastInBinaryComparison` 규칙은 `CAST(컬럼 AS 타입) 비교 리터럴` 모양에서 캐스트를 리터럴 쪽으로 옮겨, 뒤에서 데이터 소스로 pushdown될 수 있게 만든다. 대상은 캐스트 하나짜리 비교이고, 나눗셈이나 `timestamp_seconds` 같은 함수는 아니다.

확인은 실행 계획으로 한다.

```python
df = spark.read.parquet("s3://bucket/events/")
df.filter("created_at >= 1789344000000000").explain()
# 스캔 노드의 PushedFilters에 GreaterThanOrEqual(created_at,1789344000000000)이 보인다
```

같은 조건을 `timestamp_seconds(created_at / 1000000)`으로 쓰면 `PushedFilters`에 `created_at` 범위 조건이 나오지 않는다. Superset 글에서 `epoch_us`를 넣은 뒤 나간 SQL은 SELECT 쪽에만 함수가 있고 WHERE 절은 원본 컬럼과 정수의 비교였다. 함수는 결과를 보여 주는 쪽에 두고, 필터는 상수 쪽을 변환하는 방식이다.

파티션 컬럼은 사정이 조금 다르다. Spark의 `PruneFileSourcePartitions`는 결정적(deterministic)이고 파티션 컬럼만 참조하는 필터를 파티션 필터로 분류한다. 그래서 `substr(dt, 1, 7) = '2026-11'` 같은 식도 파티션 값마다 계산해서 걸러 낼 수는 있다. 다만 카탈로그가 어떤 필터를 받아 주는지는 엔진마다 다르다. Athena에서 파티션 필터를 AWS Glue로 밀어 넣으려면 파티션 키 타입이 `STRING`이어야 한다.

## 실제로 몇 파티션을 읽는지 보는 방법

Athena에서는 쿼리 앞에 `EXPLAIN`을 붙이면 SOURCE fragment 아래에 파티션 키별로 스캔할 값 목록이 나온다.

```text
dt := dt:string:PARTITION_KEY
    :: [[2026-11-17], [2026-11-18], [2026-11-19]]
```

## 작은 파일이 많으면 통계도 소용이 줄어든다

파일이 작고 많으면 건너뛰기 전에 드는 비용이 커진다.

- Athena는 쿼리를 계획할 때 매칭된 파티션 위치를 전부 나열하고, 파일마다 요청 비용이 든다. S3는 한 번의 list 요청에 1,000개까지만 돌려준다.
- 파일이 작으면 컬럼 포맷의 오버헤드가 이점보다 커진다. 파일마다 footer를 따로 읽어야 하는 것도 그 비용에 들어간다.
- Spark는 파일 하나를 여는 비용을 `spark.sql.files.openCostInBytes`(기본 4MB)만큼 읽는 것으로 치고, 읽기 파티션 하나에 `spark.sql.files.maxPartitionBytes`(기본 128MB)까지 담는다.
- 파일이 수천 개면 S3 요청 한도에 걸려 `SlowDown: Please reduce your request rate` 오류가 날 수 있다.

[Spark Streaming 잡](/posts/spark-streaming-to-hive/)은 20초마다 HDFS 파일 끝에 붙여 썼고, 그 대가인 작은 파일은 합치기 잡으로 정리했다.

작은 파일은 쓰기 전에 막는 게 가장 싸다. 쓰는 쪽에서 레코드를 더 오래 모으거나, Spark라면 `COALESCE`, `REPARTITION` 힌트로 출력 파일 수를 정한다. 이미 쌓인 파일은 따로 합친다. Iceberg 테이블이면 Spark 프로시저 하나로 된다.

```sql
-- Iceberg 1.12 문서 기준, 기본 전략은 binpack
CALL catalog_name.system.rewrite_data_files('db.sample');
```

목표 파일 크기 기본값은 테이블 속성 `write.target-file-size-bytes`를 따르고, 그 기본값은 512MB다. 전략은 `sort`도 고를 수 있다. 합치면서 정렬까지 하면 파일 수와 min/max 범위가 같이 정리된다.

파티션 키를 너무 잘게 잡는 것도 작은 파일의 원인이다. 조회가 대부분 일 단위라면 시간 단위로 파티션을 나누지 말고, 타임스탬프로 정렬해 두는 편이 낫다. 짧은 시간 범위 조회는 정렬만으로도 시간 파티션과 거의 비슷하게 효율적이다.

## 참고 자료

- [Apache Parquet: File Format](https://parquet.apache.org/docs/file-format/)
- [Apache Parquet: parquet.thrift (Statistics, RowGroup, ColumnIndex)](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
- [Apache ORC: ORC Specification v1](https://orc.apache.org/specification/ORCv1/)
- [Spark: Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Spark: ORC Files](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
- [Spark: Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark: Datetime Functions (from_unixtime, timestamp_seconds)](https://spark.apache.org/docs/latest/api/sql/datetime-functions/)
- [Spark 소스: sources/filters.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/sources/filters.scala)
- [Spark 소스: UnwrapCastInBinaryComparison.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/UnwrapCastInBinaryComparison.scala)
- [Spark 소스: PruneFileSourcePartitions.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/PruneFileSourcePartitions.scala)
- [Amazon Athena: Optimize data](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-data-optimization-techniques.html)
- [Amazon Athena: EXPLAIN and EXPLAIN ANALYZE](https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html)
- [Amazon Athena: Athena engine version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html)
- [Trino: Connectors (applyFilter, TupleDomain)](https://trino.io/docs/current/develop/connectors.html)
- [Apache Iceberg: Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)

## English summary

Parquet and ORC keep min, max, and null count statistics for row groups, stripes, and smaller units, so a reader can skip data that cannot match a filter. Partition pruning skips whole directories before any file is opened, and predicate pushdown skips parts inside each file. A filter like `from_unixtime(created_at / 1000000) >= ...` cannot be compared with those statistics, while `created_at >= 1789344000000000` can, which is why my Superset `epoch_us` change kept the WHERE clause on the raw column. Many small files add listing, open, and footer costs, so it is better to write larger files or compact them later, for example with Iceberg `rewrite_data_files`. Spark and Trino work the same way here, and the design point does not change: keep filters on the raw column.
