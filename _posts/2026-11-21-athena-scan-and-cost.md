---
title: "Athena 쿼리 비용은 어디서 정해질까"
date: 2026-11-21 03:14:00 +0900
categories: [Batch & Query, Trino·Athena]
tags: [athena, trino, partitioning, iceberg, parquet, cost, concept]
description: "Athena 요금이 쿼리 시간이 아니라 S3에서 읽은 바이트로 정해지는 구조와, WHERE 절, 파일 포맷, 테이블 포맷이 스캔량과 비용에 미치는 영향을 설명한다."
---

Athena SQL의 기본 요금은 쿼리가 돈 시간이 아니라 S3에서 읽은 바이트로 매겨진다. 그래서 같은 결과를 내는 두 쿼리라도 WHERE 절 모양, 파일 포맷, 테이블 포맷에 따라 청구 금액이 달라진다. `LIMIT 10`을 붙여도 읽는 양이 줄지 않는 경우가 많다.

예를 들어 분석 데이터가 Glue 카탈로그의 Hive 테이블과 S3 Tables의 Iceberg 테이블에 나뉘어 있고 둘 다 Athena로 읽는다고 하자. 두 테이블 종류는 읽을 파일을 고르는 방식이 다르고, 그 차이가 그대로 스캔량이 된다.

버전은 Athena 엔진 버전 3과 PyAthena 4.3.0 기준이다.

## 요금은 스캔한 바이트, 쿼리마다 최소 10MB

쿼리당 요금 모델에서 청구 기준은 쿼리가 스캔한 데이터양이다. 이 값은 메가바이트 단위로 올림하고, 쿼리 하나에 최소 10MB가 붙는다. 단가는 바뀔 수 있으니 여기서는 모델만 적는다.

| 경우 | 쿼리당 요금 |
|---|---|
| `SELECT`, CTAS, `UNLOAD`, `INSERT INTO` | 스캔한 바이트 |
| `CREATE`/`ALTER`/`DROP TABLE`, 파티션 관리 문 | 청구하지 않는다 |
| 실패한 쿼리 | 청구하지 않는다 |
| 취소된 쿼리 | 취소 시점까지 스캔한 바이트 |

S3 요청, 결과 파일 저장, Glue Data Catalog 호출은 각 서비스 요금으로 따로 나간다. 스캔량 대신 DPU 용량을 예약해 시간 단위로 내는 capacity reservation도 있고, 이 경우 스캔량 요금은 붙지 않는다. 최소 10MB 때문에 작은 쿼리를 많이 쪼개 보내면 실제로 읽은 양보다 많이 청구된다. 차트가 여러 개인 대시보드는 열 때마다 차트 수만큼 쿼리가 나간다.

쿼리가 실제로 얼마나 읽었는지는 `GetQueryExecution` 응답의 `Statistics.DataScannedInBytes`에 있다. 같은 응답의 `QueryPlanningTimeInMillis`에는 파티션 목록을 받아 오는 시간도 들어간다.

## 파티션은 읽을 디렉터리 수를 정한다

Hive 스타일 테이블에서 Athena는 메타스토어에서 파티션 목록을 받아 WHERE 조건으로 거르고, 남은 위치만 나열해 읽는다. 등록되지 않은 파티션은 에러 없이 0건이다. 등록 방법은 세 갈래다.

| 방법 | 파티션 목록의 출처 | 비용이 드는 곳 |
|---|---|---|
| `ALTER TABLE ADD PARTITION` | Glue에 등록한 항목 | 쓴 잡이 값을 알아야 한다 |
| `MSCK REPAIR TABLE` | Glue에 등록한 항목 | 실행마다 테이블 위치 전체를 나열한다 |
| 파티션 프로젝션 | 테이블 속성으로 계산 | Athena 밖의 엔진은 쓰지 못한다 |

DDL과 파티션 관리 문은 쿼리 요금이 없으니, 등록 방식의 차이는 청구서보다 계획 시간과 운영에서 난다. 등록 단계와 MSCK가 Athena에서 추가만 한다는 점, 프로젝션의 제약은 [Hive 파티션 글](/posts/hive-partitions-and-metastore/)에 적었다.

## 파티션 키와 원본 컬럼은 다른 컬럼이다

Hive 테이블에서 자주 나는 실수는 파티션 키 `dt` 대신 원본 시각 컬럼으로 거르는 것이다.

```sql
-- dt만 파티션 키다. 이 조건은 모든 파티션을 연다
SELECT count(*) FROM logs WHERE event_ts >= TIMESTAMP '2026-12-14 00:00:00';

-- 파티션 키로 범위를 먼저 자른다
SELECT count(*) FROM logs
WHERE dt >= '2026-12-14' AND event_ts >= TIMESTAMP '2026-12-14 00:00:00';
```

파티션 키에 함수를 씌우는 경우는 사정이 조금 다르다. Trino Hive 커넥터 소스로 추론하면 메타스토어에 넘기는 필터는 컬럼별 값 범위(`TupleDomain`)뿐이다. `substr(dt, 1, 7) = '2026-12'` 같은 식은 메타스토어에서 목록을 줄이지 못하고, 파티션 이름을 받아 온 뒤 엔진이 하나씩 계산해 거른다. 읽는 파일은 줄지만 계획 단계는 파티션이 쌓일수록 길어진다. Athena 문서에는 이 단계가 나와 있지 않으니, 실제로 어떤 파티션을 읽는지는 `EXPLAIN`의 파티션 값 목록으로 확인한다. 그 출력 모양과 Glue 필터 pushdown이 STRING 파티션 키를 요구한다는 점은 [컬럼 파일과 프루닝 글](/posts/columnar-files-and-pruning/)에 있다.

## 포맷과 압축은 파일 안에서 줄인다

파티션이 디렉터리를 고르면, 파일 포맷이 그 안에서 읽을 바이트를 정한다. Parquet이나 ORC는 쿼리에 나온 컬럼만 읽으니 `SELECT *`를 필요한 컬럼 목록으로 바꾸는 것만으로 스캔량이 준다. 압축된 파일은 압축된 바이트로 읽는다. row group과 stripe의 min/max 통계로 건너뛰는 단계, 그리고 그 통계를 못 쓰게 만드는 `from_unixtime(created_at / 1000000)` 같은 필터는 컬럼 파일 글의 Superset `epoch_us` 사례 그대로다. 작은 파일이 많을 때 스캔 바이트와 별개로 붙는 나열, 요청, footer 읽기 비용도 그 글에 있다.

## LIMIT은 건너뛸 파일을 알려 주지 않는다

`SELECT * FROM logs LIMIT 10`이면 싸지 않느냐는 질문을 자주 받는다. WHERE 조건은 파티션과 통계로 읽지 않을 곳을 정해 주지만, LIMIT은 결과 행 수만 자른다. `ORDER BY` 없는 LIMIT은 행이 모이면 일찍 끝나 적게 읽을 때도 있지만 보장되지 않는다. 앞에 `ORDER BY`, `GROUP BY`, 조인이 있으면 그 연산이 입력을 다 받아야 상위 10행이 나오니, 연산 순서로 추론하면 스캔량은 LIMIT이 없을 때와 같다. Athena는 `ORDER BY`와 `LIMIT`이 같이 오면 top N 전용 연산을 쓰지만, 바뀌는 것은 정렬 방식이고 읽는 입력은 그대로라고 본다. `ORDER BY` 없는 LIMIT은 결과가 실행마다 달라질 수 있어서 뒤에 나올 결과 재사용 대상에서도 빠진다. 데이터를 훑어보려면 최근 파티션 하나를 WHERE로 고정하는 편이 읽는 양을 확실히 묶는다.

## Iceberg는 메타데이터로 파일을 고른다

Iceberg 테이블은 디렉터리를 나열하지 않는다. 매니페스트 리스트의 파티션 요약과 매니페스트의 컬럼 통계로 읽을 데이터 파일을 고르고, MSCK 같은 등록 단계가 없다. Athena는 Iceberg의 hidden partitioning을 지원하므로 `day(event_ts)`로 나눈 테이블은 `event_ts` 조건만으로 파티션이 걸러진다. 쓰는 쪽이 `dt` 컬럼을 따로 채울 필요도 없다. 조건은 붙는다. hidden partitioning이 바꿔 주는 것은 원본 컬럼에 대한 비교라서, 원본 컬럼에 함수를 씌우면 효과가 줄어든다. BIGINT epoch 컬럼에는 `day` 변환을 걸 수 없다는 제약은 [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에 있다.

Athena는 Iceberg 포맷 v2 테이블을 읽고, position delete와 equality delete를 읽는 시점에 데이터 파일과 맞춰 본다(merge-on-read). CDC upsert로 delete 파일이 쌓이면 Athena 쪽 읽기도 무거워진다. 관리형 compaction이 실패하는 테이블이나, metadata.json이 너무 크다는 `HIVE_METASTORE_ERROR`도 이 메타데이터 층에서 생기는 문제다.

## CTAS와 UNLOAD는 결과를 컬럼 파일로 남긴다

한 번 무겁게 읽고 여러 번 가볍게 읽으려면 결과를 다시 써 둔다. CTAS는 테이블과 데이터를 같이 만들고, 포맷을 지정하지 않으면 Parquet으로 쓴다. Iceberg로 만들려면 `table_type`과 `location`을 주고 파티션은 `partitioning`으로 적는다.

```sql
CREATE TABLE analytics.events_daily
WITH (
  table_type = 'ICEBERG',
  location = 's3://bucket/analytics/events_daily/',
  is_external = false,
  format = 'PARQUET',
  partitioning = ARRAY['day(event_ts)']
) AS
SELECT user_id, event, CAST(event_ts AS timestamp(6)) AS event_ts
FROM logs
WHERE dt >= '2026-12-01';
```

Hive 테이블의 `timestamp`는 밀리초 정밀도라서 Iceberg CTAS에는 `timestamp(6)`으로 바꿔 넣는다. CTAS로 파티션 테이블을 만들면 한 번에 파티션 100개까지만 쓰고, 그 이상은 `INSERT INTO`로 나눠 넣는다. 실패한 CTAS는 대상 위치에 쓰다 만 파일을 남길 수 있다. CTAS는 쿼리 하나로 끝나는 대신 그 쿼리가 끝까지 살아남아야 한다.

`UNLOAD`는 테이블 없이 결과만 Parquet, ORC, Avro, JSON 파일로 쓴다. 일반 `SELECT` 결과는 압축하지 않은 CSV 하나로 저장되고, `UNLOAD`는 워커가 병렬로 직접 쓴다. [PyAthena 복합 타입 글](/posts/pyathena-complex-types/)의 MAP 값은 일반 결과 경로에서 Athena가 복합 타입을 고유 형식의 문자열로 돌려주고, 드라이버가 그 문자열을 다시 파싱하다 생긴 문제였다. PyAthena의 Arrow, pandas, Polars 커서는 `unload=True`를 주면 SELECT를 `UNLOAD`로 감싸 Parquet을 직접 읽고, Arrow 커서는 그 Parquet 스키마로 타입을 정한다. 대신 UNLOAD 출력은 파일 사이 정렬이 보장되지 않고, `time` 같은 일부 타입은 출력 컬럼으로 받지 않는다.

## 워크그룹은 상한을, 결과 재사용은 반복을 막는다

워크그룹에는 쿼리당 스캔 한도를 하나 걸 수 있다. 10MB부터 지정할 수 있고, 넘는 쿼리는 취소된다. 취소된 쿼리는 그때까지 스캔한 만큼 청구되니, 한도는 비용을 0으로 만들지 않고 상한을 정한다. 워크그룹 전체의 시간당, 일당 스캔량에는 CloudWatch 알림을 걸 수 있는데, 이 알림은 쿼리를 멈추지 않는다. 멈추려면 SNS 알림을 받고 워크그룹을 끄는 동작을 따로 연결한다.

대시보드처럼 같은 쿼리가 짧은 시간에 반복되는 경우는 결과 재사용이 맞는다. 같은 워크그룹에서 쿼리 문자열, 데이터베이스와 카탈로그, 결과 설정이 맞고 이전 결과가 최대 사용 기한 안이면 실행을 건너뛰고 그 결과를 돌려준다. 기한은 기본 60분, 최대 7일이다.

```python
from pyathena import ExecuteOptions, connect  # PyAthena 4.3.0

cursor = connect(s3_staging_dir="s3://bucket/athena-results/",
                 region_name="ap-northeast-2").cursor()
options = ExecuteOptions(work_group="bi", result_reuse_enable=True, result_reuse_minutes=30)
cursor.execute("SELECT dt, count(*) FROM logs WHERE dt >= '2026-12-08' GROUP BY dt", options=options)
```

제약도 많다. 기한이 끝날 때까지 원본 데이터 변경을 확인하지 않아 오래된 결과가 나올 수 있다. 원본 위치가 Lake Formation 데이터 위치로 등록된 테이블, 행·열 권한이 걸린 테이블, 카탈로그를 둘 이상 쓰는 쿼리는 재사용하지 않는다. Glue 카탈로그와 S3 Tables 카탈로그를 같이 쓰는 조인 쿼리가 이 조건에 걸린다. BI 도구 앞단에 Redis 같은 캐시를 두면 반복 조회를 막는 층을 Athena 바깥에 하나 더 둘 수 있다.

## 비용을 줄이는 순서

위 내용을 효과가 큰 순서로 묶으면 이렇다. 앞쪽일수록 쿼리 한 줄이나 테이블 한 번 변환으로 스캔량이 몇 배씩 준다.

| 순서 | 할 일 | 줄어드는 이유 |
|---|---|---|
| 1 | `SELECT *` 대신 필요한 컬럼만 고른다 | Parquet·ORC는 고른 컬럼만 읽는다. 컬럼 50개 중 3개만 쓰면 스캔도 그 비율 근처로 준다 |
| 2 | 파티션 키 조건을 꼭 넣는다 | Hive 스타일 테이블은 `dt` 조건이 없으면 모든 파티션을 연다. 원본 시각 컬럼 조건만으로는 걸러지지 않는다 |
| 3 | 컬럼에 함수나 형 변환을 씌우지 않는다 | `date(created_at) = ...` 대신 원본 컬럼 범위 조건을 써야 파일의 최솟값·최댓값으로 건너뛴다 |
| 4 | 텍스트 원본은 컬럼 포맷으로 바꿔 둔다 | gzip CSV·JSON은 한 컬럼만 골라도 파일 전체를 읽는다. CTAS로 Parquet·ORC 테이블을 만들어 그쪽을 읽게 한다 |
| 5 | 작은 파일을 합치고, 자주 거르는 컬럼으로 정렬한다 | 파일이 작으면 열기 비용이 커지고, 정렬이 안 돼 있으면 최솟값·최댓값 범위가 넓어 건너뛸 파일이 없다 |
| 6 | 대시보드용 중간 집계 테이블을 만든다 | 원본을 매번 훑는 대신 일 단위 집계를 CTAS·INSERT로 한 번 만들고 대시보드는 그 작은 테이블을 읽는다 |
| 7 | 같은 쿼리 반복은 결과 재사용 | 기한 안의 같은 쿼리는 실행 자체를 건너뛴다 |
| 8 | 워크그룹에 쿼리당 스캔 한도와 알림 | 실수로 날린 전체 스캔이 한도에서 멈춘다 |

앞에서 본 것처럼 `LIMIT`은 스캔을 줄인다고 보장하지 않으니, 데이터 모양을 볼 때도 하루치 파티션 조건을 주거나 샘플 테이블을 한 번 만들어 그걸 본다. 쿼리마다 최소 10MB가 청구되므로 아주 작은 쿼리를 수천 번 나눠 보내기보다 한 쿼리로 묶는 편이 싸다.

줄었는지는 숫자로 확인한다. 콘솔 쿼리 결과의 "Data scanned", 또는 `GetQueryExecution` 응답의 `DataScannedInBytes`를 바꾸기 전후로 비교한다. 단계마다 읽은 행과 바이트는 `EXPLAIN ANALYZE`로 본다.

## 참고 자료

- [Amazon Athena pricing](https://aws.amazon.com/athena/pricing/)
- [Amazon Athena FAQs](https://aws.amazon.com/athena/faqs/)
- [Amazon Athena: Athena engine version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html)
- [Amazon Athena: Optimize queries](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-query-optimization-techniques.html)
- [Amazon Athena: Query Iceberg table data](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-table-data.html)
- [Amazon Athena: Create Iceberg tables](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html)
- [Amazon Athena: CREATE TABLE AS](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html)
- [Amazon Athena: CTAS considerations and limitations](https://docs.aws.amazon.com/athena/latest/ug/ctas-considerations-limitations.html)
- [Amazon Athena: UNLOAD](https://docs.aws.amazon.com/athena/latest/ug/unload.html)
- [Amazon Athena: Per-query and per-workgroup data usage controls](https://docs.aws.amazon.com/athena/latest/ug/workgroups-setting-control-limits-cloudwatch.html)
- [Amazon Athena: Reuse query results](https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html)
- [Athena API: QueryExecutionStatistics](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatistics.html)
- [Trino 소스: HivePartitionManager.java](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/HivePartitionManager.java)
- [PyAthena: Usage (ExecuteOptions, result reuse)](https://github.com/pyathena-dev/PyAthena/blob/master/docs/usage.md)
- [PyAthena: ArrowCursor unload options](https://github.com/pyathena-dev/PyAthena/blob/master/docs/arrow.md)

## English summary

Athena bills SQL queries by the bytes they scan, rounded up to the megabyte with a 10 MB minimum, so cost depends on what the engine can skip, not on how long the query runs. Hive tables skip directories through registered partitions, which only works when the filter is on the partition key, while Iceberg tables pick files from manifests and support hidden partitioning on the source column. LIMIT does not tell the engine which files to skip, but columnar formats, CTAS, and UNLOAD reduce what later queries read. When Hive tables in Glue and Iceberg tables in S3 Tables sit side by side, cross-catalog joins cannot use Athena result reuse. Workgroup limits only cap a single query's scan, so the cheaper design choice is still how the table is partitioned and stored. In order of impact: select only needed columns, always filter on the partition key, keep functions off filtered columns, convert text files to Parquet or ORC, compact and sort files, serve dashboards from small aggregate tables, and reuse results for repeated queries.
