---
title: "클라우드 웨어하우스는 저장과 계산을 어떻게 나눌까"
date: 2026-11-21 06:14:00 +0900
categories: [Batch & Query, Data Warehouse]
tags: [snowflake, bigquery, redshift, data-warehouse, lakehouse, iceberg, elt, concept]
---

저장과 계산이 한 노드에 묶인 웨어하우스에서는 디스크가 차면 노드를 늘려야 하고, 노드를 늘리면 쓰지 않을 CPU까지 산다. 월말 집계와 대시보드 쿼리가 같은 클러스터에서 부딪혀도 따로 떼어 낼 방법이 없다. Snowflake, BigQuery, Redshift는 모두 데이터를 공유 저장소에 두고 계산을 따로 늘리지만, 계산을 무엇으로 세고 어떤 메타데이터로 읽을 범위를 줄이는지는 셋이 다르다.

버전은 2026년 11월의 Snowflake, BigQuery, Amazon Redshift 문서와 Iceberg 포맷 v2, v3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 데이터 웨어하우스 | 정해 둔 스키마로 적재한 데이터를 SQL로 집계하는 저장소. 저장 형식은 제품이 관리한다 |
| 데이터 레이크 | 객체 저장소에 파일을 열린 포맷 그대로 쌓고 여러 엔진이 읽는 곳 |
| 레이크하우스 | 레이크의 열린 파일 위에 트랜잭션과 테이블 메타데이터를 얹은 구조 |
| ETL / ELT | 밖에서 변환한 뒤 적재 / 원본을 먼저 적재하고 웨어하우스 안에서 변환 |
| micro-partition | Snowflake 테이블의 저장 단위. 압축 전 50~500MB, 컬럼별 값 범위를 메타데이터로 둔다 |
| virtual warehouse | Snowflake의 계산 클러스터. 웨어하우스끼리 계산 자원을 나누지 않는다 |
| 슬롯 | BigQuery가 쿼리 stage를 실행하는 가상 계산 단위 |
| on-demand / capacity | 처리한 바이트로 내는 요금 / 예약한 슬롯 용량에 시간으로 내는 요금 |
| RMS | Redshift Managed Storage. 노드의 SSD를 캐시로 쓰고 S3에 영구 저장한다 |
| 분산 스타일 | Redshift가 행을 slice에 나누는 방법. AUTO, EVEN, KEY, ALL |
| zone map | 1MB 블록마다 min/max를 적어 두고 범위 조건 밖의 블록을 건너뛰는 메타데이터 |

## 웨어하우스, 레이크, 레이크하우스

| | 웨어하우스 | 레이크 | 레이크하우스 |
|---|---|---|---|
| 파일 형식 | 제품 내부 형식 | Parquet, ORC, JSON 등 열린 형식 | 열린 형식 + 테이블 메타데이터 |
| 지금 테이블을 이루는 파일 | 제품이 안다 | 디렉터리와 메타스토어가 정한다 | 커밋 로그나 스냅샷이 정한다 |
| 행 단위 변경 | 된다 | 파티션을 통째로 다시 쓴다 | 된다 |

레이크하우스라는 말은 Databricks 쪽 연구자들이 2021년 CIDR 논문에서 정리했다. Parquet 같은 열린 직접 접근 형식, 머신러닝 작업 지원, 웨어하우스에 견줄 성능을 조건으로 들었다. 그 메타데이터를 Iceberg, Delta, Hudi가 각자 어디에 적는지는 [테이블 포맷 비교 글](/posts/table-formats-compared/)에 있다. 2026년의 세 웨어하우스는 모두 Iceberg 테이블을 읽고 쓰므로 세 칸의 경계가 제품 단위로 갈리지는 않는다. 같은 제품 안에서 테이블마다 고른다.

## ETL과 ELT는 변환이 도는 곳이 다르다

ETL은 별도 처리 서버에서 변환을 끝내고 결과만 대상에 넣는다. ELT는 원본을 먼저 웨어하우스에 넣고 그 안의 SQL로 변환하므로, 원본이 남아 있어 변환을 바꿔 다시 돌릴 수 있다. 대신 변환에 드는 계산이 웨어하우스 요금으로 잡힌다.

[서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 구조는 ETL이었다. Hive 로그를 Spark로 집계하고 결과 행만 MySQL 지표 테이블에 넣었다. 대상이 MySQL 같은 OLTP 데이터베이스면 무거운 집계를 대상 안에서 돌리기 어려우므로 변환이 앞에 온다. 대신 새 지표가 필요하면 Spark 쪽을 고쳐야 한다. 웨어하우스 안에서 SQL 모델로 변환을 쌓는 ELT 방식은 [dbt 증분 모델 글](/posts/dbt-incremental-models/)에 정리했다.

## 세 제품이 저장과 계산을 나누는 방식

| | Snowflake | BigQuery | Redshift |
|---|---|---|---|
| 저장 | 클라우드 저장소의 micro-partition | 컬럼 형식 관리형 저장소 | RMS (SSD 캐시 + S3) |
| 계산 단위 | virtual warehouse 크기와 클러스터 수 | 슬롯 | 프로비저닝 노드, 또는 Serverless RPU |
| 건너뛰는 근거 | micro-partition의 컬럼별 값 범위 | 파티션, 클러스터링 블록 메타데이터 | 블록별 min/max(zone map) |
| 배치를 정하는 키 | clustering key | `PARTITION BY`, `CLUSTER BY` | `DISTKEY`, `SORTKEY` |

```sql
-- Snowflake: 날짜로 줄인 식을 clustering key로
CREATE TABLE events (event_ts TIMESTAMP_NTZ, user_id NUMBER, country STRING)
  CLUSTER BY (TO_DATE(event_ts));
SELECT SYSTEM$CLUSTERING_INFORMATION('events');

-- BigQuery: 일 단위 파티션 안에서 country, user_id 순으로 클러스터링
CREATE TABLE mydataset.events (event_ts TIMESTAMP, user_id INT64, country STRING)
PARTITION BY DATE(event_ts)
CLUSTER BY country, user_id
OPTIONS (require_partition_filter = TRUE);

-- Redshift: 조인 키로 분산하고 시간으로 정렬
CREATE TABLE events (event_ts TIMESTAMP, user_id BIGINT, country VARCHAR(2))
DISTSTYLE KEY DISTKEY (user_id)
COMPOUND SORTKEY (event_ts, country);
```

## Snowflake는 micro-partition 메타데이터로 건너뛴다

Snowflake는 적재한 데이터를 압축 전 50~500MB 크기의 micro-partition으로 나누고, 각 micro-partition에 컬럼별 값 범위와 고유값 수를 적어 둔다. 쿼리는 이 범위로 필요 없는 micro-partition을 거르고, 남은 것 안에서도 필요한 컬럼만 읽는다. 서브쿼리가 들어간 조건은 서브쿼리가 상수를 돌려줘도 pruning에 쓰이지 않는다.

적재 순서가 조회 조건과 맞지 않으면 값 범위가 겹치는 micro-partition이 늘어난다. 겹침의 평균 깊이가 clustering depth이고, 작을수록 잘 모여 있다. 이때 clustering key를 정하면 Automatic Clustering이 백그라운드에서 다시 정렬한다. 사용자가 웨어하우스를 내주지 않는 서버리스 작업이지만 크레딧은 든다. 그래서 수 TB 이상이고, 조회가 선택적이며, DML보다 조회가 훨씬 많은 테이블에 권한다. 키는 보통 3~4개 컬럼까지, 카디널리티가 낮은 것부터 둔다. 타임스탬프처럼 고유값이 너무 많은 컬럼은 날짜로 줄인 식을 쓴다.

계산은 virtual warehouse가 맡는다. 웨어하우스끼리 계산 자원을 나누지 않으므로 적재용과 대시보드용을 떼어 두면 서로 느려지지 않는다. 과금은 초 단위이고 시작할 때마다 최소 60초다. 무거운 쿼리는 크기를 키우는 resize로, 동시 사용자는 클러스터를 더하는 multi-cluster(Enterprise 에디션)로 푼다. 쿼리 텍스트가 같고 데이터가 그대로면 결과 캐시가 24시간 동안 결과를 다시 쓴다. 대소문자나 별칭만 달라도 재사용되지 않는다.

## BigQuery는 슬롯을 쓰고 바이트로 청구한다

BigQuery는 쿼리를 stage로 나누고 각 stage를 슬롯이 병렬로 실행한다. 쿼리 하나가 쓸 슬롯 수는 직접 정할 수 없다. on-demand 요금은 쿼리가 처리한 바이트(TiB 단위)로 매겨지고, 프로젝트는 동시에 최대 2,000 슬롯까지 쓴다. capacity 요금은 Standard, Enterprise, Enterprise Plus 에디션의 reservation에 슬롯을 잡고 시간으로 낸다. 항상 잡혀 있는 baseline 위에 autoscaling 슬롯이 50개 단위로 붙는다.

on-demand에서는 읽은 바이트가 곧 비용이라 Athena와 같은 습관이 필요하다. 그 습관은 [Athena 스캔 비용 글](/posts/athena-scan-and-cost/)에 정리했고, 여기서는 BigQuery에서 다른 점만 적는다.

- 파티션은 시간 단위 컬럼, 적재 시각, 정수 범위 세 가지이고 테이블당 10,000개까지다.
- 클러스터링 컬럼은 4개까지다. 필터에 맞지 않는 블록은 읽지 않고 청구도 하지 않는다. 재클러스터링은 자동이고 무료다.
- 먼저 클러스터링을 쓰고, 파티션당 10GB 이상이거나 실행 전에 비용을 정확히 알아야 할 때 파티션을 더한다. 파티션이 10GB보다 작게 쪼개지면 클러스터링만 두는 편이 낫다.
- dry run이 내는 바이트는 상한이다. 클러스터링 테이블은 실제로 덜 읽을 수 있어서, `maximum_bytes_billed`가 실제로는 한도 안이었을 쿼리를 막기도 한다.
- `LIMIT`은 읽는 양을 줄이지 않는다. 클러스터링 테이블만 충분한 블록을 읽은 뒤 멈출 수 있다.

## Redshift는 행을 노드에 어떻게 나눌지 먼저 정한다

Redshift 클러스터는 leader 노드가 계획을 세우고, compute 노드가 slice 단위로 나눠 실행한다. RA3와 Graviton 기반 RG 노드는 RMS를 쓴다. 노드의 SSD를 1차 캐시로 쓰고, 블록의 온도와 나이를 보고 S3로 내리므로 노드 수를 데이터 크기가 아닌 계산량으로 정한다.

Snowflake와 BigQuery는 데이터를 어느 계산 자원에 둘지 사용자가 정하지 않는다. Redshift는 테이블마다 분산 스타일을 정한다.

| 스타일 | 행을 두는 곳 | 맞는 경우 |
|---|---|---|
| KEY | 같은 키 값을 같은 slice에 | 큰 테이블끼리 같은 키로 조인 |
| ALL | 모든 노드에 전체 복사 | 잘 바뀌지 않는 테이블. 저장량이 노드 수만큼 늘어난다 |
| EVEN | slice에 돌아가며 | 조인하지 않거나 KEY와 ALL 중 고르기 어려울 때 |
| AUTO | 작을 때 ALL, 커지면 KEY나 EVEN으로 바꾼다 | 지정하지 않았을 때의 기본값 |

작은 차원은 쿼리 중에 재분배해도 싸서 ALL로 두는 이득이 크지 않다. 조인을 아예 미리 해 둘지는 [정규화와 비정규화 글](/posts/normalization-vs-denormalization/)에서 다뤘다.

정렬 키는 zone map과 짝이다. Redshift는 컬럼 데이터를 1MB 블록에 쓰고 블록마다 min/max를 둔다. 5년 치를 날짜로 정렬해 두면 한 달 조건은 최대 98%의 블록을 건너뛸 수 있다. compound 키는 앞 컬럼부터 쓰는 조건에 맞고, interleaved 키는 어느 컬럼 조합에도 같은 비중을 주지만 8개 컬럼까지이며 `VACUUM REINDEX`가 오래 걸린다. AWS는 지금 `SORTKEY AUTO`로 만들고 자동 테이블 최적화에 맡기기를 권한다.

Redshift Serverless는 노드 대신 RPU로 계산을 센다. 1 RPU는 메모리 16GB이고 기본 base capacity는 128 RPU다. 4~512 RPU 사이에서 고르고, 일부 리전은 1024까지 된다. 새 workgroup은 price-performance 목표가 Balanced로 켜져 있어 쿼리 복잡도와 데이터 크기를 보고 RPU를 늘리고 줄인다.

## 세 제품이 Iceberg를 읽는 길

| | 제품이 카탈로그인 Iceberg | 외부 카탈로그의 Iceberg | 바깥 엔진이 읽는 길 |
|---|---|---|---|
| Snowflake | Snowflake-managed Iceberg table. 읽기·쓰기, 컴팩션, 클러스터링 | catalog integration(Glue, Iceberg REST, Open Catalog). REST 카탈로그면 쓰기도 된다 | Horizon Catalog의 Iceberg REST API로 읽기·쓰기 |
| BigQuery | Apache Iceberg managed table. 사용자 GCS 버킷에 Parquet, DML과 스트리밍 | Lakehouse runtime catalog의 Iceberg REST 카탈로그 | managed table은 `EXPORT TABLE METADATA`로 낸 v2 스냅샷, runtime catalog 테이블은 REST 카탈로그 |
| Redshift | 없음. Glue Data Catalog에 등록해 쓴다 | Glue Data Catalog, S3 table bucket | Athena, EMR 등이 같은 Glue 테이블을 읽는다 |

Snowflake-managed Iceberg 테이블은 데이터와 메타데이터를 사용자의 external volume에 두고, Snowflake가 카탈로그와 유지 작업을 맡는다. 외부 카탈로그 테이블은 Snowflake가 유지 작업을 하지 않고, 클러스터링과 클론을 쓸 수 없다. catalog-linked database는 REST 카탈로그의 네임스페이스를 자동으로 찾아 붙인다. 외부 관리 테이블 쓰기는 v2, v3 테이블에서 autocommit으로만 된다. Iceberg 버전 1~3을 지원하지만 row-level equality delete는 제외다.

Google은 2026년 4월 20일 BigLake를 Lakehouse로, BigLake metastore를 Lakehouse runtime catalog로 이름을 바꿨다. API와 IAM 이름은 아직 BigLake다. 메타데이터 JSON 경로를 직접 지정하는 Iceberg external table은 읽기 전용이고 경로를 손으로 갱신해야 해서, Google은 이제 대부분의 경우에 권하지 않는다. Lakehouse runtime catalog의 Iceberg 테이블에 BigQuery가 쓰는 기능은 2026년 6월 25일 Preview로 나왔다. managed table에서는 Storage Write API로 최근 90분 안에 들어온 데이터가 내보낸 Iceberg 메타데이터에 아직 없을 수 있다.

Redshift는 Glue Data Catalog의 Iceberg 테이블을 external schema나 자동 마운트되는 `awsdatacatalog`로 읽는다. RA3는 클러스터 밖의 Spectrum 서버에서 읽고 Spectrum 요금을 내며, RG와 Serverless는 자기 계산 자원으로 읽어 따로 청구하지 않는다. `CREATE TABLE ... USING ICEBERG`, `INSERT`, `DELETE`, `UPDATE`, `MERGE`도 된다. 다른 엔진이 만든 테이블에 쓰려면 v2나 v3이고 Parquet이어야 한다. Trino도 같은 Glue 테이블을 커넥터와 카탈로그로 붙여 읽는데, 그 구조는 [Trino 아키텍처 글](/posts/trino-architecture/)에 있다.

같은 Iceberg 테이블을 여러 엔진이 읽게 되면 쓰기와 유지 작업을 누가 맡는지가 남는다. 외부 카탈로그 테이블을 Snowflake가 정리하지 않듯, 컴팩션과 스냅샷 만료는 카탈로그 주인 쪽 일이다.

## 참고 자료

- [Lakehouse: A New Generation of Open Platforms (CIDR 2021)](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf), [AWS: The difference between ETL and ELT](https://aws.amazon.com/compare/the-difference-between-etl-and-elt/)
- [Snowflake: Key concepts and architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts), [Micro-partitions and data clustering](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions)
- [Snowflake: Clustering keys and clustered tables](https://docs.snowflake.com/en/user-guide/tables-clustering-keys), [Automatic Clustering](https://docs.snowflake.com/en/user-guide/tables-auto-reclustering)
- [Snowflake: Overview of warehouses](https://docs.snowflake.com/en/user-guide/warehouses-overview), [Using persisted query results](https://docs.snowflake.com/en/user-guide/querying-persisted-results)
- [Snowflake: Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg), [Access Iceberg tables through Horizon Catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon)
- [BigQuery: Storage overview](https://docs.cloud.google.com/bigquery/docs/storage_overview), [Understand slots](https://docs.cloud.google.com/bigquery/docs/slots), [Quotas and limits](https://docs.cloud.google.com/bigquery/quotas), [Partitioned tables](https://docs.cloud.google.com/bigquery/docs/partitioned-tables), [Clustered tables](https://docs.cloud.google.com/bigquery/docs/clustered-tables), [Control costs](https://docs.cloud.google.com/bigquery/docs/best-practices-costs)
- [BigQuery: Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/iceberg-tables), [Iceberg external tables](https://docs.cloud.google.com/bigquery/docs/iceberg-external-tables), [Lakehouse release notes](https://docs.cloud.google.com/lakehouse/docs/release-notes)
- [Redshift: Data warehouse system architecture](https://docs.aws.amazon.com/redshift/latest/dg/c_high_level_system_architecture.html), [Provisioned clusters](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html), [Distribution styles](https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html), [Sort keys](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
- [Redshift Serverless: Compute capacity](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-capacity.html), [Using Apache Iceberg tables](https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg.html), [Iceberg write SQL commands](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-sql-syntax.html), [Spectrum overview](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-overview.html)

## English summary

Snowflake, BigQuery, and Redshift all keep data in shared storage and scale compute separately, but they count compute differently: virtual warehouses, slots, and nodes or RPUs. Each skips data with its own metadata: micro-partition value ranges, partition and cluster blocks, and per-block min/max zone maps, and only Redshift asks you to choose how rows are spread across nodes. My service metric batches were ETL, aggregating Hive logs in Spark and loading only result rows into MySQL, so a new metric meant adding a data class and a query builder method to that batch. As of November 2026, all three read and write Apache Iceberg tables, so the real design choice is which catalog owns each table and who runs its compaction and snapshot expiry.
