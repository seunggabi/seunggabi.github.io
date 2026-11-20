---
title: "Trino는 쿼리를 어떻게 쪼개 메모리로 흘려보내는가"
date: 2026-11-21 05:21:00 +0900
categories: [Batch & Query, Trino·Athena]
tags: [trino, athena, query-engine, dynamic-filtering, fault-tolerant-execution, cost-based-optimizer, concept]
---

Athena에서 큰 윈도 함수나 조인이 `Query exhausted resources at this scale factor`로 실패하고, 같은 쿼리가 조인 순서만 바꾸면 통과하는 일이 있다. 둘 다 Trino의 실행 모델에서 나온다. Trino는 쿼리를 stage 트리로 쪼개 워커에 나눠 주고, stage 사이 데이터를 중간 파일 없이 메모리 버퍼로 흘려보낸다. 그래서 빠르고, 같은 이유로 메모리가 모자라거나 노드 하나가 죽으면 쿼리 전체가 실패한다.

Athena가 무엇을 읽느냐로 요금이 정해지는 이야기는 [Athena 스캔 비용 글](/posts/athena-scan-and-cost/)에, 파일 통계로 건너뛰는 단계는 [컬럼 파일과 프루닝 글](/posts/columnar-files-and-pruning/)에 적었다. 여기서는 읽은 데이터가 엔진 안에서 어떻게 움직이는지를 다룬다.

버전은 Trino 484(2026-10-15 릴리스)와 Athena 엔진 버전 3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| coordinator | SQL을 파싱하고 계획을 세우고 워커에 일을 나눠 주는 서버. 클라이언트는 여기에 붙는다 |
| worker | task를 실행하고 커넥터로 데이터를 읽고 다른 워커와 중간 데이터를 주고받는 서버 |
| connector, catalog | 데이터 소스용 어댑터와, 그 커넥터에 설정을 붙여 이름을 준 것 |
| stage | 분산 계획의 한 층. 코디네이터 쪽 모델이고 트리를 이룬다 |
| task, split | stage를 워커에서 실행하는 단위와, task가 처리하는 데이터 조각 |
| driver, operator | task 안의 가장 작은 병렬 단위와, 그 안에 이어진 scan, filter 같은 연산 |
| exchange | stage 사이에서 노드 간 데이터를 옮기는 장치 |
| retry-policy | `NONE`, `QUERY`, `TASK`. `TASK`는 중간 데이터를 exchange manager 저장소에 쓴다 |
| pushdown | 필터, 컬럼 선택, LIMIT, 집계를 커넥터나 원본 시스템에 넘기는 것 |
| dynamic filtering | 조인 build 쪽 키 값으로 probe 쪽 스캔을 실행 중에 줄이는 것 |
| CBO | 테이블 통계로 조인 순서와 분산 방식을 고르는 비용 기반 최적화 |

## 코디네이터와 워커, 커넥터와 카탈로그

클러스터에는 코디네이터 하나와 워커 여럿이 있다. 코디네이터는 계획과 스케줄링을 맡고, 데이터를 읽고 계산하는 것은 워커다. 데이터 소스는 커넥터로 붙는다. 카탈로그는 커넥터 하나에 접속 설정을 붙인 것이고, 이름은 설정 파일 이름에서 온다.

`etc/catalog/lake.properties`에 `connector.name=iceberg`와 `iceberg.catalog.type=glue`를 두면 카탈로그 이름은 `lake`가 되고, `lake.analytics.events`처럼 카탈로그, 스키마, 테이블 순으로 부른다. 같은 쿼리 안에서 `lake`의 Iceberg 테이블과 PostgreSQL 카탈로그의 테이블을 조인할 수 있다. Trino에는 자체 저장소가 없고, 데이터는 모두 커넥터 너머에 있다.

## 쿼리는 stage, task, split, driver로 내려간다

`EXPLAIN (TYPE DISTRIBUTED)`의 fragment 하나가 stage 하나다. Trino 문서의 예시를 줄이면 이렇다.

```text
Fragment 0 [SINGLE]
    └─ RemoteSource[1]
Fragment 1 [HASH]
    Aggregate(FINAL)[regionkey]
    └─ LocalExchange[HASH][$hashvalue] ("regionkey")
       └─ RemoteSource[2]
Fragment 2 [SOURCE]
    Output partitioning: HASH [regionkey][$hashvalue_10]
    └─ Aggregate(PARTIAL)[regionkey]
       └─ TableScan[tpch:nation:sf0.01, grouped = false]
```

맨 아래 SOURCE stage는 커넥터에서 데이터를 읽는다. 코디네이터가 커넥터에 테이블의 split 목록을 묻고, 어느 task가 어느 split을 처리하는지 추적한다. 이 stage가 부분 집계를 한 뒤 `regionkey` 해시로 나눠 위 stage로 보내고, HASH stage가 최종 집계를 한다. `RemoteSource[2]`가 다른 stage에서 데이터를 받는 자리다. stage는 워커에서 직접 돌지 않는다. 워커에서 도는 것은 stage를 구현한 task이고, task 하나는 driver 여러 개를 병렬로 돌린다. driver는 operator 인스턴스를 이어 붙인 것으로 입력 하나, 출력 하나를 가진 가장 작은 병렬 단위다.

## stage 사이 데이터는 파일 없이 흐른다

task는 결과를 output buffer에 넣고, 다음 stage의 task가 exchange client로 그 버퍼에서 가져간다. 기본 `query.execution-policy=phased`는 stage 사이 의존 때문에 막히지 않도록 순서를 맞춰 스케줄하고, 실행 중인 stage끼리는 데이터를 메모리로 주고받는다.

Spark의 셔플은 map task가 결과를 메모리에 두다가 넘치면 대상 파티션 순으로 정렬해 로컬 디스크 파일로 쓰고, 다음 stage의 reduce task가 그 블록을 읽는다. Exchange마다 셔플이 생긴다는 점은 [Spark 셔플 글](/posts/spark-shuffle-and-skew/)에 있다.

| | Trino 기본 (`retry-policy=NONE`) | Trino `retry-policy=TASK` | Spark |
|---|---|---|---|
| stage 사이 데이터 | 메모리 버퍼에서 바로 가져간다 | exchange manager 저장소에 쓴다 | 로컬 디스크의 셔플 파일 |
| 노드 하나 실패 | 쿼리 실패 | 그 task만 다시 실행 | 잃은 셔플 출력을 lineage로 다시 계산 |

중간 결과를 남기지 않으니 stage 사이의 디스크 쓰기와 읽기가 빠진다. 대신 중간 상태가 어디에도 없어서, 노드 하나가 자원 부족이나 장애로 빠지면 쿼리를 처음부터 다시 돌려야 한다.

## 메모리 한도를 넘으면 쿼리가 죽는다

`query.max-memory`(기본 20GB)는 쿼리 하나가 클러스터 전체에서 쓰는 user memory, `query.max-memory-per-node`(기본 힙 최대치의 30%)는 노드 하나에서 쓰는 user memory의 상한이다. revocable memory까지 포함한 `query.max-total-memory`는 기본이 `query.max-memory`의 두 배이고, `memory.heap-headroom-per-node`(기본 30%)는 Trino가 추적하지 않는 할당용으로 남겨 둔다. 넘으면 `EXCEEDED_GLOBAL_MEMORY_LIMIT`와 `Query exceeded distributed user memory limit of 20GB` 같은 메시지로 쿼리가 끝난다. 조인의 build 쪽 해시 테이블, `GROUP BY` 키가 많은 집계, 파티션이 큰 윈도 함수가 이 메모리를 쓴다.

디스크로 내리는 spill도 있다. 조인, 집계, `ORDER BY`, 윈도 함수가 지원하지만 `spill-enabled` 기본값은 `false`이고, Trino 484에서 spill은 레거시 기능이라 `TASK` retry policy와 exchange manager가 대신 권장된다. Athena에서도 노드가 디스크로 내릴 수는 있지만 느려지고, 실패를 막기에 부족할 수 있다. [연말 결산 글](/posts/year-end-recap-spark/)의 사용자별 `RANK()` 같은 쿼리를 Athena로 옮긴다면, 윈도가 다루는 행을 메모리에 들고 있어야 한다는 점이 먼저 걸린다.

## fault-tolerant execution은 중간 데이터를 밖에 쓴다

`retry-policy` 기본값은 `NONE`이다. `QUERY`는 쿼리 전체를 다시 돌리고, 작은 쿼리가 많은 클러스터에 맞는다. exchange manager 없이 `QUERY`를 쓰면 결과가 `exchange.deduplication-buffer-size`(기본 32MB)를 넘는 쿼리는 보호되지 않는다. `TASK`는 실패한 task만 다시 돌리고 exchange manager가 반드시 필요하다.

```properties
# etc/config.properties (Trino 484)
retry-policy=TASK
task.low-memory-killer.policy=total-reservation-on-blocked-nodes
# etc/exchange-manager.properties, 코디네이터와 모든 워커에 둔다
exchange-manager.name=filesystem
exchange.base-directories=s3://exchange-spooling-bucket
exchange.s3.region=ap-northeast-2
```

재시도 횟수는 `query-retry-attempts`, `task-retry-attempts-per-task` 모두 기본 4회다. `TASK`에서는 위의 노드별, 클러스터 메모리 한도가 적용되지 않고, 노드의 가용 메모리를 넘겨 실패한 task는 노드 전체 할당을 요청하며 다시 시작된다. 짧은 쿼리가 많으면 지연이 늘어서 큰 배치 쿼리용 `TASK` 클러스터를 따로 두는 편이 권장된다. Hive, Iceberg, Delta Lake와 MySQL, PostgreSQL 같은 일부 JDBC 커넥터는 지원하고, 지원하지 않는 커넥터는 `does not support query retries`로 실패한다.

## 커넥터가 받아 가는 일, pushdown

필터는 컬럼별 값 범위(`TupleDomain`)로 커넥터에 넘어가고, 함수가 씌워진 조건은 대개 Trino가 읽은 뒤 직접 평가한다. [Superset epoch_us 글](/posts/superset-epoch-us/)에서 원본 BIGINT 컬럼에 필터를 남긴 이유가 이것이다. projection pushdown은 쿼리에 나온 컬럼만 `TableScan`에 남기고, dereference pushdown은 `ROW` 안의 필드 하나만 읽는다.

LIMIT, 집계, 조인, top N pushdown은 커넥터마다 다르다. Trino 484 소스에서 Hive 커넥터 메타데이터는 필터와 projection만 받는다. Iceberg 커넥터는 여기에 LIMIT을 더 받는데, 파티션으로 처리하지 못한 필터가 남아 있으면 받지 않는다. 집계, 조인, LIMIT, top N을 모두 구현한 것은 PostgreSQL, MySQL 같은 JDBC 계열의 공통 메타데이터이고, 함수별 지원은 커넥터마다 다르다. S3 위 Hive, Iceberg 테이블의 `count(*)`나 `GROUP BY`는 워커가 계산한다. PostgreSQL 카탈로그에서는 같은 쿼리가 원본 DB로 넘어갈 수 있고, 성공하면 계획에 `Aggregate` 노드가 없고 `TableScan` 안에 함수가 들어간다. top N이 넘어가면 `TableScan`에 `sortOrder=`와 `limit=`가 붙는다.

## dynamic filtering은 조인 중에 스캔을 줄인다

해시 조인은 오른쪽(build) 테이블로 해시 테이블을 만들고 왼쪽(probe) 테이블을 흘려보낸다. dynamic filtering은 build 쪽에서 모은 조인 키 값을 probe 쪽 스캔의 조건으로 넣는다. 코디네이터도 이 값을 받아 split을 나열할 때 걸러서, Hive 커넥터에서는 조인으로 버려질 파티션을 아예 읽지 않는다(dynamic partition pruning).

```sql
-- Trino 484. events는 dt로 파티셔닝, dim_date는 작은 테이블
SELECT d.dt, count(*)
FROM lake.analytics.events e
JOIN lake.analytics.dim_date d ON e.dt = d.dt
WHERE d.is_holiday
GROUP BY d.dt;
```

WHERE에는 `events` 조건이 없지만, `dim_date`에서 휴일인 `dt` 값이 모이면 `events` 스캔이 그 파티션만 연다. 기본으로 켜져 있고(`enable-dynamic-filtering`), inner와 right 조인의 `=`, `<`, `<=`, `>`, `>=`, `IS NOT DISTINCT FROM` 조건과 `IN` semi-join에서 동작한다. build 쪽 고유값이 한도를 넘으면 min-max 범위로 바뀐다. Iceberg 커넥터는 split을 만들기 전에 필터를 최대 `iceberg.dynamic-filtering.wait-timeout`(기본 1s) 기다린다. `EXPLAIN`에서는 조인 노드의 `dynamicFilterAssignments`와 probe 쪽 `ScanFilterProject`의 `dynamicFilters`로 확인한다.

## 조인 순서는 통계가 있을 때만 바뀐다

`join_reordering_strategy`와 `join_distribution_type`의 기본값은 둘 다 `AUTOMATIC`이다. 통계로 비용을 계산해 조인 순서와 broadcast, partitioned 분산을 고르고, 한 번에 재배치하는 조인은 `optimizer.max-reordered-joins` 기본 8개, broadcast 크기 상한은 `join-max-broadcast-table-size` 기본 100MB다. 통계가 없거나 비용을 계산하지 못하면 `ELIMINATE_CROSS_JOINS`로 물러나 cross join만 없애고 쓴 순서를 대체로 따르고, 분산은 partitioned가 된다. `EXPLAIN`의 `Estimates: {rows: ? ...}`처럼 물음표가 보이면 추정이 없다는 뜻이다. Spark도 broadcast 여부를 추정 크기로 정한다는 점은 [Spark 조인 전략 글](/posts/spark-join-strategies/)과 같다. 통계는 커넥터가 준다. Hive 커넥터는 쓸 때 행 수, 파일 수 같은 기본 통계를 항상 모으고, `hive.collect-column-statistics-on-write`(기본 `true`)로 컬럼별 null 수와 고유값 수도 모은다. Iceberg 커넥터는 쓸 때(`iceberg.extended-statistics.collect-on-write`, 기본 `true`)와 `ANALYZE`로 컬럼 통계를 모으고, 고유값 수는 theta sketch로 Puffin 통계 파일에 남긴다. 조인 키와 필터, `GROUP BY` 컬럼만 골라 `ANALYZE t WITH (columns = ARRAY['user_id'])`로 돌릴 수 있다.

## Athena 엔진 버전 3과 Trino

Athena 엔진 버전 3은 Trino와 Presto 프로젝트를 지속적으로 통합하는 방식으로 만든다. Athena 엔진 버전 3 페이지에는 대응하는 Trino 릴리스 번호가 없다. SQL 연산자와 함수의 전체 목록은 Trino 문서로 연결되고, Athena가 설명하는 build, probe 해시 조인도 위와 같은 모델이다.

| | Trino 484 직접 운영 | Athena 엔진 버전 3 |
|---|---|---|
| 데이터 소스 | Trino 커넥터 | Trino 커넥터 미지원, Athena Federated Query |
| 실패 처리 | `retry-policy` 선택 | fault-tolerant execution 미지원 |
| 메모리 | `query.max-memory` 등을 설정 | 사용자 설정 없음, 넘치면 `Query exhausted resources at this scale factor` |
| CBO 통계 | 커넥터 통계, `ANALYZE` | Glue Data Catalog 컬럼 통계 (Hive, Iceberg 테이블만) |
| dynamic filtering | 기본 켜짐 | 사용, 버전 3에서 equality right join까지 |

Athena는 테이블 데이터를 직접 관리하지 않아 통계가 없으면 첫 테이블을 크다고, 두 번째를 작다고 가정할 때가 많다. 그래서 통계를 만들지 않았다면 큰 테이블을 `JOIN` 왼쪽(probe)에, 작은 테이블을 오른쪽(build)에 쓴다. Glue에서 통계를 만들면 Iceberg 테이블에는 `use_iceberg_statistics=true` 속성이 붙고, Hive 테이블은 만든 통계를 항상 쓴다. 결과가 클라이언트로 넘어오는 경로에서 생긴 문제는 [PyAthena 복합 타입 글](/posts/pyathena-complex-types/)에 있다.

## 참고 자료

- Trino 484: [Release notes](https://trino.io/docs/current/release.html), [Concepts](https://trino.io/docs/current/overview/concepts.html), [EXPLAIN](https://trino.io/docs/current/sql/explain.html), [Query management properties](https://trino.io/docs/current/admin/properties-query-management.html), [Resource management properties](https://trino.io/docs/current/admin/properties-resource-management.html)
- Trino 484: [Fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html), [Spill to disk](https://trino.io/docs/current/admin/spill.html), [Spilling properties](https://trino.io/docs/current/admin/properties-spilling.html)
- Trino 484: [Pushdown](https://trino.io/docs/current/optimizer/pushdown.html), [Dynamic filtering](https://trino.io/docs/current/admin/dynamic-filtering.html), [Cost-based optimizations](https://trino.io/docs/current/optimizer/cost-based-optimizations.html), [Optimizer properties](https://trino.io/docs/current/admin/properties-optimizer.html), [Hive connector](https://trino.io/docs/current/connector/hive.html), [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- Trino 소스: [IcebergMetadata.java](https://github.com/trinodb/trino/blob/master/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java), [HiveMetadata.java](https://github.com/trinodb/trino/blob/master/plugin/trino-hive/src/main/java/io/trino/plugin/hive/HiveMetadata.java), [DefaultJdbcMetadata.java](https://github.com/trinodb/trino/blob/master/plugin/trino-base-jdbc/src/main/java/io/trino/plugin/jdbc/DefaultJdbcMetadata.java), [ExceededMemoryLimitException.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/ExceededMemoryLimitException.java)
- [Spark 4.3 RDD Programming Guide: Shuffle operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)
- Athena: [Athena engine version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), [Optimize queries](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-query-optimization-techniques.html), [Cost-based optimizer](https://docs.aws.amazon.com/athena/latest/ug/cost-based-optimizer.html)

## English summary

Trino splits a query into a tree of stages, runs each stage as tasks on workers, and each task processes splits with parallel drivers made of operators. By default, stages pass data through in-memory output buffers instead of writing shuffle files like Spark, so queries return fast but a node failure or a memory limit such as `query.max-memory` ends the whole query. Fault-tolerant execution with `retry-policy=TASK` and an exchange manager spools that data to S3 so single tasks can be retried, while spill to disk is now legacy. Pushdown, dynamic filtering, and statistics-based join reordering decide how much data reaches the workers, and Athena engine version 3 runs the same model without fault-tolerant execution or user memory settings. My Superset `epoch_us` change kept the filter on the raw column for this reason: it gave up a more readable expression for a range the connector can push down.
