---
title: "실시간 OLAP 엔진은 무엇을 미리 해 두고 빨라지나"
date: 2026-11-21 05:26:00 +0900
categories: [BI & Serving, OLAP]
tags: [clickhouse, druid, pinot, trino, athena, olap, concept]
description: "필터를 바꿀 때마다 1초 안에 답해야 하는 대시보드를 위해, ClickHouse, Druid, Pinot이 적재 시점에 무엇을 미리 해 두는지 Trino, Athena와 견주어 살핀다."
---

대시보드 사용자가 필터를 바꿀 때마다 1초 안에 답해야 하고, 몇 분 전 이벤트까지 숫자에 들어가야 한다고 하자. 레이크하우스 위의 Trino나 Athena는 쿼리마다 테이블 메타데이터를 읽고 S3 객체를 열어 집계하므로, 그 비용을 요청마다 다시 치른다. ClickHouse, Druid, Pinot은 데이터를 자기 저장 형식으로 적재하면서 정렬, 인덱스, 사전 집계를 미리 해 두고 그 대가로 빠르게 답한다.

[지표 대시보드 API 글](/posts/metrics-dashboard-api/)의 대시보드는 다른 길을 택했다. 배치가 지표별 집계를 끝내 MySQL 테이블에 넣고, API는 날짜 범위와 서비스, 플랫폼 구분으로 자르기만 했다. 질문이 미리 정해져 있고 배치 주기만큼의 신선도로 충분할 때 맞는 구조다. 신선도가 분 단위로 내려가거나 자르는 축이 자유로워지면 그때 아래 엔진들이 후보가 된다.

버전은 ClickHouse 26.10, Apache Druid 39.0, Apache Pinot 1.6, Trino 484, Athena 엔진 버전 3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| part | ClickHouse MergeTree에서 INSERT가 파티션마다 만드는 정렬된 데이터 묶음. 백그라운드에서 병합된다 |
| 정렬 키, 기본 키 | part 안의 행 순서를 정하는 `ORDER BY`. `PRIMARY KEY`를 안 주면 정렬 키가 기본 키 |
| granule, mark | 기본 8192행 묶음. 기본 키 인덱스는 행이 아니라 granule마다 mark 하나를 둔다 |
| ReplacingMergeTree | 같은 파티션에서 정렬 키가 같은 행을 병합할 때 하나만 남긴다. 병합 전에는 중복이 보인다 |
| AggregatingMergeTree | 같은 키의 집계 상태를 병합 때 합친다. `-State`로 넣고 `-Merge`로 읽는다 |
| 증분 materialized view | ClickHouse에서 INSERT된 블록에 쿼리를 돌려 대상 테이블에 넣는 트리거 |
| Druid segment | 시간 조각(`segmentGranularity`)마다 만든 컬럼 파일. 새 버전이 같은 구간을 대체한다 |
| rollup | 적재 때 차원 값과 잘린 시각(`queryGranularity`)이 같은 행을 한 행으로 합친다 |
| deep storage | S3, HDFS에 둔 segment 원본. 서빙 노드가 여기서 내려받는다 |
| hybrid table | Pinot에서 같은 이름의 실시간 표와 오프라인 표를 브로커가 시간 경계로 이어 읽는 구성 |
| upsert | Pinot에서 기본 키마다 최신 행만 보이게 하는 기능. 키와 행 위치의 맵을 힙에 둔다 |
| star-tree index | Pinot에서 차원 분할 순서대로 미리 집계해 둔 트리. 리프 하나에 최대 T행 |

## 적재할 때 무엇을 미리 하나

| | ClickHouse | Druid | Pinot | Trino·Athena |
|---|---|---|---|---|
| 저장 단위 | 파티션 안의 part | 시간 조각별 segment | segment, 실시간 표는 consuming segment | 레이크의 Parquet·ORC 파일 |
| 적재 | INSERT. 배치나 async insert 권장 | supervisor가 관리하는 Kafka·Kinesis 태스크, 배치 태스크 | 스트림 소비, 오프라인은 Spark 등으로 만든 segment 업로드 | 없음. 다른 잡이 쓴 파일을 읽는다 |
| 미리 하는 일 | 정렬, 희소 인덱스, 병합 때 중복 제거와 집계 | rollup, 사전 인코딩, 값마다 비트맵 인덱스 | 컬럼 인덱스, star-tree, upsert 메타데이터 | 없음. 파티션과 파일 통계로 건너뛰기만 |
| 행 갱신 | ReplacingMergeTree, 경량 UPDATE, 무거운 mutation | 시간 구간 덮어쓰기만 | upsert, 부분 upsert는 실시간 표만 | 테이블 포맷이 맡는다 |

앞의 세 엔진은 늘 떠 있는 클러스터가 데이터를 자기 형식으로 들고 있다. Trino·Athena는 저장소가 없고, 신선도는 레이크에 쓰는 잡의 커밋 주기가 정한다.

## ClickHouse는 정렬과 병합에 기댄다

MergeTree 테이블에 INSERT하면 그 즉시 파티션마다 정렬된 part가 생기고, 백그라운드 병합이 part들을 합친다. 기본 키 인덱스는 granule마다 mark 하나만 두는 희소 인덱스라서, WHERE 조건이 정렬 키 앞쪽 컬럼에 걸리면 읽을 granule 범위를 좁힌다. granule은 읽기의 최소 단위다.

작은 INSERT를 자주 보내면 part가 쌓여 쿼리가 느려지고 "too many parts" 에러가 난다. 한 번에 최소 1,000행, 가능하면 1만에서 10만 행씩 넣거나, 서버가 모아서 쓰는 `async_insert=1, wait_for_async_insert=1`을 쓴다. 파티션은 관리 단위이고 쿼리 최적화 수단이 아니다. 서로 다른 파티션의 part는 병합되지 않으므로 고객 ID 같은 값으로 파티션을 나누면 안 된다. 갱신과 사전 집계도 병합에 얹혀 있고, 병합이 언제 끝날지는 정해지지 않으므로 읽는 쪽이 그 사실을 감수해야 한다.

```sql
-- ClickHouse 26.10. 이름은 예로 지었다
CREATE TABLE play_events (
    event_time DateTime,
    service    LowCardinality(String),
    user_id    UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (service, event_time);

CREATE TABLE play_daily (
    day     Date,
    service LowCardinality(String),
    plays   SimpleAggregateFunction(sum, UInt64),
    users   AggregateFunction(uniq, UInt64)
) ENGINE = AggregatingMergeTree
ORDER BY (service, day);

-- INSERT된 블록만 집계해 play_daily로 보낸다
CREATE MATERIALIZED VIEW play_daily_mv TO play_daily AS
SELECT toDate(event_time) AS day, service,
       count() AS plays, uniqState(user_id) AS users
FROM play_events
GROUP BY day, service;

-- 병합 전 행이 남아 있을 수 있으니 읽을 때 다시 GROUP BY
SELECT day, service, sum(plays) AS plays, uniqMerge(users) AS users
FROM play_daily
GROUP BY day, service;

-- 키마다 updated_at이 가장 큰 행만 남긴다. 병합 전에는 FINAL로 읽는다
CREATE TABLE user_tier (user_id UInt64, tier String, updated_at DateTime)
ENGINE = ReplacingMergeTree(updated_at) ORDER BY user_id;
SELECT * FROM user_tier FINAL WHERE user_id = 42;
```

materialized view는 INSERT 트리거라서 원본 테이블의 UPDATE, DELETE, 파티션 삭제는 대상 테이블에 반영되지 않는다. view를 만들기 전 데이터도 들어가지 않으므로 따로 INSERT ... SELECT로 채운다. `ALTER TABLE ... UPDATE`는 mutation이고 기본으로 비동기이며 자주 쓰라고 만든 기능이 아니다. 원본 행은 `play_events`에 그대로 남으니 집계 테이블이 틀어지면 다시 만들 근거가 있다.

## Druid는 시간 조각과 rollup으로 줄인다

Druid는 데이터를 시간으로 나눠 구간마다 segment를 만든다. segment는 300~700MB, 대략 500만 행 안팎을 권장한다. 문자열 차원은 사전 인코딩하고 값마다 비트맵 인덱스를 두어 필터를 빠르게 한다.

rollup은 기본으로 켜져 있다. `queryGranularity`가 분 단위면 같은 분, 같은 차원 값을 가진 이벤트가 한 행으로 합쳐지고 지표는 집계된다. 행 수가 크게 줄어드는 대신 개별 이벤트는 다시 조회할 수 없다. 스트리밍 적재는 best-effort rollup이라 같은 키가 여러 segment에 나뉘어 남을 수 있고, 나중에 compaction이 합친다.

역할은 프로세스별로 나뉜다. Overlord가 적재 태스크를 배정하고, Middle Manager가 띄운 Peon(또는 Indexer)이 Kafka를 읽는다. 스트림 태스크가 받은 행은 아직 커밋되지 않은 segment에 들어가는 순간부터 조회된다. segment가 완성되면 deep storage에 올리고 메타데이터 저장소에 기록하며, Coordinator가 Historical에 로드를 맡긴 뒤 태스크는 핸드오프를 마치고 내려간다. Broker는 실시간 태스크와 Historical에 쿼리를 나눠 보내고 결과를 합친다.

Druid에는 UPDATE 문이 없고 기본 키 단위 갱신도 없다. 바꾸려면 시간 구간을 통째로 다시 쓰고(`REPLACE`, 재색인), 자주 바뀌는 이름표 같은 값은 lookup으로 쿼리 때 붙인다. 기본 키로 기존 레코드를 낮은 지연으로 고치는 일과 큰 팩트 테이블끼리의 조인은 Druid가 약한 영역이다.

## Pinot은 실시간 표와 오프라인 표를 브로커가 잇는다

Pinot의 실시간 표는 스트림을 소비해 메모리에서 segment를 만들고, 행 수나 시간 기준에 닿으면 디스크로 내리고 deep store에 올린다. 오프라인 표는 Spark 같은 외부 실행기로 만든 segment를 올려 받는다. 같은 이름으로 둘을 함께 두면 hybrid table이 되고, 브로커는 오프라인 데이터가 있는 구간은 오프라인 표를, 나머지 최근 구간은 실시간 표를 읽는다. 스트림으로 빠르게 채우고 배치로 정정본을 덮는 구성이다.

upsert는 Druid와 가장 크게 갈리는 지점이다. 스키마에 `primaryKeyColumns`를 두면 키마다 최신 행만 보이고, 최신은 기본으로 시간 컬럼, 필요하면 `comparisonColumns`로 정한다. 조건이 있다. 입력 스트림이 기본 키로 파티셔닝되어 있어야 하고, 라우팅은 `strictReplicaGroup`이어야 한다. 키에서 행 위치로 가는 맵을 힙 메모리에 두므로 키 종류가 많으면 `metadataTTL`로 오래된 키를 맵에서 뺀다. 부분 upsert는 실시간 표에서만 된다.

star-tree index는 `dimensionsSplitOrder` 순서로 차원을 나누며 미리 집계한 트리다. 어떤 차원을 무시한 집계는 star 노드에 들어 있고, 리프 하나가 `maxLeafRecords`(T)행을 넘지 않으므로 쿼리가 리프에서 훑는 행 수에 상한이 생긴다. 필터 컬럼, GROUP BY 컬럼, 집계 함수가 모두 설정과 맞는 쿼리만 이 인덱스를 탄다. T를 줄일수록 빨라지고 저장 공간은 늘어난다.

Superset 같은 BI 도구는 Druid, Pinot에도 엔진별 SQL을 만들어 보낸다. [Superset epoch_us 글](/posts/superset-epoch-us/)에서 마이크로초 포맷을 넣을 때 Pinot은 `DATETIMECONVERT`를 쓰도록 했다.

## 레이크하우스 위 Trino·Athena는 적재가 없다

Trino는 데이터베이스를 대체하지 않는 분산 쿼리 엔진이고, 커넥터로 원천을 그 자리에서 읽는다. 적재 파이프라인과 두 번째 사본이 없으니 레이크에 커밋된 순간 바로 조회되고, 저장 비용은 S3 한 벌이다. 레이크 쪽 전체 모양은 [데이터 플랫폼 구조 글](/posts/data-platform-architecture-overview/)에 있다.

대가는 요청마다 하는 일이다. 정렬된 part나 사전 집계가 없으니 파티션과 파일 통계로 건너뛴 뒤 남은 파일을 연다. Athena는 서버를 띄우지 않는 대신 스캔한 바이트로 내고, 그 구조는 [Athena 쿼리 비용 글](/posts/athena-scan-and-cost/)에 정리했다. 동시성은 계정과 리전 단위의 Active DML queries 할당량에 묶이며, 실행 중인 쿼리와 대기 중인 쿼리를 함께 세어 넘으면 `TooManyRequestsException`이 난다. 동시성을 직접 쥐려면 capacity reservation을 산다.

같은 쿼리를 많은 사용자가 반복한다면 엔진을 바꾸기 전에 결과를 캐시하는 방법도 있다. 배치가 원천을 다시 쓸 때 그 캐시를 언제 버리는지는 [읽기 API 캐시 글](/posts/read-api-caching/)에 있다.

## 고르는 기준

| 조건 | 기우는 쪽 |
|---|---|
| 질문이 미리 정해지고 배치 주기의 신선도로 충분 | 배치가 미리 집계한 테이블과 읽기 API |
| 키 하나로 미리 계산한 답을 꺼낸다 | 키-값 저장소 |
| 분석가의 ad-hoc 쿼리, 초 단위 대기 허용, 동시 사용자 적음 | Trino·Athena over lakehouse |
| 사용자에게 열린 화면, 1초 이하, 많은 동시 요청, 시간 축 집계 | Druid, Pinot |
| 기본 키별 최신 상태를 실시간으로 | Pinot upsert, ClickHouse ReplacingMergeTree와 FINAL |
| 원본 행 보존, 자유로운 SQL, 큰 스캔을 코어 전부로 병렬 처리 | ClickHouse |

키 하나로 끝나는 경우는 [HBase 행 키 글](/posts/hbase-serving-row-keys/)의 조건 그대로라 OLAP 엔진이 필요 없다. 운영 비용은 반대 방향으로 갈린다. Druid는 Coordinator, Overlord, Broker, Router, Historical, Middle Manager에 메타데이터 저장소, ZooKeeper, deep storage까지 붙고, Pinot도 Controller, Broker, Server, Minion에 Helix가 쓰는 ZooKeeper와 deep store가 붙는다. 이 노드들은 쿼리가 없을 때도 비용이 나가고, segment 크기, 병합, 핸드오프를 누군가 지켜봐야 한다.

rollup을 켠 Druid 데이터소스에서는 개별 이벤트를 다시 꺼낼 수 없다. 집계 기준을 나중에 바꿔 다시 계산해야 한다면 원본은 엔진 밖 레이크에 남아 있어야 한다.

## 참고 자료

- ClickHouse: [MergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree), [ReplacingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree), [AggregatingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree), [CREATE VIEW](https://clickhouse.com/docs/sql-reference/statements/create/view), [Incremental materialized view](https://clickhouse.com/docs/materialized-view/incremental-materialized-view), [Insert strategy](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy), [ALTER UPDATE](https://clickhouse.com/docs/sql-reference/statements/alter/update), [Query parallelism](https://clickhouse.com/docs/concepts/core-concepts/query-parallelism), [Releases](https://github.com/ClickHouse/ClickHouse/releases)
- Druid: [Introduction](https://druid.apache.org/docs/latest/design/), [Architecture](https://druid.apache.org/docs/latest/design/architecture), [Storage](https://druid.apache.org/docs/latest/design/storage), [Segments](https://druid.apache.org/docs/latest/design/segments), [Rollup](https://druid.apache.org/docs/latest/ingestion/rollup), [Streaming ingestion](https://druid.apache.org/docs/latest/ingestion/streaming), [Data updates](https://druid.apache.org/docs/latest/data-management/update)
- Pinot: [Introduction](https://docs.pinot.apache.org/), [Architecture](https://docs.pinot.apache.org/basics/architecture), [Table](https://docs.pinot.apache.org/basics/components/table), [Upsert](https://docs.pinot.apache.org/manage-data/data-import/upsert-and-dedup/upsert), [Star-tree index](https://docs.pinot.apache.org/basics/indexing/star-tree-index)
- Trino: [Use cases](https://trino.io/docs/current/overview/use-cases.html), [Concepts](https://trino.io/docs/current/overview/concepts.html)
- Athena: [Service quotas](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html)

## English summary

Real-time OLAP engines answer fast because they do work at ingestion time. ClickHouse sorts data into parts with a sparse index and folds duplicates or aggregate states during background merges, Druid splits data into time-based segments and can roll rows up, and Pinot joins a streaming table and a batch table behind one name and supports primary-key upserts and star-tree pre-aggregation. Trino or Athena on a lakehouse skip ingestion entirely, so they are cheaper and always as fresh as the last lake commit, but they pay the file-scan cost on every query. My metrics dashboard took a fourth path: batches pre-aggregated each metric into MySQL, which fits fixed questions but trades away freshness and free-form slicing.
