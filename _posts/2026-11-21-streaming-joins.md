---
title: "끝나지 않는 스트림끼리 조인하면 상태가 남는다"
date: 2026-11-21 06:06:00 +0900
categories: [Streaming, Flink]
tags: [flink, flink-sql, spark-streaming, structured-streaming, kafka-streams, join, watermark, concept]
---

광고 노출 스트림과 클릭 스트림을 광고 ID로 조인한다고 하자. 배치라면 두 테이블을 끝까지 읽고 한 번 맞추면 끝난다. 스트림에는 끝이 없어서, 지금 들어온 클릭이 한 시간 전 노출과 맞을 수도 있고 내일 올 노출과 맞을 수도 있다. 엔진은 조건이 막아 주지 않는 한 양쪽에서 본 모든 행을 들고 있어야 한다. 스트림 조인의 종류는 결국 이 상태를 언제 지울 수 있는지, 그리고 이미 낸 결과를 고쳐야 하는지로 갈린다. 배치 쪽에서 Spark가 물리 조인을 고르는 방식은 [Spark 조인 전략 글](/posts/spark-join-strategies/)에 있다.

버전은 Flink 2.3, Spark 4.3, Kafka 4.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| regular join | 시간 조건 없는 조인. 양쪽 입력을 기본으로 영원히 상태에 둔다 |
| state TTL | 일정 시간 갱신되지 않은 상태를 지우는 설정. 크기를 묶는 대신 결과가 틀릴 수 있다 |
| interval join | 등호 조건과 양쪽 시간 범위 조건이 있는 조인. 워터마크로 상태를 지운다 |
| window join | 같은 윈도 TVF의 결과끼리 윈도 경계로 맞추는 조인. 윈도가 끝날 때 한 번 낸다 |
| temporal join | 왼쪽 행의 시각에 유효했던 오른쪽 버전과 맞추는 조인(`FOR SYSTEM_TIME AS OF`) |
| versioned table | 기본 키와 이벤트 시간이 있어 시점별 버전을 꺼낼 수 있는 테이블 |
| lookup join | 행마다 외부 저장소를 조회하는 조인. 처리 시간 기준이고 상태가 없다 |
| retraction | 앞서 낸 조인 결과를 `-D`나 `-U`로 되돌리는 메시지 |
| stream-static join | 스트림과 정적 테이블의 조인. 상태를 두지 않는다 |
| KStream-KTable join | 스트림 쪽 레코드만 조인을 일으키고 테이블 갱신은 상태만 바꾸는 Kafka Streams 조인 |

## 시간 조건이 없으면 양쪽을 끝까지 기억한다

Flink SQL의 regular join은 등호 조건만 있는 평범한 `JOIN`이다. 어떤 과거 행이든 미래 행과 맞을 수 있으므로 양쪽 입력을 상태에 계속 둔다. `table.exec.state.ttl` 기본값은 0이라 지우지 않는다. 키 수가 계속 늘면 상태도 계속 늘고, TTL을 주거나 `SELECT /*+ STATE_TTL('o'='3d', 'l'='1d') */ ...` 힌트로 양쪽에 따로 줄 수 있다.

TTL이 지운 키에 나중에 맞는 행이 오면 조인 상대가 없는 것으로 처리된다. TTL이 결과를 바꾸는 방식과 처리 시간으로 재는 점은 [Flink changelog와 RowKind 글](/posts/flink-changelog-and-rowkind/)과 [Flink 상태와 체크포인트 글](/posts/flink-state-and-checkpoints/)에 있다.

출력 모양은 조인 종류로 갈린다. 2.3 planner의 changelog 추론에서 inner와 semi join은 입력의 변경 종류를 그대로 넘긴다. 양쪽이 append-only면 결과도 append-only다. outer와 anti join은 모든 변경 종류를 낼 수 있다. 런타임 연산자는 LEFT JOIN에서 짝이 없는 왼쪽 행에 `+I[왼쪽, null]`을 먼저 내고, 나중에 오른쪽 짝이 오면 `-D[왼쪽, null]`과 `+I[왼쪽, 오른쪽]`을 낸다. 그래서 LEFT JOIN 결과를 append만 받는 sink에 쓰면 planner가 거절한다.

## Flink SQL 조인 다섯 가지

| 조인 | 조건 | 상태 | 지우는 때 | 출력 | 지원하는 종류 |
|---|---|---|---|---|---|
| regular | 등호 | 양쪽 전체 | TTL을 줄 때만 | inner, semi는 입력을 그대로, outer, anti는 retraction 포함 | inner, left, right, full, semi, anti |
| interval | 등호 + 양쪽 시간 범위 | 시간 범위 안의 행 | 워터마크가 범위를 지날 때 | append-only | inner, outer |
| window | 같은 윈도 TVF, `window_start`, `window_end` 일치 | 열린 윈도의 행 | 윈도가 끝날 때 | append-only | inner, left, right, full, semi, anti |
| temporal (이벤트 시간) | `FOR SYSTEM_TIME AS OF` 왼쪽 rowtime, 오른쪽 기본 키 | 오른쪽 버전, 대기 중인 왼쪽 행 | 워터마크가 지난 버전 | 왼쪽 변경을 그대로 | inner, left |
| lookup | `FOR SYSTEM_TIME AS OF` 왼쪽 proctime | 없음(캐시는 선택) | 해당 없음 | 왼쪽 변경을 그대로 | inner, left |

interval join과 window join은 append-only 입력만 받는다. 앞에 업데이트를 내는 집계가 있으면 쓸 수 없다. window join은 양쪽이 같은 종류의 윈도여야 해서 텀블링과 슬라이딩끼리는 맞출 수 없고, 윈도 TVF 바로 뒤에서 쓸 때 SESSION 윈도는 지원하지 않는다.

## 워터마크가 interval join의 상태를 비운다

SQL Client에서 내장 `datagen` 커넥터만으로 두 조인의 changelog 모드를 비교할 수 있다.

```sql
-- Flink 2.3 SQL Client
CREATE TABLE impressions (
  ad_id INT,
  imp_ts AS LOCALTIMESTAMP,
  WATERMARK FOR imp_ts AS imp_ts - INTERVAL '5' SECOND
) WITH ('connector' = 'datagen', 'rows-per-second' = '5',
        'fields.ad_id.min' = '1', 'fields.ad_id.max' = '20');

CREATE TABLE clicks (
  ad_id INT,
  click_ts AS LOCALTIMESTAMP,
  WATERMARK FOR click_ts AS click_ts - INTERVAL '5' SECOND
) WITH ('connector' = 'datagen', 'rows-per-second' = '2',
        'fields.ad_id.min' = '1', 'fields.ad_id.max' = '20');

-- 1) regular join: 상태가 계속 늘고 retraction이 생긴다
EXPLAIN CHANGELOG_MODE
SELECT i.ad_id, c.ad_id AS clicked
FROM impressions i LEFT JOIN clicks c ON i.ad_id = c.ad_id;

-- 2) interval join: 노출 뒤 1분 안의 클릭만 맞춘다
EXPLAIN CHANGELOG_MODE
SELECT i.ad_id, c.ad_id AS clicked
FROM impressions i LEFT JOIN clicks c
  ON i.ad_id = c.ad_id
 AND c.click_ts BETWEEN i.imp_ts AND i.imp_ts + INTERVAL '1' MINUTE;
```

1)의 `Join` 노드에는 insert 말고 삭제가 포함된 모드가, 2)의 `IntervalJoin` 노드에는 insert만 붙어야 한다(planner 소스의 추론 규칙에서 읽은 기대값이고, 출력 문자열을 직접 확인하지는 않았다). 2)에서 클릭은 노출 시각부터 1분 뒤까지만 맞는다. 바꿔 말하면 노출은 `[click_ts - 1분, click_ts]` 안에 있어야 한다. 연산자의 워터마크(두 입력 워터마크 중 작은 값)가 `imp_ts + 1분`을 넘으면 그 노출과 맞을 클릭은 더 오지 않으니 노출 행을 지운다. 클릭 행은 워터마크가 `click_ts`를 넘으면 지운다. LEFT JOIN이면 지우는 그 순간에 한 번도 짝을 못 찾은 노출을 `[노출, null]`로 낸다. 짝이 없다는 판단을 시간이 확정한 뒤에 내므로 되돌릴 일이 없고, 출력이 append-only로 남는다. 대신 null 행은 시간 범위와 워터마크 지연만큼 늦게 나온다. 워터마크가 멈추는 경우는 [Flink 시간과 윈도 글](/posts/flink-time-and-windows/)에 정리했다.

## 그때의 값을 붙이는 temporal join, 지금 값을 붙이는 lookup join

주문에 환율을 붙일 때 원하는 값은 주문 시각에 유효했던 환율이다. 이벤트 시간 temporal join이 이걸 한다.

```sql
SELECT o.order_id, o.price * r.rate AS price_krw
FROM orders o
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF o.order_time AS r
  ON o.currency = r.currency;
```

`currency_rates`는 기본 키와 워터마크가 있는 versioned table이어야 하고, 조인 조건에 그 기본 키가 들어가야 한다. Debezium이나 upsert-kafka 소스처럼 키별 변경이 흐르는 테이블이 그 자리에 온다. 연산자는 왼쪽 행을 쥐고 있다가 양쪽 워터마크가 그 시각을 넘을 때 맞는 버전을 붙여 낸다. 환율 토픽이 한동안 조용하면 워터마크가 멈추고 주문도 나가지 않는다. 배치에서 Type 2 차원의 유효 기간으로 사건 시각의 행을 찾던 일([SCD 글](/posts/dimensional-modeling-scd/))을 스트림에서 하는 셈이다.

lookup join은 문법이 같지만 왼쪽의 처리 시간 속성을 쓰고, 오른쪽은 JDBC나 HBase 같은 lookup 커넥터여야 한다. 행이 도착하는 순간 저장소에 조회해서 붙이고, 나중에 고객 정보가 바뀌어도 이미 낸 결과는 고치지 않는다. 상태가 없으니 가볍지만, 같은 입력을 다시 처리하면 그때의 값이 붙어 결과가 달라진다. 밀린 백로그를 따라잡는 중이라면 이벤트가 일어난 시각이 아닌 지금의 차원 값이 붙는다.

조회 비용은 캐시와 비동기 호출로 줄인다. JDBC 커넥터는 `lookup.cache = PARTIAL`과 `lookup.partial-cache.max-rows`, `lookup.partial-cache.expire-after-write`로 캐시를 켜고, 캐시가 짧을수록 최신에 가깝지만 DB 요청이 는다. JDBC lookup은 동기 조회만 한다. 커넥터가 비동기를 구현하면 optimizer가 비동기를 먼저 고르고, `LOOKUP` 힌트로 `async`, `output-mode`(기본 `ordered`), `capacity`(기본 100), `timeout`(기본 300s)을 정한다. 차원 행이 이벤트보다 늦게 들어오는 경우는 `'retry-predicate'='lookup_miss'`와 `fixed_delay` 재시도로 기다릴 수 있다.

[연말 결산 Spark 잡 글](/posts/year-end-recap-spark/)은 로그를 `(user_id, track_id, cnt)`로 줄인 뒤 곡 메타 테이블과 배치로 조인했다. 이 조인을 스트림으로 옮긴다면 메타가 잘 바뀌지 않으니 캐시를 둔 lookup join이 가볍고, 그 시점의 메타가 꼭 필요할 때만 temporal join으로 버전을 들고 가는 쪽을 따져 볼 수 있다. 가정한 설계이고 실제로 옮긴 잡은 아니다.

## Spark Structured Streaming은 워터마크와 시간 조건을 요구한다

Spark의 stream-stream inner join은 조건 없이도 돌지만 상태가 끝없이 자란다. 상태를 지우려면 양쪽에 워터마크를 두고, 시간 범위 조건이나 같은 이벤트 시간 윈도로 맞추는 조건을 넣는다. outer join과 semi join은 이 둘이 필수다. 짝이 없다는 null 결과를 내려면 앞으로 맞을 행이 없다는 걸 알아야 하기 때문이다.

```python
# Spark 4.3, 프로그래밍 가이드의 노출-클릭 예
imp = impressions.withWatermark("impressionTime", "2 hours")
clk = clicks.withWatermark("clickTime", "3 hours")
imp.join(clk, expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour"""), "leftOuter")
```

노출 행은 쿼리 워터마크가 `impressionTime + 1시간`을 넘어야 지울 수 있다. 쿼리 워터마크는 기본으로 입력들 중 작은 값을 따르므로(`spark.sql.streaming.multipleWatermarkPolicy=min`) 본 가장 큰 클릭 시각보다 3시간 뒤처지는 클릭 쪽이 정한다. 그래서 노출은 이벤트 시간으로 대략 4시간을 상태에 머문다. 워터마크는 마이크로배치가 끝날 때 올라가고, 새 데이터가 들어와야 다시 오른다. 한쪽 입력이 조용하면 outer join의 null 행이 그만큼 늦게 나온다. 출력 모드는 append만 된다. Flink interval join처럼 null 행을 확정된 뒤에 한 번 내고 되돌리지 않는다. 워터마크의 의미 차이와 체크포인트 위치는 [DStream과 Structured Streaming 글](/posts/dstream-vs-structured-streaming/)에 있다.

조인 상태 형식은 내부 설정 `spark.sql.streaming.join.stateFormatVersion`이 정하고 기본값은 2다. 3과 4는 네 개였던 state store를 virtual column family로 하나에 담고 RocksDB에서만 된다. 4는 워터마크 기반 정리를 최적화했다. 형식끼리 호환되지 않아 쿼리를 시작한 뒤에는 바꾸지 않는다.

stream-static join은 상태가 없다. 스트림이 왼쪽이면 inner, left outer, left semi가 되고, 스트림 쪽을 보존해야 하는 right outer와 full outer는 지원하지 않는다. 정적 쪽 변경이 이미 낸 결과를 고치지 않는 점은 lookup join과 같다.

## Kafka Streams의 KStream-KTable 조인

KStream-KTable 조인은 윈도가 없고 inner와 left만 된다. 스트림 쪽 레코드만 조인을 일으키고, 테이블 쪽 레코드는 내부 상태만 바꾼다. 테이블이 나중에 바뀌어도 앞선 결과는 다시 나오지 않는다. 두 토픽은 co-partitioned여야 한다. 테이블을 versioned store로 만들면 스트림 레코드 시각 이하에서 가장 최근 버전을 찾아 Flink temporal join과 비슷해지고, grace period를 주면 스트림 레코드를 그만큼 버퍼에 두고 늦은 테이블 갱신을 기다린다. GlobalKTable은 모든 인스턴스가 전체 테이블을 들고 있어 co-partitioning이 필요 없다.

## 참고 자료

- [Flink 2.3 Joins](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/)
- [Flink 2.3 Window Join](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-join/)
- [Flink 2.3 Hints (LOOKUP, STATE_TTL)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/hints/)
- [Flink JDBC SQL Connector (lookup cache)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/jdbc/)
- [Flink release-2.3 FlinkChangelogModeInferenceProgram](https://github.com/apache/flink/blob/release-2.3/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/optimize/program/FlinkChangelogModeInferenceProgram.scala)
- [Flink release-2.3 StreamingJoinOperator, TimeIntervalJoin](https://github.com/apache/flink/tree/release-2.3/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/join)
- [Spark 4.3.0 Structured Streaming: Join Operations](https://spark.apache.org/docs/4.3.0/streaming/apis-on-dataframes-and-datasets.html#join-operations)
- [Spark v4.3.0 SQLConf (join.stateFormatVersion, multipleWatermarkPolicy)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [Kafka 4.4 Streams DSL: Joining](https://kafka.apache.org/44/streams/developer-guide/dsl-api/)

## English summary

Joining two unbounded streams forces the engine to keep rows in state, because any past row may still match a future one. Flink SQL offers five joins that differ in when that state can go: a regular join keeps both sides until a TTL drops them and emits retractions for outer joins, interval and window joins clear state as watermarks pass and stay append-only, an event-time temporal join keeps versions of a keyed table, and a lookup join keeps nothing and queries an external store. Spark Structured Streaming needs watermarks plus a time-range condition for state cleanup and outer joins, and supports only append output. Kafka Streams triggers a KStream-KTable join only from the stream side. Bounding state with time conditions buys append-only output at the cost of delayed null results, while a lookup join is cheap but gives different answers on replay.
