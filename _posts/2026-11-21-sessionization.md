---
title: "자정을 넘긴 세션은 어느 날짜에 넣을까"
date: 2026-11-21 06:17:00 +0900
categories: [Batch & Query, SQL]
tags: [sessionization, sql, spark, flink, window-functions, concept]
---

한 사용자가 11월 1일 23:40에 앱을 열고 다음 날 00:10까지 썼다. 11월 2일 일 배치가 `dt = '2026-11-02'` 파티션만 읽으면 00:10 이벤트는 직전 이벤트가 없으니 새 세션이 된다. 방문 한 번이 세션 두 개로 세어지고, 두 번째 세션의 첫 화면은 실제로 앱을 연 화면과 다르게 남는다. 30분 규칙은 쿼리 몇 줄이면 되지만, 세션 ID를 어떻게 만들지, 같은 시각 이벤트를 어떻게 줄 세울지, 전날을 얼마나 다시 읽을지는 따로 정해야 한다.

버전은 Spark 4.3.0, Flink 2.3, SQLite 3.50.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 세션화(sessionization) | 사용자별 이벤트를 쉬는 시간 기준으로 방문 단위로 묶는 일 |
| gap, timeout | 이만큼 넘게 이벤트가 없으면 세션이 끝난다고 보는 간격. 30분이 흔하다 |
| 세션 번호 | 플래그의 누적 합. 읽은 범위가 바뀌면 같이 바뀐다 |
| 세션 ID | 사용자와 세션 시작 시각처럼 데이터만으로 다시 만들 수 있는 키 |
| tie-breaker | 같은 시각 이벤트의 순서를 고정하는 두 번째 정렬 키. 이벤트 ID 등 |
| 세션 길이 상한 | 끝없이 이어지는 세션을 정해진 길이에서 자르는 규칙 |
| carry-over | 전날 끝에 열려 있던 세션을 다음 날 실행이 이어받는 방식 |
| `session_window` | Spark 3.2.0부터 있는 세션 윈도 함수. 배치와 Structured Streaming 모두 쓴다 |
| `SESSION` TVF | Flink SQL 세션 윈도 테이블 함수. 1.19부터 스트리밍에서 쓰고 2.3에서도 배치는 안 된다 |

## 플래그와 누적 합 다음에 남는 일

`LAG`로 직전 시각을 붙이고, 간격이 30분을 넘으면 1을 찍고, 그 플래그를 누적 합으로 더하는 기본 쿼리는 [윈도 함수 글](/posts/sql-window-functions/)에 있다. 여기서는 그 결과를 저장 가능한 세션 ID로 바꾸는 부분을 이어서 본다. 아래 예는 자정을 넘는 세션, 같은 시각 이벤트 두 건, 2시간 상한에 걸리는 긴 세션을 함께 넣었다.

```sql
-- SQLite 3.50.4에서 실행 (Python 3.14 sqlite3 모듈)
WITH events (event_id, user_id, ts) AS (VALUES
  (1, 'u1', '2026-11-01 23:40:00'), (2, 'u1', '2026-11-01 23:55:00'), (4, 'u1', '2026-11-02 00:10:00'),
  (3, 'u1', '2026-11-02 00:10:00'), (5, 'u1', '2026-11-02 01:00:00'), (6, 'u2', '2026-11-02 09:00:00'),
  (7, 'u2', '2026-11-02 09:25:00'), (8, 'u2', '2026-11-02 09:50:00'), (9, 'u2', '2026-11-02 10:15:00'),
  (10, 'u2', '2026-11-02 10:40:00'), (11, 'u2', '2026-11-02 11:05:00')
),
flagged AS (
  SELECT *,  -- 첫 이벤트는 LAG가 NULL이라 비교가 NULL이 되고 ELSE 1로 간다
    CASE WHEN unixepoch(ts) - unixepoch(LAG(ts) OVER w) <= 1800 THEN 0 ELSE 1 END AS is_new
  FROM events
  WINDOW w AS (PARTITION BY user_id ORDER BY ts, event_id)
),
numbered AS (
  SELECT *,
    SUM(is_new) OVER (PARTITION BY user_id ORDER BY ts, event_id ROWS UNBOUNDED PRECEDING) AS session_no
  FROM flagged
),
started AS (
  SELECT *, MIN(ts) OVER (PARTITION BY user_id, session_no) AS session_start
  FROM numbered
)
SELECT event_id, user_id, ts, is_new,
  user_id || '@' || session_start AS session_id,
  (unixepoch(ts) - unixepoch(session_start)) / 7200 AS part  -- 2시간 상한 조각
FROM started
ORDER BY user_id, ts, event_id;
```

```text
event_id|user_id|ts|is_new|session_id|part
1|u1|2026-11-01 23:40:00|1|u1@2026-11-01 23:40:00|0
2|u1|2026-11-01 23:55:00|0|u1@2026-11-01 23:40:00|0
3|u1|2026-11-02 00:10:00|0|u1@2026-11-01 23:40:00|0
4|u1|2026-11-02 00:10:00|0|u1@2026-11-01 23:40:00|0
5|u1|2026-11-02 01:00:00|1|u1@2026-11-02 01:00:00|0
6|u2|2026-11-02 09:00:00|1|u2@2026-11-02 09:00:00|0
7|u2|2026-11-02 09:25:00|0|u2@2026-11-02 09:00:00|0
8|u2|2026-11-02 09:50:00|0|u2@2026-11-02 09:00:00|0
9|u2|2026-11-02 10:15:00|0|u2@2026-11-02 09:00:00|0
10|u2|2026-11-02 10:40:00|0|u2@2026-11-02 09:00:00|0
11|u2|2026-11-02 11:05:00|0|u2@2026-11-02 09:00:00|1
```

세션 번호 대신 `사용자@시작 시각`을 ID로 쓴 이유는 재현성이다. 세션 번호는 "이 사용자의 몇 번째 세션"이라서 하루치만 읽든 일주일치를 읽든 값이 달라진다. 시작 시각 ID는 그 세션을 이루는 이벤트만 같으면 어느 범위로 계산해도 같은 값이 나온다. 이벤트 1~4는 날짜가 달라도 같은 ID를 받았다. 비교를 `<= 1800`으로 쓴 것도 경계 정의다. 정확히 30분 뒤 이벤트는 같은 세션에 남는다. Spark와 Flink의 세션 윈도도 소스 기준으로 같은 쪽이다. Spark는 다음 이벤트 시작이 현재 세션 끝(마지막 이벤트 + gap) 이하이면 합치고, Flink `TimeWindow.intersects`도 `this.end >= other.start`로 맞닿은 윈도를 합친다. SQL을 `< 1800`으로 쓰면 정확히 30분 간격인 이벤트에서 엔진 결과와 어긋난다.

## 같은 시각 이벤트와 두 번의 정렬

이벤트 3과 4는 같은 초에 찍혔다. 위 쿼리는 플래그를 매길 때와 누적 합을 낼 때 정렬을 두 번 한다. `ORDER BY ts`만 쓰면 같은 시각 행의 순서는 엔진이 정하고, 두 정렬이 서로 다른 순서를 고를 수 있다. 간격이 40분 벌어진 뒤 같은 초에 두 건이 오면, 첫 정렬에서 A가 앞서 A에 1이 찍히고 두 번째 정렬에서 B가 앞서면 B는 아직 1이 더해지기 전 번호를 받는다. 같은 방문이 이전 세션과 새 세션으로 갈린다. 정렬 키에 `event_id`를 더하면 두 정렬이 같은 순서를 쓴다. 세션 번호가 우연히 맞더라도 순서는 다른 값에 남는다. 첫 화면(`FIRST_VALUE(page)`), 세션 안의 이벤트 순번, 이탈 화면이 모두 tie-breaker에 따라 바뀐다. 클라이언트 시각을 초 단위로 자르면 동점이 늘어나므로, 원래 정밀도를 남기거나 수신 순서 같은 단조 증가 값을 함께 저장한다.

## 자정을 넘는 세션과 전날을 읽는 범위

일 파티션으로 돌리는 배치는 세션을 어디서 끊을지 셋 중 하나를 고른다.

| 방식 | 읽는 범위 | 결과 | 대가 |
|---|---|---|---|
| 자정에서 자른다 | 그날 파티션만 | 23:40~00:10 방문이 세션 둘 | 날짜끼리 독립이라 가장 단순 |
| 전날 꼬리를 H시간 함께 읽는다 | 그날 + 전날 마지막 H시간 | H보다 오래 이어진 세션은 시작점이 잘린다 | 읽는 양이 늘지만 날짜별로 따로 돌릴 수 있다 |
| 전날 끝의 열린 세션을 이어받는다 | 그날 + 사용자별 전날 마지막 이벤트 한 행 | 세션 길이에 상관없이 맞다 | D일 실행이 D-1 결과에 기대므로 백필을 날짜 순서대로 해야 한다 |

꼬리를 읽는 방식에서 전날 행은 `LAG`의 문맥으로만 쓰고 그날 이벤트만 쓴다. 읽은 범위의 첫 행에서 시작한 세션은 실제 시작이 더 앞일 수 있으니, 그런 세션 수를 따로 세어 H가 충분한지 확인할 수 있다. 어느 테이블에 쓰는지도 정해야 한다. 이벤트 단위 테이블을 이벤트 날짜로 나누면, 시작 시각 ID 덕분에 11월 1일 파티션과 2일 파티션이 다시 쓰지 않아도 같은 세션 ID를 가진다. 세션 단위 테이블(세션당 한 행, 길이와 이벤트 수)은 다르다. 시작 날짜로 나누면 2일 실행이 1일 파티션의 세션 행을 고쳐야 하고, 끝 날짜로 나누면 1일 실행 시점에는 그 세션이 아직 닫히지 않았다. [퍼널 분석 글](/posts/funnel-analysis/)의 세션 grain 퍼널은 분모가, [코호트 리텐션 글](/posts/cohort-retention/)의 활성을 세션 기준으로 잡으면 날짜별 활성 수가 이 날짜 귀속에 따라 바뀐다.

## 늦은 이벤트는 세션을 합친다

gap 규칙만 있으면 이벤트가 추가될 때 간격은 줄어들기만 한다. 그래서 늦게 온 이벤트는 세션을 늘리거나 두 세션을 하나로 합칠 뿐 쪼개지는 않는다. 10:00와 10:50 세션 사이에 10:25 이벤트가 늦게 들어오면 10:50 세션의 이벤트가 모두 10:00 세션 ID로 바뀐다. 세션 시작 전 30분 안의 이벤트가 늦게 오면 시작 시각이 당겨져 그 세션 전체의 ID가 바뀐다. 그래서 세션 ID로 행을 찾아 upsert하는 재처리는 옛 ID 행을 남긴다. 늦은 이벤트가 닿은 날짜부터 그 세션이 이어진 날짜까지 파티션을 통째로 다시 계산해 바꾸는 편이 단순하다. 늦은 데이터를 받아 주는 기간과 파티션 교체 방식은 [배치와 스트림 계층 글](/posts/batch-and-stream-layers/)에 정리했다. 전날 세션을 이어받는 방식을 쓰면 D-1 재처리가 D, D+1로 차례로 번진다. [로그 수집 API 글](/posts/log-collection-api/)의 API는 요청마다 만료를 30분으로 다시 거는 트랜잭션 ID 쿠키를 붙였고, 30분 안에 다시 로그가 오면 같은 ID가 이어진다. 이렇게 수집 시점에 붙은 ID는 저장된 값이라 나중에 gap을 바꾸거나 늦은 이벤트로 세션을 합치는 재계산에 쓸 수 없다. 원천 이벤트 시각으로 다시 묶은 세션 ID를 따로 두면 두 값의 차이를 볼 수 있다.

## 세션 길이에 상한을 둘 때

탭을 열어 두거나 봇이 일정 간격으로 요청하면 30분 안의 이벤트가 끝없이 이어져 몇 시간짜리 세션이 생긴다. 위 예의 `part`는 세션 시작부터 2시간마다 조각 번호를 매겨 `session_id || '#' || part`로 나누는 방식이다. 윈도 함수만으로 되고 결과가 결정적이지만, 조각 경계가 원래 세션 시작에 묶여 있어 긴 세션은 여전히 시작점까지 읽어야 한다. "상한을 넘긴 뒤 첫 이벤트에서 새 세션"처럼 앞 세션 결과가 다음 세션 시작을 정하는 규칙은 순차 처리라서 재귀 CTE나 Flink `KeyedProcessFunction`처럼 사용자별 상태를 들고 이벤트를 차례로 보는 처리가 필요하다. Spark `session_window`와 Flink 세션 윈도에는 최대 길이 옵션이 없다.

## Spark session_window

`session_window(time_column, gap_duration)`은 Spark 3.2.0부터 있고, 결과는 `start`, `end`를 가진 구조체다. `end`는 고정 gap이면 세션 마지막 이벤트 시각에 gap을 더한 값이다. 함수 문서의 예에서 00:00:00과 00:04:30 이벤트는 5분 gap으로 `00:00:00 ~ 00:09:30` 세션 하나가 된다. Spark 4.3.0 SQL로는 다음처럼 쓴다.

```sql
SELECT user_id, session_window.start, session_window.end, count(*) AS events
FROM events
GROUP BY user_id, session_window(event_time, '30 minutes');
```

gap에는 행마다 다른 값을 주는 식도 쓸 수 있고, 0 이하로 계산된 행은 집계에서 빠진다. 이 함수는 `GROUP BY`용이라 결과가 세션당 한 행이다. 이벤트마다 세션 ID를 달려면 결과를 `start <= event_time < end` 조건으로 다시 조인하거나 앞의 윈도 함수 방식을 쓴다. Structured Streaming에서는 워터마크와 함께 쓰고 제약이 둘 있다. output mode로 Update를 쓸 수 없고, grouping 키에 `session_window` 말고 컬럼이 하나 이상 있어야 한다. 배치 쿼리는 `session_window`만으로 묶는 전역 세션도 된다. 기본으로는 세션 윈도 집계에서 부분 집계를 하지 않는데, 로컬 파티션에 같은 키가 많으면 `spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition`을 켜서 정렬 비용을 내고 부분 집계를 할 수 있다.

## Flink 세션 윈도

DataStream API에서는 `EventTimeSessionWindows.withGap(Duration.ofMinutes(30))`을 쓰고, 행마다 gap을 다르게 하려면 `withDynamicGap`에 `SessionWindowTimeGapExtractor`를 넘긴다. 레코드마다 윈도를 만들고 gap 안의 윈도를 합치므로 병합을 지원하는 트리거와 `ReduceFunction`, `AggregateFunction`, `ProcessWindowFunction` 같은 윈도 함수를 써야 한다. 윈도 종류와 워터마크는 [Flink 시간과 윈도 글](/posts/flink-time-and-windows/)에 있다. 세션 윈도에서 allowed lateness 안의 늦은 레코드는 두 윈도 사이를 이어 합칠 수 있고, 합쳐진 윈도가 다시 발화한다. 배치에서 본 "늦은 이벤트가 세션을 합친다"가 스트림에서는 갱신 결과로 나온다. SQL에서는 `SESSION` 윈도 TVF를 쓴다.

```sql
-- Flink 2.3 SQL, 스트리밍 모드
SELECT user_id, window_start, window_end, COUNT(*) AS events
FROM TABLE(SESSION(TABLE events PARTITION BY user_id, DESCRIPTOR(event_time), INTERVAL '30' MINUTES))
GROUP BY user_id, window_start, window_end;
```

이 TVF는 1.19에서 스트리밍 모드로 들어왔다. 2.3 문서에서도 배치 모드는 지원하지 않고, 세션 윈도 위의 window join, TopN, deduplication은 beta다. 배치에서 세션을 집계해야 하면 deprecated 상태인 `GROUP BY SESSION(event_time, INTERVAL '30' MINUTE)` 그룹 윈도 집계가 배치와 스트리밍 모두에서 아직 동작한다.

## 참고 자료

- [Spark 4.3.0: Built-in datetime functions (session_window)](https://spark.apache.org/docs/4.3.0/api/sql/datetime-functions/)
- [Spark 4.3.0: Structured Streaming, types of time windows](https://spark.apache.org/docs/4.3.0/streaming/apis-on-dataframes-and-datasets.html)
- [Spark 소스 v4.3.0: MergingSessionsIterator.scala](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/MergingSessionsIterator.scala)
- [Flink 2.3: Windows (DataStream)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/)
- [Flink 2.3: Windowing TVF](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/)
- [Flink 2.3: Window Aggregation (group window functions)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-agg/)
- [Flink 1.19 release announcement (session window TVF)](https://flink.apache.org/2024/03/18/announcing-the-release-of-apache-flink-1.19/)
- [Flink 소스 release-2.3: TimeWindow.java](https://github.com/apache/flink/blob/release-2.3/flink-runtime/src/main/java/org/apache/flink/streaming/api/windowing/windows/TimeWindow.java)
- [SQLite: Window Functions](https://www.sqlite.org/windowfunctions.html)

## English summary

Gap-based sessions come from LAG, a new-session flag and a running sum, but a stored session ID should be rebuilt from data, such as the user plus the session start time, because a session number changes with the range you read. Both sorts in the query need the same tie-breaker, or two events in the same second can land in different sessions. A session that crosses midnight forces a choice between cutting at midnight, re-reading the tail of the previous day, or carrying open sessions forward, and late events can only merge sessions, so reprocessing replaces whole date partitions. Spark has session_window since 3.2.0 and Flink SQL has a SESSION window TVF in streaming since 1.19, neither with a maximum length. My log API issued a transaction ID cookie whose 30-minute expiry was reset on every request; an ID fixed at collection time like that cannot be recomputed when the gap rule or late data change, so a batch-built ID belongs next to it.
