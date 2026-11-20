---
title: "퍼널 전환율은 단계 순서와 시간 창에서 갈린다"
date: 2026-11-21 05:00:00 +0900
categories: [Batch & Query, SQL]
tags: [funnel, sql, trino, spark, window-functions, concept]
---

상품 보기, 장바구니, 구매 세 단계 퍼널에서 구매한 사용자 수를 상품을 본 사용자 수로 나누면 숫자는 바로 나온다. 그런데 장바구니를 먼저 누르고 상품을 본 사용자, 3주 뒤에 산 사용자, 같은 구매 이벤트가 두 번 들어온 사용자를 어떻게 셀지 정하지 않으면, 같은 로그에서 쿼리마다 다른 전환율이 나온다. 퍼널은 단계 이름보다 순서, 시간 창, 세는 단위를 먼저 정해야 하는 지표다.

버전은 Trino 484, Spark 4.3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 퍼널 단계 | 이벤트 이름과 조건으로 정한 사용자 행동 하나 |
| 진입 cohort | 첫 단계를 밟은 집합. 모든 전환율의 분모가 여기서 나온다 |
| strict 순서 | 단계 사이에 다른 이벤트가 끼면 실패 |
| loose 순서 | 다른 이벤트가 끼어도 단계 순서만 맞으면 성공 |
| 전환 시간 창 | 진입 시각부터 다음 단계를 인정하는 기간 |
| grain | 사용자, 세션, 날짜 중 무엇을 한 번으로 세는가 |
| step-to-step 전환율 | k단계 도달 수 / k-1단계 도달 수 |
| overall 전환율 | k단계 도달 수 / 1단계 도달 수 |
| MATCH_RECOGNIZE | 정렬된 행에 정규식 같은 패턴을 거는 SQL 절. Trino에 있고 Spark SQL에는 없다 |
| 늦게 온 이벤트 | 집계를 끝낸 날짜의 퍼널을 뒤늦게 바꾸는 이벤트 |
| Simpson's paradox | 세그먼트마다 오른 비율이 합치면 내려가는 현상 |

## 단계, 순서, 시간 창을 먼저 적는다

사용자 이벤트가 `view, search, cart, purchase` 순으로 들어왔다고 하자. 순서 규칙에 따라 결과가 갈린다.

| 규칙 | view → cart → purchase 퍼널 결과 |
|---|---|
| strict | view와 cart 사이에 search가 있어 1단계에서 멈춘다 |
| loose | 순서가 맞으므로 3단계 모두 도달 |
| 순서 없음 | 세 이벤트가 있기만 하면 도달. cart가 view보다 먼저여도 인정 |

시간 창은 기준점을 함께 정한다. 진입 시각부터 7일인지, 직전 단계부터 7일인지에 따라 같은 사용자가 들어가고 빠진다. 진입이 여러 번인 사용자는 첫 진입 하나만 볼지, 진입마다 따로 볼지도 정해야 한다. 첫 진입만 보면 계산은 쉽지만, 첫 방문에서 떠났다가 2주 뒤 다시 와서 산 사용자는 전환으로 잡히지 않는다.

## 사용자, 세션, 날짜 중 무엇을 셀까

같은 퍼널도 grain에 따라 분모가 다르다. 사용자 grain은 사람 한 명을 한 번 세고, 세션 grain은 방문 한 번을 한 번 센다. 세션 grain이라면 세션 ID가 묶는 키가 된다. [로그 수집 API 글](/posts/log-collection-api/)의 API는 30분 만료 트랜잭션 ID 쿠키를 붙였고, 로그인하지 않은 사용자의 행동도 이 ID로 묶을 수 있었다. 세션 퍼널을 만든다면 이 ID가 그대로 세션 키 후보다. 날짜 grain은 보통 진입한 날짜로 cohort를 나눈다. 이때 날짜를 어느 시간대로 자르는지가 숫자를 바꾼다. Trino에서는 `date(event_time AT TIME ZONE 'Asia/Seoul')`처럼 시간대를 명시하고, Spark의 `TIMESTAMP`(LTZ)는 세션 로컬 시간대로 해석되므로 세션 설정을 맞춘다.

퍼널 단계별 사용자 수는 고유 사용자 수라서 더할 수 없다. [지표 테이블 grain 글](/posts/metric-table-grain/)에서 정리한 non-additive 규칙이 그대로 적용된다. 일간 퍼널 7행을 더해 주간 퍼널을 만들면 이틀 들어온 사람이 두 번 세어지고, 전환율만 저장하면 세그먼트를 합칠 때 가중치가 사라진다. 그래서 퍼널 테이블에는 단계별 도달 수를 넣고 비율은 읽을 때 나눈다.

step-to-step 전환율은 k단계 도달자가 k-1단계 도달자 안에 있을 때만 다음 단계로 넘어간 비율이라는 뜻을 가진다. 단계를 순서 없이 각각 세면 이 포함 관계가 깨져 100%를 넘을 수도 있다.

## 순서를 지키며 세는 SQL

흔한 실수는 사용자별로 단계마다 `MIN(event_time)`을 따로 구한 뒤 `t2 > t1`을 비교하는 것이다. 아래 예의 u2는 09:00에 cart, 09:10에 view, 09:20에 다시 cart를 했다. 단계별 최솟값만 비교하면 cart 09:00이 view보다 빨라 전환에서 빠진다. 이전 단계 시각 이후의 이벤트에서 최솟값을 구해야 한다.

```sql
-- Trino 484, Spark 4.3 문법. 결과: step1 4, step2 3, step3 2
WITH events (user_id, event_name, event_time) AS (VALUES
  ('u1', 'view', TIMESTAMP '2026-11-01 10:00:00'), ('u1', 'cart', TIMESTAMP '2026-11-01 10:05:00'),
  ('u1', 'purchase', TIMESTAMP '2026-11-01 10:20:00'), ('u2', 'cart', TIMESTAMP '2026-11-01 09:00:00'),
  ('u2', 'view', TIMESTAMP '2026-11-01 09:10:00'), ('u2', 'cart', TIMESTAMP '2026-11-01 09:20:00'),
  ('u3', 'view', TIMESTAMP '2026-11-01 12:00:00'), ('u3', 'cart', TIMESTAMP '2026-11-09 12:00:00'),
  ('u4', 'view', TIMESTAMP '2026-11-02 08:00:00'), ('u4', 'view', TIMESTAMP '2026-11-02 08:01:00'),
  ('u4', 'cart', TIMESTAMP '2026-11-02 08:03:00'), ('u4', 'purchase', TIMESTAMP '2026-11-02 08:10:00'),
  ('u4', 'purchase', TIMESTAMP '2026-11-02 08:10:00')
),
s1 AS (
  SELECT user_id, MIN(event_time) AS t1
  FROM events WHERE event_name = 'view' GROUP BY user_id
),
s2 AS (
  SELECT s1.user_id, s1.t1, MIN(e.event_time) AS t2
  FROM s1 LEFT JOIN events e
    ON e.user_id = s1.user_id AND e.event_name = 'cart'
   AND e.event_time > s1.t1 AND e.event_time <= s1.t1 + INTERVAL '7' DAY
  GROUP BY s1.user_id, s1.t1
),
s3 AS (
  SELECT s2.user_id, s2.t2, MIN(e.event_time) AS t3
  FROM s2 LEFT JOIN events e
    ON e.user_id = s2.user_id AND e.event_name = 'purchase'
   AND e.event_time > s2.t2 AND e.event_time <= s2.t1 + INTERVAL '7' DAY
  GROUP BY s2.user_id, s2.t2
)
SELECT COUNT(*) AS step1, COUNT(t2) AS step2, COUNT(t3) AS step3 FROM s3;
```

u3의 cart는 8일 뒤라 시간 창 밖이고, u4의 중복 view와 purchase는 `MIN`과 `COUNT`가 한 번만 센다. 이 쿼리는 첫 진입 기준이다. 왼쪽이 사용자당 한 행이라 조인 결과는 사용자의 해당 단계 이벤트 수만큼만 늘어난다. 이벤트 테이블끼리 단계마다 self-join하면 view n개와 cart m개인 사용자가 n×m행이 되는 것과 다르다. 같은 계산을 `MIN(CASE WHEN ...) OVER (PARTITION BY user_id)`로 이전 단계 시각을 행마다 붙이고, 다음 단계를 조건부 집계(`FILTER (WHERE ...)`)로 구하는 방식으로도 쓸 수 있다. 조인 대신 단계마다 윈도 계산이 한 번씩 든다. Trino에서는 `MATCH_RECOGNIZE`로 순서와 시간 창을 패턴 하나에 담을 수 있다.

```sql
-- Trino 484. 진입마다 한 행이 나온다
SELECT user_id, t1, t2, t3
FROM events
MATCH_RECOGNIZE (
  PARTITION BY user_id
  ORDER BY event_time
  MEASURES FIRST(A.event_time) AS t1, FIRST(B.event_time) AS t2, FIRST(C.event_time) AS t3
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A (X*? B (X*? C)?)?)
  DEFINE
    A AS event_name = 'view',
    B AS event_name = 'cart' AND event_time <= FIRST(A.event_time) + INTERVAL '7' DAY,
    C AS event_name = 'purchase' AND event_time <= FIRST(A.event_time) + INTERVAL '7' DAY
)
```

`DEFINE`에 없는 `X`는 항상 참이라 아무 이벤트나 받고, `*?`는 가장 적게 건너뛰는 쪽을 먼저 고른다. `X`를 빼고 `PATTERN (A (B C?)?)`로 쓰면 strict 순서가 된다. 매치가 끝난 뒤 다음 view에서 새 매치가 시작되므로 결과는 겹치지 않는 진입마다 한 행이고, 사용자 grain으로 바꾸려면 바깥에서 사용자별로 가장 멀리 간 단계를 고른다. 매치되지 않은 행은 결과에 나오지 않는다. Spark 4.3의 SQL 레퍼런스에는 `MATCH_RECOGNIZE`가 없어서, Spark에서는 위의 조인이나 윈도 함수 방식으로 쓴다.

## 중복, 늦게 온 이벤트, 다시 계산하는 범위

같은 이벤트가 재전송으로 두 번 들어오면 사용자 수 퍼널은 `MIN`과 `COUNT(DISTINCT)` 덕분에 버티지만, 이벤트 수를 세는 퍼널은 부풀어 오른다. 이벤트 ID가 있으면 퍼널 전에 `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ...)`로 한 건만 남긴다.

늦게 온 이벤트는 이미 계산한 날짜를 바꾼다. 11월 1일 진입 cohort의 퍼널은 시간 창 7일과 이벤트가 늦게 도착하는 기간이 지나야 확정된다. 그래서 매일 오늘 하루만 계산하지 않고 최근 며칠의 진입 날짜를 다시 계산한다. [서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 job은 시작일과 종료일을 받아 하루씩 돌았고, 공용 insert가 unique 키로 겹치는 행을 지운 뒤 넣었다. 그런 구조라면 날짜 범위를 넓히는 것만으로 같은 job이 재계산을 맡는다. 이벤트 시각 자체가 틀린 경우도 있다. 로그 수집 API는 7일 전부터 1일 뒤 범위를 벗어난 이벤트 시각을 서버 시각으로 바꾸고 원래 값은 에러 로그에 남겼다. 이렇게 보정된 이벤트는 퍼널에서 서버 시각 순서로 정렬된다. 엉뚱한 날짜로 튀지 않는 대신, 클라이언트가 실제로 누른 순서와 어긋날 수 있다.

미리 계산할지 그때그때 쿼리할지는 질문이 고정돼 있는지로 정한다. 매일 보는 퍼널 몇 개는 `(진입 날짜, 플랫폼, 단계)` grain 테이블로 미리 만들고, 단계나 시간 창을 바꿔 보는 질문은 원천 이벤트를 Trino로 직접 읽는다. 지표 배치에서도 대시보드에 없는 추출 요청은 Hive를 읽는 Zeppelin 노트북으로 처리했다.

## 세그먼트를 합치면 뒤집히는 전환율

| 주 | 웹 | 앱 | 전체 |
|---|---|---|---|
| 1주 | 100 / 1,000 (10%) | 300 / 1,000 (30%) | 400 / 2,000 (20%) |
| 2주 | 330 / 3,000 (11%) | 310 / 1,000 (31%) | 640 / 4,000 (16%) |

웹과 앱 모두 전환율이 올랐는데 전체는 20%에서 16%로 내려갔다. 전환율이 낮은 웹 진입자가 세 배로 늘어 비중이 바뀌었기 때문이다. Simpson's paradox다. 세그먼트별 분자와 분모를 저장해 두어야 이런 변화를 비중 변화와 전환 변화로 나눠 볼 수 있다.

봇 트래픽은 진입 단계만 부풀려 전환율을 끌어내린다. 퍼널 전에 걸러야 하고, 거른 기준도 퍼널 정의에 같이 적는다. 구매한 사용자만 모아 그들이 거친 경로를 보는 분석은 떠난 사용자를 처음부터 빼므로 survivorship bias가 생긴다. 퍼널의 분모는 언제나 진입 cohort 전체다.

## 참고 자료

- [Trino 484: MATCH_RECOGNIZE](https://trino.io/docs/current/sql/match-recognize.html)
- [Trino 484: Aggregate functions (FILTER, count_if, min_by)](https://trino.io/docs/current/functions/aggregate.html)
- [Trino 484: Date and time functions (AT TIME ZONE)](https://trino.io/docs/current/functions/datetime.html)
- [Spark 4.3: SELECT](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html)
- [Spark 4.3: GROUP BY (FILTER clause)](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html)
- [Spark 4.3: Window Functions](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Spark 4.3: Data Types (TIMESTAMP_LTZ, TIMESTAMP_NTZ)](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [Stanford Encyclopedia of Philosophy: Simpson's Paradox](https://plato.stanford.edu/entries/paradox-simpson/)

## English summary

A funnel number depends on three choices made before any SQL: step order (strict, loose, or none), the conversion window and its anchor, and the grain (user, session, or day). Comparing each step's independent minimum time is a common bug; each step has to look for the first event after the previous step, which a join chain does on both Trino and Spark, and Trino's MATCH_RECOGNIZE can express in one pattern. In my log API, a 30-minute transaction ID cookie let anonymous users' actions be grouped, and my indicator batches took a date range and replaced rows by unique key, so a rerun over recent days is the same job. Precomputed funnel tables keep step counts per segment instead of rates, trading flexibility for cheap daily reads, while new questions go back to raw events.
