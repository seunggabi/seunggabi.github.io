---
title: "코호트 리텐션 표는 무엇을 정하고 어떻게 쌓을까"
date: 2026-11-21 05:20:00 +0900
categories: [Batch & Query, SQL]
tags: [retention, cohort, sql, spark, trino, concept]
description: "코호트 리텐션 표에서 0%와 아직 모름이 섞이는 문제를 짚고, N-day와 unbounded, bracket 리텐션의 정의와 SQL로 쌓는 방법을 Spark, Trino 기준으로 설명한다."
---

11월 2일 가입 cohort의 D7 리텐션 칸이 0%로 나왔다. 아무도 돌아오지 않은 게 아니라, 데이터가 11월 8일까지만 들어와서 7일째가 아직 오지 않았을 뿐이다. 리텐션 표는 이런 빈칸 하나에도 "0"과 "아직 모름"이 섞인다. 같은 로그로도 cohort를 무엇으로 묶는지, 누구를 활성으로 보는지, N일째를 어떻게 세는지에 따라 숫자가 달라진다. [퍼널 분석 글](/posts/funnel-analysis/)이 진입 cohort가 다음 단계로 넘어가는 비율을 봤다면, 리텐션은 같은 cohort가 며칠 뒤에도 다시 오는 비율을 본다.

버전은 Spark 4.3, Trino 484 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| cohort | 같은 날(주) 같은 첫 사건을 겪은 사용자 묶음 |
| 획득 cohort, 행동 cohort | 첫 방문·가입 날짜로 묶느냐, 첫 구매 같은 행동으로 묶느냐 |
| 활성 사용자 | 그날 "왔다"고 인정하는 이벤트를 한 번 이상 남긴 사용자 |
| N-day 리텐션 | 정확히 N일째에 활성인 비율 |
| unbounded 리텐션 | N일째 또는 그 이후 아무 날에 활성인 비율 |
| bracket 리텐션 | 1~7일, 8~14일처럼 정한 구간 안에 한 번이라도 활성인 비율 |
| cohort 삼각형 | 행은 cohort 날짜, 열은 N. 최근 cohort일수록 오른쪽이 비어 삼각형이 된다 |
| right-censoring | 관측 기간이 끝나 아직 알 수 없는 칸. 0이 아니라 NULL이다 |
| DAU, WAU, MAU | 하루, 7일, 30일(또는 28일) 창의 고유 활성 사용자 수 |
| stickiness | DAU / MAU. 한 달 사용자 중 하루에 오는 비중 |
| HLL sketch | 고유 수를 근사하고 합칠 수 있는 요약값 |

## cohort와 활성을 먼저 정의한다

cohort는 분모다. 첫 방문 날짜로 묶으면 앱을 설치만 하고 떠난 사용자까지 들어가고, 가입 날짜로 묶으면 가입한 사람만 남아 리텐션이 높게 나온다. 첫 구매로 묶은 행동 cohort는 "산 사람이 다시 오는가"를 본다. 셋은 서로 다른 질문이라 한 대시보드에서 섞으면 안 된다.

첫 방문 날짜는 매번 전체 로그에서 `MIN(dt)`로 구하면 이력 전체를 스캔해야 한다. 그래서 `(user_id, cohort_dt)` 테이블을 상태로 들고 가며 매일 새 사용자만 더한다. 늦게 온 이벤트가 기존 `cohort_dt`보다 이르면 사용자가 다른 cohort로 옮겨 가므로, 갱신은 두 값 중 이른 쪽을 남기는 MERGE로 한다.

활성의 정의는 숫자를 가장 크게 바꾼다. 앱 실행만으로 활성이라 하면 푸시를 받아 잠깐 열린 사용자도 들어간다. 음악 서비스라면 재생 같은 핵심 행동을 기준으로 삼을 수 있다. 사용자를 묶는 ID도 며칠을 넘어 이어져야 한다. [로그 수집 API 글](/posts/log-collection-api/)의 트랜잭션 ID 쿠키는 30분 안에 다시 로그가 와야 이어지는 ID라 세션을 묶을 수는 있어도 일 단위 리텐션의 키로는 쓸 수 없다. 재설치하면 새 ID가 생기는 기기 ID도 같은 사람을 새 cohort로 넣는다.

날짜 경계도 정의의 일부다. 23시 50분에 처음 오고 다음 날 0시 10분에 다시 온 사용자는 달력 기준으로는 D1이고, 24시간 경과 기준으로는 D0이다. 이벤트 시각을 서비스 시간대의 날짜로 바꾼 뒤 날짜끼리 빼면 달력 기준이 된다.

## N-day, unbounded, bracket

| 방식 | D7에 잡히는 사용자 | 성질 |
|---|---|---|
| N-day | 정확히 7일째에 온 사용자 | 날마다 들쭉날쭉하다. 칸은 그날 활동으로 한 번 정해진다 |
| unbounded | 7일째 또는 그 뒤 아무 날에 온 사용자 | N이 커질수록 줄기만 한다. 사용자가 나중에 돌아오면 과거 칸이 바뀐다 |
| bracket | 정한 구간(예: 1~7일) 안에 한 번이라도 온 사용자 | 주간 앱처럼 매일 오지 않는 서비스에 맞는다 |

이름은 도구마다 다르다. Amplitude는 N-day를 Return On, unbounded를 Return On or After, bracket을 Return On (Custom)이라고 부르고, unbounded를 rolling 리텐션이라 부르는 곳도 있다. 그래서 지표 정의서에는 이름보다 "N일째 정확히" 또는 "N일째 이후 아무 날"처럼 조건을 적는다.

## 삼각형을 SQL로 만들기

아래는 바로 돌려 볼 수 있는 예다. `activity`는 사용자와 날짜가 한 번씩만 나오는 일간 활성 테이블이고, 데이터는 11월 8일까지 들어왔다.

```sql
-- Spark 4.3. 결과는 아래 표
WITH activity (user_id, dt) AS (VALUES
  ('u1', DATE '2026-11-01'), ('u1', DATE '2026-11-02'), ('u1', DATE '2026-11-08'),
  ('u2', DATE '2026-11-01'), ('u2', DATE '2026-11-04'),
  ('u3', DATE '2026-11-02'), ('u3', DATE '2026-11-03'),
  ('u4', DATE '2026-11-02')
),
cohort AS (
  SELECT user_id, MIN(dt) AS cohort_dt FROM activity GROUP BY user_id
),
days AS (
  SELECT c.cohort_dt, c.user_id, date_diff(a.dt, c.cohort_dt) AS day_n
  FROM cohort c JOIN activity a ON a.user_id = c.user_id
)
SELECT cohort_dt,
       COUNT(DISTINCT user_id) AS cohort_users,
       COUNT(DISTINCT IF(day_n = 1, user_id, NULL)) AS d1,
       COUNT(DISTINCT IF(day_n >= 3, user_id, NULL)) AS d3_or_after,
       IF(date_add(cohort_dt, 7) <= DATE '2026-11-08',
          COUNT(DISTINCT IF(day_n = 7, user_id, NULL)), NULL) AS d7
FROM days
GROUP BY cohort_dt
ORDER BY cohort_dt;
```

| cohort_dt | cohort_users | d1 | d3_or_after | d7 |
|---|---|---|---|---|
| 2026-11-01 | 2 | 1 | 2 | 1 |
| 2026-11-02 | 2 | 1 | 0 | NULL |

11월 1일 cohort의 D3 이후 값이 2인 것은 u2가 3일째에, u1이 7일째에 왔기 때문이다. N-day로 D3를 세면 u2 한 명이다. 11월 2일 cohort의 D7은 11월 9일이 와야 정해지므로 NULL이다. 여기에 0을 넣으면 최근 cohort의 곡선이 갑자기 꺾여 보이고, cohort를 평균 낼 때 리텐션이 낮게 나온다.

Spark의 `date_diff(endDate, startDate)`는 끝 날짜가 앞에 온다. Trino는 `date_diff('day', cohort_dt, dt)`로 단위를 먼저 받고 `dt - cohort_dt`를 돌려주므로 인자 순서가 반대다. Trino의 `date_diff`에 timestamp를 그대로 넣으면 경과한 온전한 단위만 세므로, 달력 기준 일수가 필요하면 날짜로 바꾼 뒤 넣는다. Trino로 옮길 때는 `date_add(cohort_dt, 7)`도 `cohort_dt + INTERVAL '7' DAY`로 바꾼다.

## 미리 쌓는 grain과 대각선

대시보드가 읽는 테이블은 `(cohort_dt, platform, day_n)` grain에 `cohort_users`와 `retained_users`를 넣는다. 비율 대신 분자와 분모를 넣는 이유는 [지표 테이블 grain 글](/posts/metric-table-grain/)에 적은 non-additive 규칙과 같다. 여러 cohort의 평균 D7은 관측이 끝난 cohort만 골라 `SUM(retained_users) / SUM(cohort_users)`로 구한다. 비율의 단순 평균은 작은 cohort에 큰 cohort와 같은 무게를 준다.

N-day 칸은 `cohort_dt + day_n`이 활동 날짜 하나로 정해진다. 그래서 하루치 활동으로 삼각형의 대각선 한 줄을 채울 수 있고, 활동 날짜로 파티션을 나누면 그날을 다시 돌려도 그 대각선만 바뀐다.

```sql
-- Spark 4.3. daily_active는 (user_id, dt)가 unique, 최근 90일 cohort만 본다
INSERT OVERWRITE retention_cells PARTITION (activity_dt = '2026-11-08')
SELECT c.cohort_dt, c.platform,
       date_diff(DATE '2026-11-08', c.cohort_dt) AS day_n,
       COUNT(*) AS retained_users
FROM user_cohort c
JOIN daily_active a ON a.user_id = c.user_id AND a.dt = DATE '2026-11-08'
WHERE c.cohort_dt >= date_sub(DATE '2026-11-08', 90)
GROUP BY c.cohort_dt, c.platform;
```

늦게 온 로그는 최근 며칠의 활동 날짜를 다시 돌려 반영한다. [서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 job은 날짜 범위를 받아 하루씩 돌았고 일간·주간 리텐션이 따로 있었다. 이런 구조라면 하루 단위 실행이 대각선 하나를 맡는다.

unbounded와 bracket은 이렇게 쌓을 수 없다. unbounded 칸은 사용자가 나중에 돌아올 때마다 과거 값이 커지고, bracket은 구간 안 고유 사용자 수라 N-day 칸을 더해 만들 수 없다. unbounded는 `user_cohort`에 사용자별 마지막 활동 날짜를 함께 갱신해 두면 `COUNT_IF(last_active_dt >= date_add(cohort_dt, N))`으로 매번 다시 셀 수 있다. bracket은 사용자 단위 활동에서 다시 센다.

## DAU, MAU와 stickiness

MAU는 달력 한 달일 수도 있고 오늘까지 30일(28일) 창일 수도 있다. 28일 창은 요일 구성이 매번 같아 주말 효과가 덜 흔들린다. 어느 쪽이든 고유 사용자 수라 DAU 30개를 더해 MAU를 만들 수는 없다. 매일 30일 창을 원천에서 다시 세는 대신, 날짜별로 HLL sketch를 저장하고 창 안의 sketch를 합치면 근사값을 싸게 얻는다. Spark는 `hll_sketch_agg`, `hll_union_agg`, `hll_sketch_estimate`를, Trino는 `approx_set`, `merge`, `cardinality`를 쓴다. 정확한 값이 필요한 보고용 숫자에는 쓰지 않는다.

stickiness는 보통 그 기간 평균 DAU를 MAU로 나눈다. 0.2라면 한 달 사용자가 평균적으로 한 달에 6일쯤 온다는 뜻이다. 다만 MAU가 신규 유입으로 커지는 달에는 기존 사용자의 습관이 그대로여도 값이 내려간다. 리텐션과 달리 cohort를 나누지 않은 숫자라, 신규와 기존 사용자의 비중 변화가 그대로 섞인다.

## 참고 자료

- [Spark 4.3: Datetime functions (date_diff, date_add, date_sub, date_trunc)](https://spark.apache.org/docs/latest/api/sql/datetime-functions/)
- [Spark 4.3: Built-in functions (hll_sketch_agg, hll_union_agg)](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)
- [Spark 4.3: INSERT TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)
- [Trino 484: Date and time functions (date_diff)](https://trino.io/docs/current/functions/datetime.html)
- [Trino 484: HyperLogLog functions](https://trino.io/docs/current/functions/hyperloglog.html)
- [Amplitude: Interpret your retention analysis](https://amplitude.com/docs/en/analytics/charts/retention-analysis/retention-analysis-interpret)

## English summary

A retention table depends on three definitions made before any SQL: what event starts a cohort, what counts as active, and whether day N means exactly day N, day N or later, or a bracket of days. Cells that a recent cohort has not reached yet are unknown, not zero, so they stay NULL and are left out of cross-cohort averages. My log API's transaction ID cookie expired after 30 minutes, so it groups sessions but cannot key a multi-day cohort, and my indicator batches ran one day at a time over a date range, which fits filling one diagonal of the cohort triangle per day. N-day cells can be precomputed per activity date and overwritten safely, while unbounded and bracket retention need user-level state, trading storage for correct numbers when users come back late.
