---
title: "윈도 함수의 기본 프레임은 동점에서 어긋난다"
date: 2026-11-21 05:29:00 +0900
categories: [Batch & Query, SQL]
tags: [sql, window-functions, trino, spark, sqlite, concept]
---

날짜순 누적 매출을 `SUM(amount) OVER (ORDER BY day)`로 구하면 같은 날짜의 두 행이 같은 누적값을 받는다. 같은 창에서 `LAST_VALUE(amount)`를 부르면 파티션의 마지막 값 대신 현재 행의 값이 나온다. 둘 다 프레임을 적지 않았을 때 붙는 기본 프레임대로 계산한 결과다. 윈도 함수는 `PARTITION BY`, `ORDER BY`, 프레임 세 가지를 따로 읽어야 결과를 예측할 수 있다.

버전은 Trino 484, Spark 4.3.0, SQLite 3.50.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| `PARTITION BY` | 계산을 나누는 단위. `GROUP BY`와 달리 행 수가 줄지 않는다 |
| 윈도 `ORDER BY` | 파티션 안의 행 순서. 순위와 프레임 경계의 기준 |
| 프레임 | 현재 행에서 집계에 넣는 행의 범위 |
| `ROWS` | 물리적인 행 개수로 경계를 센다 |
| `RANGE` | `ORDER BY` 값으로 경계를 정한다. 값이 같은 peer 행은 함께 들어간다 |
| `GROUPS` | peer 묶음 개수로 센다. Trino와 SQLite에 있고 Spark에는 없다 |
| 기본 프레임 | `ORDER BY`가 있으면 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| `ROW_NUMBER`, `RANK`, `DENSE_RANK` | 동점 처리만 다른 순위 함수 |
| `NTILE(n)` | 정렬된 행을 크기 차이가 최대 1인 버킷 n개로 나눈다 |
| `LAG`, `LEAD` | 앞뒤 n번째 행의 값. `IGNORE NULLS`로 null을 건너뛴다 |
| `QUALIFY` | 윈도 결과로 행을 거르는 절. Spark 4.2.0부터 있고 Trino에는 없다 |
| gaps-and-islands | 연속된 구간끼리 같은 그룹 키를 만들어 묶는 패턴 |

## 기본 프레임은 현재 행의 peer까지다

`ORDER BY`만 있고 프레임이 없으면 Trino와 Spark 모두 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`를 쓴다. `RANGE`의 `CURRENT ROW`는 현재 행 하나가 아니라 정렬 값이 같은 마지막 peer까지다. `ORDER BY`도 없으면 Spark는 파티션 전체(`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`)를 프레임으로 잡는다.

```sql
-- SQLite 3.50.4에서 실행. GROUPS 줄은 Trino 484에서도 되고 Spark 4.3에서는 빼야 한다
WITH sales (id, day, amount) AS (VALUES
  (1, '2026-11-01', 10), (2, '2026-11-02', 20),
  (3, '2026-11-02', 30), (4, '2026-11-03', 40)
)
SELECT id, day, amount,
  SUM(amount) OVER (ORDER BY day) AS range_sum,
  SUM(amount) OVER (ORDER BY day, id ROWS UNBOUNDED PRECEDING) AS rows_sum,
  SUM(amount) OVER (ORDER BY day GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS two_days,
  LAST_VALUE(amount) OVER (ORDER BY day, id) AS last_default,
  LAST_VALUE(amount) OVER (ORDER BY day, id
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_all
FROM sales ORDER BY id;
```

| id | day | amount | range_sum | rows_sum | two_days | last_default | last_all |
|---|---|---|---|---|---|---|---|
| 1 | 11-01 | 10 | 10 | 10 | 10 | 10 | 40 |
| 2 | 11-02 | 20 | 60 | 30 | 60 | 20 | 40 |
| 3 | 11-02 | 30 | 60 | 60 | 60 | 30 | 40 |
| 4 | 11-03 | 40 | 100 | 100 | 90 | 40 | 40 |

`range_sum`은 11-02의 두 행이 모두 60이다. 날짜 단위 누적이라면 맞는 값이고, 행 단위 누적이 필요하면 `ROWS`를 쓴다. 이때 `ORDER BY day`만 두면 동점 두 행 중 어느 쪽이 먼저 더해질지 정해져 있지 않아 실행마다 30과 40이 바뀔 수 있다. 그래서 `id` 같은 고유 키를 뒤에 붙였다. `GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW`는 행 수와 상관없이 직전 날짜 묶음과 현재 날짜 묶음을 더한다.

`LAST_VALUE`는 기본 프레임이 현재 행에서 끝나기 때문에 정렬 키가 고유하면 자기 값을 돌려준다. 파티션의 마지막 값이 필요하면 `UNBOUNDED FOLLOWING`까지 프레임을 넓히거나, 정렬을 뒤집어 `FIRST_VALUE`를 쓴다.

이동 평균도 프레임 종류가 결과를 바꾼다. `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`는 날짜가 빠진 데이터에서 7일보다 긴 기간을 덮는다. 달력 기준 7일이면 `RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW`처럼 값 기준 경계를 쓴다(Trino는 날짜·시각 정렬 키에 interval offset을 받는다). 값 기준 offset이 있는 `RANGE`는 Trino와 Spark 모두 `ORDER BY` 키를 하나만 허용한다. 윈도 `SUM`으로 일간 고유 사용자 수를 7일 더해도 7일 고유 사용자 수는 나오지 않는다. 이 non-additive 규칙은 [지표 테이블 grain 글](/posts/metric-table-grain/)에 있다.

## 순위 함수는 동점에서만 갈린다

```sql
-- SQLite 3.50.4에서 실행. Trino, Spark에서도 같은 문법
WITH s (song, cnt) AS (VALUES ('a', 100), ('b', 90), ('c', 90), ('d', 80))
SELECT song, cnt,
  ROW_NUMBER() OVER (ORDER BY cnt DESC, song) AS row_number,
  RANK()       OVER (ORDER BY cnt DESC) AS rank,
  DENSE_RANK() OVER (ORDER BY cnt DESC) AS dense_rank,
  NTILE(3)     OVER (ORDER BY cnt DESC, song) AS ntile3
FROM s ORDER BY cnt DESC, song;
-- 결과 (row_number, rank, dense_rank, ntile3)
-- a: 1, 1, 1, 1 / b: 2, 2, 2, 1 / c: 3, 2, 2, 2 / d: 4, 4, 3, 3
```

`RANK`는 동점 뒤에 번호를 건너뛰고 `DENSE_RANK`는 건너뛰지 않는다. `NTILE(3)`은 4행을 2, 1, 1로 나눠 남는 행을 앞 버킷에 준다. Trino에서 순위 함수와 `LAG`, `LEAD`에는 프레임을 적을 수 없다. [연말 결산 잡 글](/posts/year-end-recap-spark/)은 사용자별 상위 N개를 `RANK`로 골랐고, 재생 수가 같을 때는 인기 점수를 두 번째 정렬 기준으로 넣었다. `RANK`에서 동점은 `rnk <= N`에 N개보다 많은 행을 남기므로, 정렬 키를 늘려 동점 자체를 줄이는 쪽이다.

중복 제거는 `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at, payload)`에서 `rn = 1`만 남긴다. 퍼널 전에 이벤트 ID로 한 건만 남기는 단계는 [퍼널 분석 글](/posts/funnel-analysis/)에 썼다. `ORDER BY`가 동점이면 어느 행이 남는지가 실행마다 바뀌므로, 남길 행을 결정하는 키를 끝까지 적는다. Spark 4.2.0부터는 서브쿼리 없이 `QUALIFY`로 쓸 수 있다.

```sql
-- Spark 4.2.0 이상. Trino 484는 QUALIFY가 없어 서브쿼리 바깥에서 rn = 1로 거른다
SELECT * FROM raw_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at, payload) = 1;
```

## LAG와 누적 SUM으로 세션 나누기

세션은 "직전 이벤트와 30분 넘게 떨어지면 새 세션"으로 정의할 때가 많다. `LAG`로 직전 시각을 붙여 새 세션 시작에 1을 찍고, 그 플래그를 `ROWS` 누적 합으로 더하면 세션 번호가 된다. [로그 수집 API 글](/posts/log-collection-api/)은 요청마다 만료를 30분으로 다시 거는 트랜잭션 ID 쿠키로 같은 일을 수집 시점에 했다. 아래 쿼리는 같은 규칙을 저장된 이벤트에서 나중에 계산한다.

```sql
-- SQLite 3.50.4에서 실행. 결과 session_no: u1은 1, 1, 2, 2 / u2는 1
WITH events (user_id, ts) AS (VALUES
  ('u1', '2026-11-01 10:00:00'), ('u1', '2026-11-01 10:20:00'),
  ('u1', '2026-11-01 11:00:00'), ('u1', '2026-11-01 11:10:00'),
  ('u2', '2026-11-01 09:00:00')
),
flagged AS (
  SELECT user_id, ts,
    CASE WHEN LAG(ts) OVER w IS NULL
           OR ts > datetime(LAG(ts) OVER w, '+30 minutes')  -- Trino, Spark: ts > LAG(ts) OVER w + INTERVAL '30' MINUTE
         THEN 1 ELSE 0 END AS is_new
  FROM events
  WINDOW w AS (PARTITION BY user_id ORDER BY ts)
)
SELECT user_id, ts,
  SUM(is_new) OVER (PARTITION BY user_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS session_no
FROM flagged ORDER BY user_id, ts;
```

10:20과 11:00 사이가 40분이라 세 번째 이벤트에서 세션 2가 시작된다. 같은 사용자에게 같은 시각 이벤트가 둘 있으면 `ROWS` 누적의 순서가 흔들리므로 이벤트 ID를 정렬 키에 더한다.

연속 접속 일수 같은 gaps-and-islands는 날짜에서 `ROW_NUMBER`만큼 빼서 묶는다. 11-01, 11-02, 11-03에 1, 2, 3을 빼면 모두 10-31이 되고, 11-05와 11-06에서 4, 5를 빼면 11-01이 된다. 이 값으로 `GROUP BY`하면 3일짜리와 2일짜리 구간 두 개가 나온다. SQLite에서는 `date(day, '-' || ROW_NUMBER() OVER w || ' days')`, Trino에서는 `date_add('day', -ROW_NUMBER() OVER w, day)`, Spark에서는 `date_sub(day, ROW_NUMBER() OVER w)`로 쓴다. 같은 날짜가 두 번 있으면 먼저 `DISTINCT`로 날짜를 한 번씩만 남겨야 구간이 끊기지 않는다.

비어 있는 값을 직전 값으로 채울 때는 `LAST_VALUE(price) IGNORE NULLS OVER (PARTITION BY item ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`를 쓴다. Trino와 Spark 모두 `IGNORE NULLS`를 함수 괄호 뒤, `OVER` 앞에 적고 `LAG`, `LEAD`, `NTH_VALUE`, `FIRST_VALUE`, `LAST_VALUE`에 쓸 수 있다. SQLite 3.50.4에는 이 절이 없다.

## 엔진은 파티션 키로 모으고 정렬한다

Spark는 `PARTITION BY` 키로 `Exchange hashpartitioning`을 하고, 파티션 안에서 `PARTITION BY`와 `ORDER BY` 순으로 `Sort`한 뒤 `Window` 연산자가 한 줄씩 훑는다. Trino도 `PARTITION BY` 키로 partitioned exchange를 넣는다. `PARTITION BY`가 없으면 두 엔진 모두 모든 행을 한 곳으로 모은다. Spark는 이때 "No Partition Defined for Window operation!" 경고를 남긴다. 테이블 전체 누적 합이나 전역 순위가 큰 테이블에서 느린 이유다. 같은 `PARTITION BY`와 `ORDER BY`를 쓰는 윈도 함수는 Spark에서 `Window` 연산자 하나로 묶인다. 명세가 다르면 연산자가 늘고 정렬이나 셔플이 다시 든다.

한 파티션의 행은 전부 한 태스크에서 처리된다. 행이 아주 많은 사용자 한 명은 긴 태스크 하나가 되고, Spark AQE는 윈도 쪽 skew를 쪼개지 않는다. 셔플과 skew 판정, 그리고 `rnk <= N` 필터로 셔플 전에 상위 N개만 남기는 Spark의 `WindowGroupLimit`은 [Spark 셔플과 skew 글](/posts/spark-shuffle-and-skew/)에 정리했다. Trino도 `row_number`나 `rank` 결과를 상한으로 거르면 옵티마이저가 윈도를 `TopNRanking`으로 바꾼다(세션 속성 `optimize_top_n_ranking`). 그 밖의 방법은 윈도 앞에서 행을 줄이는 것과 파티션 키를 잘게 나누는 것이다. 세션을 `(user_id, 날짜)`로 나누면 hot 사용자의 행이 날짜 수만큼 흩어지지만, 자정을 넘는 세션은 두 개로 잘린다. 파티션 키를 바꾸면 계산의 뜻이 바뀐다.

## 참고 자료

- [Trino 484: Window functions](https://trino.io/docs/current/functions/window.html)
- [Trino 484: SELECT (WINDOW clause, frame, default frame)](https://trino.io/docs/current/sql/select.html)
- [Trino blog: Introducing new window features (RANGE offset, GROUPS)](https://trino.io/blog/2021/03/10/introducing-new-window-features)
- [Trino 소스: SqlBase.g4 (frameExtent, GROUPS, EXCLUDE)](https://github.com/trinodb/trino/blob/master/core/trino-grammar/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4)
- [Trino 소스: PushdownFilterIntoWindow.java](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushdownFilterIntoWindow.java)
- [Spark 4.3.0: Window Functions](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Spark 4.3.0: SELECT (QUALIFY, WINDOW)](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html)
- [Spark 4.2.0 release notes (SPARK-31561 QUALIFY)](https://spark.apache.org/releases/spark-release-4-2-0.html)
- [Spark 소스: WindowResolution.scala (기본 프레임)](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/WindowResolution.scala)
- [Spark 소스: WindowExecBase.scala (requiredChildDistribution)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowExecBase.scala)
- [SQLite: Window Functions](https://www.sqlite.org/windowfunctions.html)

## English summary

A window function is read in three parts: the partition, the order inside it, and the frame. With an ORDER BY and no frame, both Trino and Spark default to RANGE from the start to the current row's last peer, so tied rows share a running total and LAST_VALUE returns the current row. ROW_NUMBER, RANK and DENSE_RANK differ only on ties, and my year-end recap jobs added a popularity score as a second sort key to break play-count ties. Sessions come from LAG plus a running SUM of new-session flags, the same 30-minute rule my log API applied with a cookie at collection time. Engines shuffle by the partition key and sort, so one huge key becomes one long task, and splitting it with a finer key changes what the window means.
