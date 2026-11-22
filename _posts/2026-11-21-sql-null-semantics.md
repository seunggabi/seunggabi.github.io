---
title: "NULL 하나가 SQL 결과를 조용히 바꾸는 곳들"
date: 2026-11-21 06:11:00 +0900
categories: [Batch & Query, SQL]
tags: [sql, null, spark, trino, postgresql, mysql, sqlite, concept]
description: "NOT IN은 NULL이 한 행만 있어도 빈 결과를 내고 <> 조건은 NULL 행을 함께 뺀다. 3값 논리가 SQL 결과를 조용히 바꾸는 지점을 여러 엔진 기준으로 정리한다."
---

`WHERE status <> 'refunded'`는 환불 주문만 빼는 것처럼 보이지만 `status`가 NULL인 주문도 함께 뺀다. `WHERE id NOT IN (SELECT user_id FROM orders)`는 `user_id`에 NULL이 한 행만 있어도 빈 결과를 낸다. 두 쿼리 모두 에러 없이 끝난다. SQL은 NULL과의 비교를 참도 거짓도 아닌 UNKNOWN으로 두고, 절마다 UNKNOWN을 다르게 다룬다.

버전은 Spark 4.3.0, Trino 484, PostgreSQL 19, MySQL 9.7 LTS, SQLite 3.50.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 3값 논리 | 조건식은 TRUE, FALSE, UNKNOWN 중 하나다. NULL과 비교하면 UNKNOWN |
| UNKNOWN 처리 | `WHERE`, `ON`, `CASE WHEN`은 TRUE만 통과시키고, `CHECK` 제약은 UNKNOWN도 통과시킨다 |
| `NOT IN` | 목록에 NULL이 있으면 어떤 값에도 TRUE가 되지 않는다 |
| `NOT EXISTS`, `LEFT ANTI JOIN` | 맞는 행이 있는지만 본다. 서브쿼리의 NULL에 흔들리지 않는다 |
| `COUNT(*)`, `COUNT(col)` | 행 수와 NULL이 아닌 값의 수 |
| `SUM`, `AVG` | NULL을 빼고 계산한다. 입력이 모두 NULL이거나 비어 있으면 NULL |
| NULL 그룹 | `GROUP BY`, `DISTINCT`, `UNION`은 NULL끼리 같은 값으로 묶는다 |
| null-safe 비교 | `IS [NOT] DISTINCT FROM`, Spark와 MySQL의 `<=>` |
| NULL 정렬 | 기본 위치가 엔진마다 다르다. `NULLS FIRST`, `NULLS LAST`로 고정한다 |
| ANSI 모드 | Spark 4.0부터 기본. 잘못된 캐스트가 NULL 대신 에러를 낸다 |

## 비교는 TRUE, FALSE, UNKNOWN 세 값을 낸다

`NULL = NULL`도 `1 <> NULL`도 UNKNOWN이다. 논리 연산은 결과가 확정될 때만 UNKNOWN을 벗어난다. `FALSE AND UNKNOWN`은 FALSE, `TRUE OR UNKNOWN`은 TRUE이고, `NOT UNKNOWN`은 여전히 UNKNOWN이다. 그래서 `WHERE cond`와 `WHERE NOT cond`의 결과를 합쳐도 전체 행이 나오지 않는다. 빠진 행은 `cond`가 UNKNOWN인 행이다.

`CASE`에서도 같은 일이 생긴다. `CASE status WHEN NULL THEN 'none' END`는 `status = NULL`을 비교하므로 어떤 행에도 맞지 않는다. `CASE WHEN status IS NULL THEN ...`으로 쓴다. `CASE WHEN amount > 0 THEN 'pos' ELSE 'other' END`의 `other`에는 0과 음수에 더해 NULL도 들어간다. 반대로 `CHECK (amount > 0)`은 NULL을 막지 못한다. PostgreSQL은 검사식이 TRUE이거나 NULL이면 제약을 만족한 것으로 본다. NULL을 막으려면 `NOT NULL`을 따로 건다.

## 작은 테이블로 돌려 본 결과

아래 스크립트는 Python 3.14.7의 내장 `sqlite3`(SQLite 3.50.4)로 실행했다. 주문 12번은 `user_id`가, 11번은 `amount`가, 13번은 `status`가 NULL이다.

```python
import sqlite3

con = sqlite3.connect(":memory:")
con.executescript("""
CREATE TABLE users (id INT); INSERT INTO users VALUES (1), (2), (3);
CREATE TABLE orders (id INT, user_id INT, amount INT, status TEXT);
INSERT INTO orders VALUES (10, 1, 100, 'paid'), (11, 1, NULL, 'paid'),
  (12, NULL, 50, 'paid'), (13, 2, 70, NULL);
""")
lj = "SELECT u.id, o.id FROM users u LEFT JOIN orders o ON o.user_id = u.id"
queries = {
    "not_in": "SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM orders)",
    "not_exists": "SELECT id FROM users u WHERE NOT EXISTS "
                  "(SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    "status_ne": "SELECT count(*) FROM orders WHERE status <> 'refunded'",
    "aggs": "SELECT count(*), count(amount), sum(amount), avg(amount), "
            "sum(CASE WHEN amount IS NULL THEN amount END) FROM orders",
    "left_where": lj + " WHERE o.status = 'paid' ORDER BY u.id",
    "left_on": lj + " AND o.status = 'paid' ORDER BY u.id",
    "group_by": "SELECT user_id, count(*) FROM orders GROUP BY user_id",
    "null_safe": "SELECT NULL = NULL, NULL IS NOT DISTINCT FROM NULL, 1 IS DISTINCT FROM NULL",
}
for name, sql in queries.items():
    print(f"{name:10} {con.execute(sql).fetchall()}")
```

```text
not_in     []
not_exists [(3,)]
status_ne  [(3,)]
aggs       [(4, 3, 220, 73.33333333333333, None)]
left_where [(1, 10), (1, 11)]
left_on    [(1, 10), (1, 11), (2, None), (3, None)]
group_by   [(None, 1), (1, 2), (2, 1)]
null_safe  [(None, 1, 1)]
```

주문이 없는 사용자 3을 찾는 두 쿼리가 다른 답을 냈고, 주문 4건 중 `status <> 'refunded'`를 통과한 것은 3건이다.

## NOT IN과 NOT EXISTS가 갈리는 지점

`3 NOT IN (1, 1, NULL, 2)`는 `3 <> 1 AND 3 <> 1 AND 3 <> NULL AND 3 <> 2`로 풀린다. 마지막 비교가 UNKNOWN이라 전체가 잘해야 UNKNOWN이 되고, `WHERE`는 그 행을 버린다. 바깥 값이 NULL일 때도 서브쿼리에 행이 하나라도 있으면 같은 이유로 빠진다. 서브쿼리가 비어 있을 때만 TRUE다.

`NOT EXISTS`는 상관 조건 `o.user_id = u.id`에 맞는 행이 있는지만 본다. `user_id`가 NULL인 주문은 어떤 사용자와도 맞지 않을 뿐 다른 사용자의 결과를 지우지 않는다. Spark의 `LEFT ANTI JOIN`도 같은 뜻이다. `NOT IN`을 쓸 이유가 있으면 서브쿼리에 `WHERE user_id IS NOT NULL`을 붙인다. 실행 계획도 다르다. Spark는 `NOT IN` 서브쿼리를 "NULL이 하나라도 섞이면 버린다"는 조건이 붙은 null-aware anti join으로 바꾼다. 조건에 `OR IS NULL`이 들어가 일반 equi-join이 아니어서, Spark 소스 주석대로 대개 broadcast nested loop join이 된다. 컬럼 하나짜리 `NOT IN`만 해시 조회로 바꾸는 최적화(`spark.sql.optimizeNullAwareAntiJoin`, 기본 true)가 있다. `NOT EXISTS`는 처음부터 해시 키가 있는 anti join이다.

## 집계는 NULL을 빼고 센다

`aggs` 줄에서 `count(*)`는 4, `count(amount)`는 3이다. `sum`은 NULL을 건너뛴 220이고, `avg`는 220을 4가 아닌 3으로 나눈 73.3이다. NULL을 0으로 봐야 하는 지표라면 `AVG(COALESCE(amount, 0))`처럼 의도를 적어야 분모가 맞는다. 마지막 칸처럼 입력이 모두 NULL인 `SUM`은 0이 아니라 NULL이다. 대상 행이 하나도 없을 때도 `COUNT`는 0, `SUM`, `AVG`, `MAX`는 NULL을 돌려준다. 일별 합계를 다른 값과 더하는 쿼리에서는 `COALESCE(SUM(x), 0)`으로 받아야 합계 전체가 NULL이 되지 않는다. [퍼널 분석 글](/posts/funnel-analysis/)은 이 성질을 그대로 쓴다. 다음 단계 시각을 `LEFT JOIN`과 `MIN`으로 붙이고, 닿지 못한 사용자는 그 칸이 NULL로 남으므로 `COUNT(t2)`, `COUNT(t3)`가 곧 단계별 사용자 수가 된다.

그룹과 중복 제거에서는 NULL끼리 같은 값이다. `group_by` 줄에서 `user_id`가 NULL인 주문은 한 그룹으로 묶였다. `DISTINCT`와 `UNION`, `INTERSECT`, `EXCEPT`도 NULL을 null-safe하게 비교한다. 예외가 `COUNT(DISTINCT col)`로, NULL은 고유값 개수에 들어가지 않는다.

## 조인 키의 NULL은 맞지 않고 한곳에 모인다

`ON a.k = b.k`에서 양쪽 키가 모두 NULL이어도 비교는 UNKNOWN이라 맞지 않는다. inner join이면 그 행은 사라지고, outer join이면 반대편이 NULL인 채로 남는다. outer join의 조건을 어디에 두느냐도 결과를 바꾼다. `left_where`는 `WHERE o.status = 'paid'`가 `LEFT JOIN`으로 채운 NULL 행을 다시 버려서 사용자 2, 3이 사라졌다. 결과로는 inner join과 같고, Spark 옵티마이저의 `EliminateOuterJoin` 규칙도 이런 NULL 거부 조건을 보면 실제로 inner join으로 바꾼다. 오른쪽 테이블을 거르는 조건은 `left_on`처럼 `ON`에 둔다. 퍼널 글의 단계 쿼리도 이벤트 이름과 시간 창 조건을 모두 `ON`에 둔다. [연말 결산 잡 글](/posts/year-end-recap-spark/)의 월별 곡 항목은 사용자 목록에 `RIGHT OUTER JOIN`을 하고, 값이 없는 칸에는 `NVL`로 빈 문자열을 넣었다. 저장 함수가 NULL은 건너뛰고 빈 문자열은 쓰기 때문에, 다시 돌려도 지난 값이 남지 않는다.

맞지 않는 NULL 키도 셔플 비용은 낸다. Spark는 inner join이면 조인 키에서 `isnotnull` 필터를 양쪽에 추론해 셔플 전에 NULL 키 행을 버린다. `LEFT OUTER JOIN`은 왼쪽 행이 결과에 남아야 하므로 오른쪽에만 필터를 넣는다. 셔플 해시는 NULL 컬럼을 건너뛰고 seed 값을 그대로 쓰기 때문에, 키가 NULL인 왼쪽 행은 전부 같은 셔플 파티션으로 간다. 비로그인 이벤트처럼 NULL 키가 많으면 이 파티션 하나가 skew가 된다. AQE skew join은 `LEFT OUTER`의 왼쪽을 쪼갤 수 있고, 판정 조건은 [Spark 셔플과 skew 글](/posts/spark-shuffle-and-skew/)에 있다. 판정에 걸리지 않으면 NULL 키 행을 따로 떼어 조인 없이 `UNION ALL`로 붙이는 방법이 있다.

## null-safe 비교와 MERGE 키

| 엔진 | 같다 | 다르다 |
|---|---|---|
| Trino, PostgreSQL | `a IS NOT DISTINCT FROM b` | `a IS DISTINCT FROM b` |
| Spark | `a <=> b`, `a IS NOT DISTINCT FROM b` | `NOT (a <=> b)`, `a IS DISTINCT FROM b` |
| MySQL | `a <=> b` | `NOT (a <=> b)` |
| SQLite | `a IS b`, `a IS NOT DISTINCT FROM b` | `a IS NOT b`, `a IS DISTINCT FROM b` |

Spark는 `<=>` 조인도 해시나 sort-merge 조인의 키로 쓴다. 키를 `coalesce(k, 타입 기본값)`과 `isnull(k)` 한 쌍으로 바꿔서 비교하기 때문이다. 그래서 NULL 키끼리 맞게 되지만, NULL 키 행이 한 파티션에 모이는 점은 그대로다.

`MERGE INTO t USING s ON t.k = s.k`에서 원천 키가 NULL이면 대상의 어떤 행과도 맞지 않아 `WHEN NOT MATCHED`로 간다. 실행할 때마다 같은 행이 다시 INSERT되고 대상에 NULL 키 행이 쌓인다. 에러도 없고, 대상 한 행에 원천 여러 행이 맞을 때 나는 카디널리티 검사에도 걸리지 않는다. 키가 NULL일 수 있으면 `ON t.k <=> s.k`(Trino, PostgreSQL은 `IS NOT DISTINCT FROM`)로 쓰거나, 적재 전에 대리 키로 채우거나, NULL 키 행을 따로 걸러 둔다. null-safe로 바꾸면 원천의 NULL 키 행 둘이 대상 한 행에 맞을 수 있으니, 원천을 키마다 한 행으로 줄이는 단계는 [증분 적재와 MERGE 글](/posts/incremental-load-and-merge/)대로 둔다. PostgreSQL의 `UNIQUE` 제약도 기본으로 NULL끼리 다르다고 보므로 NULL 키 중복을 막지 못한다. PostgreSQL 15부터 `UNIQUE NULLS NOT DISTINCT`로 바꿀 수 있다.

## NULL 정렬 위치는 엔진마다 다르다

| 엔진 | `ASC` 기본 | `DESC` 기본 | `NULLS FIRST/LAST` |
|---|---|---|---|
| Spark | 앞 | 뒤 | 있음 |
| Trino | 뒤 | 뒤 | 있음 |
| PostgreSQL | 뒤 | 앞 | 있음 |
| MySQL | 앞 | 뒤 | 없음. `ORDER BY col IS NULL, col`로 흉내 낸다 |
| SQLite | 앞 | 뒤 | 있음 |

PostgreSQL은 NULL을 가장 큰 값처럼, MySQL과 SQLite는 가장 작은 값처럼 다루고, Trino는 방향과 상관없이 끝에 둔다. 같은 `ORDER BY amount DESC LIMIT 10`이 PostgreSQL에서는 NULL 금액부터 돌려준다. 윈도 함수의 `ORDER BY`도 같은 기본값을 따르므로 `ROW_NUMBER() ... = 1`로 고르는 행이 엔진을 옮기면 바뀔 수 있다. 동점과 정렬 키 이야기는 [윈도 함수 글](/posts/sql-window-functions/)에 있다. 엔진을 옮길 쿼리에는 `NULLS LAST`를 명시한다.

## Spark 4는 캐스트 실패를 NULL로 숨기지 않는다

Spark 4.0부터 `spark.sql.ansi.enabled`가 기본 true다. 3.x까지는 `CAST('a' AS INT)`가 NULL을 돌려줬고 정수 overflow는 값이 돌아 음수가 됐다. 4.x에서는 각각 `CAST_INVALID_INPUT`, `ARITHMETIC_OVERFLOW` 에러로 쿼리가 실패한다. 0으로 나누기와 배열 범위를 벗어난 인덱스도 에러다.

3.x에서 "캐스트하고 NULL이면 버린다"로 짠 잡은 4.x로 올리면 잘못된 행 하나에 실패한다. NULL을 받겠다면 `try_cast`, `try_divide`, `try_element_at`처럼 `try_` 함수로 의도를 적는다. Trino도 `CAST`는 실패하고 `try_cast`가 NULL을 돌려준다. 예전 동작은 `spark.sql.ansi.enabled=false`나 환경 변수 `SPARK_ANSI_SQL_MODE=false`로 되돌릴 수 있지만, 그러면 잘못된 입력이 다시 NULL 속으로 숨는다. `try_cast` 뒤에 생긴 NULL 개수를 따로 세면 입력 품질을 숫자로 볼 수 있다.

## 참고 자료

- [Spark 4.3.0: NULL Semantics](https://spark.apache.org/docs/latest/sql-ref-null-semantics.html)
- [Spark 4.3.0: ANSI Compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)
- [Spark 4.3.0: Migration Guide (3.5 to 4.0, ANSI 기본값)](https://spark.apache.org/docs/latest/sql-migration-guide.html)
- [Spark 4.3.0: ORDER BY Clause](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-orderby.html)
- Spark 4.3.0 소스: [subquery.scala (NOT IN)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/subquery.scala), [Optimizer.scala (InferFiltersFromConstraints)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala), [joins.scala (EliminateOuterJoin)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala), [patterns.scala (EqualNullSafe)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/planning/patterns.scala), [OptimizeSkewedJoin.scala](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala)
- Trino 484: [Comparison operators](https://trino.io/docs/current/functions/comparison.html), [SELECT (ORDER BY)](https://trino.io/docs/current/sql/select.html), [Conversion functions](https://trino.io/docs/current/functions/conversion.html)
- PostgreSQL 19: [Sorting Rows](https://www.postgresql.org/docs/current/queries-order.html), [Constraints](https://www.postgresql.org/docs/current/ddl-constraints.html)
- MySQL 9.7 LTS: [Working with NULL Values](https://dev.mysql.com/doc/refman/9.7/en/working-with-null.html), [Comparison Operators](https://dev.mysql.com/doc/refman/9.7/en/comparison-operators.html)
- [SQLite: SELECT (NULLS FIRST/LAST)](https://www.sqlite.org/lang_select.html)

## English summary

SQL compares NULL as UNKNOWN, and WHERE, ON and CASE keep only TRUE, so a filter like status <> 'refunded' quietly drops rows with a NULL status. NOT IN returns nothing once the subquery holds a NULL, while NOT EXISTS and LEFT ANTI JOIN only check for a matching row. Aggregates skip NULLs, SUM over only NULLs is NULL, and the funnel query in my earlier post relies on COUNT(col) to count users who reached each step. NULL join keys never match, yet in a Spark left outer join they still hash to one shuffle partition, and a MERGE on a nullable key inserts the same row on every run unless the key comparison is null-safe. Spark 4 turns failed casts into errors by default, which trades silent NULLs for failed jobs, and try_cast keeps the old behavior only where I ask for it.
