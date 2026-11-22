---
title: "더하고 다시 돌릴 수 있게 지표 테이블의 한 행 정하기"
date: 2026-11-20 05:03:00 +0900
categories: [Transformation & Modeling, Data Modeling]
tags: [data-modeling, kimball, mysql, iceberg, idempotency, concept]
description: "일간 활성 사용자 7행을 더하면 주간 수치와 어긋나는 이유를 출발점으로, 더하고 다시 돌릴 수 있도록 지표 테이블의 한 행(grain)을 정하는 기준을 정리한다."
---

일간 활성 사용자 테이블에서 월요일부터 일요일까지 7행을 더하면 주간 활성 사용자와 같거나 큰 숫자가 나오고, 이틀 이상 들어온 사용자가 한 명이라도 있으면 커진다. 월요일과 화요일에 모두 들어온 사람이 두 번 세어지기 때문이다. 쿼리는 에러 없이 돌고, 숫자만 틀린다. 지표 테이블에서 생기는 이런 실수는 대부분 한 행이 무엇을 뜻하는지, 그 행의 값을 더해도 되는지를 정하지 않은 데서 나온다.

[서비스 지표 배치 글](/posts/pyspark-indicator-batch/)에서 Hive 로그로 재생 수, 신규·기존 사용자 비율, 일간·주간 리텐션 같은 지표를 만들어 MySQL 지표 테이블에 넣었다. 이 글은 그런 테이블을 설계할 때 정해야 하는 것들을 그 배치에 비춰 적는다.

버전은 MySQL 8.4, Iceberg 1.12 기준이다.

## 한 행이 무엇인지부터 정한다

Kimball은 차원 모델링에서 grain 선언을 가장 중요한 단계로 둔다. grain은 테이블 한 행이 무엇을 뜻하는지에 대한 약속이다. 디멘전과 측정값은 grain을 정한 다음에 고르고, 모든 컬럼이 그 grain에 맞아야 한다. 서로 다른 grain을 한 테이블에 섞지 않는다.

예시로 지표 테이블 셋의 grain을 정하면 이렇다. 테이블과 컬럼 이름은 이 글에서 지어낸 것이다.

| 테이블 | 한 행 | unique 키 |
|---|---|---|
| 일간 재생 | 하루, 나라, 플랫폼 하나의 재생 집계 | `(dt, country, platform)` |
| 주간 활성 사용자 | 한 주, 플랫폼 하나의 활성 사용자 수 | `(week_start, platform)` |
| 주간 리텐션 | 한 주의 cohort, 플랫폼 하나의 잔존 집계 | `(week_start, platform)` |

grain이 섞이면 합계가 조용히 틀린다. 일간 행과 주간 행이 한 테이블에 있으면 기간으로 더할 때 같은 날이 두 번 들어간다. 플랫폼 컬럼에 `web`, `app`과 함께 전체를 뜻하는 `all` 행을 넣어도 마찬가지다. 플랫폼으로 묶지 않고 더하면 전체가 한 번 더 더해진다. 지표 배치는 같은 지표를 웹과 앱으로 나눠 보는 경우가 많았고, 그 조건을 query builder 메서드로 두었다. 웹·앱으로 나눈 지표라면 플랫폼이 grain의 일부이고, unique 키에도 들어가야 한다.

## 요약 테이블에 없는 축은 원천에서 센다

지표 테이블에서 `dt`, `country`, `platform`은 필터와 그룹에 쓰는 디멘전이고, 재생 수와 사용자 수는 팩트다. 로그 한 줄이 측정 사건 하나인 원천이 가장 낮은 grain이고, MySQL 지표 테이블은 그걸 날짜와 플랫폼으로 묶은 요약 테이블이다. 요약 grain은 빠른 대신 자주 묻는 질문을 미리 정해 둔 것이라, 테이블에 없는 축으로 잘라 보려면 원천으로 돌아가야 한다. 지표 배치에서도 대시보드에 없는 추출 요청은 Hive를 읽는 Zeppelin 노트북으로 처리했다.

## 더해도 되는 값과 안 되는 값

| 종류 | 예 | 더할 수 있는 방향 |
|---|---|---|
| additive | 재생 수, 좋아요 수 | 모든 디멘전 |
| semi-additive | 그날 기준 구독자 수 같은 스냅숏 | 나라·플랫폼으로는 되고 날짜로는 안 된다 |
| non-additive | 고유 사용자 수, 비율, 리텐션 | 사용자가 겹칠 수 있는 방향으로는 그대로 더하면 안 된다 |

고유 사용자 수는 합이 아니라 합집합이다. 사용자 A가 월·화에, B가 월요일에 들어왔다면 일간 값은 2와 1이고 합은 3이지만 주간 값은 2다. 플랫폼도 같다. 웹과 앱을 둘 다 쓰는 사람이 있으면 웹 사용자 수와 앱 사용자 수의 합은 전체보다 크다.

그래서 주간 고유 사용자 수는 일간 테이블에서 만들 수 없고, 원천에서 그 주 전체를 한 번에 센다.

```sql
-- 주간 활성 사용자: 일간 행을 더하지 않고 원천에서 7일을 한 번에 센다
SELECT DATE '2026-11-09' AS week_start,
       platform,
       COUNT(DISTINCT user_id) AS active_users
FROM events
WHERE dt BETWEEN DATE '2026-11-09' AND DATE '2026-11-15'
GROUP BY platform;
```

지표 배치에도 일간 리텐션과 주간 리텐션이 따로 있었고, 주의 시작·끝 계산은 공용 패키지에 있었다.

## 비율 대신 분자와 분모를 넣는다

비율은 non-additive다. 웹의 주간 리텐션이 100명 중 10명(10%), 앱이 300명 중 90명(30%)이라면 두 비율의 평균은 20%지만 실제 값은 400명 중 100명, 25%다. 비율만 저장하면 사람 수에 따른 가중치가 사라져서, 위로 묶을 때 맞는 값을 다시 만들 수 없다.

Kimball의 방법은 비율을 이루는 additive한 구성 요소를 저장하고, 그것들을 먼저 더한 다음 마지막에 나누는 것이다. 리텐션이라면 `cohort_users`와 `retained_users`를 넣고, 읽을 때 `SUM(retained_users) / SUM(cohort_users)`로 계산한다.

다만 분자와 분모도 고유 사용자 수라서, 묶는 그룹끼리 사용자가 겹치지 않을 때만 더할 수 있다. 웹과 앱 cohort에 같은 사람이 있으면 위 합계는 그 사람을 두 번 센다. 플랫폼을 가리지 않은 전체 리텐션이 필요하면 그 grain의 행을 따로 원천에서 만들어 다른 테이블에 둔다.

## 다시 돌려도 한 벌만 남는 키

grain을 정하면 unique 키가 정해진다. 이 키가 있어야 같은 날짜를 다시 돌렸을 때 행이 두 벌이 되지 않는다.

```sql
CREATE TABLE weekly_retention (
  week_start     DATE        NOT NULL,
  platform       VARCHAR(16) NOT NULL,
  cohort_users   BIGINT      NOT NULL,
  retained_users BIGINT      NOT NULL,
  UNIQUE KEY uk_week_platform (week_start, platform)
);

INSERT INTO weekly_retention (week_start, platform, cohort_users, retained_users)
VALUES ('2026-11-09', 'app', 300, 90) AS new
ON DUPLICATE KEY UPDATE
  cohort_users   = new.cohort_users,
  retained_users = new.retained_users;
```

MySQL 8.4에서는 새 행을 `VALUES()` 함수 대신 `AS new` 같은 행 별칭으로 가리킨다. `VALUES()`는 deprecated이고 나중에 빠질 수 있다. unique 인덱스가 여러 개인 테이블에서는 이 문장이 그중 한 행만 갱신하므로, 키를 grain 하나로 두는 편이 안전하다.

같은 결과를 내는 방법은 여럿이고, 무엇이 남는지가 다르다.

| 방식 | 다시 돌리면 | 대가 |
|---|---|---|
| 키 기준 delete-then-insert | 새 결과와 키가 겹치는 행을 지우고 넣는다 | 두 문장이 한 트랜잭션이 아니면 중간 실패 때 그 키가 빈다 |
| `INSERT ... ON DUPLICATE KEY UPDATE` | 겹치는 행의 지정한 컬럼만 바꾼다 | UPDATE 절에 없는 컬럼은 이전 값이 남는다 |
| `REPLACE` | 겹치는 행을 지우고 새로 넣는다 | 넣지 않은 컬럼은 이전 값이 아니라 기본값이 된다 |
| 이미 있으면 건너뛰기 | 기존 행을 그대로 둔다 | 값이 바뀌어도 갱신되지 않는다 |
| Iceberg `MERGE INTO` | 키가 맞으면 UPDATE, 없으면 INSERT | 원천 한 행만 대상 한 행을 바꿀 수 있고, 아니면 에러다 |

지표 배치에서는 공용 MySQL insert가 unique 키로 겹치는 행을 먼저 지우고 넣었다. 그래서 백필 때 날짜 범위만 넓혀 같은 job을 돌려도 행이 두 번 들어가지 않았고, 백필용 코드를 따로 둘 필요가 없었다. 광고 리포트 배치의 Spring Batch Writer는 저장 전에 이미 있는 내역을 걸러 낸다. 실행 기록을 DB에 남기지 않는 대신 같은 날짜를 다시 돌려도 겹쳐 쌓이지 않게 한 선택이고, 바뀐 값은 기존 행에 반영되지 않는다.

앞의 세 방식과 MERGE는 모두 새 결과에 있는 키만 건드린다. 다시 돌린 결과에서 어떤 플랫폼 값이 빠졌다면, 그 키의 옛 행은 그대로 남는다.

```sql
MERGE INTO db.weekly_retention t
USING staged_weekly_retention s
ON t.week_start = s.week_start AND t.platform = s.platform
WHEN MATCHED THEN UPDATE SET
  t.cohort_users = s.cohort_users,
  t.retained_users = s.retained_users
WHEN NOT MATCHED THEN INSERT *;
```

## 날짜로 파티션을 나누면 다시 돌리는 범위가 작아진다

옛 행까지 정리하려면 키가 아니라 날짜 파티션을 통째로 바꾼다. `INSERT OVERWRITE ... PARTITION (dt = ...)`는 그 파티션의 기존 행을 모두 새 결과로 바꾸고, Iceberg에서는 Spark 기본 모드가 static이라 `PARTITION` 절 없이 쓰면 테이블 전체가 바뀌므로 dynamic overwrite나 MERGE를 쓴다.

처리 날짜를 DAG run에서 받아 그 파티션만 덮어쓰는 방법은 [멱등한 Airflow DAG 글](/posts/airflow-idempotent-dags/)에 적었다. 지표 배치 job도 날짜 범위를 받아 하루씩 돌았으므로 다시 돌리는 단위가 하루였다.

## 참고 자료

- [Kimball: Grain](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/)
- [Kimball: Fact Table Structure](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/fact-table-structure/)
- [Kimball: Dimension Table Structure](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/dimension-table-structure/)
- [Kimball: Additive, Semi-Additive, and Non-Additive Facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/additive-semi-additive-non-additive-fact/)
- [Kimball: Aggregate Fact Tables or Cubes](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/aggregate-fact-table-cube/)
- [MySQL 8.4: INSERT ... ON DUPLICATE KEY UPDATE](https://dev.mysql.com/doc/refman/8.4/en/insert-on-duplicate.html)
- [MySQL 8.4: REPLACE](https://dev.mysql.com/doc/refman/8.4/en/replace.html)
- [Iceberg: Spark Writes (MERGE INTO, INSERT OVERWRITE)](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Spark SQL: INSERT TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)

## English summary

A metric table needs a declared grain first: what one row means, and which columns form its unique key. Counts can be summed, but distinct users and ratios cannot, so a week of daily active users overcounts weekly active users whenever someone comes back on a second day, and a ratio should be stored as its numerator and denominator. In my indicator batches, daily and weekly retention were separate metrics with shared week start and end helpers, and I kept reruns safe with a shared insert that deleted rows overlapping the unique key before inserting. Upserts and key-based deletes touch only the keys in the new result, so a stale key stays; overwriting a whole date partition clears it, at the cost of rewriting more data.
