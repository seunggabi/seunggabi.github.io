---
title: "쿼리 하나가 1,001번이 되는 N+1 문제"
date: 2026-11-22 12:50:00 +0900
categories: [Batch & Query, SQL]
tags: [n-plus-one, orm, jpa, hibernate, sqlalchemy, django, sql, spark, concept]
---

```text
SELECT id, name FROM campaign WHERE status = 'ACTIVE'            -- 1번
SELECT * FROM ad_group WHERE campaign_id = 101                    -- 캠페인마다 1번
SELECT * FROM ad_group WHERE campaign_id = 102
SELECT * FROM ad_group WHERE campaign_id = 103
... (997줄 더)
```

캠페인 1,000개와 각 캠페인의 광고 그룹을 내보내는 스크립트의 쿼리 로그가 이런 모양이면 N+1이다. 목록을 가져오는 쿼리 1번에, 목록의 행 N개마다 연관 데이터를 가져오는 쿼리가 1번씩 붙는다. 쿼리 하나하나는 인덱스를 타고 1~2 ms에 끝나고, MySQL `long_query_time` 기본값이 10초라 슬로 쿼리 로그에는 아무것도 남지 않는다. 그런데 왕복이 1,001번이라, DB와 앱 사이 왕복 시간이 1 ms만 돼도 쿼리 실행 시간과 별개로 1초가 더 붙는다. 캠페인이 10만 개가 되면 같은 코드가 몇 분짜리가 된다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| N+1 | 목록 쿼리 1번 뒤에 목록 행마다 쿼리를 1번씩 더 보내는 접근 패턴 |
| lazy loading | 연관 객체를 처음 접근하는 순간 쿼리로 읽어 오는 ORM 동작 |
| fetch join | 연관 데이터를 조인으로 한 쿼리에 같이 읽는 방법 |
| batch fetch | 연관 데이터를 부모 키 여러 개의 `IN` 목록으로 묶어 읽는 방법 |
| raiseload | 지연 로딩이 일어나면 쿼리 대신 예외를 내게 하는 SQLAlchemy 설정 |

## ORM은 같은 일을 속성 접근 뒤에 숨긴다

손으로 쓴 루프라면 쿼리가 반복문 안에 보인다. ORM에서는 `campaign.ad_groups`라는 속성 접근이 쿼리가 된다.

```python
# SQLAlchemy 2.x. relationship의 lazy 기본값은 "select"
campaigns = session.scalars(select(Campaign).where(Campaign.status == "ACTIVE")).all()
for c in campaigns:
    rows.extend(to_row(c, g) for g in c.ad_groups)   # 여기서 캠페인마다 SELECT 1번
```

SQLAlchemy의 relationship 기본 로딩 전략은 `select`, 곧 접근할 때 따로 읽는 방식이다. Django도 `campaign.adgroup_set.all()`을 루프에서 부르면 같은 일이 생긴다. JPA는 `@ManyToOne`과 `@OneToOne`의 기본 fetch가 `EAGER`인데, 즉시 로딩이 N+1을 막아 주지는 않는다. Hibernate에서 JPQL로 `select g from AdGroup g`를 실행하면 광고 그룹 목록을 먼저 읽고, 조인으로 가져오지 않은 `EAGER` 연관 `campaign`은 서로 다른 캠페인 id마다 추가 쿼리로 채운다. 쿼리가 언제 나가는지만 바뀐다.

## 횟수로 찾는다

그래서 시간이 아니라 실행 횟수를 봐야 한다.

| 어디서 | 무엇을 본다 |
|---|---|
| PostgreSQL | `pg_stat_statements`의 `calls`. `campaign_id = 101`과 `= 102`가 한 줄로 정규화되므로, 그 쿼리의 `calls`가 작업 실행 횟수보다 몇 자리 크면 의심한다 |
| MySQL | `performance_schema.events_statements_summary_by_digest`의 `COUNT_STAR` |
| SQLAlchemy | `create_engine(..., echo=True)`로 쿼리 로그를 보고, 테스트에서는 `lazy="raise"`나 `raiseload()`로 지연 로딩을 예외로 바꾼다 |
| Django | 테스트에서 `assertNumQueries(n)`로 쿼리 수를 고정한다 |
| Hibernate | `hibernate.generate_statistics`로 세션별 쿼리 수를 남긴다 |

데이터 3건짜리 테스트에서 N+1은 쿼리 4번이라 시간으로는 드러나지 않지만, 쿼리 수 검사는 바로 실패한다. 그래서 N+1은 테스트의 쿼리 수 검사로 잡는 것이 가장 이르고 싸다.

## 고치는 방법은 조인과 IN 묶음이다

| 방법 | 쿼리 수 | SQLAlchemy | Django | JPA·Hibernate |
|---|---|---|---|---|
| 조인으로 한 번에 | 1 | `joinedload()` | `select_related()` | `join fetch`, entity graph(Spring Data의 `@EntityGraph`) |
| 부모 키를 IN으로 묶어 | 1 + 묶음 수 | `selectinload()` | `prefetch_related()` | `@BatchSize`, `hibernate.default_batch_fetch_size` |

조인 방식은 왕복이 1번이지만 결과가 자식 행 수만큼 나오고 부모 컬럼이 그만큼 반복된다. 캠페인 1,000개에 광고 그룹이 평균 50개면 캠페인 컬럼이 5만 행에 반복돼 돌아온다. 자식 컬렉션 두 개를 함께 조인하면 부모마다 두 컬렉션의 곱만큼 행이 생긴다. Django의 `select_related()`가 외래 키와 일대일 같은 단일 값 관계에만 쓰이는 것도 이 때문이다.

IN 묶음 방식은 부모 쿼리 뒤에 `WHERE campaign_id IN (...)` 쿼리를 따로 보낸다. 행이 불어나지 않고, 자식 컬렉션이 여럿이어도 컬렉션마다 쿼리 하나씩이다. SQLAlchemy `selectinload()`는 부모 키를 최대 500개씩 끊어 보내고, Django `prefetch_related()`는 관계마다 쿼리를 한 번 더 보낸 뒤 파이썬에서 붙인다. 일대다 컬렉션에는 대개 이쪽이 맞고, 다대일·일대일처럼 행마다 붙는 대상이 하나뿐인 관계는 조인이 맞다.

## 컬렉션 조인에 LIMIT을 걸면 조인된 행이 잘린다

컬렉션을 조인으로 가져오면서 `LIMIT`을 걸면 SQL의 `LIMIT`이 부모가 아니라 조인된 행을 자른다. 캠페인 20개를 원했는데 조인된 20행이 돌아와 앞쪽 캠페인 몇 개와 잘린 광고 그룹 목록만 남는다. SQLAlchemy `joinedload()`는 `LIMIT`이 있으면 원래 쿼리를 서브쿼리로 감싸고 그 바깥에서 조인해 이 문제를 피한다. Hibernate는 이 조합에서 SQL에 `LIMIT`을 넣지 않고 전체를 읽은 뒤 메모리에서 자르며 경고를 남긴다. `hibernate.query.fail_on_pagination_over_collection_fetch`를 켜면 경고 대신 예외가 나서, 운영에서 조건에 맞는 결과 전체를 메모리로 올리는 일을 미리 막는다.

해법은 두 단계로 나누는 것이다. 먼저 부모 키만 페이지로 고르고, 그 키로 부모와 자식을 읽는다.

```sql
-- 1단계: 부모 키 20개. InnoDB에서 INDEX (status)면 테이블을 읽지 않고 끝난다
SELECT id FROM campaign WHERE status = 'ACTIVE' ORDER BY id LIMIT 20;

-- 2단계: 고른 키로 부모와 자식을 한 번씩
SELECT * FROM campaign WHERE id IN (:ids);
SELECT * FROM ad_group WHERE campaign_id IN (:ids) ORDER BY campaign_id;
```

1단계가 인덱스만으로 끝나는 이유는 [커버링 인덱스 글](/posts/covering-index/)에 적었다.

## 파이프라인에도 같은 모양이 있다

ORM이 없어도 N+1은 생긴다. 원천 행마다 코드 테이블을 조회하는 파이썬 ETL, 이벤트마다 사용자 정보 API를 부르는 보강 단계, Spark UDF 안에서 행마다 JDBC 조회를 하는 잡이 다 같은 구조다. Spark에서는 동시에 도는 task 수만큼 병렬로 원천 DB에 단건 조회가 꽂힌다.

고치는 방향도 같다. 조회 대상이 작으면 한 번에 읽어 메모리에 둔다. Spark라면 차원 테이블을 DataFrame으로 읽어 broadcast join으로 붙이고, 파이썬이라면 `dict` 하나로 바꾼다. 조회 대상이 크면 그 테이블도 DataFrame으로 읽어 일반 조인으로 붙이는 것이 먼저다([Spark 조인 전략 글](/posts/spark-join-strategies/)). 원천을 통째로 읽을 수 없을 때만 `mapPartitions`로 파티션의 키를 모아 `IN` 쿼리 한 번씩으로 묻는다. API도 묶음 조회 엔드포인트가 있으면 그쪽을 쓴다.

## 묻고 답하기

Q. IN 목록에 키를 몇 개까지 넣어도 되나?

DB마다 한계가 다르다. PostgreSQL 확장 프로토콜은 바인드 파라미터 수를 16비트 정수로 보내므로 65,535개를 넘길 수 없다. MySQL은 쿼리 전체가 `max_allowed_packet`을 넘으면 안 되고, 서버 측 prepared statement는 자리표시자도 65,535개까지다. 한계에 닿기 전에도 목록이 길수록 파싱과 계획 비용이 늘고 실행 계획이 바뀔 수 있어서, 수백~수천 개 단위로 끊어 보내는 편이 안전하다. SQLAlchemy도 Oracle의 IN 목록 1,000개 제한 같은 백엔드 한계를 피하려고 500개씩 끊는다. PostgreSQL에서는 `WHERE campaign_id = ANY(:ids)`로 배열 하나를 넘기면 파라미터 수 한계를 피한다.

Q. 언제 지연 로딩을 그대로 둬도 되나?

목록 중 실제로 연관 데이터를 여는 행이 몇 개뿐이면 지연 로딩이 읽는 양이 적다. 화면에서 펼친 항목 몇 개만 자식을 보는 경우다. 연관 객체가 이미 2차 캐시에 있어 DB에 가지 않는 경우도 있다. 문제는 N이 데이터 크기를 따라 커지는 코드이고, 배치와 내보내기 작업은 거의 항상 이쪽에 든다.

## 참고 자료

- [SQLAlchemy 2.0, Relationship Loading Techniques](https://docs.sqlalchemy.org/en/20/orm/queryguide/relationships.html)
- [Django, QuerySet API reference (select_related, prefetch_related)](https://docs.djangoproject.com/en/stable/ref/models/querysets/), [Testing tools (assertNumQueries)](https://docs.djangoproject.com/en/stable/topics/testing/tools/)
- [Hibernate ORM 7.4 User Guide](https://docs.hibernate.org/orm/7.4/userguide/html_single/Hibernate_User_Guide.html)
- [PostgreSQL 19, pg_stat_statements](https://www.postgresql.org/docs/19/pgstatstatements.html), [MySQL 8.4, Statement Summary Tables](https://dev.mysql.com/doc/refman/8.4/en/performance-schema-statement-summary-tables.html), [The Slow Query Log](https://dev.mysql.com/doc/refman/8.4/en/slow-query-log.html)

## English summary

An N+1 problem is one query for a list and then one more query for each row in the list. Each query is fast, so the slow query log stays empty, but the round trips add up and grow with the data. ORMs hide it behind attribute access: SQLAlchemy loads relationships lazily by default, and JPA's EAGER default for many-to-one still runs extra selects after a JPQL query. It shows up in query counts, not timings, so pg_stat_statements calls or a query count assertion in tests catch it. The fix is a join for single-valued relations or an IN batch for collections, and for paged results the parent keys come first and the children are loaded by those keys. The same shape shows up in pipelines that look up a table or an API per row, and the fix is the same there too: ask once for many keys.
