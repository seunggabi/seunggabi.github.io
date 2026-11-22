---
title: "테이블은 언제 나누고 언제 합칠까"
date: 2026-11-21 05:24:00 +0900
categories: [Transformation & Modeling, Data Modeling]
tags: [data-modeling, normalization, kimball, star-schema, spark, iceberg, parquet, concept]
description: "고객 도시 같은 값이 주문 행마다 중복되면 갱신 이상이 생긴다. 정규화로 테이블을 나누는 기준과, 분석에서 스타 스키마나 OBT로 다시 합치는 기준을 정리한다."
---

주문 상세 테이블의 모든 행에 고객의 도시가 들어 있다고 하자. 고객이 이사하면 그 고객의 주문 행을 전부 고쳐야 하고, 일부만 고치면 같은 고객이 두 도시에 사는 것처럼 보인다. 반대로 도시를 고객 테이블에만 두면 분석 쿼리마다 수억 행짜리 주문과 고객을 조인한다. 어느 쪽을 고를지는 데이터를 얼마나 자주 고치는지, 어떤 모양으로 읽는지에 달려 있다.

버전은 Spark 4.3.0, Iceberg 1.12 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 정규화 | 한 사실을 한 곳에만 두도록 테이블을 나누는 일 |
| 1NF | 한 칸에 값 하나. 목록이나 중첩 레코드를 넣지 않는다 |
| 2NF | 키 컬럼 일부에만 의존하는 컬럼이 없다 |
| 3NF | 키가 아닌 다른 컬럼을 거쳐 키에 의존하는(이행) 컬럼이 없다 |
| 갱신 이상 | 중복된 값 중 일부만 바뀌어 서로 어긋나는 상태. 삽입·삭제 이상도 있다 |
| OLTP / OLAP | 키로 몇 행을 자주 고치는 작업 / 많은 행의 몇 컬럼을 훑어 집계하는 작업 |
| 스타 스키마 | 팩트 하나에 평평하게 편 차원 테이블들이 붙은 모양 |
| 스노우플레이크 | 차원 안의 계층을 다시 3NF로 나눈 모양 |
| OBT(one big table) | 차원 속성을 팩트 행에 미리 복사해 둔 넓은 테이블 |
| 중첩 타입 | `STRUCT`, `ARRAY`, `MAP`. 일대다 관계를 행 안에 담는다 |
| 그때 값 / 지금 값 | 사건 당시의 속성(as-was)으로 묶을지, 현재 속성(as-is)으로 묶을지 |

## 주문 테이블 하나로 보는 정규형 세 단계

`order_items(order_id, line_no, product_id, product_name, customer_id, customer_city, qty)`가 있고, 키는 `(order_id, line_no)`다.

- `product_ids = '100,200'`처럼 한 칸에 목록을 넣으면 1NF가 아니다. 상품마다 행을 나눈다.
- `customer_id`는 `line_no`와 상관없이 `order_id`만으로 정해진다. 키 일부에 의존하므로 2NF를 어긴다. 주문 단위 컬럼을 `orders`로 뺀다.
- `orders`로 옮긴 `customer_city`는 `order_id → customer_id → customer_city`로 정해진다. 이행 의존이므로 3NF를 어긴다. `customers`로 뺀다. `product_name`도 같은 이유로 `products`로 간다.

결과는 `orders`, `order_lines`, `customers`, `products` 네 테이블이다. 도시는 `customers`에 한 번만 있으니 이사는 한 행 UPDATE로 끝난다.

나누기 전에는 세 가지 이상이 생긴다. 도시를 일부 행만 고치면 값이 어긋나는 갱신 이상, 주문이 없는 고객의 도시는 기록할 곳이 없는 삽입 이상, 고객의 마지막 주문을 지우면 도시 정보도 사라지는 삭제 이상이다. Kimball도 ER 모델의 장점으로 고객 주소 변경이 주소 마스터 한 행 조회로 끝난다는 점을 든다.

## 쓰기가 잦은 쪽과 읽기가 넓은 쪽

OLTP는 사용자 요청마다 키로 몇 행을 읽고 고친다. 고치는 곳이 한 곳이어야 트랜잭션이 짧고, 갱신 이상을 데이터베이스 구조가 막는다. 그래서 정규화한다.

OLAP는 한 번 적재한 행을 많이 읽는다. 쿼리 하나가 수억 행의 컬럼 몇 개를 집계하고, 행을 고치는 주체는 사용자가 아니라 배치 파이프라인이다. 중복된 값이 어긋나지 않게 하는 책임이 스키마에서 적재 로직으로 옮겨 가는 대신, 읽을 때 조인이 줄어든다.

저장 방식도 이 차이를 따른다. 행 저장은 키로 찾은 한 행의 모든 컬럼을 같이 읽고, 컬럼 저장은 필요한 컬럼만 읽는다. 컬럼 파일이 어디서 건너뛰는지는 [컬럼 파일과 pruning 글](/posts/columnar-files-and-pruning/)에 있다. 여기서 중요한 것은 압축이다. Parquet 사전 인코딩은 column chunk마다 사전 페이지를 두고 값을 정수 ID로 바꿔 쓴다. 고객 도시처럼 종류가 적은 값을 주문 행마다 복사해도 행마다 몇 비트짜리 ID가 늘어나는 정도다. 사전이 너무 커지면 plain 인코딩으로 넘어가므로, 고유값이 많은 긴 문자열을 복사하면 저장 비용이 그대로 늘어난다.

## 스타, 스노우플레이크, OBT, 중첩 타입

| 모양 | 읽을 때 조인 | 차원 속성이 바뀌면 다시 쓰는 곳 | 1NF |
|---|---|---|---|
| 3NF | 많다 | 그 속성이 있는 한 행 | 지킨다 |
| 스노우플레이크 | 팩트-차원에 차원-차원까지 | 계층 테이블의 한 행 | 지킨다 |
| 스타 | 팩트-차원 한 단계 | 차원 테이블의 해당 행들 | 지킨다 |
| OBT | 없다 | 그 값을 복사한 팩트 행 전부 | 지킨다 |
| 중첩 타입 | 없다 | 그 값을 담은 행 전부 | 일부러 깬다 |

스타 스키마의 차원은 넓고 평평한 비정규화 테이블이다. 상품 → 카테고리 → 부서 같은 다대일 계층을 한 차원 테이블에 펴 둔다. 이 계층을 3NF로 다시 나누면 스노우플레이크가 된다. 담긴 정보는 같다. Kimball은 스노우플레이크를 피하라고 한다. 사용자가 따라가기 어렵고 쿼리 성능에 불리할 수 있으며, 반복 문자열을 코드로 바꿔 아끼는 공간은 무시할 만하다는 이유다. 차원이 다른 차원을 가리키는 outrigger는 예외로 드물게 허용한다. 팩트와 차원, SCD 타입은 [차원 모델링 글](/posts/dimensional-modeling-scd/)에 정리했다.

OBT는 한 단계 더 가서 차원 속성까지 팩트 행에 복사한다. 중첩 타입은 일대다 관계를 행 안에 넣는다. 주문 한 행에 고객 정보를 `STRUCT`로, 주문 상품 목록을 `ARRAY<STRUCT>`로 담으면 주문과 상품을 다시 맞추는 조인과 셔플이 없다.

```sql
-- Spark 4.3 spark-sql, 기본 카탈로그의 Parquet 테이블
CREATE TABLE orders_nested (
  order_id BIGINT,
  order_ts TIMESTAMP,
  customer STRUCT<customer_id: BIGINT, city: STRING>,
  items    ARRAY<STRUCT<product_id: BIGINT, product_name: STRING, qty: INT>>
) USING parquet;

INSERT INTO orders_nested VALUES
  (1, TIMESTAMP '2026-11-01 10:00:00',
   named_struct('customer_id', 7L, 'city', 'Seoul'),
   array(named_struct('product_id', 100L, 'product_name', 'cable', 'qty', 2),
         named_struct('product_id', 200L, 'product_name', 'charger', 'qty', 1)));

-- 도시와 상품별 수량: 배열을 행으로 펴서 묶는다
SELECT o.customer.city, i.product_name, SUM(i.qty) AS qty
FROM orders_nested o
LATERAL VIEW inline(o.items) i AS product_id, product_name, qty
GROUP BY o.customer.city, i.product_name;

-- 실행 계획의 ReadSchema에 customer.city만 남는다
EXPLAIN SELECT customer.city FROM orders_nested;
```

Spark는 Parquet과 ORC에서 쿼리에 필요 없는 중첩 필드를 읽지 않는다(`spark.sql.optimizer.nestedSchemaPruning.enabled`, 내부 설정, 기본 `true`). Iceberg는 중첩 필드에도 ID를 붙여 두어서, struct 안에 필드를 더하거나 이름을 바꿔도 기존 데이터가 엉뚱한 필드에 붙지 않는다. 대가도 있다. 배열 원소 하나를 고치려면 그 행 전체를 다시 써야 하고, 원소 수가 많아질 수 있는 관계를 배열에 넣으면 행 하나가 계속 커진다.

## 조인을 미리 해 둘 만한 경우

분산 엔진에서 조인 비용은 대부분 셔플이다. 차원이 작으면 Spark가 차원 전체를 executor마다 보내는 broadcast 조인을 고르고(`spark.sql.autoBroadcastJoinThreshold` 기본 10MB), 큰 팩트는 셔플 없이 지나간다. 그래서 차원이 작은 스타 스키마는 그대로 둬도 싼 경우가 많다. BigQuery도 스타 스키마는 이미 분석에 맞춘 모양이라 더 비정규화해도 성능이 눈에 띄게 나아지지 않을 수 있다고 본다. 조인 전략이 어떻게 정해지는지는 [Spark 조인 전략 글](/posts/spark-join-strategies/)에 있다.

미리 합칠 만한 경우는 따로 있다.

- 양쪽이 다 커서 매 쿼리가 셔플하는 조인. 수억 행 사용자 차원과 이벤트 팩트가 그렇다.
- 읽는 질문이 정해져 있고 응답 시간이 짧아야 하는 서빙 테이블.
- 함께 읽는 속성이 거의 바뀌지 않는 경우. 나라, 카테고리, 상품의 출시일 같은 값이다.

[연말 결산 Spark 잡 글](/posts/year-end-recap-spark/)이 두 번째 경우였다. 원천 로그를 먼저 `(user_id, track_id, cnt)`로 줄인 뒤 메타 테이블과 조인했고, 결과는 사용자 한 명이 HBase 한 행이 되도록 접어 넣었다. 조인은 배치에서 끝나고, API는 행 키 하나로 읽기만 한다. 새 질문이 생기면 배치를 바꿔야 하는 것이 그 대가다. [지표 테이블 grain 글](/posts/metric-table-grain/)의 요약 테이블도 날짜, 나라, 플랫폼 값으로 미리 묶어 둔 요약 테이블이다. 테이블에 없는 축으로 자르려면 원천으로 돌아가야 했다.

반대로 자주 바뀌는 속성은 차원에 남기고 조인한다. 멤버십 등급, 가격, 주문 상태를 넓은 테이블에 복사하면 바뀔 때마다 복사본을 전부 다시 써야 한다. Iceberg에서는 copy-on-write면 해당 행이 든 데이터 파일을, merge-on-read면 delete 파일과 바뀐 행의 새 데이터 파일을 쓴다. 정규화가 막아 주던 갱신 이상을 이제 파이프라인이 막아야 한다.

## 넓은 테이블에서 지난 숫자를 지키는 법

비정규화한 테이블에 속성을 복사할 때는 어느 시점의 값을 복사하는지부터 정한다.

| 복사하는 값 | 맞게 답하는 질문 | 다른 질문을 하려면 |
|---|---|---|
| 사건 당시 값 | 그때 유료였던 사용자의 재생 수 | 지금 등급으로 다시 묶으려면 차원과 다시 조인 |
| 적재 시점의 현재 값 | 적재 직후의 현재 기준 숫자 | 속성이 바뀔 때마다 과거 행까지 다시 써야 함 |

현재 값을 복사하면 Type 1 차원을 팩트에 굳혀 둔 것과 같다. Kimball은 Type 1로 덮어쓴 속성에 기대는 집계는 다시 계산해야 한다고 하는데, OBT는 테이블 전체가 그 집계다. 고치지 않은 파티션의 행과 고친 파티션의 행이 서로 다른 값으로 남는다.

사건 당시 값을 복사하면 적재할 때 Type 2 차원에서 사건 시각이 들어가는 행을 찾아 붙인다. 이 조인과 유효 기간 설계는 앞의 차원 모델링 글에 있다. 이때 속성 값만 복사하지 말고 차원의 대리 키도 같이 넣어 둔다. 키가 있으면 "지금 값으로 다시 보자"는 질문에 차원과 한 번 조인해 답할 수 있다. 키 없이 값만 남긴 넓은 테이블은 그 질문에 답하려면 원천부터 다시 만들어야 한다.

## 참고 자료

- [Kimball: A Dimensional Modeling Manifesto](https://www.kimballgroup.com/1997/08/a-dimensional-modeling-manifesto/)
- [Kimball Design Tip #105: Snowflakes, Outriggers, and Bridges](https://www.kimballgroup.com/2008/09/design-tip-105-snowflakes-outriggers-and-bridges/)
- [Kimball: Snowflaked Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/snowflake-dimension/)
- [Kimball: Dimension Table Structure](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/dimension-table-structure/)
- [Kimball: Outrigger Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/outrigger-dimension/)
- [Kimball: Type 1 Overwrite](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-1/)
- [Database normalization](https://en.wikipedia.org/wiki/Database_normalization), [Second normal form](https://en.wikipedia.org/wiki/Second_normal_form), [Third normal form](https://en.wikipedia.org/wiki/Third_normal_form)
- [Parquet: Encodings (dictionary encoding)](https://parquet.apache.org/docs/file-format/data-pages/encodings/)
- [Spark SQL: Generator Functions (explode, inline)](https://spark.apache.org/docs/latest/api/sql/generator-functions/)
- [Spark v4.3.0 SQLConf (nestedSchemaPruning, autoBroadcastJoinThreshold)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [Iceberg: Evolution](https://iceberg.apache.org/docs/latest/evolution/)
- [BigQuery: Use nested and repeated fields](https://docs.cloud.google.com/bigquery/docs/best-practices-performance-nested)

## English summary

Normalization keeps each fact in one place, so an OLTP update touches one row and the schema prevents update, insert, and delete anomalies. Analytical tables are read far more than they are written, so star schemas flatten dimensions, and one big table or nested STRUCT and ARRAY columns go further and remove joins at read time. In a distributed engine a small dimension is broadcast cheaply, so pre-joining pays off mainly for large-to-large joins, fixed serving reads, and attributes that rarely change. In my year-end recap jobs, the batch did all joins and wrote one HBase row per user, which made reads a single key lookup but meant every new question needed a batch change. Copying the value at event time together with the dimension key keeps both past and current groupings answerable, while copying only the current value turns the wide table into a Type 1 copy that must be rewritten whenever the attribute changes.
