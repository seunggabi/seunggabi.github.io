---
title: "SCD로 지난 숫자를 지키는 차원 모델링"
date: 2026-11-20 06:03:00 +0900
categories: [Transformation & Modeling, Data Modeling]
tags: [data-modeling, kimball, scd, iceberg, spark, cdc, concept]
description: "사용자 속성이 바뀌어도 지난 집계가 틀어지지 않도록, 팩트와 차원의 grain을 정하고 SCD로 변경 이력을 보존하는 Kimball식 차원 모델링을 설명한다."
---

사용자 하나가 지난달 중순 멤버십을 무료에서 유료로 바꿨다. 사용자 테이블이 현재 값만 들고 있으면, 오늘 다시 뽑은 지난달 멤버십별 재생 수에서 그 사람의 지난달 재생이 전부 유료 쪽으로 옮겨 간다. 원천 로그도 쿼리도 그대로인데 이미 보고한 숫자가 바뀐다. 차원 모델링에서 slowly changing dimension(SCD)이라고 부르는 문제다.

버전은 Spark 4.3, Iceberg 1.12, Debezium 3.7 기준이다.

## 팩트는 사건을 세고 차원은 사건을 설명한다

팩트 테이블 한 행은 재생 한 번, 결제 한 건 같은 측정 사건이거나 그걸 묶은 집계다. 숫자 컬럼(측정값)과 차원을 가리키는 키를 갖는다. 차원 테이블은 사용자, 상품, 날짜처럼 사건을 거르고 묶는 데 쓰는 설명 속성을 갖는다. 리포트의 `WHERE`와 `GROUP BY`에 들어가는 컬럼은 대부분 차원 속성이다.

Kimball의 순서는 grain을 먼저 선언하고, 차원과 측정값을 그 grain에 맞춰 고르는 것이다. 측정값은 더할 수 있는 방향에 따라 additive, semi-additive, non-additive로 나뉜다. 두 가지 모두 [지표 테이블 grain 글](/posts/metric-table-grain/)에 예시와 함께 정리했다.

non-additive 중 비율이 가장 자주 틀린다. 비율끼리 더하거나 평균 내지 않고, 분자와 분모를 각각 더한 다음 마지막에 나눈다. 예를 들어 국가별 퍼센트를 먼저 구한 뒤 "기타"로 합치면 "기타" 칸이 100%를 넘을 수 있다. 분자·분모 행을 국가와 날짜로 맞춰 붙이고, "기타"로 접은 상태에서 `SUM(분자) / SUM(분모)`로 나눠야 한다.

## 두 팩트를 나란히 놓으려면 차원이 같아야 한다

conformed dimension은 서로 다른 팩트 테이블이 같은 컬럼 이름과 같은 값 집합으로 쓰는 차원이다. 차원이 맞으면 결제 팩트와 재생 팩트를 따로 집계한 결과를 같은 행 머리글(국가, 날짜)에 맞춰 한 리포트에 붙일 수 있다. Kimball은 이걸 drill-across라고 부른다.

한 팩트는 국가를 `KR`로, 다른 팩트는 `South Korea`로 들고 있으면 붙인 리포트의 행이 어긋난다. "기타"로 접는 나라 목록이 팩트마다 달라도 마찬가지다. 둘 다 에러 없이 틀린 숫자를 낸다.

지표마다 SQL 블록을 만들어 `UNION`으로 이어 붙이는 대시보드라면, 블록마다 같은 "주요 국가 목록 + 기타" 뼈대를 쓰게 된다. [서비스 지표 배치 글](/posts/pyspark-indicator-batch/)에서는 웹과 앱을 가르는 기준이 원천마다 달랐다. 로그 테이블은 user agent로, 다른 테이블은 서비스 타입 컬럼으로 갈라야 했고, 이 조건을 query builder 메서드 두 개로 두었다. 차원 모델링 용어로 다시 읽으면, 각각 국가와 플랫폼이라는 차원을 한 곳에서 정의해 여러 지표가 같은 값을 쓰게 한 구조다.

## 속성이 바뀌면 덮어쓸지 행을 늘릴지 정한다

| 방식 | 속성이 바뀌면 | 지난 팩트는 |
|---|---|---|
| Type 1 | 그 행의 값을 덮어쓴다 | 새 값으로 다시 묶인다. 이력은 사라진다 |
| Type 2 | 새 값으로 행을 하나 더 만든다 | 그때의 행을 계속 가리킨다 |
| Type 3 | 현재 값은 덮어쓰고 직전 값을 별도 컬럼에 둔다 | 현재 값과 직전 값 중 골라 본다 |
| Type 6 | Type 2 행마다 같은 속성의 현재 값 컬럼을 더 둔다 | 그때 값과 지금 값 둘 다로 본다 |

Type 1은 단순하다. 다만 덮어쓴 속성으로 미리 만든 집계 테이블이 있으면 그 집계도 다시 계산해야 한다. 오타 수정처럼 과거를 고쳐도 되는 속성은 Type 1로 두고, 앞의 멤버십처럼 그때 무엇이었는지가 숫자를 바꾸는 속성은 Type 2로 둔다. 한 차원 안에서도 속성마다 다르게 정할 수 있다.

Type 3은 직전 값 하나만 나란히 보면 될 때 쓰고, 드물게 쓰인다. Type 6은 Type 2에 Type 1 컬럼을 더한 모양이라, 값이 바뀌면 같은 사용자의 모든 행에서 현재 값 컬럼을 덮어쓴다.

## 대리 키와 유효 기간

Type 2에서는 한 사용자가 여러 행을 가지므로 원천의 `user_id`가 차원의 기본 키가 될 수 없다. 행마다 대리 키(surrogate key)를 새로 붙이고, 팩트는 이 키를 들고 간다. 원천 키는 같은 사용자의 행들을 잇는 용도로 남긴다. 여기에 유효 시작, 유효 종료, 현재 행 표시 컬럼 세 개가 최소로 붙는다.

```sql
CREATE TABLE lake.dim_user (
  user_sk     BIGINT,     -- 대리 키, 행마다 다르다
  user_id     BIGINT,     -- 원천 키, 같은 사용자의 행을 잇는다
  membership  STRING,     -- Type 2로 추적
  country     STRING,     -- Type 2로 추적
  valid_from  TIMESTAMP,
  valid_to    TIMESTAMP,  -- 현재 행은 9999-12-31
  is_current  BOOLEAN
) USING iceberg;
```

팩트를 적재할 때는 사건 시각이 들어가는 차원 행을 찾아 그 대리 키를 붙인다.

```sql
SELECT p.play_id, p.event_ts, d.user_sk
FROM plays p
JOIN lake.dim_user d
  ON p.user_id = d.user_id
 AND p.event_ts >= d.valid_from
 AND p.event_ts <  d.valid_to;
```

구간은 시작을 포함하고 끝을 뺀다. 이전 행의 `valid_to`와 새 행의 `valid_from`이 같은 시각이라, `BETWEEN`으로 양끝을 다 넣으면 바뀐 그 순간의 사건이 두 행에 붙어 두 번 세어진다. 현재 행의 `valid_to`를 NULL 대신 먼 미래로 두면 이 조건 하나로 현재 행까지 찾는다.

Kimball은 대리 키로 1부터 순서대로 늘어나는 정수를 쓴다. 여러 Spark 태스크가 나눠 쓰는 배치에서 순번을 내려면 기존 최댓값을 읽어 이어 붙이는 단계가 필요하다. 아래 예시는 이 글에서 고른 방식으로, 그 단계 없이 `xxhash64(user_id, valid_from)`으로 키를 만든다. 같은 행은 다시 돌려도 같은 키가 나오는 대신, 64비트 해시라 충돌 가능성이 0은 아니고 순서의 의미도 없다.

## Iceberg 테이블에 SCD2를 MERGE 한 번으로 넣는다

배치마다 바뀐 사용자를 `user_changes`(사용자당 한 행, 바뀐 시각 `changed_at`)로 모았다고 하자. 속성이 바뀐 기존 사용자에게는 현재 행을 닫고 새 행을 넣는 두 가지를 해야 한다. MERGE에서 매치된 대상 행은 UPDATE나 DELETE만 할 수 있고, INSERT는 매치되지 않은 원천 행에만 걸린다. 그래서 바뀐 사용자를 원천에 두 번 넣는다.

```sql
MERGE INTO lake.dim_user t
USING (
  -- 모든 변경 행: 기존 사용자면 현재 행과 매치되고, 신규 사용자면 그대로 INSERT된다
  SELECT s.user_id AS merge_key, s.* FROM user_changes s
  UNION ALL
  -- 속성이 바뀐 기존 사용자만 한 번 더: 키가 NULL이라 매치되지 않고 새 행이 된다
  SELECT NULL AS merge_key, s.*
  FROM user_changes s
  JOIN lake.dim_user d ON d.user_id = s.user_id AND d.is_current
  WHERE NOT (d.membership <=> s.membership AND d.country <=> s.country)
) s
ON t.user_id = s.merge_key AND t.is_current
WHEN MATCHED AND NOT (t.membership <=> s.membership AND t.country <=> s.country) THEN
  UPDATE SET t.valid_to = s.changed_at, t.is_current = false
WHEN NOT MATCHED THEN
  INSERT (user_sk, user_id, membership, country, valid_from, valid_to, is_current)
  VALUES (xxhash64(s.user_id, s.changed_at), s.user_id, s.membership, s.country,
          s.changed_at, TIMESTAMP '9999-12-31 00:00:00', true);
```

비교는 `<=>`(NULL 안전 비교)로 한다. `<>`는 한쪽이 NULL이면 NULL을 돌려줘서, 비어 있던 값이 채워진 변경을 놓친다. 값이 같은 원천 행은 아무것도 바꾸지 않으므로, 같은 배치를 다시 돌려도 버전이 늘지 않는다.

Iceberg MERGE는 원천의 한 행만 대상의 한 행을 바꿀 수 있고, 아니면 에러를 낸다. `user_changes`에 같은 사용자가 두 번 있으면 실패하니 사용자당 한 행으로 먼저 줄인다. 그런데 한 배치 안에서 두 번 바뀐 사용자를 마지막 값으로 줄이면 중간 버전이 사라진다. 중간 버전까지 남기려면 변경을 시간순으로 정렬하고 `LEAD(changed_at)`로 버전마다 `valid_to`를 계산해 여러 행을 넣어야 하고, 이 MERGE 한 문장으로는 안 된다. Iceberg는 copy-on-write에서 바꿀 행이 든 데이터 파일만 다시 써서 MERGE를 커밋하고(merge-on-read는 delete 파일을 쓴다), SQL MERGE에는 Iceberg Spark extensions가 필요하다.

## CDC 테이블은 아직 차원이 아니다

예를 들어 MySQL CDC를 Iceberg에 upsert로 적재하는 파이프라인이라면 Debezium source가 binlog를 읽어 테이블마다 Kafka 토픽으로 보내고, Iceberg sink가 upsert 모드로 쓴다. 중복 행을 주기 잡으로 정리하는 적용 방식은 [CDC 개념 글](/posts/change-data-capture/)에 있다. 결과는 원천 테이블의 현재 상태를 키마다 한 행으로 들고 있는 테이블이다. 원천을 따라가는 복제본으로는 맞는 모양이다. 차원 모델링 쪽에서 보면 이 테이블은 원천 키 기준의 Type 1이다. 대리 키도 유효 기간도 없고, 지난 값은 다음 변경이 덮는다. 차원으로 쓰려면 그 위에서 정할 것이 남는다.

| 정할 것 | CDC 쪽에 있는 것 | 차원에 필요한 것 |
|---|---|---|
| 변경 시각 | `source.ts_ms`(DB에서 바뀐 시각), `ts_ms`(커넥터가 처리한 시각) | `valid_from`에 어느 시각을 쓸지 |
| 이력 | 변경 이벤트의 `before`, `after`. 토픽에서는 보존 기간까지만 남는다 | 보존 기간과 상관없이 남는 버전 행 |
| 추적할 속성 | 컬럼 하나만 바뀌어도 이벤트가 나온다 | Type 2 컬럼이 바뀐 이벤트만 새 버전 |
| 스냅샷 | 스냅샷으로 읽은 행은 `op = r`로 다시 나온다 | 값이 같으면 버전을 만들지 않기 |
| 삭제 | `op = d` 이벤트 | 차원 행을 지울지, 닫기만 할지 |

앞의 MERGE는 값이 같은 원천 행을 무시하므로, 스냅샷으로 같은 행이 다시 와도 버전이 늘지 않는다. 반대로 upsert된 현재 상태 테이블을 배치마다 비교하는 방식은 두 배치 사이에 여러 번 바뀐 값 중 마지막 것만 본다. 중간 이력까지 필요하면 변경 이벤트를 따로 보관해 거기서 버전을 만든다. 그렇게 만든 Type 2 행은 원천이 행을 지우거나 토픽 보존 기간이 지나도 남는다.

## 참고 자료

- [Kimball: Grain](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/)
- [Kimball: Additive, Semi-Additive, and Non-Additive Facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/additive-semi-additive-non-additive-fact/)
- [Kimball: Conformed Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/conformed-dimension/)
- [Kimball: Dimension Surrogate Keys](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/dimension-surrogate-key/)
- [Kimball: Type 1](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-1/), [Type 2](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/), [Type 3](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-3/), [Type 6](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-6/)
- [Iceberg: Spark Writes (MERGE INTO)](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Spark SQL: MERGE INTO](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-merge-into.html)
- [Spark SQL: Built-in Functions (xxhash64, lead)](https://spark.apache.org/docs/latest/api/sql/)
- [Debezium 3.7 MySQL connector (change event structure, op, source.ts_ms)](https://debezium.io/documentation/reference/3.3/connectors/mysql.html)

## English summary

A dimension that keeps only current values quietly moves past facts to new attribute values, so last month's numbers change. A Type 2 dimension adds a row per change with a surrogate key and a half-open validity range, and each fact carries the key of the row that was valid at event time. On Iceberg, one Spark MERGE can close the current row and insert the new one if changed keys appear twice in the source. A CDC pipeline with an upsert sink lands the latest row per key, which works as a Type 1 copy of the source but is not yet a dimension. Building Type 2 on top of it costs another table and batch step, and comparing current-state snapshots still misses changes made between two runs.
