---
title: "Flink SQL의 UPDATE는 왜 두 줄로 나올까"
date: 2026-11-21 03:42:00 +0900
categories: [Streaming, Flink]
tags: [flink, flink-sql, cdc, debezium, kafka, iceberg, concept]
---

Debezium 토픽을 Flink SQL로 읽어 상태별 주문 수를 센다고 하자. 주문 하나가 `PAID`에서 `SHIPPED`로 바뀌면 결과가 네 줄 나온다. `PAID` 개수를 고치는 두 줄과 `SHIPPED` 개수를 고치는 두 줄이다. 이 결과를 append만 하는 sink에 쓰면 상태마다 행이 계속 쌓이고, 키로 덮어쓰는 sink에 쓰면 상태마다 마지막 숫자만 남는다. 어느 쪽이 맞는지는 Flink가 행마다 붙이는 변경 종류와 sink가 그걸 받는 방식에 달려 있다.

[CDC 개념 글](/posts/change-data-capture/)에서 본 Debezium 이벤트도 Flink 안에서는 이 변경 종류가 붙은 행이 된다.

버전은 Flink 2.3, Iceberg 1.12 기준이다.

## 테이블은 변경 로그로 흐른다

Flink SQL에서 스트림 위의 테이블은 계속 바뀌는 동적 테이블이다. 그 위의 쿼리는 끝나지 않고, 입력이 바뀔 때마다 결과 테이블을 고친다. 결과가 바뀌는 모양은 행 단위 변경 네 가지로 나가고, 행마다 붙는 이 표시가 `RowKind`다.

| RowKind | 표기 | 뜻 |
|---|---|---|
| `INSERT` | `+I` | 새 행 |
| `UPDATE_BEFORE` | `-U` | 바뀌기 전 행. 앞서 낸 결과를 되돌린다 |
| `UPDATE_AFTER` | `+U` | 바뀐 뒤 행 |
| `DELETE` | `-D` | 행 삭제 |

소스, 연산자, sink는 자기가 내거나 받을 수 있는 RowKind 조합을 changelog mode로 선언한다. 동적 테이블을 스트림으로 내보내는 방식은 세 가지다.

| 스트림 | 담는 변경 | UPDATE 하나가 | 받는 쪽에 필요한 것 |
|---|---|---|---|
| append-only | `+I` | 생기지 않는다 | 없음 |
| retract | `+I`, `-U`, `+U`, `-D` | 메시지 두 개 | 없음. 행 전체를 맞춰 되돌린다 |
| upsert | `+I`, `+U`, `-D` | 메시지 하나 | 유일 키. 그 키의 이전 값을 덮어쓴다 |

upsert는 `-U`가 없어 메시지가 적다. 대신 받는 쪽이 키를 알아야 한다. retract는 키 없이도 맞출 수 있지만 바뀔 때마다 이전 행을 통째로 다시 보낸다.

## 집계는 앞서 낸 결과를 되돌린다

SQL Client만 있으면 외부 커넥터 없이 바로 볼 수 있다.

```sql
-- Flink 2.3 SQL Client (./bin/sql-client.sh)
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

-- 1) 이름별 개수
SELECT name, COUNT(*) AS cnt
FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS t(name)
GROUP BY name;

-- 2) 1)의 결과를 개수별로 다시 센다
SELECT cnt, COUNT(*) AS names
FROM (
  SELECT name, COUNT(*) AS cnt
  FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS t(name)
  GROUP BY name
)
GROUP BY cnt;
```

```text
1)  op  name   cnt        2)  op  cnt  names
    +I  Bob    1              +I  1    1
    +I  Alice  1              -U  1    1
    +I  Greg   1              +U  1    2
    -U  Bob    1              -U  1    2
    +U  Bob    2              +U  1    3
                              -U  1    3
                              +U  1    2
                              +I  2    1
```

두 번째 `Bob`이 오면 1)은 이미 내보낸 `Bob 1`을 `-U`로 되돌리고 `+U Bob 2`를 낸다. 2)는 그 `-U`를 받아 `cnt=1` 그룹에서 하나를 빼고, `+U`로 `cnt=2` 그룹을 새로 만든다. `cnt=1`이 3이던 순간은 마지막 결과에 없는 중간값이다. 모든 줄을 append하는 sink에는 이 여덟 줄이 다 남는다. 병렬도를 높이면 키가 다른 행끼리의 순서는 바뀔 수 있지만 마지막 상태는 같다.

`GROUP BY` 결과는 그룹 키마다 하나뿐이라 업데이트가 생긴다. 반대로 1시간 텀블링 윈도 집계는 윈도가 닫힐 때 행을 한 번 내고 끝나서 append-only다. 업데이트를 만드는 쿼리는 이미 내보낸 결과를 고칠 수 있도록 상태를 들고 있어야 하고, 그 상태는 그룹 키 수만큼 자란다.

## Debezium과 Canal 메시지가 RowKind가 되는 자리

`debezium-json` 포맷은 `op` 필드를 보고 메시지 하나를 행 한두 개로 바꾼다.

| Debezium `op` | 나오는 행 |
|---|---|
| `c` (insert), `r` (스냅샷 읽기) | `after`로 `+I` |
| `u` | `before`로 `-U`, `after`로 `+U` |
| `d` | `before`로 `-D` |
| tombstone (값이 null) | 건너뛴다 |

`u`와 `d`는 `before`가 있어야 한다. PostgreSQL 테이블이 기본값 `REPLICA IDENTITY DEFAULT`면 `before`에 바뀌기 전 행 전체가 오지 않아 Flink SQL이 해석하지 못하고, `FULL`로 바꿔야 한다. `canal-json`은 메시지 하나의 `data` 배열에 행이 여럿 들어 있고, UPDATE의 `old`에는 바뀐 컬럼만 있다. Flink는 `old`에 없는 컬럼을 바뀐 뒤 값으로 채워 `-U`를 만들고, DDL인 `CREATE` 메시지는 건너뛴다.

CDC 개념 글에 적었듯 Debezium은 at-least-once라서 장애 뒤 같은 이벤트가 다시 온다. 중복된 `+I`는 집계를 두 번 올리고, 결과가 틀리거나 예외가 날 수 있다. 그래서 중복이 올 수 있으면 소스에 기본 키를 두고 중복 처리를 켠다.

```sql
-- Flink SQL, Kafka 커넥터 5.0(Flink 2.2용 빌드) 옵션 기준
CREATE TABLE orders_cdc (
  id BIGINT,
  customer_id BIGINT,
  status STRING,
  amount DECIMAL(10, 2),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'shop.orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);

SET 'table.exec.source.cdc-events-duplicate' = 'true';
```

이 설정을 켜면 planner가 소스 뒤에 기본 키로 묶은 상태 연산자를 넣어 변경 로그를 정규화한다. CDC 개념 글의 `MERGE`는 offset을 비교해 키마다 최신 이벤트를 골랐다. 이 연산자는 offset을 보지 않고 키마다 마지막으로 받은 행을 기준으로 삼는다. Debezium 키가 PK라 같은 행의 이벤트가 한 파티션에 순서대로 있다는 점에 기댄다. Kafka 커넥터는 append-only 포맷 테이블에 기본 키를 두면 거절하고, `debezium-json`처럼 변경을 담는 포맷에서만 허용한다.

## upsert-kafka 소스 뒤에 붙는 ChangelogNormalize

`upsert-kafka`는 토픽을 키별 최신 값의 흐름으로 본다. 소스로 읽을 때 값이 있으면 그 키의 insert나 update, 값이 null이면 delete다. sink로 쓸 때는 `+I`와 `+U`를 값으로, `-D`를 tombstone으로 쓰고 기본 키로 파티션을 고른다. DDL에 `PRIMARY KEY ... NOT ENFORCED`가 있어야 하고, 기본 `at-least-once`로 중복이 써져도 읽는 쪽은 키마다 마지막 값을 쓰니 결과는 같다.

이 소스가 내는 변경 로그에는 `-U`가 없고, tombstone에서 나온 `-D`에는 키만 있다. 뒤의 집계가 그룹 값을 되돌리려면 바뀌기 전 행 전체가 필요하다. 그래서 planner는 소스 뒤에 `ChangelogNormalize`를 넣는다. 기본 키로 shuffle한 뒤 키마다 마지막 행 전체를 상태로 들고, `+U`가 오면 들고 있던 행으로 `-U`를 만들고, tombstone이 오면 들고 있던 행으로 `-D`를 낸다. 지워지지 않은 모든 키의 행 전체가 상태에 남으니, 상태 크기가 원천 테이블 크기를 따라간다.

2.3 planner 코드에는 이 연산자를 빼는 경로도 있다(FLINK-37475). 뒤쪽이 `+U`만 필요로 할 때, 예를 들어 같은 키의 upsert sink로 거의 그대로 넘길 때다. 소스 쪽 필터가 이 연산자로 들어왔거나, 메타데이터 컬럼을 읽거나, `cdc-events-duplicate`를 켰으면 남는다.

sink에서도 기본 키가 동작을 가른다. JDBC sink는 기본 키가 있으면 upsert(MySQL은 `INSERT .. ON DUPLICATE KEY UPDATE`, PostgreSQL은 `INSERT .. ON CONFLICT .. DO UPDATE`)로 쓰고, 없으면 append 모드라 UPDATE와 DELETE를 받지 못한다. 쿼리에서 나온 업데이트 키와 sink 기본 키가 어긋나면 `table.exec.sink.upsert-materialize`(기본 `AUTO`)가 sink 앞에 상태 연산자를 하나 더 붙인다.

upsert 토픽은 키마다 지금 값만 남는다. [SCD 글](/posts/dimensional-modeling-scd/)에서 CDC upsert 테이블을 원천 키 기준의 Type 1로 본 것과 같은 모양이다. 이력이 필요하면 `-U`와 `+U` 쌍이나 원본 변경 이벤트를 따로 남겨야 한다.

## 상태 TTL을 걸면 결과가 틀릴 수 있다

`table.exec.state.ttl` 기본값은 0이고, 상태를 지우지 않는다. 집계와 `ChangelogNormalize`의 상태는 키가 늘수록 자란다. TTL을 주면 그 시간 동안 갱신되지 않은 키의 상태가 지워지는데, 그 뒤에 그 키의 변경이 오면 결과가 어긋난다. 2.3 런타임 코드에서 읽은 동작은 이렇다.

| 연산자 | 상태가 지워진 키에 온 변경 | 결과 |
|---|---|---|
| 그룹 집계 | `-U`, `-D` | 버린다. sink에는 되돌리지 못한 값이 남는다 |
| 그룹 집계 | `+I`, `+U` | 누산기를 새로 만들고 `+I`를 낸다. 개수를 처음부터 다시 센다 |
| `ChangelogNormalize` | `+U` | 이전 행이 없어 `-U` 없이 `+I`로 낸다 |
| `ChangelogNormalize` | delete | 지울 행이 없다고 보고 아무것도 내지 않는다 |

고객별 주문 수에 TTL 하루를 걸었다면, 하루 넘게 주문이 없던 고객의 예전 주문이 취소될 때 그 `-D`는 버려지고 sink에는 취소 전 개수가 남는다. TTL은 상태 크기를 묶는 대신 결과의 결정성을 내준다. 조인과 그룹 집계는 `STATE_TTL` 힌트로 연산자별 TTL을 줄 수 있다.

TTL이 처리 시간으로 재지고 TTL 설정이 체크포인트에 저장되지 않는 점은 [Flink 상태와 체크포인트 글](/posts/flink-state-and-checkpoints/)에 있다.

## RowKind가 Iceberg 파일이 되는 방식

Iceberg Flink sink는 v2 이상 테이블에 기본 키(identifier 필드)가 있을 때 변경을 delete 파일로 바꾼다. upsert를 끄면 `-U`와 `-D`를 그 행의 삭제로, `+I`와 `+U`를 새 행으로 쓴다. upsert를 켜면 `-U`는 무시하고, `+I`와 `+U`마다 키 삭제를 먼저 쓴 뒤 새 행을 쓴다. 삭제는 같은 체크포인트 안에서 그 writer가 쓴 행이면 position delete, 아니면 equality delete가 된다.

upsert 모드는 `-U`가 없는 upsert 스트림도 받고, 같은 키의 `+I`가 두 번 와도 살아 있는 행은 하나다. 그 대가로 처음 보는 키의 insert에도 equality delete가 하나씩 붙는다. 그 delete가 읽기 비용으로 넘어가는 과정은 [Iceberg 행 단위 변경 글](/posts/iceberg-row-level-deletes/)에 있다. 기본 키가 없는 테이블은 insert 전용 writer가 맡아서, 들어온 `-U`와 `-D`도 일반 행으로 써진다(sink 코드를 읽고 추정한 동작이다).

## 참고 자료

- [Flink 2.3 Dynamic Tables](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/dynamic_tables/)
- [Flink 2.3 Debezium Format](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/formats/debezium/)
- [Flink 2.3 Upsert Kafka SQL Connector](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/upsert-kafka/)
- [Flink 2.3 Configuration (table.exec.state.ttl, cdc-events-duplicate, upsert-materialize)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/config/)
- [Flink 2.3 Hints (STATE_TTL)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/hints/)
- [Flink release-2.3 소스: RowKind, DeduplicateFunctionHelper, GroupAggHelper, ChangelogNormalizeRequirementResolver](https://github.com/apache/flink/tree/release-2.3/flink-table)
- [flink-connector-kafka KafkaDynamicTableFactory (기본 키 검사)](https://github.com/apache/flink-connector-kafka/blob/main/flink-connector-kafka/src/main/java/org/apache/flink/streaming/connectors/kafka/table/KafkaDynamicTableFactory.java)
- [Iceberg Flink Writes (UPSERT)](https://iceberg.apache.org/docs/latest/flink-writes/)
- [Iceberg flink/v2.3 BaseDeltaTaskWriter](https://github.com/apache/iceberg/blob/main/flink/v2.3/flink/src/main/java/org/apache/iceberg/flink/sink/BaseDeltaTaskWriter.java)

## English summary

Flink SQL treats a stream as a dynamic table and sends every change as a row tagged with a RowKind: +I, -U, +U or -D. An aggregation retracts its old result before emitting the new one, while an upsert stream drops -U and relies on a key at the sink. Debezium formats turn source events into these rows, and an upsert-kafka source needs a stateful ChangelogNormalize step to rebuild -U and full deletes. The Iceberg upsert writer makes duplicates harmless, but it pays one equality delete per insert that compaction must later absorb. A state TTL bounds memory in the same way, at the cost of results that can drift.
