---
title: "Kafka 멱등 프로듀서는 어떤 중복까지 막을까"
date: 2026-11-21 05:03:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, idempotence, transactions, exactly-once, outbox, concept]
---

`enable.idempotence=true`인 프로듀서로 보냈는데 토픽에 같은 주문 이벤트가 두 번 들어 있다. 설정이 꺼진 것도, 브로커가 잘못한 것도 아니다. 멱등 프로듀서는 한 프로듀서 세션이 같은 배치를 재시도할 때만 중복을 지우고, 프로세스가 재시작했거나 애플리케이션이 `send()`를 두 번 부른 경우는 처음부터 다른 레코드로 본다.

중복을 막는 자리는 세 층이다. 브로커가 시퀀스 번호로 지우는 층, 트랜잭션으로 읽기와 쓰기를 묶는 층, Kafka 밖의 sink가 키로 지우는 층이다. 커밋 순서와 전달 보장 세 가지는 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)에 있다.

버전은 Kafka 4.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| Producer ID (PID) | 프로듀서 인스턴스마다 새로 받는 ID. `transactional.id`가 없으면 재시작 때 바뀐다 |
| 프로듀서 epoch | 같은 PID의 세대 번호. 낮은 epoch의 쓰기와 커밋은 거절된다 |
| 시퀀스 번호 | PID별, 파티션별로 1씩 늘어나는 레코드 번호 |
| `max.in.flight.requests.per.connection` ≤ 5 | 리더가 PID마다 기억하는 최근 배치가 5개라서 생긴 상한 |
| `OutOfOrderSequenceException` | 기대한 다음 번호가 아니고 최근 배치와도 겹치지 않는 시퀀스가 왔다 |
| `transactional.id` | 재시작해도 같은 프로듀서로 인식시키는 사용자 지정 ID |
| 트랜잭션 코디네이터 | `__transaction_state` 파티션의 리더 브로커. 트랜잭션 상태를 기록하고 마커를 쓴다 |
| 제어 마커 | 파티션에 쓰이는 COMMIT, ABORT 레코드. 오프셋을 차지하지만 앱에는 안 보인다 |
| LSO (last stable offset) | 아직 열린 트랜잭션의 첫 오프셋. `read_committed` 컨슈머는 여기까지만 읽는다 |
| `sendOffsetsToTransaction` | 컨슈머 오프셋을 출력 레코드와 같은 트랜잭션에 넣는 API |
| KIP-890 | 서버 쪽 검증과 트랜잭션마다 epoch를 올리는 방식으로 매달린 트랜잭션을 막는다 |
| 멱등 키, outbox | Kafka 밖에서 중복을 거르는 이벤트 ID와, DB 커밋과 이벤트 발행을 묶는 패턴 |

## 브로커는 PID와 시퀀스로 재시도를 알아본다

멱등 프로듀서는 처음 연결할 때 PID와 epoch를 받고, 파티션마다 레코드에 시퀀스 번호를 붙여 보낸다. 파티션 리더는 PID별로 epoch, 마지막 시퀀스, 최근 배치 5개의 시퀀스 범위와 오프셋을 들고 있다. 배치가 들어오면 리더는 이렇게 가른다.

| 들어온 배치 | 리더의 처리 |
|---|---|
| epoch가 같고 최근 5개 배치 중 하나와 시퀀스 범위가 같다 | 다시 쓰지 않고, 처음 쓴 오프셋으로 성공 응답 |
| 첫 시퀀스가 마지막 시퀀스 + 1 | 로그에 추가 |
| epoch는 같은데 시퀀스가 이어지지 않는다 | `OutOfOrderSequenceException` |

응답이 늦어 재시도한 배치는 첫 줄에 걸린다. 5라는 숫자가 여기서 나온다. 응답을 기다리는 요청이 5개를 넘으면 재시도된 배치가 리더가 기억하는 범위 밖으로 밀려나 중복인지 가릴 수 없다. 그래서 멱등을 켜려면 `max.in.flight.requests.per.connection`(기본 5)이 5 이하여야 한다. 대신 5개를 동시에 보내도 순서는 지켜진다. 멱등이 꺼진 채 이 값을 1보다 크게 두면 실패한 배치의 재시도가 뒤 배치보다 늦게 써질 수 있지만, 켜져 있으면 리더가 시퀀스 틈을 거절하고 프로듀서가 순서대로 다시 보낸다. `acks=all`이 필요한 이유도 같은 선에 있다. 리더만 쓰고 응답한 배치는 리더가 바뀌면 사라질 수 있고, 새 리더는 그 시퀀스를 모르니 다음 배치가 틈으로 보인다(동작에서 나온 추론이다).

기본값 이력도 자주 묻는다. KIP-679로 Kafka 3.0부터 `acks` 기본값이 `all`, `enable.idempotence` 기본값이 `true`가 됐다. 다만 3.0.0과 3.1.0은 설정 검증 버그(KAFKA-13598) 때문에 명시하지 않으면 멱등이 실제로 켜지지 않았고, 3.0.1, 3.1.1, 3.2.0에서 고쳐졌다. 리더가 PID 상태를 기억하는 기간은 마지막 쓰기 뒤 `producer.id.expiration.ms`(기본 1일)까지다.

## 재시작과 애플리케이션 재시도는 다른 레코드다

PID는 프로듀서 인스턴스마다 새로 받는다. 프로세스가 응답을 받기 전에 죽고 새 프로세스가 같은 메시지를 다시 보내면, PID가 다르니 브로커는 새 레코드로 받는다. 애플리케이션이 `send()` 실패를 잡아 같은 내용을 다시 보내도 새 시퀀스가 붙는다. 이 두 경로의 중복은 멱등 프로듀서로 지울 수 없다.

로컬에서 확인할 수 있다. 콘솔 프로듀서는 기본이 `acks=-1`, 재시도 3회라서 멱등이 켜진다. 아래는 명시적으로 켜서 설정이 충돌하면 바로 에러가 나게 했다.

```bash
# Kafka 4.4.0, localhost:9092에 브로커 하나
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic idem-test --partitions 1

# 두 번 실행한다. 실행마다 몇 줄 입력하고 Ctrl+C
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic idem-test \
  --command-property enable.idempotence=true

bin/kafka-transactions.sh --bootstrap-server localhost:9092 \
  describe-producers --topic idem-test --partition 0
```

마지막 명령은 리더가 들고 있는 상태를 `ProducerId`, `ProducerEpoch`, `LastSequence` 같은 열로 보여 준다. 콘솔 프로듀서를 두 번 띄웠으니 `ProducerId`가 다른 행이 두 개 나온다. 두 번째 실행에서 첫 번째와 같은 줄을 쳐도 리더에게는 다른 프로듀서의 레코드다.

## transactional.id와 epoch가 옛 인스턴스를 막는다

재시작을 넘어 같은 프로듀서로 인식시키려면 `transactional.id`를 준다. `initTransactions()`는 세 가지를 한다.

1. `transactional.id` 해시로 `__transaction_state`의 파티션을 고르고, 그 파티션의 리더가 이 ID의 코디네이터가 된다.
2. 코디네이터는 이 ID에 묶인 PID를 돌려주면서 epoch를 올린다.
3. 이전 인스턴스가 열어 둔 트랜잭션은 마무리하거나 중단한다.

epoch가 오른 뒤 옛 인스턴스가 쓰면 거절되고, 트랜잭션을 시작하거나 커밋하려 하면 `ProducerFencedException`을 받는다. 네트워크가 끊겼다 돌아온 옛 인스턴스가 이미 넘겨받은 일을 또 쓰는 경우가 이걸로 막힌다. 이 예외와 `OutOfOrderSequenceException`은 복구할 수 없어서 프로듀서를 닫아야 한다.

읽고 처리해서 쓰는 루프에서는 `sendOffsetsToTransaction(offsets, consumer.groupMetadata())`로 입력 오프셋도 같은 트랜잭션에 넣는다(루프 코드는 전달 보장 글에 있다). 오프셋은 트랜잭션이 커밋돼야 커밋된 것으로 친다.

`commitTransaction()`을 부르면 코디네이터는 트랜잭션 로그에 PREPARE_COMMIT을 쓰고, 출력 파티션과 `__consumer_offsets`에 COMMIT 마커를 쓴 뒤, COMPLETE_COMMIT을 쓴다. PREPARE_COMMIT이 로그에 남은 순간 결과가 정해지고, 코디네이터가 바뀌어도 새 코디네이터가 마커 쓰기를 마저 한다.

## read_committed 컨슈머는 LSO에서 멈춘다

`read_committed` 컨슈머는 LSO까지만 받는다. 그래서 트랜잭션 하나가 오래 열려 있으면 그 뒤에 커밋된 레코드와 트랜잭션을 쓰지 않은 레코드까지 그 파티션에서 같이 멈춘다. 중단된 트랜잭션의 레코드는 걸러지고 마커는 오프셋만 차지하니, 받은 오프셋에 빈틈이 있는 게 정상이다. `read_committed` 컨슈머의 fetch lag 지표도 high watermark 대신 LSO 기준으로 잰다.

버려진 트랜잭션은 `transaction.timeout.ms`(기본 60초, 브로커 상한 `transaction.max.timeout.ms` 15분)가 지나면 코디네이터가 중단한다. 남는 문제는 코디네이터가 모르는 트랜잭션 쓰기다. 늦게 도착한 요청이나 클라이언트 버그로 코디네이터 기록에 없는 트랜잭션 레코드가 파티션에 남으면 타임아웃으로도 끝나지 않고 LSO가 멈춘다. KIP-890은 이런 매달린 트랜잭션을 두 겹으로 막는다.

| 방어 | 하는 일 | Kafka 4.4에서 |
|---|---|---|
| 서버 쪽 검증 | 파티션이 트랜잭션에 추가됐는지 확인한 뒤에만 트랜잭션 레코드를 받는다 | `transaction.partition.verification.enable` 기본 `true` |
| 트랜잭션 프로토콜 v2 | 커밋, 중단 때마다 epoch를 올리고, 파티션 추가는 첫 produce 때 서버가 한다 | `transaction.version=2`일 때 켜짐(4.0 업그레이드를 `kafka-features.sh`로 마무리하면 오름), 4.0 이상 클라이언트가 사용 |

epoch가 트랜잭션마다 오르니 이전 트랜잭션에서 늦게 온 쓰기는 낮은 epoch로 거절된다. 이미 멈춘 트랜잭션은 `kafka-transactions.sh`의 `find-hanging`으로 찾고 `abort`로 끝낸다.

## Kafka 밖에서는 이벤트가 생긴 곳의 키로 거른다

트랜잭션의 보장은 Kafka 안에서 끝난다. DB, Redis, 검색 엔진으로 나가는 sink는 같은 이벤트를 두 번 받는다고 보고 결과가 같게 만든다. 이때 키는 PID나 오프셋과 상관없이 이벤트가 처음 생긴 곳에서 붙여야 한다. 그래야 재시작이나 애플리케이션 재시도로 생긴 중복 레코드에도 같은 값이 실려 온다.

| 방법 | 키 | 같은 이벤트가 다시 오면 | 한계 |
|---|---|---|---|
| 키 기준 upsert | 엔티티 PK | 같은 행을 덮어쓴다 | 늦게 온 옛 값이 덮어쓰지 않게 버전이나 offset 비교가 필요 |
| 처리 기록 테이블 | 이벤트 ID | 유니크 제약에 걸려 건너뛴다 | 효과와 같은 DB 트랜잭션이어야 한다 |
| Redis `SET NX EX` | 이벤트 ID | nil 응답으로 건너뛴다 | TTL이 지난 재전달은 못 거른다 |
| outbox | 헤더 `id` | 받는 쪽이 위 방법으로 거른다 | 기본 발행은 at-least-once |

CDC 이벤트를 키 기준 MERGE로 적용하는 모양은 [CDC 글](/posts/change-data-capture/)에, Redis 키가 만료와 축출로 사라지는 경로는 [Redis 중복 거르기 글](/posts/redis-dedup-and-windows/)에 있다. 배치에서는 같은 원칙을 날짜 파티션 덮어쓰기로 푼다([Airflow 멱등 DAG 글](/posts/airflow-idempotent-dags/)). 처리 기록 테이블은 기록과 효과를 한 DB 트랜잭션에 넣는 게 전부다.

```sql
-- 구조만 보여 주는 스케치 (PostgreSQL)
BEGIN;
INSERT INTO processed_events (event_id) VALUES (:event_id) ON CONFLICT (event_id) DO NOTHING;
-- 0행이면 이미 처리한 이벤트다. ROLLBACK하고 오프셋만 커밋한다
UPDATE wallet SET balance = balance + :amount WHERE user_id = :user_id;
COMMIT;  -- 그다음에 Kafka 오프셋을 커밋한다
```

오프셋 커밋 전에 죽어 같은 메시지가 다시 와도 INSERT가 0행이 되어 잔액은 한 번만 바뀐다.

outbox는 발행 쪽 문제를 푼다. 서비스가 DB에 주문을 쓰고 Kafka에 이벤트를 따로 보내면 둘 중 하나만 성공할 수 있다. outbox 패턴은 주문 행과 outbox 행을 같은 DB 트랜잭션으로 쓰고, CDC 커넥터가 DB 로그에서 outbox 행을 읽어 발행한다. Debezium Outbox Event Router는 `aggregateid`를 메시지 키로, outbox 행의 `id`를 헤더 `id`로 넘긴다. 키가 같으면 같은 파티션이라 엔티티 단위 순서가 남고, 헤더 `id`가 받는 쪽의 멱등 키가 된다. 커넥터는 재시작 때 이벤트를 다시 보낼 수 있으니 받는 쪽 중복 제거는 그대로 필요하다.

[로그 수집 API 글](/posts/log-collection-api/)의 API는 쿠키로 30분짜리 트랜잭션 ID를 붙였다. 이런 구조에서 멱등 프로듀서가 막는 건 API 서버와 브로커 사이의 재시도뿐이다. 앱이 같은 요청을 다시 보내거나 API 서버가 재시작되면 새 레코드가 된다. 트랜잭션 ID는 로그인하지 않은 사용자의 행동을 세션으로 묶는 값이라 한 세션의 이벤트에 모두 같은 값이 붙는다. 세션 분석용 키와 중복 판정용 키는 단위가 다르다.

## 참고 자료

- [Apache Kafka 4.4 Producer Configs](https://kafka.apache.org/44/configuration/producer-configs/)
- [Apache Kafka 4.4 Broker Configs](https://kafka.apache.org/44/configuration/broker-configs/)
- [Apache Kafka 4.4 Design: Message Delivery Semantics](https://kafka.apache.org/44/design/design/)
- [Apache Kafka 4.4 Transaction Protocol](https://kafka.apache.org/44/operations/transaction-protocol/)
- [KafkaProducer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [KafkaConsumer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [KIP-679: Producer will enable the strongest delivery guarantee by default](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default)
- [KIP-890: Transactions Server-Side Defense](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)
- [ProducerStateEntry.java (apache/kafka)](https://github.com/apache/kafka/blob/4.4.0/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateEntry.java)
- [TransactionsCommand.java (apache/kafka)](https://github.com/apache/kafka/blob/4.4.0/tools/src/main/java/org/apache/kafka/tools/TransactionsCommand.java)
- [Debezium 3.7 Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)

## English summary

The idempotent producer removes duplicates only when one producer session retries a batch: the partition leader keeps the last five batches per producer ID and answers a repeated sequence range with the original offsets, which is why in-flight requests are capped at five and `acks` must be `all`. A producer that restarts without a `transactional.id` gets a new producer ID, and an application that calls `send()` again writes a new record. Transactions keep one producer ID across restarts, fence old instances by bumping the epoch, and with transaction protocol v2 (KIP-890, Kafka 4.0 and later) bump it on every transaction, while `read_committed` consumers stop at the last stable offset. Outside Kafka, the sink needs a key born with the event. My log collection API tagged events with a 30-minute session ID, a key built for session analysis, so catching client resends would need a per-event ID as well.
