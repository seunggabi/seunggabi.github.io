---
title: "Kafka 전달 보장, 개념과 실무에서 고르는 방법"
date: 2026-11-20 04:41:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, consumer-group, offset, exactly-once, deduplication, concept]
description: "Kafka 컨슈머가 처리와 오프셋 커밋 중 무엇을 먼저 하느냐에 따라 달라지는 at-most-once, at-least-once, exactly-once 전달 보장을 정리한다."
---

Kafka 컨슈머가 죽었다가 다시 뜨면 이미 처리한 메시지를 또 받거나, 받기만 하고 처리하지 못한 메시지를 건너뛴다. 어느 쪽이 되는지는 메시지를 처리하는 일과 "여기까지 읽었다"를 기록하는 일 중 무엇을 먼저 하느냐가 정한다. at-most-once, at-least-once, exactly-once는 그 순서에 붙은 이름이다. 이 글은 먼저 세 가지가 무엇인지 정리하고, 실무에서 어떤 조합을 고르는지로 넘어간다. 버전은 Kafka 4.4 기준이다.

## 오프셋은 책갈피다

같은 `group.id`를 쓰는 컨슈머들이 한 그룹이고, 그룹 안에서 파티션 하나는 컨슈머 하나에만 배정된다. 컨슈머가 어디까지 읽었는지는 오프셋으로 남는다. 오프셋을 커밋하면 Kafka 내부 토픽에 "다음에 읽을 위치"가 저장된다. 그래서 마지막으로 처리한 레코드의 오프셋에 1을 더해 커밋한다. 컨슈머가 다시 뜨거나 파티션이 다른 컨슈머로 넘어가면, 마지막으로 커밋된 위치부터 읽는다. 커밋된 오프셋이 없으면 `auto.offset.reset`(기본 `latest`)을 따른다.

책으로 치면 처리는 읽는 일이고, 커밋은 책갈피를 옮기는 일이다. 둘 사이에서 컨슈머가 죽으면 다음에는 책갈피가 있는 곳부터 읽는다.

## 세 가지 보장은 순서의 이름이다

| 보장 | 순서 | 둘 사이에서 죽으면 |
|---|---|---|
| at-most-once | 커밋 → 처리 | 책갈피는 넘어갔는데 처리를 못 했으니 그 메시지는 영영 처리되지 않는다 (유실) |
| at-least-once | 처리 → 커밋 | 처리는 끝났는데 책갈피가 그대로라 같은 메시지를 다시 처리한다 (중복) |
| exactly-once | 처리 결과와 커밋을 한 번에 저장 | 둘 다 남거나 둘 다 남지 않는다 |

일반 컨슈머 API에는 이런 이름의 설정이 없다. 코드에서 처리와 커밋을 어떤 순서로 짰는지가 곧 보장이다. Kafka Streams는 예외라서 `processing.guarantee=exactly_once_v2`로 켠다(기본 `at_least_once`). 설정으로 정해지는 곳은 따로 있다.

| 구간 | 무엇이 정하나 |
|---|---|
| 프로듀서 → 브로커 | `acks`, 재시도, `enable.idempotence` 설정 |
| 브로커 → 컨슈머 → 처리 결과 | 컨슈머 코드의 처리와 커밋 순서 |
| Kafka에서 읽어 Kafka로 쓰기 | Kafka Streams `processing.guarantee`, 또는 트랜잭션 API 사용 여부 |
| Flink 같은 프레임워크 | 체크포인트 모드와 싱크의 전달 보장 설정 |

## 자동 커밋은 순서를 정해 주지 않는다

`enable.auto.commit`의 기본값은 `true`이고, `auto.commit.interval.ms`(기본 5000ms)마다 오프셋을 커밋한다. 커밋 주기가 되면 그때까지 `poll()`이 돌려준 위치를 커밋한다. 처리가 끝났는지는 보지 않는다.

`poll()`로 받은 레코드를 다음 `poll()` 전에 같은 스레드에서 다 처리하면 결과적으로 at-least-once가 된다. 처리를 다른 스레드나 프레임워크에 넘기면 이 조건이 깨진다. 처리가 끝나기 전에 다음 `poll()`이 위치를 커밋해서, 그 사이에 죽으면 메시지가 빠진다. Spark Streaming(DStream)의 Kafka 연동 문서가 자동 커밋을 쓰면 전달 보장이 정해지지 않는다(undefined semantics)고 적는 이유다.

## 실무 기본값: 처리 후 커밋 + 받는 쪽 멱등

대부분의 컨슈머는 at-least-once로 두고, 같은 메시지를 두 번 받아도 결과가 같게 만든다. 유실은 되돌릴 수 없지만 중복은 받는 쪽에서 흡수할 수 있어서다.

```java
// enable.auto.commit=false
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<String, String> r : records) {
        upsert(r.key(), r.value());   // 같은 키는 덮어쓰므로 두 번 받아도 결과가 같다
    }
    consumer.commitSync();            // 이번 poll로 받은 위치까지 커밋
}
```

이 형태에서 지킬 것은 넷이다.

| 할 일 | 이유 |
|---|---|
| `enable.auto.commit=false`, 처리 뒤 `commitSync()` | 커밋이 처리를 앞지르지 않게 순서를 코드로 고정한다 |
| 받는 쪽을 멱등으로 | 기본 키 upsert, 이벤트 ID로 이미 처리했는지 확인, 날짜 파티션 덮어쓰기 중 하나 |
| 같은 대상은 같은 키로 발행 | 순서는 파티션 안에서만 지켜진다. 같은 대상의 변경이 한 파티션에 모여야 늦게 온 옛 값이 새 값을 덮지 않는다 |
| 여러 번 모아 커밋한다면 리밸런스 때도 커밋 | `ConsumerRebalanceListener.onPartitionsRevoked`에서 처리한 위치까지 커밋하지 않으면, 파티션을 넘겨받은 컨슈머가 그만큼 다시 처리한다 |
| 항상 실패하는 레코드는 따로 뺀다 | 처리 후 커밋 구조에서는 그 레코드 하나가 파티션 전체를 멈춘다. 몇 번 재시도한 뒤 DLQ 토픽으로 보내고 커밋한다 |

`poll()` 한 번마다 커밋하는 위 코드는 리밸런스가 `poll()` 안에서 일어나므로 넘겨줄 때 처리 중인 레코드가 없다. 처리량 때문에 몇 번에 한 번씩 커밋하거나 `commitAsync()`를 쓰면 리밸런스 항목이 필요하다.

한 번 받은 묶음의 처리가 `max.poll.interval.ms`(기본 5분)를 넘으면 컨슈머가 그룹에서 빠지고 `commitSync()`가 `CommitFailedException`을 던진다. 그 묶음은 파티션을 넘겨받은 컨슈머가 다시 처리한다. 처리가 길면 `max.poll.records`(기본 500)를 줄인다.

멱등 upsert도 순서가 섞이면 옛 값이 새 값을 덮을 수 있다. 원천이 여럿이거나 재처리가 겹칠 수 있으면 버전이나 이벤트 시각 컬럼을 두고 `WHERE version < :new`일 때만 덮어쓴다.

## 정확히 한 번이 필요할 때: 결과와 위치를 같이 저장한다

중복을 받는 쪽에서 흡수할 수 없는 경우가 있다. 금액을 더하는 집계는 같은 메시지를 두 번 반영하면 값이 틀어지고, 키로 덮어써서 되돌릴 수도 없다. 이때는 처리 결과와 읽은 위치를 원자적으로 함께 저장한다. 저장하는 곳에 따라 방법이 셋이다.

| 결과를 쓰는 곳 | 방법 |
|---|---|
| Kafka 토픽 | 트랜잭션: 출력 레코드와 컨슈머 오프셋을 한 트랜잭션으로 커밋 |
| 트랜잭션이 되는 DB | 결과와 오프셋을 같은 DB 트랜잭션에 쓰고, 시작할 때 그 오프셋으로 `seek()` |
| 그 밖의 싱크 | Flink 체크포인트와 트랜잭션 싱크, 또는 체크포인트와 멱등 싱크 |

Kafka에서 읽어 Kafka로 쓰는 경우의 트랜잭션은 이렇게 쓴다.

```java
// producer: transactional.id 설정, consumer: enable.auto.commit=false
producer.initTransactions();
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    if (records.isEmpty()) continue;
    try {
        producer.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> r : records) {
            producer.send(new ProducerRecord<>("out", r.key(), transform(r.value())));
            offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
        producer.close();   // 복구할 수 없는 예외
        throw e;
    } catch (KafkaException e) {
        producer.abortTransaction();
        // 컨슈머 위치는 이미 앞으로 갔으니 마지막 커밋 오프셋으로 되돌려 다시 읽는다
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
        for (TopicPartition tp : consumer.assignment()) {
            OffsetAndMetadata o = committed.get(tp);
            if (o != null) consumer.seek(tp, o.offset()); else consumer.seekToBeginning(List.of(tp));
        }
    }
}
```

출력 토픽을 읽는 쪽은 `isolation.level=read_committed`로 바꿔야 한다. 기본값은 `read_uncommitted`라서, 그대로 두면 중단된 트랜잭션의 레코드도 읽는다.

결과를 MySQL이나 PostgreSQL에 쓴다면 오프셋을 Kafka에 커밋하지 않고 결과 테이블과 같은 DB에 둔다.

```java
// enable.auto.commit=false, auto.offset.reset=earliest
// 오프셋은 Kafka가 아니라 결과와 같은 DB에 둔다. db 쪽 메서드는 예시다
consumer.subscribe(List.of("orders"), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> parts) { }
    public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        for (TopicPartition tp : parts) {
            db.loadOffset(tp).ifPresent(next -> consumer.seek(tp, next));
        }
    }
});
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    Tx tx = db.begin();
    try {
        for (ConsumerRecord<String, String> r : records) {
            tx.apply(r.value());
            tx.saveOffset(r.topic(), r.partition(), r.offset() + 1);
        }
        tx.commit();   // 결과와 위치가 함께 남는다
    } catch (Exception e) {
        tx.rollback(); // 함께 사라지고, 다음 시작 때 DB의 오프셋부터 다시 읽는다
        throw e;
    }
}
```

Flink 체크포인트도 같은 원리다. Flink는 복구할 때 Kafka에 커밋된 오프셋을 쓰지 않는다. 읽은 오프셋을 연산자 상태와 함께 체크포인트에 저장하고, 복구할 때 그 오프셋과 상태로 함께 되돌아간다. Kafka에 커밋하는 오프셋은 모니터링용이다. 그래서 Flink 안의 상태는 정확히 한 번 반영되지만, 밖으로 쓰는 결과까지 정확히 한 번이 되려면 싱크가 체크포인트에 맞춰 커밋하는 트랜잭션 싱크이거나 멱등이어야 한다.

## 프로듀서 쪽 중복과 앞단 중복

프로듀서도 중복을 만든다. 브로커는 썼는데 응답이 늦어 프로듀서가 재시도하면 같은 레코드가 두 번 들어간다. `enable.idempotence`가 이걸 막는다. 현재 기본값은 `true`이고 `acks=all`, `retries>0`, `max.in.flight.requests.per.connection<=5`가 함께 맞아야 한다. 맞지 않는 설정을 주면 꺼지고, 명시적으로 켰는데 설정이 맞지 않으면 `ConfigException`이 난다. 이 보호는 한 프로듀서 세션 안의 재시도에만 해당한다. 애플리케이션이 같은 내용을 두 번 `send()`하면 서로 다른 레코드 두 개다.

클라이언트가 같은 요청을 다시 보내는 앞단 중복은 Kafka 설정으로 막을 수 없다. 메시지에 이벤트 ID를 붙이고, 받는 쪽이 그 ID로 이미 처리했는지 확인해야 한다. 확인과 저장을 Redis의 `SET key value NX EX ttl` 한 번으로 묶으면 두 요청이 동시에 "없음"을 보는 일은 없다. 다만 키를 처리 전에 잡으므로 처리가 실패하면 키를 지워야 재시도가 통하고, TTL은 재전달이 올 수 있는 기간보다 길어야 한다. 결과를 DB에 쓴다면 이벤트 ID를 결과와 같은 트랜잭션에 unique 키로 넣는 편이 단순하다.

## 고르는 순서

| 상황 | 고르는 방법 |
|---|---|
| 대부분의 컨슈머 | 자동 커밋 끄고 처리 후 커밋, 받는 쪽 멱등 |
| 같은 메시지 두 번이 숫자를 틀리게 하고 키로 덮을 수 없음 | 결과와 오프셋을 같은 곳에 원자적으로 저장 |
| Kafka에서 읽어 Kafka로 쓰기 | Kafka Streams `exactly_once_v2`, 직접 짠다면 트랜잭션 + 읽는 쪽 `read_committed` |
| 상태가 큰 스트림 처리 | Flink 체크포인트 + 트랜잭션 싱크 또는 멱등 싱크 |
| 유실이 중복보다 나은 경우(지표 샘플링, 실시간 화면 갱신) | at-most-once도 선택지 |
| 클라이언트 재전송 | 이벤트 ID로 받는 쪽에서 거르기 |

이전 회사에서 맡은 [Spark Streaming으로 Hive에 쌓는 잡](/posts/spark-streaming-to-hive/)은 오프셋을 자동 커밋에 맡겼다. 재시작하면 메시지가 겹치거나 빠질 수 있었다. 겹친 줄은 매일 그날 폴더를 `dropDuplicates()`로 지우는 일 배치가 정리했고, 빠진 메시지는 배치로 되살릴 수 없어 필요할 때 별도 컨슈머 그룹으로 처음부터 다시 읽는 복구 모드를 썼다. 중복은 뒤의 배치가 정리하고, 유실은 다시 읽어서 메우는 구조다. [API 동기화를 Kafka 구독으로 바꾼 글](/posts/api-sync-to-kafka-subscribe/)에서는 받는 쪽 서비스가 중복으로 들어온 변경을 내부 로직에서 처리했다. 위 표에서 받는 쪽 멱등에 해당한다.

## 묻고 답하기

Q. 자동 커밋만 끄면 exactly-once인가?

아니다. 처리 뒤에 커밋하도록 순서를 고정한 것이라 at-least-once다. 처리와 커밋 사이에 죽으면 같은 메시지를 다시 처리한다. 정확히 한 번은 결과와 위치를 한 번에 저장할 때만 된다.

Q. Flink를 쓰면 전달 보장 걱정이 없나?

Flink 내부 상태는 체크포인트로 정확히 한 번이 된다. 하지만 외부 DB나 API로 쓰는 결과는 싱크가 체크포인트에 맞춰 커밋하거나 멱등이어야 한다. 그렇지 않으면 장애 뒤 마지막 체크포인트부터 다시 처리하면서 그 사이 결과가 한 번 더 나간다.

## 참고 자료

- [Apache Kafka 4.4 Design: Message Delivery Semantics](https://kafka.apache.org/44/design/design/)
- [Apache Kafka 4.4 Consumer Configs](https://kafka.apache.org/44/configuration/consumer-configs/), [Producer Configs](https://kafka.apache.org/44/configuration/producer-configs/)
- [KafkaConsumer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html), [KafkaProducer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [Spark Streaming + Kafka Integration Guide (0-10)](https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html)
- [Flink Kafka connector: fault tolerance and delivery guarantees](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Redis SET command](https://redis.io/docs/latest/commands/set/)

## English summary

A Kafka consumer loses or repeats messages after a crash depending on whether it commits its offset before or after processing; at-most-once, at-least-once and exactly-once are names for that order, not consumer settings. Auto commit fixes no order, so the usual choice is to turn it off, commit after processing, and make the receiving side idempotent with upserts or event IDs. When a repeat would break a number and cannot be overwritten by key, the result and the offset have to be stored together: Kafka Streams exactly_once_v2 or transactions for Kafka-to-Kafka work, the same database transaction with a seek on start for a database sink, or Flink checkpoints with a transactional or idempotent sink. In my Spark Streaming job I left offsets to auto commit and let a daily dedup batch and a recovery mode handle the gaps, and in the Kafka sync change the receiving services already handled repeated changes.
