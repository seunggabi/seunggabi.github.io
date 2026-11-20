---
title: "Kafka 전달 보장은 오프셋 커밋 순서가 정한다"
date: 2026-11-20 04:41:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, consumer-group, offset, exactly-once, deduplication, concept]
---

Kafka 컨슈머가 죽었다가 다시 뜨면 이미 처리한 메시지를 또 받거나, 받기만 하고 처리하지 못한 메시지를 건너뛴다. 어느 쪽이 되는지는 처리와 오프셋 커밋의 순서가 정한다. 이 글은 파티션, 키, 컨슈머 그룹, 오프셋 커밋, 세 가지 전달 보장을 이 순서 문제 하나로 묶어 정리하고, 내가 쓴 글 세 편을 예로 든다. 버전은 Kafka 4.4 기준이다.

## 순서는 파티션 안에서만 지켜진다

토픽은 파티션 여러 개로 나뉘고, 순서는 한 파티션 안에서만 보장된다. 프로듀서가 파티션을 직접 고르지 않으면 기본 파티셔너가 정한다.

| 레코드 | 파티션 선택 |
|---|---|
| 키 있음 | 키를 직렬화한 바이트의 murmur2 해시를 파티션 수로 나눈 나머지 |
| 키 없음 | sticky 파티션 하나에 몰아 보내다가, 배치가 일정 크기를 넘으면 다른 파티션으로 바꾼다 |

키가 같으면 같은 파티션에 들어가니, 같은 키끼리는 순서가 지켜진다. 대신 나머지 연산이라서 파티션 수를 늘리면 키와 파티션의 짝이 바뀐다. 늘린 뒤 들어오는 레코드는 같은 키라도 다른 파티션에 갈 수 있다.

예를 들어 광고 클릭 토픽의 메시지 키를 광고 계정 ID로 두면 같은 광고주의 클릭은 같은 파티션에 모인다. 이렇게 하면 광고주 단위 순서가 지켜지는 대신, 클릭이 많은 광고주 하나가 파티션 하나를 무겁게 만들 수 있다.

## 컨슈머 그룹과 오프셋

같은 `group.id`를 쓰는 컨슈머들이 한 그룹이다. 그룹 안에서 파티션 하나는 컨슈머 하나에만 배정된다. 그래서 한 그룹이 병렬로 읽을 수 있는 정도는 파티션 수를 넘지 못한다.

컨슈머가 어디까지 읽었는지는 오프셋으로 남는다. 커밋한 오프셋은 Kafka 내부 토픽에 메시지로 저장되고, 커밋 값은 "다음에 읽을 위치"다. 그래서 마지막으로 처리한 레코드의 오프셋에 1을 더해 커밋한다. 커밋된 오프셋이 없으면 `auto.offset.reset`(기본 `latest`)을 따른다.

## 커밋 순서와 세 가지 보장

컨슈머 쪽 보장은 처리와 위치 저장의 순서로 갈린다.

| 보장 | 순서 | 처리 중에 죽으면 |
|---|---|---|
| at-most-once | 읽기 → 위치 저장 → 처리 | 위치는 이미 넘어갔으니 그 메시지는 처리되지 않는다 (유실) |
| at-least-once | 읽기 → 처리 → 위치 저장 | 처리는 끝났는데 위치가 남지 않아 다시 받는다 (중복) |
| exactly-once | 처리 결과와 위치를 한 번에 저장 | 둘 다 남거나 둘 다 남지 않는다 |

at-least-once의 중복은 처리가 멱등이면 문제가 되지 않는다. 기본 키가 있어서 같은 메시지를 두 번 받아도 같은 레코드를 덮어쓰는 경우다. exactly-once는 결과와 오프셋을 같은 곳에 원자적으로 쓸 수 있을 때만 된다.

## 자동 커밋은 어느 쪽인가

`enable.auto.commit`의 기본값은 `true`이고, `auto.commit.interval.ms`(기본 5000ms)마다 오프셋을 백그라운드에서 커밋한다. 처리 완료와는 상관없이 시간이 되면 커밋한다는 뜻이다.

자동 커밋도 at-least-once가 될 수는 있다. 조건은 `poll()`이 돌려준 레코드를 다음 `poll()`이나 `close()` 전에 다 처리하는 것이다. 이 조건이 깨지면 커밋된 오프셋이 실제 처리 위치를 앞질러 레코드가 빠진다.

처리를 다른 스레드나 프레임워크에 넘기면 이 조건은 쉽게 깨진다. Spark Streaming(DStream)의 Kafka 0-10 연동에서 자동 커밋을 쓰면, poll한 메시지가 아직 Spark 출력으로 이어지지 않았을 수 있어 전달 보장이 정해지지 않는다(undefined semantics). 권장 방식은 `enable.auto.commit`을 `false`로 두고, 출력이 끝난 뒤 `commitAsync`로 커밋하는 것이다. 이렇게 해도 출력은 at-least-once라서 출력 쪽이 멱등이어야 한다. 새 API인 Structured Streaming은 Kafka 오프셋을 컨슈머 커밋에 맡기지 않고 처리한 오프셋 범위를 체크포인트에 저장하며, 파일 싱크로 쓴 출력은 exactly-once다.

[Spark Streaming으로 Hive에 쌓는 글](/posts/spark-streaming-to-hive/)의 스트리밍 잡은 오프셋을 컨슈머 자동 커밋에 맡겼고, 재시작하면 메시지가 겹치거나 빠질 수 있었다. 겹친 줄은 매일 그날 폴더를 `dropDuplicates()`로 지우는 일 배치가 정리했다. 빠진 메시지는 복구 모드(별도 컨슈머 그룹으로 다시 읽기)로 되찾고, 그때 생긴 중복은 같은 일 배치가 지웠다. 스트리밍 잡은 단순하게 두고, 정합성은 뒤의 배치와 복구 모드가 맡는 구조다.

줄 전체를 비교하는 `dropDuplicates()`로 중복이 잡힌 데는 앞단 구조도 한몫했다. [로그 수집 API 글](/posts/log-collection-api/)의 API는 요청에 담긴 이벤트를 이벤트마다 Kafka 메시지 하나로 나눠 보냈고, 스트리밍 잡은 메시지 하나를 한 줄로 이어 붙였다. Kafka에서 다시 받은 메시지는 바이트까지 같은 줄이 된다. 줄 비교는 Kafka 재전달로 생긴 중복을 지우는 용도다. 클라이언트 재전송까지 잡으려면 수집 API에서 이벤트 ID를 붙여야 한다.

## 멱등 프로듀서와 트랜잭션이 막아 주는 범위

프로듀서 쪽에서도 중복이 생긴다. 브로커는 썼는데 응답이 늦어 프로듀서가 재시도하면 같은 레코드가 두 번 들어간다. `enable.idempotence`가 이걸 막는다. 현재 기본값은 `true`이고, `acks`를 `all`이 아닌 값으로 두는 것처럼 맞지 않는 설정이 있으면 꺼진다. 명시적으로 켰는데 설정이 맞지 않으면 `ConfigException`이 난다.

멱등 프로듀서의 범위는 좁다. 한 프로듀서 세션 안에서 보낸 메시지의 재시도 중복만 막는다. 애플리케이션이 같은 내용을 두 번 `send()`하면 서로 다른 레코드 두 개다.

Kafka에서 읽어 Kafka로 쓰는 경우는 트랜잭션으로 exactly-once를 만들 수 있다. 출력 레코드와 컨슈머 오프셋을 한 트랜잭션에 넣는 방식이다.

```java
// Kafka 4.4 Java 클라이언트 기준 스케치
// producer: transactional.id 설정, consumer: enable.auto.commit=false, isolation.level=read_committed
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
        // 컨슈머 위치는 이미 앞으로 갔으니, 마지막 커밋 오프셋으로 seek해야 다시 읽는다
    }
}
```

자주 놓치는 부분이 둘 있다. 컨슈머 `isolation.level`의 기본값은 `read_uncommitted`라서, 뒤에서 읽는 쪽이 `read_committed`로 바꾸지 않으면 중단된 트랜잭션의 레코드도 읽는다. 그리고 이 보장은 Kafka 안에서 끝난다. 외부 시스템까지 exactly-once로 쓰려면 그 시스템이 협조해야 한다. HDFS 파일, Redis, MySQL로 나가는 순간 다시 at-least-once와 멱등 출력 문제로 돌아온다.

## 뒤에서 지우는 중복이 현실적인 경우

중복의 출처가 Kafka 바깥이면 Kafka 설정으로는 막을 수 없다. 같은 광고 클릭이 두 번 들어오는 건 로그 토픽에 서로 다른 메시지 두 개가 들어온 것이다. 이런 중복은 클릭 핸들러가 클릭에서 검증용 키를 만들고, Redis에 그 키가 있으면 따로 빼고, 없으면 키를 저장한 뒤 다음 토픽으로 보내는 식으로 거를 수 있다.

| 방법 | 중복을 판단하는 단위 | 잡는 것 | 못 잡는 것 |
|---|---|---|---|
| 멱등 프로듀서 | 프로듀서 세션 안의 재시도 | 브로커 응답 지연에 따른 재시도 | 앱이 두 번 보낸 같은 내용 |
| 트랜잭션 + `read_committed` | Kafka 안의 읽기-처리-쓰기 | Kafka 간 처리 중 재시작 | 외부 시스템 출력, 앞단 중복 |
| 일 배치 `dropDuplicates()` | 하루 폴더 안의 같은 줄 | 재전달로 생긴 같은 줄 | 유실, 내용이 조금 다른 중복 |
| Redis 키 확인 | 클릭에서 만든 검증용 키 | 같은 클릭의 반복 | 키 저장 뒤 전송 실패 시 클릭이 유실되는 경우 |

확인과 저장은 `SET NX EX` 한 번으로 묶을 수 있다. 키가 없을 때만 저장하고 이미 있으면 null을 돌려주니, 두 요청이 동시에 "없음"을 보는 일이 없다. 키를 먼저 쓰면 전송 실패 때 유실 쪽으로, 나중에 쓰면 중복 쪽으로 기운다. 클릭 과금에서는 같은 클릭을 두 번 청구하는 쪽이 광고주에게 더 큰 문제다.

## 참고 자료

- [Apache Kafka 4.4 Design: Message Delivery Semantics](https://kafka.apache.org/44/design/design/)
- [Apache Kafka 4.4 Consumer Configs](https://kafka.apache.org/44/configuration/consumer-configs/)
- [Apache Kafka 4.4 Producer Configs](https://kafka.apache.org/44/configuration/producer-configs/)
- [KafkaConsumer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [KafkaProducer Javadoc (4.4)](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [BuiltInPartitioner.java (apache/kafka)](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java)
- [Spark 4.3.0 Streaming + Kafka Integration Guide (0-10)](https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html)
- [Spark 4.3.0 Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html)
- [Spark 4.3.0 Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)
- [Redis SET command](https://redis.io/docs/latest/commands/set/)

## English summary

Kafka keeps order only inside a partition, and the default partitioner maps a key to a partition by hash, so adding partitions moves keys. Whether a consumer loses or repeats messages after a crash depends on whether it saves its offset before or after processing. The idempotent producer and transactions give exactly-once only for Kafka-to-Kafka work with `read_committed` readers. In my Spark Streaming job I left offsets to auto commit, removed duplicates with a daily `dropDuplicates()` batch, and reread missed messages with a recovery mode on a separate consumer group. For repeated ad clicks from outside Kafka, a Redis key check in front of the topic helps, and saving the key before sending leans toward losing a click rather than billing the same click twice.
