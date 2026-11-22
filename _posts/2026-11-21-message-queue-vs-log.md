---
title: "메시지 큐는 지우고 분산 로그는 남긴다"
date: 2026-11-21 06:13:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, rabbitmq, sqs, kinesis, pulsar, share-group, concept]
description: "메시지 큐는 ack를 받으면 메시지를 지우고 분산 로그는 오프셋만 옮기며 남겨 둔다. 이 차이가 replay와 소비 방식을 어떻게 가르는지 Kafka, RabbitMQ, SQS, Pulsar로 비교한다."
---

같은 행동 로그를 적재 잡과 다른 시스템으로 넘기는 릴레이가 둘 다 읽어야 하고, 장애가 나면 어제 오후부터 다시 읽어야 한다. 반대로 건마다 몇 분씩 걸리는 작업을 워커 수십 개가 나눠 가져가고, 실패한 건만 다시 돌리다 끝내 안 되는 건은 따로 빼 두어야 하는 경우도 있다. 앞의 요구는 분산 로그가, 뒤의 요구는 메시지 큐가 잘 맞는다. 큐는 메시지마다 상태를 들고 있다가 확인(ack)을 받으면 지운다. 로그는 레코드를 그대로 두고 소비자마다 읽은 위치 하나만 기억한다.

버전은 Kafka 4.4.0, RabbitMQ 4.4, Pulsar 5.0 LTS, 그리고 2026년 11월의 Amazon SQS·Kinesis Data Streams 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 분산 로그 | 레코드를 지우지 않고 오프셋 순서로 쌓는 저장소. 소비는 위치만 옮긴다 |
| 메시지 큐 | 메시지를 소비자 하나에 넘기고 ack를 받으면 지운다 |
| 오프셋 커밋 | 컨슈머 그룹이 파티션마다 어디까지 처리했는지 남기는 숫자 하나 |
| replay | 오프셋을 과거로 돌려 같은 레코드를 다시 읽는 일. retention 안에서만 된다 |
| push / pull | 브로커가 소비자에게 밀어 주는가, 소비자가 가져가는가 |
| prefetch | RabbitMQ에서 ack 없이 소비자에게 나가 있을 수 있는 메시지 수(`basic.qos`) |
| visibility timeout | SQS가 받아 간 메시지를 다른 소비자에게 숨기는 시간. 기본 30초, 최대 12시간 |
| message group | SQS FIFO에서 순서를 지키는 단위. 그룹 안에서는 한 번에 한 건만 나간다 |
| quorum queue | Raft로 복제하는 RabbitMQ 큐. 4.0부터 `delivery-limit` 기본 20 |
| RabbitMQ Streams | RabbitMQ 안의 append-only 로그. 읽어도 지워지지 않는다 |
| share group | 파티션을 나눠 갖지 않고 레코드 단위로 ack하는 Kafka 그룹(KIP-932) |
| DLQ | 정해진 횟수 넘게 실패한 메시지를 옮겨 두는 큐나 토픽 |

## 로그는 소비자마다 숫자 하나만 기억한다

Kafka 컨슈머는 브로커에서 fetch로 가져가는 pull 방식이다. 컨슈머 그룹은 파티션마다 "여기까지 처리했다"는 오프셋 하나를 `__consumer_offsets`에 커밋한다. 레코드 자체에는 누가 읽었는지가 적히지 않는다. 그래서 세 가지가 따라온다. 첫째, fan-out에 추가 저장 비용이 없다. 컨슈머 그룹을 하나 더 붙이면 오프셋 몇 개가 늘 뿐이고 레코드는 복사되지 않는다. [Logstash 릴레이 글](/posts/logstash-kafka-relay/)의 Logstash도 [로그 수집 API](/posts/log-collection-api/)가 보낸 토픽으로 보이는 두 토픽을 자기 그룹으로 구독했고, 그래서 같은 토픽을 읽는 다른 컨슈머 그룹과 상관없이 모든 메시지를 따로 받는다.

둘째, replay가 오프셋을 옮기는 일로 끝난다. 그룹이 멈춘 상태에서 시각으로 되돌리면 된다.

```bash
# Kafka 4.4.0, 그룹의 컨슈머가 모두 멈춘 상태에서
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group hive-loader --topic events --reset-offsets \
  --to-datetime 2026-11-20T15:00:00.000 --execute
```

[Spark Streaming 적재 글](/posts/spark-streaming-to-hive/)의 복구 모드는 잡 이름과 컨슈머 그룹 이름을 바꿔 평소 잡과 따로 토픽을 처음부터 다시 읽을 수 있게 했다. 평소 잡의 오프셋을 건드리지 않고 같은 레코드를 한 번 더 받는 방법이다. 되돌릴 수 있는 범위는 토픽의 retention이 정하고, 그 규칙은 [Kafka 보존과 compaction 글](/posts/kafka-log-retention-and-compaction/)에 있다.

셋째, 순서와 병렬성이 파티션에 묶인다. 그룹 안에서 파티션 하나는 컨슈머 하나만 읽으므로 순서는 파티션 안에서만 지켜지고, 워커 수는 파티션 수를 넘어도 놀기만 한다. 파티션과 lag의 관계는 [Kafka 파티션과 lag 글](/posts/kafka-partitions-and-lag/)에 정리했다. 이 구조의 약점은 건 단위 실패다. 오프셋이 숫자 하나라서 "300번은 실패, 301번은 성공"을 표현할 수 없다. 처리 못 하는 레코드 하나가 있으면 그 파티션을 붙잡고 재시도하거나, 다른 토픽에 써 두고 오프셋을 넘겨야 한다. 한 건이 몇 분씩 걸리면 `max.poll.interval.ms`(기본 5분) 안에 다음 `poll()`을 못 불러 컨슈머가 그룹에서 빠지기도 한다. 일반 컨슈머 그룹에는 내장 DLQ가 없다. Kafka Connect는 sink 커넥터에만, Kafka Streams는 4.2부터 예외 핸들러(KIP-1034)에 DLQ를 둔다.

## RabbitMQ는 메시지마다 ack를 받는다

RabbitMQ 소비자는 `basic.consume`으로 등록해 두고 브로커가 밀어 주는 메시지를 받는다. `basic.get`으로 한 건씩 당기는 폴링은 권장하지 않는 방식이다. 밀어 주는 양은 prefetch(`basic.qos`)가 막는다. ack하지 않은 메시지가 prefetch만큼 쌓이면 그 소비자에게는 더 보내지 않는다. 소비자는 건마다 `basic.ack`, 또는 `basic.nack`/`basic.reject`로 답한다. requeue를 켜고 거절하거나 채널이 끊기면 메시지는 다시 큐로 돌아가 다른 소비자에게 간다. ack 없이 오래 붙잡는 소비자를 끊는 delivery acknowledgement timeout은 기본 30분이고, 4.3부터는 quorum queue에서만 지원한다.

큐 종류에 따라 내구성과 재배달 규칙이 다르다.

- classic queue는 복제하지 않는다. 미러링은 4.0에서 없어졌다.
- quorum queue는 Raft로 복제하고 기본 3개 멤버로 시작한다. 과반이 받아야 publisher confirm이 나간다. 4.0부터 `delivery-limit` 기본값이 20이라, 그보다 많이 다시 배달된 메시지는 버려지거나 dead-letter exchange가 있으면 그쪽으로 간다. dead-lettering은 기본이 at-most-once이고, `dead-letter-strategy=at-least-once`는 `overflow=reject-publish`와 함께 따로 켜야 한다.
- stream은 읽어도 지우지 않는 로그다. 소비자는 `x-stream-offset`으로 `first`, `last`, `next`(기본), 오프셋, 시각 가운데 어디서 읽을지 고른다. 3.11부터 super stream으로 파티션을 나누고 single active consumer로 순서를 지킨다. 다만 `x-max-age`, `x-max-length-bytes`의 기본값이 없어서 보존을 정하지 않으면 디스크가 찰 때까지 자란다.
- quorum queue와 stream은 과반 합의라서 3노드 중 2노드가 살아 있어야 쓴다. Kafka 쪽 복제(ISR과 `min.insync.replicas`)는 [Kafka 복제와 ISR 글](/posts/kafka-replication-and-isr/)에 있다.

## SQS는 받아 간 메시지를 숨겼다가 지운다

SQS는 `ReceiveMessage`로 가져가는 pull 방식이다. 한 번에 최대 10건, long polling 대기는 최대 20초다. 받은 메시지는 큐에서 사라지지 않고 visibility timeout(기본 30초) 동안 다른 소비자에게 보이지 않는다. 그 안에 `DeleteMessage`를 부르지 않으면 다시 보인다. 처리 시간이 timeout보다 길면 `ChangeMessageVisibility`로 늘리는데, 첫 수신부터 12시간을 넘길 수는 없다. timeout을 처리 시간보다 짧게 잡으면 처리 중인 메시지를 다른 워커가 또 가져간다.

| 항목 | standard | FIFO |
|---|---|---|
| 순서 | 보장하지 않는다 | `MessageGroupId` 안에서 지킨다 |
| 중복 | at-least-once라 생길 수 있다 | 5분 안의 같은 deduplication ID는 넣지 않는다 |
| 처리량 | 사실상 제한 없음 | 파티션당 액션별 300 TPS(배치 10건이면 3,000건/초), high throughput 모드는 더 높다 |
| 그룹 동작 | `MessageGroupId`를 주면 fair queue로 그룹 간 몫을 맞춘다 | 그룹의 메시지가 처리 중이면 그 그룹의 다음 메시지는 나가지 않는다 |

보존은 기본 4일(1분~14일), 메시지 크기는 최대 1MiB다. DLQ는 redrive policy의 `maxReceiveCount`로 건다. 그만큼 받아 가고도 지워지지 않은 메시지가 DLQ로 옮겨진다. standard 큐는 DLQ에서도 원래 큐에 들어온 시각으로 만료를 계산하므로 DLQ의 보존 기간을 원래 큐보다 길게 잡아야 한다. FIFO는 옮길 때 시각을 새로 잡지만, DLQ로 빠진 메시지 때문에 그룹 안 순서는 깨진다.

## Kinesis와 Pulsar는 로그 위에 큐 동작을 더했다

Kinesis Data Streams는 Kafka와 같은 로그 모델이다. provisioned 모드에서 샤드 하나는 초당 1MB 또는 1,000건을 받고, 읽기는 샤드당 2MB/초를 모든 소비자가 나눠 쓴다. `GetRecords`는 샤드당 초당 5번이다. 소비자가 여럿이면 enhanced fan-out으로 등록해 소비자마다 샤드당 2MB/초를 따로 받는다. 이때는 `SubscribeToShard`가 HTTP/2로 밀어 주고, 지연은 평균 70ms 정도다. 등록 소비자는 스트림당 20개(On-demand Advantage는 50개)까지다. 보존은 기본 24시간, 최대 365일이다. 그 등록 소비자 자리에 queue를 붙일 수도 있다. On-demand 스트림에서만 되고, 소비자 인스턴스가 샤드를 리스하지 않고 다음에 보이는 레코드를 받아 ack한다. 순서는 보장하지 않고 at-least-once로 배달한다. 레코드는 스트림에 그대로 남아 다른 스트림 소비자는 영향을 받지 않는다.

Pulsar는 브로커와 저장소를 나눴다. 브로커는 상태가 없고, 토픽의 로그(managed ledger)와 구독 위치(cursor)는 BookKeeper bookie가 복제해 들고 있다. 읽는 방식은 구독 타입이 정한다. 같은 토픽에 로그처럼 읽는 구독과 큐처럼 읽는 구독을 같이 둘 수 있다. retention 정책이 없으면 모든 구독이 ack한 메시지는 바로 지운다. ack timeout은 기본으로 꺼져 있고, DLQ(`maxRedeliverCount`, 기본 이름 `<topic>-<subscription>-DLQ`)는 Shared와 Key_Shared에서만 쓴다.

| 구독 타입 | 동작 | 순서 |
|---|---|---|
| Exclusive(기본) | 소비자 하나만 붙는다 | 지킨다 |
| Failover | 하나가 받다가 죽으면 다음 소비자가 넘겨받는다 | 지킨다. 넘겨받을 때 중복이 생길 수 있다 |
| Shared | 소비자들에게 나눠 준다 | 지키지 않는다 |
| Key_Shared | 같은 키는 같은 소비자에게 간다 | 키 안에서 지킨다 |

## Kafka share group은 4.2부터 운영용이다

KIP-932 "Queues for Kafka"는 4.0에서 early access, 4.1에서 preview를 거쳐 4.2에서 production-ready가 됐다. 4.4는 share group용 DLQ(KIP-1191)를 더했다. 토픽은 그대로 로그이고, 그 위에서 읽는 방식만 큐처럼 바뀐다.

- 컨슈머 수가 파티션 수를 넘어도 된다. 파티션을 나눠 갖지 않고 레코드를 잠가(acquire) 가져간다.
- 잠금은 `share.record.lock.duration.ms`(기본 30초) 동안 유지된다. 시간이 지나면 다른 컨슈머가 받을 수 있다.
- 컨슈머는 레코드마다 `ACCEPT`, `RELEASE`, `REJECT`, `RENEW`로 답한다. `RENEW`는 4.2에서 들어온 잠금 연장(KIP-1222)이다.
- 배달 횟수가 `share.delivery.count.limit`(기본 5)에 닿거나 `REJECT`된 레코드는 archived가 되어 다시 나가지 않는다. 4.4에서 `share.version=2`이고 그룹에 `errors.deadletterqueue.topic.name`을 주면 그 토픽으로 간다. 토픽 이름은 `errors.deadletterqueue.topic.name.prefix`(기본 `dlq.`)로 시작해야 하고, 자동 생성(기본 꺼짐)을 켜지 않았다면 직접 만들어 `errors.deadletterqueue.group.enable=true`를 걸어 둔다.
- 순서는 한 배치 안의 오프셋 순서뿐이다. 다시 배달된 레코드는 순서가 뒤섞인다.
- 새 그룹은 `share.auto.offset.reset`(기본 `latest`)에 따라 시작하고, 그룹 상태는 `__share_group_state` 토픽에 남는다.

4.4로 새로 포맷한 클러스터는 `share.version=2`로 시작한다(소스의 `ShareVersion`과 `MetadataVersion` 기준). 오래된 클러스터를 올렸다면 `kafka-features.sh`로 이 기능 레벨을 올려야 한다. 단일 노드 quickstart에서 확인하는 순서다. 콘솔 share 컨슈머에 `--release`를 붙이면 받은 레코드를 매번 `RELEASE`해서 배달 횟수가 늘고, `--reject`는 바로 archived로 보낸다.

```bash
bin/kafka-features.sh --bootstrap-server localhost:9092 describe | grep share.version
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic jobs --partitions 1
bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --entity-type groups --entity-name workers --add-config share.auto.offset.reset=earliest

# 터미널 두 개에서 같은 그룹으로 띄우면 파티션 1개를 둘이 나눠 받는다
bin/kafka-console-share-consumer.sh --bootstrap-server localhost:9092 --topic jobs --group workers
bin/kafka-share-groups.sh --bootstrap-server localhost:9092 --describe --group workers
```

## 파이프라인에서 고르는 기준

| 요구 | 맞는 쪽 | 이유 |
|---|---|---|
| 같은 이벤트를 여러 시스템이 각자 읽는다 | Kafka 컨슈머 그룹, Kinesis enhanced fan-out, RabbitMQ stream | 읽는 쪽마다 위치만 따로 둔다 |
| 어제 데이터를 다시 처리한다 | Kafka, Kinesis, 보존을 건 Pulsar·RabbitMQ stream | 읽어도 지워지지 않는다 |
| 키별 순서와 병렬 처리를 같이 원한다 | Kafka 키 파티셔닝, SQS FIFO message group, Pulsar Key_Shared | 순서 단위를 키로 좁힌다 |
| 건마다 오래 걸리고 실패한 건만 다시 돌린다 | SQS, RabbitMQ quorum queue, Kafka share group, Kinesis queue | 메시지마다 잠금과 배달 횟수를 센다 |
| 끝내 실패한 건을 따로 모은다 | SQS redrive, quorum queue + DLX, Pulsar DLQ, share group DLQ(4.4) | 내장 DLQ가 있다 |

Spark Streaming 적재는 수집 API가 보낸 토픽을 읽었고, Logstash 릴레이는 그 토픽으로 보이는 토픽을 자기 그룹으로 따로 읽었다. 복구 모드는 다른 그룹으로 처음부터 다시 읽는다. 둘 다 로그 쪽 성질이다. 스트리밍은 오프셋을 자동 커밋에 맡겼고, 재시작이나 복구로 겹친 줄은 매일 도는 배치가 `dropDuplicates()`로 지웠다.

어느 쪽을 골라도 기본 배달 보장은 at-least-once다. SQS FIFO의 중복 제거는 보내는 쪽 5분 창에 한정되고, 소비 쪽 중복은 처리 로직이 견뎌야 한다. 커밋 순서와 중복의 관계는 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)에 있다. Kafka share group, Kinesis queue, RabbitMQ stream처럼 양쪽이 상대 동작을 들여오고 있지만 share group은 순서를 포기한 대가로 병렬성을 얻고, RabbitMQ stream은 retention을 직접 정해야 한다는 제약은 남는다.

## 참고 자료

- [Apache Kafka 4.4 Upgrade Notes (share group 4.1~4.4 변경)](https://kafka.apache.org/44/getting-started/upgrade/)
- [Apache Kafka 4.4 Group Configs](https://kafka.apache.org/44/configuration/group-configs/)
- [Apache Kafka 4.2.0](https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/), [4.4.0](https://kafka.apache.org/blog/2026/10/22/apache-kafka-4.4.0-release-announcement/) Release Announcement
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [ShareVersion.java (4.4.0)](https://github.com/apache/kafka/blob/4.4.0/server-common/src/main/java/org/apache/kafka/server/common/ShareVersion.java)
- [RabbitMQ 4.4 Consumers](https://www.rabbitmq.com/docs/consumers), [Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues), [Streams](https://www.rabbitmq.com/docs/streams)
- [RabbitMQ 4.0.1 release notes (classic queue mirroring 제거)](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v4.0.1)
- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Amazon SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)
- [Amazon SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Kinesis Data Streams quotas and limits](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
- [Kinesis enhanced fan-out consumers](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html)
- [Kinesis: Consume records with a queue](https://docs.aws.amazon.com/streams/latest/dev/queues-overview.html)
- [Apache Pulsar 5.0 Architecture Overview](https://pulsar.apache.org/docs/5.0.x/concepts-architecture-overview/), [Messaging Concepts](https://pulsar.apache.org/docs/5.0.x/concepts-messaging/)

## English summary

A message queue keeps state per message and deletes it once a consumer acknowledges it, while a distributed log keeps every record and stores only a position per consumer. That is why Kafka gives cheap fan-out and replay but ties ordering and parallelism to partitions, while RabbitMQ quorum queues and SQS give per-message acks, redelivery limits and dead-letter queues. In the music service pipeline I worked on, a Spark Streaming loader read the collection API's topics, a Logstash relay read what appear to be the same topics with its own consumer group, and the loader's recovery mode could reread a topic from the start under a separate consumer group, which are log properties. The systems now borrow from each other: Kafka share groups became production-ready in 4.2 and gained a DLQ in 4.4, Kinesis added queues, and RabbitMQ has streams. The trade-off remains that queue-style consumption buys parallelism per record by giving up ordering.
