---
title: "한 번 늘리면 줄일 수 없는 Kafka 파티션과 순서, 병렬성"
date: 2026-11-20 05:10:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, partition, consumer-group, rebalance, consumer-lag, concept]
description: "컨슈머를 늘려도 처리량이 그대로인 이유를 출발점으로, Kafka 파티션 수가 순서 보장과 병렬성, 리밸런스, 컨슈머 랙에 미치는 영향을 설명한다."
---

컨슈머를 여섯 개에서 열두 개로 늘렸는데 처리량이 그대로다. `kafka-consumer-groups.sh --describe --members`를 보면 새로 띄운 컨슈머 여섯 개의 `#PARTITIONS`가 0이다. 토픽 파티션이 여섯 개라서 생기는 일이다.

버전은 Kafka 4.4 기준이다.

## 파티셔너는 키가 있을 때와 없을 때 다르게 고른다

프로듀서가 파티션을 직접 정하지 않고 `partitioner.class`도 비워 두면 내장 파티셔너가 고른다. 키가 있으면 키 바이트의 murmur2 해시를 파티션 수로 나눈 나머지로 고른다는 점은 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)에 적었다. 여기서는 키가 없을 때를 더 본다.

| 시기 | 키가 없는 레코드의 파티션 선택 |
|---|---|
| 2.4 이전 | 레코드마다 파티션을 돌아가며 고른다 |
| 2.4 (KIP-480) | sticky 파티션 하나에 보내다가 새 배치가 만들어질 때 바꾼다 |
| 3.3 이후 (KIP-794) | 한 파티션에 `batch.size`(기본 16384바이트) 이상 보내면 바꾼다. 느린 브로커의 파티션에는 덜 보낸다 |

KIP-480의 방식은 새 배치가 늦게 만들어지는 쪽, 곧 응답이 느린 브로커의 파티션에 더 오래 머물러 레코드가 더 몰렸다. KIP-794가 바꾼 기준은 바이트 수이고, `partitioner.adaptive.partitioning.enable`(기본 `true`)이 브로커 속도에 맞춰 보내는 양을 조절한다. `partitioner.ignore.keys`를 `true`로 두면 키가 있는 레코드도 이 sticky 경로를 탄다.

순서는 한 파티션 안에서만 지켜진다. 키가 없는 레코드끼리는 처음부터 순서 보장이 없고, 키가 같은 레코드끼리만 같은 파티션에 모여 순서가 남는다.

예를 들어 광고 클릭 토픽이 광고 계정 ID를 메시지 키로 쓰면 같은 광고주의 클릭이 한 파티션에 모여 광고주 단위 순서가 남는다. 그 대가로 클릭이 많은 광고주가 있으면 그 파티션의 lag만 따로 커질 수 있다. 이 경우 컨슈머를 늘려도 그 파티션은 여전히 컨슈머 하나가 읽는다.

## 그룹 안에서 파티션 하나는 컨슈머 하나가 읽는다

같은 `group.id`를 쓰는 컨슈머가 한 그룹이고, 그룹 안에서 파티션 하나는 한 컨슈머에만 배정된다. 그래서 한 그룹의 병렬도는 구독한 파티션 수가 상한선이다. 파티션보다 많은 컨슈머는 배정받지 못하고 쉰다. 다른 멤버가 빠질 때 대신 들어갈 예비 인력 노릇은 한다.

그룹이 다르면 서로 상관없다. 그룹마다 모든 파티션을 따로 읽고, 커밋된 오프셋도 lag도 그룹별로 따로 있다.

[Logstash 릴레이 글](/posts/logstash-kafka-relay/)의 파이프라인은 개발과 운영이 서로 다른 컨슈머 그룹 ID를 썼다. 두 파이프라인이 같은 클러스터의 같은 토픽을 읽는다면 각자 모든 메시지를 받는다. lag를 볼 때도 두 그룹을 따로 봐야 한다.

## 리밸런스는 배정을 얼마나 멈추고 옮기는가

멤버가 들어오거나 나가거나 구독 파티션이 바뀌면 그룹은 배정을 다시 한다. 기존(classic) 프로토콜에서 이 일은 두 방식으로 나뉜다.

| 방식 | 리밸런스 동안 | 대표 assignor |
|---|---|---|
| eager | 모든 멤버가 가진 파티션을 전부 내려놓고 다시 받는다. 그동안 그룹 전체가 멈춘다 | `RangeAssignor`, `RoundRobinAssignor`, `StickyAssignor` |
| cooperative (KIP-429, 2.4) | 다른 멤버로 옮겨 갈 파티션만 내려놓는다. 내려놓은 파티션은 이어지는 두 번째 리밸런스에서 새 주인에게 간다 | `CooperativeStickyAssignor` |

`partition.assignment.strategy`의 기본값은 `[RangeAssignor, CooperativeStickyAssignor]`이고, 이 목록이면 실제로는 `RangeAssignor`를 쓴다. 기본값이 eager라는 뜻이다. 목록에서 `RangeAssignor`를 빼고 한 번 롤링 재시작하면 cooperative로 바뀐다. 처음부터 range만 쓰던 그룹은 두 assignor를 함께 넣는 재시작을 한 번 먼저 거친다.

배포처럼 잠깐 내려갔다 올라오는 재시작마다 리밸런스가 두 번(떠날 때, 돌아올 때) 도는 것도 비용이다. static membership(KIP-345, 2.3)이 이걸 줄인다. 컨슈머마다 고유한 `group.instance.id`를 주면 그 컨슈머는 닫힐 때 그룹을 떠난다고 알리지 않는다. `session.timeout.ms`(기본 45초) 안에 같은 ID로 돌아오면 리밸런스 없이 원래 배정을 그대로 돌려받는다. 같은 ID를 두 인스턴스가 쓰면 한쪽이 `FENCED_INSTANCE_ID` 오류를 받고 멈춘다.

대가는 그 공백 동안 그 컨슈머의 파티션을 아무도 읽지 않는다는 점이다. 그 파티션들의 lag가 재시작 시간만큼 쌓였다가 돌아온 뒤에 줄어든다. 세션 타임아웃을 길게 잡을수록 진짜로 죽은 컨슈머의 파티션이 넘어가는 시간도 길어진다.

4.0부터는 새 컨슈머 그룹 프로토콜(KIP-848)이 정식 기능이다. 클라이언트에서 `group.protocol=consumer`로 켜고, 배정은 브로커의 그룹 코디네이터가 계산한다. 전체 멤버가 한꺼번에 맞추는 동기화 단계 없이 옮겨야 할 파티션만 옮긴다. 이 모드에서는 `partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`를 클라이언트에서 쓰지 않고, 서버 쪽 assignor(`uniform`이 기본, `range`도 있다)와 브로커 설정 `group.consumer.session.timeout.ms`가 대신한다. 4.4에서도 클라이언트 기본값은 아직 `classic`이고, KIP-1274는 5.0에서 기본값을 `consumer`로 바꾸는 계획을 잡아 두었다.

## lag는 커밋된 오프셋과 로그 끝의 차이다

```
$ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group click-reader

GROUP         TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG    CONSUMER-ID       HOST        CLIENT-ID
click-reader  clicks  0          48210           48230           20     consumer-1-3f...  /127.0.0.1  consumer-1
click-reader  clicks  1          47990           48001           11     consumer-1-3f...  /127.0.0.1  consumer-1
click-reader  clicks  2          31544           52870           21326  consumer-2-9a...  /127.0.0.1  consumer-2
```

값은 설명을 위해 만든 예시다. 열마다 뜻은 이렇다.

| 열 | 값 |
|---|---|
| CURRENT-OFFSET | 그룹이 그 파티션에 커밋한 오프셋. 커밋이 없으면 `-` |
| LOG-END-OFFSET | `read_uncommitted` 기준 최신 오프셋, 곧 high watermark. 아직 복제가 끝나지 않은 레코드는 들어가지 않는다 |
| LAG | LOG-END-OFFSET에서 CURRENT-OFFSET을 뺀 값. 커밋이 없으면 `-` |

CURRENT-OFFSET은 컨슈머가 메모리에서 들고 있는 위치가 아니라 커밋된 값이다. 그래서 컨슈머가 실제로는 따라잡았어도 다음 커밋 전까지는 lag가 남아 보인다. 커밋 시점이 전달 보장을 어떻게 바꾸는지는 앞의 전달 보장 글에 적었다.

숫자 하나보다 파티션별 모양이 더 많은 걸 말해 준다. 위 예시처럼 한 파티션만 lag가 크면 그 파티션에 레코드가 몰렸거나 그 파티션을 맡은 컨슈머가 느린 것이다. 모든 파티션의 lag가 같이 늘면 그룹 전체의 처리 속도가 유입을 못 따라가는 것이다. 이 경우 파티션 수까지는 컨슈머를 늘려 풀 수 있지만, 한 파티션에 몰린 lag는 컨슈머를 늘려도 줄지 않는다.

컨슈머가 하나도 없어도 `--describe`는 커밋된 오프셋으로 lag를 계산하고 `has no active members`를 함께 찍는다. 리밸런스 중이면 `is rebalancing` 경고가 붙는다.

## 파티션 수는 늘릴 수만 있다

`kafka-topics.sh --alter --partitions`는 파티션을 늘리기만 한다. Kafka는 토픽의 파티션 수를 줄이는 기능을 제공하지 않는다. 늘릴 때도 기존 데이터는 재분배하지 않고, 해시 나머지가 바뀌므로 같은 키의 새 레코드가 다른 파티션으로 갈 수 있다.

줄이는 기능이 없는 이유는 구조에서 짐작할 수 있다(추론이다). 없앨 파티션의 레코드를 다른 파티션 뒤에 붙이면 그 레코드들은 새 오프셋을 받는다. 그 파티션에 커밋된 그룹 오프셋은 가리킬 곳이 없어지고, 파티션 안 순서도 두 파티션의 레코드가 섞여 깨진다.

그래서 파티션 수를 줄이려면 원하는 파티션 수로 새 토픽을 만들고, 프로듀서와 컨슈머를 옮겨야 한다. [로그 수집 API 글](/posts/log-collection-api/)의 API는 스키마 버전마다 경로와 토픽을 따로 두고, 스키마를 바꿀 때 새 경로와 새 토픽을 만든 뒤 옛 버전은 클라이언트가 다 옮겨 갈 때까지 남겨 두었다. 목적은 스키마였지만, 토픽을 갈아타는 절차는 파티션 수를 바꿀 때와 같은 모양이다.

## 참고 자료

- [Apache Kafka 4.4 Producer Configs](https://kafka.apache.org/44/configuration/producer-configs/)
- [Apache Kafka 4.4 Consumer Configs](https://kafka.apache.org/44/configuration/consumer-configs/)
- [Apache Kafka 4.4 Basic Kafka Operations](https://kafka.apache.org/44/operations/basic-kafka-operations/)
- [Apache Kafka 4.4 Consumer Rebalance Protocol](https://kafka.apache.org/44/operations/consumer-rebalance-protocol/)
- [KIP-480: Sticky Partitioner](https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner)
- [KIP-794: Strictly Uniform Sticky Partitioner](https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner)
- [KIP-429: Kafka Consumer Incremental Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol)
- [KIP-345: Introduce static membership protocol to reduce consumer rebalances](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-1274: Deprecate and remove support for Classic rebalance protocol in KafkaConsumer](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1274%3A+Deprecate+and+remove+support+for+Classic+rebalance+protocol+in+KafkaConsumer)
- [ConsumerGroupCommand.java (apache/kafka 4.4)](https://github.com/apache/kafka/blob/4.4/tools/src/main/java/org/apache/kafka/tools/consumer/group/ConsumerGroupCommand.java)
- [GroupOffsetsResetter.java (apache/kafka 4.4)](https://github.com/apache/kafka/blob/4.4/tools/src/main/java/org/apache/kafka/tools/GroupOffsetsResetter.java)

## English summary

A Kafka partition is both the unit of ordering and the unit of parallelism: records with the same key stay in one partition, and in a consumer group each partition is read by only one consumer. Records without a key go to a sticky partition that, since Kafka 3.3, changes after `batch.size` bytes. Rebalancing moves partitions between members, and cooperative-sticky assignment and static membership reduce how much stops during deploys, while the new broker-side protocol in Kafka 4.x does incremental assignment on the server. Lag in `kafka-consumer-groups.sh --describe` is the log end offset minus the committed offset, so it is best read per partition. Keying ad clicks by ad account keeps each advertiser in order, but one busy advertiser can then build lag on one partition that more consumers cannot fix.
