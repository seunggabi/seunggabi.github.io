---
title: "Kafka 복제는 브로커 몇 대가 죽을 때까지 버틸까"
date: 2026-11-21 05:12:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, replication, isr, high-watermark, kraft, concept]
---

`replication.factor=3`, `min.insync.replicas=2`인 토픽에서 브로커 두 대를 연달아 재시작했다. `acks=all` 프로듀서는 이런 에러를 내며 재시도하는데, 같은 파티션을 읽는 컨슈머는 멀쩡히 돈다.

```text
org.apache.kafka.common.errors.NotEnoughReplicasException: ...
```

설정이 의도한 대로 동작한 결과다. 복제본 셋 중 하나만 살아 있으면 Kafka는 읽기는 계속 내주고, 쓰기 승인은 멈춘다. 어디까지 버티고 어디서 멈추는지는 ISR, high watermark, `acks`와 `min.insync.replicas`의 조합이 정한다.

버전은 Kafka 4.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 리더, 팔로워 | 파티션마다 쓰기와 읽기를 받는 리더 하나, 리더 로그를 fetch로 따라가는 팔로워 여럿 |
| ISR | 리더를 충분히 따라온 복제본 집합. 리더도 포함한다 |
| `replica.lag.time.max.ms` | 팔로워가 이 시간(기본 30초) 동안 fetch하지 않거나 리더 LEO를 따라잡지 못하면 ISR에서 빠진다 |
| LEO (log end offset) | 복제본마다 다음에 쓸 오프셋 |
| high watermark (HW) | ISR이 모두 복제한 지점. 컨슈머는 여기까지만 읽는다 |
| `acks` | 프로듀서가 기다리는 승인. `0`, `1`, `all`(기본) |
| `min.insync.replicas` | `acks=all` 쓰기가 성공하려면 필요한 최소 ISR 수. 기본 1 |
| `NotEnoughReplicas` | ISR이 최소보다 적어 `acks=all` 쓰기를 거절했다. 재시도 가능한 예외 |
| unclean leader election | ISR 밖 복제본을 리더로 뽑는 것. 기본 꺼짐, 켜면 유실을 감수한다 |
| ELR (KIP-966) | ISR 밖이지만 커밋된 데이터를 다 가진 복제본. 4.1부터 새 클러스터에서 기본으로 켜진다 |
| preferred leader | 복제본 목록의 첫 번째. 재시작 뒤 리더를 이쪽으로 되돌린다 |
| KRaft | ZooKeeper를 대신하는 컨트롤러 쿼럼. 4.0부터 ZooKeeper 모드가 없다 |

## ISR은 시간으로 정한다

파티션의 쓰기는 리더 하나가 받고, 팔로워는 리더에게 fetch 요청을 보내 같은 로그를 쌓는다. 리더는 이 중 따라오고 있는 복제본을 ISR로 관리한다. 기준은 시간이다. 팔로워가 `replica.lag.time.max.ms`(기본 30초) 동안 fetch를 보내지 않았거나, 그동안 리더의 LEO까지 따라오지 못했으면 리더가 ISR에서 뺀다. 브로커가 통째로 죽은 경우는 컨트롤러가 먼저 안다. 브로커는 컨트롤러에 하트비트를 보내고, `broker.session.timeout.ms`(기본 9초) 동안 하트비트가 없으면 컨트롤러가 그 브로커를 펜싱하고 ISR에서 뺀다.

high watermark는 ISR의 모든 복제본이 가진 지점이다. 팔로워의 fetch 요청에는 자기가 받을 다음 오프셋이 들어 있어서, 리더는 이것으로 각 팔로워의 LEO를 알고 HW를 올린다. 팔로워는 fetch 응답으로 리더의 HW를 받는다. HW 아래의 레코드가 커밋된 레코드이고, 컨슈머는 HW까지만 받는다(`read_committed` 컨슈머는 그보다 앞인 LSO에서 멈춘다. [멱등 프로듀서 글](/posts/kafka-idempotency/) 참고). `kafka-consumer-groups.sh`의 LOG-END-OFFSET 열이 이름과 달리 HW라는 점은 [파티션과 lag 글](/posts/kafka-partitions-and-lag/)에 적었다.

리더가 바뀌면 HW 위쪽, 커밋되지 않은 꼬리는 복제본마다 다를 수 있다. 팔로워는 leader epoch를 기준으로 새 리더의 로그와 갈라진 지점을 찾아 그 뒤를 잘라 낸다(KIP-101). 커밋되지 않은 레코드는 이때 사라질 수 있다.

데이터 복제는 과반 투표가 아니다. 과반 방식은 장애 f개를 견디려면 복제본 2f+1개가 필요하지만, ISR 방식은 ISR에 남은 복제본 하나만 있어도 커밋된 레코드를 지킨다. 그래서 복제본 f+1개로 장애 f개를 견딘다. 대신 ISR이 줄어든 상태에서 쓰기를 계속 받을지는 따로 정해야 하고, 그게 `min.insync.replicas`다.

## acks와 min.insync.replicas는 같이 정한다

| `acks` | 리더가 응답하는 시점 | 리더가 응답 직후 죽으면 |
|---|---|---|
| `0` | 기다리지 않는다. 받은 오프셋은 항상 `-1`, 재시도도 의미 없다 | 브로커가 받았는지조차 모른다 |
| `1` | 리더 로그에 쓴 뒤 | 팔로워가 아직 못 받은 레코드는 사라진다 |
| `all` (`-1`, 기본) | 현재 ISR 전부가 받은 뒤 | ISR에 하나라도 남아 있으면 남는다 |

`acks=all`이 기다리는 대상은 복제본 전부가 아닌 지금의 ISR 전부다. ISR이 리더 하나로 줄었다면 리더만 쓰고 승인한다. 이 구멍을 막는 게 `min.insync.replicas`다. ISR 수가 이 값보다 적으면 리더는 `acks=all` 쓰기를 `NotEnoughReplicas`로 거절한다. 레코드를 로그에 붙인 뒤에 ISR이 줄어든 걸 알게 되면 `NotEnoughReplicasAfterAppend`가 나는데, 이미 쓴 레코드라 재시도하면 중복이 생긴다. 둘 다 재시도 가능한 예외라서 프로듀서는 `delivery.timeout.ms`(기본 2분) 안에서 다시 보낸다. 재시도 중복을 지우는 멱등 프로듀서가 `acks=all`을 요구하는 이유는 앞의 멱등 프로듀서 글에 있다.

`min.insync.replicas`의 기본값은 1이고 `default.replication.factor`도 기본 1이다. 둘 다 운영에서 명시해야 하는 값이다. 흔한 조합인 RF 3, 최소 ISR 2가 견디는 범위는 이렇다.

| 내려간 브로커 (그 파티션의 복제본 기준) | `acks=all` 쓰기 | 읽기 | 커밋된 데이터 |
|---|---|---|---|
| 0 | 된다 | 된다 | 복제본 3개 |
| 1 | 된다. ISR이 최소와 같은 상태(at min ISR) | 된다. 리더가 죽었으면 ISR에서 새 리더 | 최소 2개 |
| 2 | `NotEnoughReplicas`로 거절 | 남은 리더가 HW까지 내준다 | 1개. 이 복제본이 디스크를 잃으면 사라진다 |
| 3 | 파티션 오프라인 | 안 된다 | ISR이나 ELR 복제본이 돌아올 때까지 기다린다 |

최소 ISR을 3으로 올리면 롤링 재시작처럼 브로커 하나만 내려가도 쓰기가 멈춘다. 1로 두면 리더 혼자 승인한 레코드가 그 리더의 디스크와 같이 사라질 수 있다. 2는 브로커 하나를 잃어도 쓰기가 계속되고, 승인된 레코드는 늘 두 곳 이상에 있는 값이다.

한 가지 더 있다. Kafka는 기본으로 레코드마다 fsync하지 않고(`log.flush.interval.messages` 기본값이 long 최댓값) 내구성을 복제에 맡긴다. 그래서 "두 곳 이상"이 두 브로커의 페이지 캐시에만 있는 순간이 있고, 같은 랙이나 AZ가 같이 꺼지면 함께 날아간다. 복제본을 장애 영역에 나누는 일이 그래서 필요하다.

## 리더 선출과 unclean, ELR

리더가 죽으면 컨트롤러가 ISR에서 새 리더를 고른다. ISR에 살아 있는 복제본이 없을 때가 문제다. `unclean.leader.election.enable`(기본 `false`)을 켜면 ISR 밖 복제본도 리더가 될 수 있다. 파티션은 빨리 살아나지만 그 복제본이 갖지 못한 커밋된 레코드는 사라지고, 다른 복제본은 새 리더에 맞춰 로그를 잘라 낸다. 끄면 ISR 복제본이 돌아올 때까지 파티션이 오프라인으로 남는다. 가용성과 일관성 사이의 선택이고, 0.11.0.0부터 기본은 일관성 쪽이다. KRaft에서 이 설정을 동적으로 켜면 주기적으로 도는 unclean 선출 스레드(기본 5분)를 기다리게 되니, 바로 필요하면 `kafka-leader-election.sh --election-type unclean`을 쓴다.

KIP-966의 ELR은 그 사이를 메운다. 4.x에서는 ISR이 최소 ISR보다 작으면 HW가 오르지 않는다(strict min ISR). ISR이 최소 아래로 줄어드는 순간 빠진 복제본은 그 시점까지의 커밋 데이터를 다 갖고 있으니, 컨트롤러가 이들을 ELR로 기록해 둔다. 리더를 뽑을 때 순서는 ISR, 펜싱되지 않은 ELR, 펜싱되지 않은 마지막 리더다. 막는 장면은 이렇다. 마지막 ISR 복제본이 페이지 캐시의 데이터를 잃은 채 비정상 종료했다가 리더로 돌아오면, 다른 복제본이 그에 맞춰 잘라 내 한 브로커의 유실이 전체 유실이 된다. ELR이 있으면 커밋 데이터를 가진 다른 복제본을 고를 수 있다.

ELR에는 따라오는 변화가 있다.

| 항목 | ELR이 켜졌을 때 |
|---|---|
| 켜는 법 | 4.1 이상으로 새로 만든 클러스터는 기본. 4.0이나 업그레이드한 클러스터는 `kafka-features.sh`로 `eligible.leader.replicas.version=1` |
| `acks=1` 쓰기 | ISR이 최소보다 적어도 리더는 승인하지만 HW가 오르지 않아 컨슈머에게 안 보인다. `acks=0/1` 토픽은 최소 ISR을 1로 두라는 게 KIP의 안내다 |
| `min.insync.replicas` 설정 위치 | 브로커 단위 값은 지워지고 클러스터 단위와 토픽 단위만 남는다. 바꾸면 ELR 상태가 초기화된다 |
| 확인 | `kafka-topics.sh --describe` 출력에 `Elr`, `LastKnownElr` 열이 붙는다 |

모든 복제본이 내려갔을 때 가장 많은 데이터를 가진 복제본을 찾아 리더로 세우는 unclean recovery는 KIP-966의 2부로 나뉘어 있고, 4.4의 ELR 문서는 1부만 다룬다.

로컬 클러스터에서 RF 3 토픽을 만들고 상태를 보는 명령이다.

```bash
# Kafka 4.4.0, 브로커 1,2,3이 있는 KRaft 클러스터
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders \
  --partitions 1 --replication-factor 3 --config min.insync.replicas=2

# 브로커 2, 3을 차례로 정상 종료한 뒤
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
#   Topic: orders  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1  Elr: 3  LastKnownElr:

bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-min-isr-partitions
```

출력 줄은 위 순서로 종료했을 때 나올 모양을 적은 예시다. 브로커 2가 내려갈 때는 ISR이 `1,3`으로 최소와 같아 ELR이 비어 있고, 브로커 3이 내려가 ISR이 최소 아래로 떨어질 때 3만 ELR에 들어간다. 2는 그 뒤에 커밋된 레코드를 갖고 있지 않다. `--at-min-isr-partitions`는 ISR이 최소와 같은 파티션, `--unavailable-partitions`는 리더가 없는 파티션을 보여 준다.

## preferred leader와 랙

복제본 목록의 첫 번째가 preferred leader다. 브로커가 재시작하면 그 브로커가 맡던 리더는 다른 복제본으로 넘어가고, 돌아온 브로커는 팔로워로 붙는다. 그대로 두면 리더가 일부 브로커에 몰린다. `auto.leader.rebalance.enable`(기본 `true`)이면 컨트롤러가 `leader.imbalance.check.interval.seconds`(기본 300초)마다 리더를 preferred 쪽으로 되돌린다. 끄면 `kafka-leader-election.sh --election-type preferred`로 직접 옮긴다.

`broker.rack`을 주면 복제본을 랙에 나눠 배치한다. 한 파티션은 min(랙 수, RF)개의 랙에 걸친다. 클라우드에서는 AZ를 랙으로 두는 경우가 많다. AZ 셋에 RF 3이면 AZ 하나가 통째로 꺼져도 위 표의 "1대"에 해당한다. 브로커에 `replica.selector.class`(`RackAwareReplicaSelector`)를, 컨슈머에 `client.rack`을 줘서 가까운 팔로워에서 읽게 하는 follower fetching(KIP-392)과 그 비용 효과는 [데이터 플랫폼 비용 글](/posts/data-platform-cost-levers/)에 적었다. 팔로워도 커밋된 레코드만 내주므로, HW가 팔로워에 전해지는 만큼 리더에서 읽을 때보다 조금 늦게 보인다.

## KRaft 쿼럼은 메타데이터만 맡는다

리더 선출, ISR 변경, ELR, 브로커 펜싱은 모두 컨트롤러의 일이다. Kafka 4.0부터 ZooKeeper 모드가 없어졌고, 컨트롤러 노드들이 Raft로 `__cluster_metadata` 로그를 복제한다. 쿼럼의 리더가 활성 컨트롤러이고, ISR이 바뀌면 그 변경이 이 로그의 레코드가 된다. ZooKeeper 클러스터는 브리지 릴리스(마지막은 3.9)에서 KRaft로 옮긴 뒤에야 4.x로 올릴 수 있다.

과반 투표는 이 메타데이터 쿼럼에만 쓴다. 컨트롤러는 과반이 살아 있어야 동작하므로 장애 N개를 견디려면 2N+1대가 필요하다. 3대면 1대, 5대면 2대다. 데이터 파티션은 앞에서 본 ISR 방식 그대로라, 컨트롤러 3대와 RF 3 토픽은 같은 "3"이어도 견디는 방식이 다르다. 쿼럼 구성은 고정 목록(`controller.quorum.voters`)과, 3.9부터 컨트롤러를 추가하고 뺄 수 있는 동적 쿼럼(`controller.quorum.bootstrap.servers`) 중 포맷할 때 고르고, 4.1부터는 `kraft.version`을 1로 올려 고정 목록을 동적 쿼럼으로 바꿀 수 있다. 브로커와 컨트롤러를 한 프로세스에 두는 combined 모드는 중요한 운영 환경에는 권하지 않는다.

## 복제가 지키는 범위와 그 밖

복제는 브로커가 승인한 레코드를 지킨다. 승인 전에 프로듀서가 버린 레코드나, 컨슈머가 커밋한 뒤 처리하지 못한 레코드는 다른 층의 일이다. 컨슈머 쪽 커밋 순서는 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)에 있다.

[로그 수집 API 글](/posts/log-collection-api/)의 API처럼 요청을 받아 Kafka로 넘기는 수집 계층도 프로듀서라서, 위의 `acks`와 최소 ISR 조합이 그대로 적용된다.

[Spark Streaming으로 Hive에 쌓는 글](/posts/spark-streaming-to-hive/)의 스트리밍 잡은 복구 모드로 다른 컨슈머 그룹을 써서 Kafka를 처음부터 다시 읽을 수 있었다. 다시 읽기는 Kafka에 남아 있는 레코드를 대상으로 하므로, unclean 선출로 잘려 나간 레코드는 범위 밖이다. 그 레코드를 지키는 것은 RF, 최소 ISR, unclean 선출 설정이다.

## 참고 자료

- [Apache Kafka 4.4 Design: Replication](https://kafka.apache.org/44/design/design/)
- [Apache Kafka 4.4 Broker Configs](https://kafka.apache.org/44/configuration/broker-configs/)
- [Apache Kafka 4.4 Producer Configs](https://kafka.apache.org/44/configuration/producer-configs/)
- [Apache Kafka 4.4 Eligible Leader Replicas](https://kafka.apache.org/44/operations/eligible-leader-replicas/)
- [Apache Kafka 4.4 Basic Operations (preferred replicas, racks)](https://kafka.apache.org/44/operations/basic-kafka-operations/)
- [Apache Kafka 4.4 KRaft](https://kafka.apache.org/44/operations/kraft/)
- [Apache Kafka 4.4 Upgrade notes](https://kafka.apache.org/44/getting-started/upgrade/)
- [KIP-101: Alter Replication Protocol to use Leader Epoch rather than High Watermark for Truncation](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
- [KIP-392: Allow consumers to fetch from closest replica](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica)
- [KIP-966: Eligible Leader Replicas](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas)
- [NotEnoughReplicasAfterAppendException.java (apache/kafka 4.4.0)](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/errors/NotEnoughReplicasAfterAppendException.java)
- [TopicCommand.java (apache/kafka 4.4.0)](https://github.com/apache/kafka/blob/4.4.0/tools/src/main/java/org/apache/kafka/tools/TopicCommand.java)

## English summary

Kafka replicates each partition from one leader to followers, and the leader keeps an in-sync replica set based on time: a follower that does not catch up within `replica.lag.time.max.ms` drops out, and consumers only read up to the high watermark that every ISR member has. `acks=all` waits for the current ISR, so `min.insync.replicas` decides when writes stop; with three replicas and a minimum of two, one broker can fail with no impact, two failures block writes but not reads, and Kafka 4.x adds eligible leader replicas so a safe out-of-ISR replica can lead instead of losing data. Leader election runs in the KRaft controller quorum, which uses majority voting only for metadata. My Spark Streaming job could re-read a topic in a recovery mode, but that only reaches records Kafka still holds, so the replication settings are what protect them. The minimum ISR is a trade-off: a higher value protects acknowledged records in more places, and every step up makes a single broker restart more likely to stop writes.
