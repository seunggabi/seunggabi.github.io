---
title: "초당 10만 건 이벤트로 Kafka 파티션, 디스크, 네트워크 크기 잡기"
date: 2026-11-22 17:10:00 +0900
categories: [System Design, Messaging]
tags: [kafka, capacity-planning, partition, replication, system-design]
---

설계 문제에서 Kafka 규모를 물으면 숫자 하나로 답하기 쉽다. "30MB/s니까 파티션 6개"처럼. 그런데 파티션 수, 브로커 디스크, 네트워크는 서로 다른 숫자에서 나온다. 디스크는 하루 총량, 즉 평균에서 나오고, 파티션과 네트워크는 초당 최댓값, 즉 피크에서 나온다. 거기에 복제 계수가 디스크와 네트워크에 곱해진다. 이 글은 [급상승 검색어 설계](/posts/design-top-k-trending/)의 검색 이벤트를 예로 들어 그 순서를 처음부터 끝까지 계산한다.

기준: Kafka 4.4 (2026-11). 숫자는 설계용 가정이다.

## 주어진 것과 정할 것

| 가정 | 값 |
|---|---|
| 하루 이벤트 | 20억 건 |
| 이벤트 한 건 | 300 B (압축 전 JSON) |
| 피크 | 평균의 4배 |
| Kafka 보관 | 3일 (장애 뒤 다시 읽을 기간) |
| 복제 계수 | 3, `min.insync.replicas=2`, 프로듀서 `acks=all` |
| 읽는 쪽 | 컨슈머 그룹 3개 (실시간 집계, 레이크 적재, 감사) |
| 성장 | 2년 안에 10배 |

정할 것은 셋이다. 파티션 수, 브로커 디스크, 브로커 네트워크.

## 1단계: 평균과 피크를 나눠 적는다

```text
평균  20억 / 86,400초 ≈ 23,000건/초
피크  23,000 × 4 ≈ 93,000건/초  →  설계 피크 10만 건/초로 올림

평균 대역  23,000 × 300 B ≈ 7 MB/s
피크 대역  100,000 × 300 B = 30 MB/s
하루 총량  20억 × 300 B = 600 GB
```

단위를 한 번씩 소리 내서 읽는다. 300 B × 20억 = 6 × 10¹¹ B = 600 GB다. 여기서 B, KB, MB, GB, TB를 한 칸씩 헷갈리면 답이 1,000배 틀린다. 하루를 전부 피크로 계산하는 것도 흔한 실수다. 10만 × 300 B × 86,400초는 약 2.6 TB로, 실제 하루 총량의 4배가 넘는다.

| 숫자 | 어디에 쓰나 |
|---|---|
| 하루 총량 600 GB (평균에서 나옴) | 디스크, 보관 비용, 레이크 크기 |
| 피크 30 MB/s | 파티션 수, 네트워크, 컨슈머 병렬도 |

## 2단계: 파티션 수는 피크와 처리 속도로 정한다

일반 컨슈머 그룹에서는 파티션 하나를 컨슈머 하나만 읽는다. 4.2부터 정식이 된 share group은 예외지만, 키 순서와 키별 상태가 필요한 이 토픽에는 맞지 않는다. 그래서 피크를 따라잡는 데 필요한 컨슈머 수가 곧 파티션 수의 하한이다.

```text
필요 파티션 ≥ max(피크 ÷ 파티션당 쓰기 속도, 피크 ÷ 컨슈머 하나의 처리 속도)
```

쓰기 쪽은 파티션당 수십 MB/s도 무리가 없어서 보통 읽는 쪽이 정한다. 실시간 집계 컨슈머 하나가 역직렬화와 상태 갱신을 하며 초당 1만 6천 건, 약 5 MB/s를 처리한다고 잡으면 이렇다.

```text
지금   30 MB/s ÷ 5 MB/s = 6개
2년 뒤 300 MB/s ÷ 5 MB/s = 60개
```

2년 뒤 필요한 60개를 2의 거듭제곱으로 올려 처음부터 64개로 시작한다. 6개로 시작해 나중에 늘리면 되지 않느냐고 물을 수 있는데, 키가 있는 토픽은 그게 어렵다. 기본 파티셔너는 키 해시를 파티션 수로 나눈 나머지로 파티션을 고르므로, 파티션 수가 바뀌면 같은 `user_id`가 다른 파티션으로 간다. 사용자별 순서가 섞이고, 사용자 키로 상태를 쥔 컨슈머(중복 제거, 속도 제한)는 상태가 다른 인스턴스에 흩어진다. 파티션은 줄일 수도 없다. 그래서 키 있는 토픽은 성장까지 보고 한 번에 정한다. 파티션 수와 리밸런스는 [파티션과 lag 글](/posts/kafka-partitions-and-lag/)에 있다.

64개가 과한지는 반대쪽 비용으로 본다. 파티션마다 로그 세그먼트 파일과 리더·팔로워 복제 작업이 생긴다. 64개 × 복제 3을 브로커 3대에 나누면 브로커당 레플리카 64개다. Confluent의 옛 권장치(브로커당 2천~4천 개)와 비교해도 한참 아래이고, KRaft는 클러스터 단위 한계를 더 올렸다.

## 3단계: 디스크는 하루 총량 × 보관 × 복제

```text
600 GB/일 × 3일 × 복제 3 = 5.4 TB  (압축 전)
```

프로듀서 압축을 켜면 디스크와 네트워크가 함께 준다. JSON 로그는 `zstd`나 `lz4`로 몇 배씩 줄어드는 경우가 많지만 데이터마다 다르니, 실제 샘플로 압축률을 재고 나서 반영한다. 압축 전 숫자로 잡아 두면 남는 쪽으로 틀린다.

디스크는 꽉 채우지 않는다. 평균보다 큰 날, 장애 때 보관 연장, 아직 지울 수 없는 활성 세그먼트, 브로커가 죽었을 때 옮겨 오는 파티션을 생각해 사용률 60% 안팎을 목표로 잡으면, 5.4 TB ÷ 0.6 ≈ 9 TB가 필요하다. 브로커 3대면 대당 3 TB다. 보관 기간 3일은 "실시간 집계가 멈췄을 때 다시 읽을 수 있는 최대 기간"이라 운영 대응 시간과 맞춘다. 1년 보관 같은 장기 보관은 Kafka가 아니라 레이크가 맡는다.

## 4단계: 네트워크는 복제와 컨슈머 그룹 수를 곱한다

피크 30 MB/s가 들어올 때 클러스터 전체에서 오가는 양이다.

| 흐름 | 계산 | 피크 |
|---|---|---|
| 프로듀서 → 리더 | 30 MB/s | 30 MB/s 들어옴 |
| 리더 → 팔로워 2개 | 30 × 2 | 60 MB/s 나가고 60 MB/s 들어옴 |
| 리더 → 컨슈머 그룹 3개 | 30 × 3 | 90 MB/s 나감 |
| 합계 | | 들어옴 90 MB/s, 나감 150 MB/s |

브로커 3대에 고르게 나뉘면 대당 들어옴 30 MB/s, 나감 50 MB/s다. 10 Gbps 네트워크 카드 하나가 약 1.25 GB/s이니 지금은 여유가 크다. 가까운 팔로워에서 읽게 해도(`replica.selector.class`) 클러스터 전체 양은 같고 브로커별 몫만 바뀐다. 컨슈머 그룹이 하나 늘 때마다 나가는 양이 피크만큼 늘어난다는 점은 따로 적어 둔다. 레이크 적재나 재처리용 그룹을 붙이는 일은 흔하고, 재처리 그룹은 밀린 데이터를 피크보다 빠르게 읽는다.

## 5단계: 10배가 되면 무엇이 먼저 모자라나

| 항목 | 지금 | 10배 | 판단 |
|---|---|---|---|
| 피크 대역 | 30 MB/s | 300 MB/s | |
| 필요 파티션 | 6 | 60 | 64개로 버티지만 여유가 없다. 이때 128개로 새 토픽을 만들어 옮기는 계획을 세워 둔다 |
| 디스크 (3일, 복제 3, 60%) | 9 TB | 90 TB | 브로커를 늘리거나, tiered storage(RemoteStorageManager 플러그인 필요)로 오래된 세그먼트를 오브젝트 스토리지에 한 벌만 두고 로컬에는 몇 시간 치만 남긴다 |
| 브로커당 나감 (3대) | 50 MB/s | 500 MB/s | 10 Gbps의 40%. 브로커를 늘려 나눈다 |

10배에서 여유가 가장 적은 것은 파티션(60/64)이다. 디스크는 브로커당 30 TB가 되므로 장비 한 대가 감당할 용량을 넘는 순간 브로커를 늘리거나 tiered storage를 써야 한다. 네트워크는 40%라 아직 남지만, 브로커 하나가 죽으면 남은 두 대가 대당 600 MB/s를 내보내므로 여유는 이 숫자를 기준으로 본다.

## 자주 틀리는 곳

| 실수 | 결과 | 막는 법 |
|---|---|---|
| 단위 변환을 건너뜀 | 600 GB를 600 TB로 쓰는 식으로 1,000배 틀림 | 지수로 적고 B → GB → TB를 한 칸씩 읽는다 |
| 하루를 피크로 계산 | 디스크를 4배 넘게 잡음 | 디스크는 평균(하루 총량), 파티션·네트워크는 피크 |
| 복제 계수를 빠뜨림 | 디스크와 브로커 간 트래픽을 1/3로 잡음 | 디스크와 네트워크 줄에 "× 복제"를 항상 쓴다 |
| 지금 필요한 만큼만 파티션 | 키 있는 토픽을 나중에 늘리면 키 매핑이 바뀜 | 성장까지 보고 정하고, 늘릴 때는 새 토픽으로 옮긴다 |
| 컨슈머 그룹 수를 빼먹음 | 나가는 트래픽을 과소평가 | 그룹마다 피크를 한 번씩 더한다 |

## 묻고 답하기

Q. 피크를 왜 평균의 4배로 잡나?

서비스마다 다르다. 하루 트래픽이 저녁 몇 시간에 몰리는 서비스는 평균의 3~5배가 흔하고, 이벤트나 푸시 발송 직후 순간 피크는 더 높다. 실제 서비스라면 지난 몇 주의 분 단위 최댓값을 보고 정하고, 그 값이 없을 때 설계 문제에서는 4배 같은 가정을 먼저 말하고 시작한다. 가정을 숫자로 적어 두면 나중에 실측과 바로 비교할 수 있다.

Q. 컨슈머 하나 5 MB/s는 어디서 나온 숫자인가?

브로커가 아니라 컨슈머가 정한 숫자다. 메시지를 읽기만 하면 파티션 하나에서 수십 MB/s도 나오지만, 역직렬화하고 상태를 갱신하고 외부에 쓰면 컨슈머 하나의 처리량이 훨씬 낮다. 실제로는 컨슈머 하나를 띄워 피크와 비슷한 부하를 주고 처리량을 재서 이 자리에 넣는다. 그룹마다 처리 속도가 다르니, 파티션 수는 가장 느린 그룹 기준으로 정한다.

## 참고 자료

- [Apache Kafka 4.4 Documentation: Design, Replication](https://kafka.apache.org/44/design/design/)
- [Apache Kafka 4.4 Producer Configs (compression.type, acks)](https://kafka.apache.org/44/configuration/producer-configs/), [Topic Configs (retention.ms, min.insync.replicas)](https://kafka.apache.org/44/configuration/topic-configs/)
- [Apache Kafka: Tiered Storage](https://kafka.apache.org/44/operations/tiered-storage/)
- [Confluent: How to choose the number of topics/partitions in a Kafka cluster](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)

## English summary

Kafka sizing uses two different numbers. Disk comes from the daily total, which follows the average rate: 2 billion events of 300 bytes is 600 GB a day, times 3 days of retention and a replication factor of 3 gives 5.4 TB before compression, or about 9 TB at 60% disk use. Partitions and network come from the peak: four times the average rounds up to 100,000 events a second, or 30 MB/s. At 5 MB/s per consumer that needs 6 partitions, but a keyed topic should start at 64 because adding partitions later moves keys. Network multiplies the peak by replication and by the number of consumer groups. At ten times the load, partitions have the least headroom (60 of 64), disk grows to 30 TB per broker and needs more brokers or tiered storage, and the network should be checked with one broker down.
