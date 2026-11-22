---
title: "배포할 때마다 끊기던 API 동기화를 Kafka 구독으로 바꾼 이유"
date: 2026-11-21 14:10:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, messaging, consumer-group, idempotency, ad-tech, deployment]
pin: true
---

이전 회사의 커머스 광고 플랫폼에는 데이터가 바뀌면 그 내용을 다른 서비스에 알려 주는 동기화 서버가 있었다. 처음에는 동기화 서버가 받는 쪽 서비스의 API를 비동기로 호출했다. 그런데 받는 쪽 서버를 배포하는 동안 들어간 호출은 끊겼고, 끊긴 변경은 그 서비스에 반영되지 않았다. 그래서 동기화 서버가 Kafka 토픽에 발행하고 받는 쪽 서비스가 구독하는 구조로 바꿨다. 바꾼 뒤로 배포 때문에 동기화가 빠진 일은 없었다.

회사 내부 이름과 서비스 이름은 뺐다.

## 호출 방식은 받는 쪽이 그 순간 떠 있어야 한다

API 호출로 변경을 전하면, 변경 하나가 반영되려면 호출하는 순간 받는 쪽이 요청을 받을 수 있어야 한다. 받는 쪽이 재시작 중이면 보통 연결이 거부되거나, 처리하던 요청이 프로세스 종료와 함께 끝난다. 호출을 비동기로 보내면 동기화 서버는 받는 쪽 응답을 기다리지 않지만, 그렇다고 실패한 호출이 어딘가에 남는 것은 아니다. 그래서 배포 중에 나간 변경은 빠졌다.

## 토픽에 남기고 각자 읽게 했다

바꾼 구조에서 동기화 서버는 변경을 토픽에 발행하고 끝낸다. 받는 쪽 서비스는 토픽을 구독한다. Kafka 구독에서는 읽는 쪽이 컨슈머 그룹으로 읽고, 어디까지 읽었는지 오프셋을 커밋한다.

```text
전: 동기화 서버 --API 호출--> 받는 서비스들        (받는 쪽 재시작 중에 나간 변경은 빠짐)
후: 동기화 서버 --> Kafka 토픽 --> 구독하는 서비스들     (재시작 동안은 읽을 거리가 쌓임)
```

받는 쪽을 배포하는 동안에는 소비가 잠깐 멈출 뿐이다. 새 인스턴스가 뜨면 마지막으로 커밋한 오프셋부터 다시 읽으니, 배포 중에 발행된 변경도 빠지지 않는다. 단, 토픽 보관 기간(retention) 안에서다. 보관 기간보다 오래 멈춰 있으면 그 사이 메시지는 지워진다. 받는 쪽이 하나 늘어날 때도 동기화 서버는 그대로 두고, 새 서비스가 새 컨슈머 그룹으로 같은 토픽을 읽으면 된다. 토픽이 메시지를 보관하는 방식과 큐와의 차이는 [메시지 큐와 로그 글](/posts/message-queue-vs-log/)에 정리했다.

## 같은 메시지가 두 번 올 수 있다

컨슈머가 메시지를 처리하고 오프셋을 커밋하기 전에 죽으면, 다시 떴을 때 그 메시지를 한 번 더 읽는다. 배포가 바로 그 순간을 만든다. 그래서 받는 쪽은 같은 변경을 두 번 받아도 결과가 같아야 한다. 이 플랫폼의 받는 쪽 서비스는 중복으로 들어온 변경을 내부 로직에서 이미 처리하고 있었다. 전달 보장 단계별로 무엇이 중복되고 무엇이 빠지는지는 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)과 [멱등 처리 글](/posts/kafka-idempotency/)에 있다.

순서도 따져야 한다. Kafka는 파티션 안에서만 순서를 지킨다. 같은 대상의 변경은 같은 키로 발행해 한 파티션에 모아야, 앞의 변경이 늦게 도착해 뒤의 변경을 덮어쓰는 일이 없다.

## 실패가 에러 대신 lag로 보인다

API 방식에서는 실패가 호출 에러로 바로 보인다. 구독 방식에서는 받는 쪽이 죽어 있어도 동기화 서버는 아무 에러 없이 발행을 계속하고, 실패는 지연으로만 드러난다. 그래서 감시 대상을 컨슈머 그룹별 lag로 바꿔야 한다. 배포 동안 쌓였다가 다시 줄어드는 lag와 계속 늘어나는 lag를 구분하는 기준이 필요하다. lag를 읽는 법은 [파티션과 lag 글](/posts/kafka-partitions-and-lag/)에 있다.

## 묻고 답하기

Q. 동기화 서버에 재시도 큐를 붙이면 되지 않나?

일반적으로는 실패한 호출을 재시도 큐에 넣는 방법도 있다. 다만 동기화 서버가 재시작해도 큐가 남으려면 디스크에 쓰는 로그가 필요하고, 받는 쪽마다 어디까지 보냈는지도 따로 관리해야 한다. 그걸 제대로 만들면 결국 Kafka의 토픽과 컨슈머 오프셋을 다시 만드는 셈이다.

Q. 받는 쪽 배포를 무중단으로 만들면 되지 않나?

일반적으로 무중단 배포를 해도 종료되는 인스턴스가 처리 중인 요청을 끝내고 내려가게 서비스마다 맞춰야 하고, 받는 쪽이 장애로 오래 죽어 있는 경우는 막지 못한다. 브로커를 두면 받는 쪽이 멈춘 동안의 변경이 보관 기간 안에서는 토픽에 남는다.

## English summary

On the commerce ad platform at my previous company, a sync server sent each data change to other services by calling their APIs asynchronously. Whenever a receiving service was being deployed, those calls failed and the changes never arrived. I changed the sync server to publish changes to a Kafka topic, and the receiving services subscribe to it. A deploy now only pauses consumption, and the service picks up from its last committed offset. Duplicates can still happen, and the receiving logic already handled repeated changes. A retry queue on the sender would need a durable log and per-receiver read positions, which is what a Kafka topic and consumer offsets already are.
