---
title: "Logstash로 Kafka 로그를 TCP 로그 시스템에 넘기기"
date: 2026-11-20 05:01:00 +0900
categories: [Ingestion & CDC, Logstash]
tags: [logstash, kafka, kubernetes]
description: "Kafka에 쌓인 행동 로그를 TCP로 한 줄씩 받는 사내 로그 시스템에 넘기려고 Logstash를 가운데 두고 JSON을 풀어 형식에 맞춘 과정을 정리한다."
---

Kafka에 쌓인 행동 로그를 사내의 다른 로그 시스템에도 보내야 했다. 그쪽은 TCP로 정해진 형식의 한 줄씩 받는 시스템이라, Kafka 메시지를 그대로 넘길 수 없었다. 그래서 가운데에 Logstash를 두고 Kafka 메시지를 읽어 JSON을 풀고, 이벤트 하나를 한 줄 문자열로 바꿔 보냈다. 이전 회사에서 음악·오디오 서비스의 데이터 쪽을 맡으면서 만든 것이다.

회사 내부 이름과 토픽, 주소, 설정 값은 뺐다. 버전은 Logstash 7.9 기준이다.

## 입력은 수집 API 쪽 토픽 두 개

[로그 수집 API](/posts/log-collection-api/)는 로그를 나라와 스키마 버전별 Kafka 토픽으로 보낸다. Logstash는 수집 API가 보낸 토픽으로 보이는 두 토픽을 구독했다. 파이프라인 파일은 개발과 운영 두 벌이고, 다른 점은 컨슈머 그룹 ID, 클라이언트 ID, Kafka 주소, 보낼 곳 주소뿐이다. 필터와 출력 형식은 같다.

`auto_offset_reset`은 `earliest`로 두었다. 이 값은 그룹에 커밋된 오프셋이 없을 때만 쓰인다. 처음 뜬 그룹은 남아 있는 가장 오래된 메시지부터 읽고, 그다음부터는 커밋된 오프셋에서 이어 읽는다. Kafka에서 파티션 하나는 구독하는 그룹마다 컨슈머 하나가 읽으므로, 이 그룹은 같은 토픽을 읽는 다른 컨슈머 그룹과 상관없이 모든 메시지를 따로 받는다.

## 메시지 하나가 한 줄이 되기까지

파이프라인은 필터 세 개와 출력 하나다. 아래는 구조만 옮긴 스케치다. 필드 이름과 형식의 고정 부분은 바꿨다.

```ruby
# 구조만 보여 주는 스케치
input {
  kafka {
    topics            => ["<topic_a>", "<topic_b>"]
    group_id          => "<env_group>"
    auto_offset_reset => "earliest"
  }
}
filter {
  json  { source => "message"  target => "log" }
  split { field => "[log][events]"  remove_field => ["message"] }
  ruby  { code => "event.set('epoch', event.get('@timestamp').to_i)" }
}
output {
  tcp {
    mode  => "client"
    host  => "<receiver>"
    port  => <port>
    codec => line { format => "<고정 필드> %{epoch} <고정 필드들> %{log}" }
  }
}
```

단계마다 이벤트의 모양은 이렇게 바뀐다.

| 단계 | 하는 일 | 지난 뒤의 이벤트 |
|---|---|---|
| kafka 입력 | 기본 코덱 `plain`으로 메시지를 읽는다 | `message`에 JSON 문자열 하나, `@timestamp`는 Logstash가 처음 본 시각 |
| json | `message`를 파싱해 `target` 필드 아래에 넣는다 | 모델 클래스 기준으로 보면 헤더, 클라이언트 정보, 이벤트 배열이 한 필드 아래 객체로 들어간다 |
| split | 이벤트 배열의 원소마다 이벤트를 만들고, 성공하면 `message`를 지운다 | 배열 자리에 원소 하나만 남고, 원래 문자열은 없어진다 |
| ruby | `@timestamp`를 초 단위 정수로 바꿔 새 필드에 넣는다 | 정수 시각 필드가 하나 늘어난다 |
| tcp 출력, line 코덱 | `format` 문자열로 한 줄을 만들어 보낸다 | 고정 필드와 초 단위 시각, JSON 본문이 공백으로 이어진 한 줄 |

수집 API가 이미 이벤트마다 메시지를 나눠 보내므로, 메시지 하나의 이벤트 배열에는 보통 원소가 하나다. 그래서 split이 하는 일은 대개 원소 하나짜리 배열을 벗겨 객체 하나로 만드는 것이다. 원소가 여럿 든 메시지가 들어와도 원소마다 이벤트가 따로 생기므로, 받는 쪽에는 어느 경우든 이벤트 하나가 한 줄로 간다.

split으로 만든 이벤트에는 헤더와 클라이언트 정보가 그대로 남는다. 받는 쪽은 다른 줄을 찾지 않고 한 줄 안에서 이벤트와 그 맥락을 같이 본다.

`%{log}`처럼 객체 필드를 `format`에 넣으면 Logstash는 그 객체를 JSON 문자열로 직렬화해 끼운다. 그래서 `json` 필터로 풀었던 로그가 출력에서 다시 JSON이 된다. 배열 필드였다면 쉼표로 이어 붙인다.

날짜 필터가 없으면 `@timestamp`는 Logstash가 메시지를 처음 읽은 시각이다. 그래서 받는 쪽이 한 줄 앞에서 보는 초 단위 시각은 수신 시각이고, 이벤트가 일어난 시각은 JSON 본문 안에 그대로 남는다.

## Pod 하나에 띄우고 설정은 자동 리로드

Logstash는 사내 쿠버네티스 플랫폼에 Logstash 7.9 이미지로 Pod 하나를 띄워 돌렸다. 파이프라인은 `--config.reload.automatic`을 주고 실행했다.

이 옵션을 주면 Logstash가 기본 3초마다 파이프라인 파일이 바뀌었는지 확인하고, 바뀌었으면 새 파이프라인으로 갈아 끼운다. 새 설정의 문법이 틀렸거나 입출력을 초기화하지 못하면 이전 파이프라인이 그대로 돈다. 필터 하나를 고칠 때 Pod를 다시 만들 필요가 없다.

이 구성에서 고른 것과 그 비용은 이렇다.

| 고른 것 | 얻는 것 | 비용 |
|---|---|---|
| Deployment 없이 Pod 하나 | 매니페스트가 몇 줄로 끝난다 | 노드가 죽으면 쿠버네티스가 Pod를 다시 만들지 않는다 |
| 기본 메모리 큐 | 디스크 볼륨 없이 돈다 | 프로세스가 비정상 종료되면 큐에 있던 이벤트는 사라진다 |
| 자동 리로드 | 필터를 고치면 몇 초 안에 반영된다 | 문법은 맞지만 뜻이 틀린 설정도 그대로 반영된다 |

Kafka 입력은 기본으로 오프셋을 주기적으로 자동 커밋하고, Pod를 다시 띄우면 그룹에 커밋된 오프셋부터 이어 읽는다. 읽은 위치를 Kafka가 들고 있으니 릴레이 쪽은 상태를 따로 갖지 않아도 된다. 그래서 Logstash는 메시지를 풀어 한 줄로 바꾸는 일만 하는 작은 프로세스로 둘 수 있었다.

## 참고 자료

- [Logstash Kafka input plugin 7.9](https://www.elastic.co/guide/en/logstash/7.9/plugins-inputs-kafka.html)
- [Logstash json filter 7.9](https://www.elastic.co/guide/en/logstash/7.9/plugins-filters-json.html)
- [Logstash split filter 7.9](https://www.elastic.co/guide/en/logstash/7.9/plugins-filters-split.html)
- [Logstash date filter 7.9](https://www.elastic.co/guide/en/logstash/7.9/plugins-filters-date.html)
- [Logstash tcp output 7.9](https://www.elastic.co/guide/en/logstash/7.9/plugins-outputs-tcp.html)
- [Logstash line codec 7.9](https://www.elastic.co/guide/en/logstash/7.9/plugins-codecs-line.html)
- [Logstash StringInterpolation.java (7.9)](https://github.com/elastic/logstash/blob/7.9/logstash-core/src/main/java/org/logstash/StringInterpolation.java)
- [Reloading the Config File](https://www.elastic.co/guide/en/logstash/7.9/reloading-config.html)
- [Persistent Queues](https://www.elastic.co/guide/en/logstash/7.9/persistent-queues.html)
- [Kafka design: consumer position](https://kafka.apache.org/26/documentation.html#design_consumerposition)
- [Kubernetes Pod lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)

## English summary

At a previous company I ran Logstash 7.9 to relay behavior logs from Kafka to another in-house log system that accepts single lines over TCP. The pipeline parsed each message with the json filter and used split on the event array, which usually held one element because the collection API already sent one event per message. A ruby filter turned `@timestamp`, the time Logstash read the message, into epoch seconds, and the line codec wrote each event as one line with the parsed object serialized back to JSON. It ran as a single Kubernetes Pod with automatic config reload. Because Kafka kept the committed offsets, the relay could stay a small stateless process that only reshaped messages.
