---
title: "Avro 스키마 진화는 읽는 쪽이 쓴 쪽의 스키마를 알 때만 동작한다"
date: 2026-11-20 04:51:00 +0900
categories: [Ingestion & CDC, Avro]
tags: [avro, schema-registry, kafka, schema-evolution, iceberg, concept]
---

프로듀서가 이벤트에 필드 하나를 더했는데, 배포 순서를 잘못 맞추면 컨슈머가 역직렬화에서 멈춘다. Avro 바이너리에는 필드 이름도 타입도 들어 있지 않아서, 읽는 쪽은 쓴 쪽의 스키마를 모르면 바이트를 해석할 수 없다. 이 차이를 다루는 방식은 크게 둘이다. Avro와 스키마 레지스트리로 읽는 쪽에 라이터 스키마를 찾아 주는 방식이 있고, [로그 수집 API 글](/posts/log-collection-api/)의 토픽처럼 스키마 버전마다 토픽을 따로 둔 JSON 방식이 있다. 이 글은 Avro의 스키마 해석 규칙, 레지스트리의 호환성 모드와 subject를 정리하고 두 방식을 비교한다. 버전은 Avro 1.12 스펙과 Confluent Platform 8.4 기준이다.

## 라이터 스키마와 리더 스키마

Avro는 데이터를 쓸 때 쓴 스키마(writer's schema)와 애플리케이션이 기대하는 스키마(reader's schema)를 나눈다. 둘이 다르면 스펙의 스키마 해석(schema resolution) 규칙이 차이를 메운다. 레코드끼리는 이렇게 맞춘다.

| 상황 | 결과 |
|---|---|
| 필드 순서가 다름 | 이름으로 맞추니 상관없다 |
| 라이터에만 있는 필드 | 그 값은 무시한다 |
| 리더에만 있고 default가 있는 필드 | default 값을 쓴다 |
| 리더에만 있고 default가 없는 필드 | 에러 |
| 같은 이름, 다른 타입 | 승격 가능하면 맞추고, 아니면 에러 |

승격은 `int`에서 `long`, `float`, `double`로, `long`에서 `float`, `double`로, `float`에서 `double`로, 그리고 `string`과 `bytes` 사이에서만 된다. enum은 라이터의 심볼이 리더 enum에 없으면 리더의 default 심볼을 쓰고, default가 없으면 에러다.

default는 읽을 때만 쓰인다. default가 있어도 쓸 때 그 필드를 빼면 안 된다. Avro에는 `optional` 키워드가 없어서, 비워 둘 수 있는 필드는 `null`을 첫 자리에 둔 union으로 만들고 `"default": null`을 준다.

```json
{
  "type": "record",
  "name": "Click",
  "namespace": "example.ad",
  "fields": [
    {"name": "click_id", "type": "string"},
    {"name": "ad_account_id", "type": "long"},
    {"name": "placement", "type": ["null", "string"], "default": null}
  ]
}
```

`placement`가 v2에서 추가됐다고 하자. v2 리더가 v1 데이터를 읽으면 `placement`가 없으니 `null`이 들어간다. v1 리더가 v2 데이터를 읽으면 `placement`는 무시된다. 양쪽 다 된다.

필드 이름 변경은 Avro에서 "옛 필드 삭제 + 새 필드 추가"다. 이름으로 맞추기 때문이다. 리더 쪽 필드에 `aliases`로 옛 이름을 적으면 구현체가 옛 이름으로 쓴 데이터를 새 이름으로 읽어 줄 수 있지만, 스펙상 alias 사용은 구현체의 선택이다.

## 레지스트리는 라이터 스키마를 메시지마다 실어 보낸다

Kafka 메시지에 스키마 전체를 넣으면 메시지보다 스키마가 더 크다. Confluent 직렬화기는 스키마를 레지스트리에 등록하고, 메시지 앞에 그 ID만 붙인다.

```text
byte 0      : 0 (형식 버전)
byte 1..4   : 스키마 ID (4바이트, big-endian)
byte 5..    : Avro 바이너리
```

역직렬화기는 ID로 라이터 스키마를 받아 온다. 컨슈머가 생성된 클래스처럼 자기 스키마로 읽으면, 그 둘 사이에서 위의 해석 규칙이 돈다. "읽는 쪽이 라이터 스키마를 알아야 한다"는 Avro의 전제를 레지스트리가 채우는 셈이다.

프로듀서에 Confluent의 Kafka Avro serializer와 스키마 레지스트리를 쓰면 이 형식이 된다. 토픽을 읽는 쪽은 메시지마다 붙은 ID로 그 메시지를 쓴 스키마를 찾는다.

## 호환성 모드는 "어느 쪽을 먼저 배포하느냐"다

레지스트리는 새 스키마를 등록할 때 subject에 걸린 호환성 모드로 검사하고, 통과하지 못하면 등록을 거절한다. 직렬화기는 기본적으로 스키마를 자동 등록하므로, 호환되지 않는 스키마를 단 프로듀서는 배포 후 첫 전송에서 실패한다.

| 모드 | 보장 | 검사 대상 | 먼저 올릴 쪽 |
|---|---|---|---|
| `BACKWARD` (기본) | 새 스키마로 직전 버전 데이터를 읽는다 | 직전 버전 하나 | 컨슈머 |
| `FORWARD` | 직전 스키마로 새 데이터를 읽는다 | 직전 버전 하나 | 프로듀서 |
| `FULL` | 둘 다 | 직전 버전 하나 | 순서 무관 |
| `*_TRANSITIVE` | 위와 같은 조건 | 등록된 모든 버전 | 위와 같음 |
| `NONE` | 검사 안 함 | 없음 | 직접 맞춘다 |

해석 규칙에 대 보면 왜 그런지 나온다. default 없는 필드를 추가하면 새 리더가 옛 데이터에서 그 필드를 못 찾으니 `BACKWARD` 위반이다. default 없는 필드를 지우면 옛 리더가 새 데이터에서 그 필드를 못 찾으니 `FORWARD` 위반이다. `null` union에 default를 준 필드만 더하고 빼면 `FULL`까지 통과한다.

기본값이 `BACKWARD`인 이유는 되감기다. 컨슈머를 토픽 처음으로 되감아도 최신 스키마로 옛 메시지를 읽을 수 있어야 한다. 다만 비교 대상이 직전 버전 하나라서 놓치는 경우가 있다.

| 버전 | 필드 | 직전 버전과 `BACKWARD` |
|---|---|---|
| v1 | `id`, `score: string` | |
| v2 | `id` | 통과 (필드 삭제) |
| v3 | `id`, `score: int` default `0` | 통과 (default 있는 필드 추가) |

v3 리더로 v1 데이터를 읽으면 `score`가 양쪽에 있고 `string`은 `int`로 승격되지 않으니 에러다. 토픽에 v1 메시지가 남아 있다면 `BACKWARD_TRANSITIVE`여야 v3 등록이 막힌다. 모드는 `PUT /config/<subject>`로 subject마다 바꾸고, 등록 전에 호환 여부를 먼저 물어볼 수 있다. `versions/latest`로 물으면 `*_TRANSITIVE` 모드에서도 최신 버전 하나와만 비교한다. 버전을 빼고 `POST /compatibility/subjects/<subject>/versions`로 물어야 `*_TRANSITIVE` 모드에서 등록된 모든 버전과 비교한다.

```bash
# 등록 전에 최신 버전 하나와 호환되는지 확인, 실패 이유까지
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "<escape한 스키마 JSON>"}' \
  "http://localhost:8081/compatibility/subjects/clicks-value/versions/latest?verbose=true"
```

## 호환성의 단위는 subject다

호환성은 토픽이 아니라 subject마다 걸리고, 기본 `TopicNameStrategy`에서는 `<토픽>-value` subject 하나가 그 토픽의 스키마 계보 하나다. 그래서 노출, 클릭처럼 성격이 다른 이벤트를 한 토픽에 Avro로 섞으려면 `RecordNameStrategy`나 `TopicRecordNameStrategy`로 subject 전략을 바꾸거나, union 스키마와 참조로 묶어야 한다.

## 버전 필드를 단 JSON 로그

JSON 로그는 메시지 안에 필드 이름이 있어서 라이터 스키마 없이도 파싱은 된다. 대신 어떤 필드가 있어야 하는지, 타입이 무엇인지는 누구도 검사하지 않는다. 그 검사를 어디에 두느냐가 설계다.

로그 수집 API 글의 API는 엔드포인트를 나라와 로그 스키마 버전으로 나눴고, 각 엔드포인트는 정해진 토픽으로 보냈다. 스키마를 바꿀 일이 생기면 새 버전 경로와 새 토픽을 만들고, 옛 버전은 클라이언트가 다 옮겨 갈 때까지 그대로 뒀다. 메시지마다 로그 헤더의 버전이 토픽의 버전과 같은지 보고, 다르면 Kafka로 보내지 않고 에러 로그만 남겼다.

| | Avro + 레지스트리 | 수집 API의 버전별 JSON 토픽 |
|---|---|---|
| 스키마 차이를 막는 곳 | 스키마 등록 시점, 호환성 모드 | 메시지 수신 시점, 버전 비교 |
| 한 토픽 안의 버전 | 호환되는 여러 버전 | 하나 |
| 읽는 쪽이 하는 일 | 리더 스키마와 해석 규칙 | 토픽이 곧 버전, 해석 없음 |
| 깨지는 변경 | 새 subject나 새 토픽 | 새 엔드포인트와 새 토픽 |
| 잘못 보낸 클라이언트 | 프로듀서 배포 후 전송 실패 | 해당 메시지만 버리고 에러 로그 |

버전별 토픽은 토픽 하나가 버전 하나라서, 읽는 쪽이 호환 규칙을 몰라도 된다. 앱 클라이언트는 사용자가 업데이트하기 전까지 옛 버전으로 계속 보내므로, 옛 경로를 살려 두는 방식과 잘 맞는다. 대가는 클라이언트가 옮겨 가는 동안 같은 로그가 버전 수만큼의 토픽으로 나뉘어 들어온다는 점이다. 읽는 쪽은 그동안 토픽을 여럿 구독하고 버전별 차이를 직접 맞춰야 한다.

## 테이블로 가면 이름 대신 ID로 맞춘다

Avro는 필드를 이름으로 맞추지만, [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에서 본 것처럼 Iceberg는 모든 필드에 ID를 두고 데이터 파일에서도 ID로 컬럼을 찾는다.

둘 사이의 싱크는 이름을 ID로 바꾸는 지점이다. [Iceberg Kafka Connect NPE 글](/posts/iceberg-kafka-connect-mixed-case/)의 NPE도 설정에 적힌 식별자 컬럼 이름을 ID로 바꾸는 그 경계에서 났다.

## 참고 자료

- [Apache Avro 1.12 Specification (Schema Resolution, Aliases)](https://avro.apache.org/docs/++version++/specification/)
- [Confluent Schema Evolution and Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Confluent Formats, Serializers, and Deserializers (subject name strategy, wire format)](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
- [Confluent Schema Registry API Reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [Apache Iceberg Table Spec (Schema Evolution)](https://iceberg.apache.org/spec/#schema-evolution)
- [Apache Iceberg Evolution](https://iceberg.apache.org/docs/latest/evolution/)

## English summary

Avro data carries no field names, so a reader needs the writer's schema, and the spec's resolution rules match fields by name, use reader defaults for missing fields, and ignore extra ones. The Confluent serializer puts a 4-byte schema ID in front of each message so consumers can fetch the writer's schema from the registry. The registry checks each new schema per subject: BACKWARD lets new readers read old data and is the default, FORWARD is the reverse, FULL is both, and only the transitive modes check every past version. In my log collection API, I used JSON logs with one endpoint and one topic per schema version and dropped messages whose version did not match. That keeps each topic to a single version with no resolution on the reader side, at the cost of more topics while clients migrate.
