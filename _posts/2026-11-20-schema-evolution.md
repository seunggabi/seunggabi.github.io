---
title: "Avro 스키마 진화는 읽는 쪽이 쓴 쪽의 스키마를 알 때만 동작한다"
date: 2026-11-20 04:51:00 +0900
categories: [Ingestion & CDC, Avro]
tags: [avro, schema-registry, kafka, schema-evolution, iceberg, concept]
description: "Avro 필드를 바꿨을 때 컨슈머가 왜 깨지는지, 호환성 모드와 배포 순서가 어디까지 막아 주는지를 예시로 정리한다."
---

이벤트에 필드 하나를 더했는데 컨슈머가 멈출 때가 있다. 이 글은 그 이유와 막는 방법을 정리한다. Avro 1.12 스펙과 Confluent Platform 8.4 기준이다.

## 세 줄 요약

- Avro 데이터에는 값만 있고 필드 이름이 없다. 읽는 쪽은 쓴 쪽의 스키마를 알아야 한다.
- 필드를 더하거나 뺄 때는 기본값(default)을 둔다.
- 기본값과 배포 순서로도 못 막는 변경이 있다. 타입 변경, 이름 변경, 의미 변경이다.

## 이름표가 없는 데이터

JSON은 값마다 이름이 붙어 있다.

```json
{"click_id": "a1", "account": 7}
```

Avro 바이너리는 값만 순서대로 이어 붙인다. 개념만 그리면 이렇다.

```text
a1 7
```

`7`이 계정 번호인지 가격인지는 스키마를 봐야 안다. 그래서 스키마가 두 개 나온다.

- **라이터 스키마**: 데이터를 쓸 때 쓴 스키마
- **리더 스키마**: 읽는 프로그램이 기대하는 스키마

배포하는 동안에는 두 스키마가 다를 수 있다. Avro는 두 스키마의 필드를 **이름으로** 짝지어 차이를 메운다.

| 상황 | 결과 |
|---|---|
| 필드 순서가 다름 | 이름으로 맞추니 상관없다 |
| 쓴 쪽에만 있는 필드 | 버린다 |
| 읽는 쪽에만 있고 기본값이 있는 필드 | 기본값을 채운다 |
| 읽는 쪽에만 있고 기본값이 없는 필드 | 에러 |
| 이름은 같은데 타입이 다름 | 승격할 수 있으면 읽고, 아니면 에러 |

승격은 타입을 넓혀 읽는 것이다. `int`는 `long`, `float`, `double`로, `long`은 `float`, `double`로, `float`는 `double`로 읽을 수 있다. `string`과 `bytes`는 서로 읽을 수 있다. 반대로 좁히는 것은 안 된다.

### 예시: 필드 추가

v2에서 `placement`를 추가했다. 비워 둘 수 있는 필드는 `null`을 첫 자리에 둔 union으로 만들고 기본값을 `null`로 준다.

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

- v2 컨슈머가 v1 데이터를 읽으면 `placement`가 없으니 `null`을 채운다.
- v1 컨슈머가 v2 데이터를 읽으면 `placement`를 버린다.

양쪽 다 읽힌다. 기본값은 **읽을 때만** 쓰인다. 기본값이 있어도 v2로 쓸 때는 그 필드를 채워야 한다.

## 스키마는 어디서 가져오나

메시지마다 스키마 전체를 넣으면 너무 크다. Confluent 직렬화기는 스키마를 레지스트리에 등록하고, 메시지 앞에 **스키마 번호만** 붙인다.

```text
byte 0      : 0 (형식 버전)
byte 1..4   : 스키마 ID (4바이트, big-endian)
byte 5..    : Avro 바이너리
```

컨슈머는 이 번호로 라이터 스키마를 받아 온다. 그다음 자기 리더 스키마와 위 표의 규칙으로 맞춘다.

## 호환성 모드

프로듀서와 컨슈머는 동시에 바뀌지 않는다. 한동안 한쪽은 새 스키마, 한쪽은 옛 스키마다. 어느 조합이 읽혀야 하는지 정하는 것이 호환성 모드다. 레지스트리는 새 스키마를 등록할 때 이 모드로 검사하고, 통과하지 못하면 거절한다. 직렬화기는 기본적으로 스키마를 자동 등록하므로, 호환되지 않는 스키마를 단 프로듀서는 첫 전송에서 실패한다.

| 모드 | 읽혀야 하는 조합 | 먼저 올릴 쪽 |
|---|---|---|
| `BACKWARD` (기본) | **새** 컨슈머가 **옛** 데이터를 읽는다 | 컨슈머 |
| `FORWARD` | **옛** 컨슈머가 **새** 데이터를 읽는다 | 프로듀서 |
| `FULL` | 둘 다 | 순서 무관 |
| `NONE` | 검사하지 않는다 | 직접 맞춘다 |

앞의 세 모드는 **직전 버전 하나**와만 비교한다. 이름 뒤에 `_TRANSITIVE`가 붙으면 등록된 **모든 버전**과 비교한다.

필드를 더하고 뺄 때 통과하는 모드는 이렇다.

| 변경 | `BACKWARD` | `FORWARD` | `FULL` |
|---|---|---|---|
| 기본값이 **있는** 필드 추가 | O | O | O |
| 기본값이 **있는** 필드 삭제 | O | O | O |
| 기본값이 **없는** 필드 추가 | X | O | X |
| 기본값이 **없는** 필드 삭제 | O | X | X |

이유는 하나다. **읽는 쪽 스키마에는 있는데 데이터에 없는 필드는, 기본값이 있어야 채울 수 있다.** 데이터에만 있는 필드는 버리면 된다.

## 기본값이 있어도 깨지는 경우

기본값은 **데이터에 그 필드가 아예 없을 때**만 쓰인다.

**타입 변경.** `score`를 `string`에서 `int`로 바꾸고 기본값 `0`을 줘도, 옛 데이터에는 `score`가 있다. 기본값을 쓰지 않고 문자를 숫자로 읽으려다 에러가 난다. `int`에서 `long`처럼 넓히는 변경은 `BACKWARD`는 통과하지만 `FORWARD`는 통과하지 못한다.

**이름 변경.** `user_id`를 `userId`로 바꾸면 Avro는 "`user_id` 삭제 + `userId` 추가"로 본다. 두 필드에 모두 기본값이 있으면 호환성 검사를 **통과한다.** 그런데 새 컨슈머는 `userId`에 기본값을 받고, 원래 값은 버려진다. 에러가 없어서 가장 늦게 발견된다. 리더 필드의 `aliases`에 옛 이름을 적는 방법이 있지만, 스펙상 지원 여부는 구현체의 선택이다.

**enum 값 추가.** `[IOS, ANDROID]`에 `WEB`을 더하면 옛 컨슈머는 `WEB`을 모른다. 읽으려면 **enum 타입 자체의** 기본값이 있어야 한다. 필드의 `default`와는 다르다. enum은 처음 만들 때 `UNKNOWN` 같은 타입 기본값을 넣어 둔다.

**null 허용을 필수로 변경.** `["null", "string"]`을 `string`으로 바꾸면, 새 컨슈머는 `null`이 든 옛 데이터를 못 읽는다.

**의미 변경.** `amount`의 단위를 원에서 센트로 바꿔도 타입이 같으면 아무도 못 잡는다. 값만 틀린다.

타입, 이름, 의미를 바꿔야 하면 **새 필드를 추가한다.** 모두 옮겨 간 뒤에 옛 필드를 지운다.

## 배포 순서만 맞추면 될까

아니다. 순서는 **그 방향으로 호환되는 변경**일 때만 효과가 있다. `string`을 `int`로 바꾼 변경은 어느 쪽을 먼저 올려도 한쪽이 깨진다.

한 방향만 되는 변경은 순서를 맞춰도 함정이 있다. 기본값 없는 필드를 추가하면 `FORWARD`만 되므로 프로듀서를 먼저 올린다. 옛 컨슈머는 새 필드를 버리니 괜찮다. 문제는 그다음에 컨슈머를 올릴 때다.

```text
토픽: [v1][v1][v1][v2][v2]
       └ 새 컨슈머가 여기를 읽으면 에러
```

배포해도 **옛 데이터는 보존 기간 동안 토픽에 남는다.** 컨슈머가 밀려 있거나, 오프셋을 되감거나, 새 컨슈머 그룹이 처음부터 읽으면 v1을 만난다.

순서 자체를 지키기 어려운 때도 있다.

- **컨슈머가 여러 팀**: 한 팀만 늦어도 순서가 깨진다.
- **프로듀서가 모바일 앱**: 사용자가 업데이트할 때까지 옛 버전이 계속 온다.
- **롤백**: 컨슈머를 되돌리면 이미 쌓인 새 데이터를 읽어야 한다.
- **롤링 배포**: 옛 인스턴스와 새 인스턴스가 함께 돈다.

| 상황 | 필요한 것 |
|---|---|
| 순서를 지킬 수 있고 옛 데이터를 다시 안 읽음 | 방향에 맞는 순서로 배포 |
| 순서를 장담 못 함, 롤백 대비 | `FULL` |
| 옛 데이터를 다시 읽음 | `BACKWARD` 계열의 `_TRANSITIVE` (`FULL`이면 `FULL_TRANSITIVE`) |
| 호환되지 않는 변경 | 새 필드나 새 토픽 |

필드에 기본값을 두고 `FULL_TRANSITIVE`를 쓰면 배포 순서 걱정이 크게 준다. 그래도 이름 변경과 의미 변경은 사람이 챙겨야 한다.

## 직전 버전만 비교하면 놓치는 경우

기본값이 `BACKWARD`인 이유는 되감기다. 컨슈머를 토픽 처음으로 되감아도 최신 스키마로 옛 메시지를 읽을 수 있어야 한다. 그런데 직전 버전 하나만 비교하면 이런 일이 생긴다.

| 버전 | 필드 | 직전 버전과 `BACKWARD` 검사 |
|---|---|---|
| v1 | `id`, `score: string` | |
| v2 | `id` | 통과 (필드 삭제) |
| v3 | `id`, `score: int` 기본값 `0` | 통과 (기본값 있는 필드 추가) |

v3 컨슈머가 v1 데이터를 읽으면 `score`가 양쪽에 있고, `string`은 `int`로 읽을 수 없어 에러다. v1 메시지가 토픽에 남아 있다면 `BACKWARD_TRANSITIVE`여야 v3 등록이 막힌다.

모드는 `PUT /config/<subject>`로 바꾸고, 등록 전에 호환 여부를 먼저 물어볼 수 있다. `versions/latest`로 물으면 `_TRANSITIVE` 모드에서도 최신 버전 하나와만 비교한다. 모든 버전과 비교하려면 버전을 빼고 `POST /compatibility/subjects/<subject>/versions`로 묻는다.

```bash
# 등록 전에 최신 버전 하나와 호환되는지 확인, 실패 이유까지
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "<escape한 스키마 JSON>"}' \
  "http://localhost:8081/compatibility/subjects/clicks-value/versions/latest?verbose=true"
```

## 호환성은 subject마다 걸린다

호환성을 검사하는 단위는 토픽이 아니라 subject다. subject는 레지스트리에서 스키마 버전이 쌓이는 이름이다. 기본 `TopicNameStrategy`에서는 `<토픽>-value`가 그 토픽의 subject다. 노출, 클릭처럼 성격이 다른 이벤트를 한 토픽에 섞으려면 `RecordNameStrategy`나 `TopicRecordNameStrategy`로 바꾸거나, union 스키마와 참조로 묶어야 한다.

## 다른 방식: 버전마다 토픽을 나눈 JSON

JSON은 메시지에 필드 이름이 있어 스키마 없이도 파싱된다. 대신 어떤 필드와 타입이 있어야 하는지는 따로 검사해야 한다.

[로그 수집 API 글](/posts/log-collection-api/)의 API는 엔드포인트를 나라와 로그 스키마 버전으로 나누고, 각각 정해진 토픽으로 보냈다. 스키마를 바꾸면 새 경로와 새 토픽을 만들고, 옛 버전은 클라이언트가 다 옮겨 갈 때까지 뒀다. 로그 헤더의 버전이 토픽의 버전과 다르면 Kafka로 보내지 않고 에러 로그만 남겼다.

| | Avro + 레지스트리 | 버전별 JSON 토픽 |
|---|---|---|
| 스키마 차이를 막는 때 | 스키마를 등록할 때 | 메시지를 받을 때 |
| 한 토픽 안의 버전 | 호환되는 여러 버전 | 하나 |
| 읽는 쪽이 하는 일 | 리더 스키마로 맞춰 읽기 | 토픽이 곧 버전 |
| 깨지는 변경 | 새 subject나 새 토픽 | 새 엔드포인트와 새 토픽 |
| 잘못 보낸 클라이언트 | 전송 실패 | 그 메시지만 버리고 에러 로그 |

버전별 토픽은 읽는 쪽이 호환 규칙을 몰라도 된다. 사용자가 업데이트하기 전까지 옛 버전을 보내는 앱과도 잘 맞는다. 대신 옮겨 가는 동안 같은 로그가 여러 토픽으로 나뉘어 들어온다. 읽는 쪽은 토픽을 여럿 구독하고 버전 차이를 직접 맞춰야 한다.

## 테이블로 가면 ID로 맞춘다

Avro는 필드를 이름으로 맞춘다. [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에서 본 것처럼 Iceberg는 모든 필드에 ID를 두고 데이터 파일에서도 ID로 컬럼을 찾는다.

둘 사이의 싱크는 이름을 ID로 바꾸는 지점이다. [Iceberg Kafka Connect NPE 글](/posts/iceberg-kafka-connect-mixed-case/)의 NPE도 설정에 적힌 식별자 컬럼 이름을 ID로 바꾸는 그 경계에서 났다.

## 참고 자료

- [Apache Avro 1.12 Specification (Schema Resolution, Aliases)](https://avro.apache.org/docs/++version++/specification/)
- [Confluent Schema Evolution and Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Confluent Formats, Serializers, and Deserializers (subject name strategy, wire format)](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
- [Confluent Schema Registry API Reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [Apache Iceberg Table Spec (Schema Evolution)](https://iceberg.apache.org/spec/#schema-evolution)
- [Apache Iceberg Evolution](https://iceberg.apache.org/docs/latest/evolution/)

## English summary

Avro data carries no field names, so a reader needs the writer's schema. The spec matches fields by name, fills missing fields from reader defaults, and ignores extra ones. The Confluent serializer puts a 4-byte schema ID in front of each message so consumers can fetch the writer's schema from the registry. BACKWARD (the default) lets new readers read old data, FORWARD is the reverse, FULL is both, and only the transitive modes check every past version. Defaults make adding and removing fields safe, but they do not cover incompatible type changes, renames, new enum symbols without an enum-level default, or changes in meaning. Deploy order only helps for changes that are compatible in that direction, because old messages stay in the topic. In my log collection API, I used JSON logs with one endpoint and one topic per schema version and dropped messages whose version did not match.
