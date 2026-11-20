---
published: false
title: "gRPC와 REST를 한 서비스로 열 때 오류와 재시도 규칙 정하기"
date: 2026-11-20 05:11:00 +0900
categories: [Platform & Infra, gRPC]
tags: [grpc, rest, protobuf, http2, retry, concept]
---

같은 광고 결정 로직을 gRPC로도 부르고 REST로도 부르면, 로직은 하나여도 호출하는 쪽이 받는 실패는 둘이다. REST 클라이언트는 4xx와 5xx를 보고, gRPC 클라이언트는 HTTP 200 응답의 트레일러에 담긴 `INVALID_ARGUMENT`나 `UNAVAILABLE`을 보며, 프록시가 `grpc-status` 없이 만든 HTTP 400은 `INTERNAL`로 받는다. 한 서비스가 두 입구를 같은 애플리케이션 서비스 위에 열 때 생기는 일이다.

버전은 grpc-java 1.85, proto3 문법, RFC 9110 기준이다.

## 바이트에는 필드 이름이 아니라 번호가 남는다

protobuf 바이너리는 필드를 번호로 찾는다. 번호는 1부터 536,870,911까지 쓸 수 있고, 19000에서 19999는 구현용으로 막혀 있다. 1에서 15는 태그가 1바이트라 자주 오는 필드에 주는 편이 작다.

```protobuf
// 구조만 보여 주는 스케치
syntax = "proto3";

package example.ad;

message DecisionRequest {
  reserved 3;
  reserved "page_name";

  string ad_id = 1;
  string os = 2;
  optional string query = 4;
}

message DecisionResponse {}

service AdService {
  rpc Decide(DecisionRequest) returns (DecisionResponse);
}
```

메시지를 쓰기 시작한 뒤에는 번호가 계약이다. 새 번호로 필드를 더하는 것은 안전하고, 옛 리더는 모르는 필드로 보관했다가 다시 쓸 때 그대로 내보낸다. 필드를 지울 때는 번호를 `reserved`로 막는다. 지운 번호를 다시 쓰면 옛 데이터가 새 필드로 잘못 읽히기 때문이다. 이름도 같이 막아야 JSON과 TextFormat이 계속 파싱된다.

proto3의 암묵적 필드는 0이나 빈 문자열을 보낸 것과 안 보낸 것을 구분하지 못한다. 구분이 필요하면 위의 `query`처럼 `optional`을 단다.

[스키마 진화 글](/posts/schema-evolution/)의 Avro는 필드를 이름으로 맞추고 읽는 쪽이 쓴 쪽 스키마를 알아야 한다. protobuf는 번호로 맞추고 모르는 번호는 알 수 없는 필드로 남겨 두므로, 리더가 라이터 스키마를 몰라도 읽는다. 대신 REST 입구가 트랜스코딩처럼 protobuf 메시지를 JSON으로 그대로 주고받는다면 이름이 다시 계약이 되고, 필드 이름 변경이 REST 쪽에서만 깨진다. REST 입구가 헤더와 쿼리 파라미터를 컨트롤러에서 직접 받는다면 적어도 요청 쪽은 proto 필드 이름에 묶이지 않는다.

## HTTP/2 연결 하나에 RPC 여러 개

gRPC 요청은 언제나 `POST /<서비스 전체 이름>/<메서드>`다. 본문의 각 메시지 앞에는 압축 여부 1바이트와 길이 4바이트가 붙고, 결과는 트레일러의 `grpc-status`와 `grpc-message`로 온다. HTTP 상태는 오류여도 200이다.

RPC 하나는 HTTP/2 스트림 하나다. 한 연결 위에 스트림 여러 개가 동시에 열리고 프레임이 섞여 오가므로, 클라이언트는 연결을 오래 붙잡고 RPC를 계속 실어 보낸다. 연결 단위로 나누는 L4 로드 밸런서는 연결을 통째로 백엔드 하나에 넘기므로, 그 연결 위의 RPC는 모두 같은 백엔드로 간다. 스트림 단위로 나누려면 L7 밸런서나 클라이언트 쪽 밸런싱이 필요하다. 메서드가 POST로 고정이라 URL을 키로 쓰는 HTTP 캐시도 끼어들 자리가 없다.

## 상태 코드는 한쪽 방향으로만 바꾼다

gRPC 상태 코드는 0부터 16까지 17개다. 서버가 REST로 같은 실패를 알릴 때는 `google.rpc.Code`에 적힌 HTTP 대응을 기준으로 삼을 수 있다.

| gRPC 코드 | 뜻 | HTTP 대응 | 그대로 다시 보내도 되나 |
|---|---|---|---|
| `INVALID_ARGUMENT` | 시스템 상태와 무관하게 인자가 잘못됨 | 400 | 아니다 |
| `FAILED_PRECONDITION` | 시스템이 그 작업을 할 상태가 아님 | 400 | 상태를 고치기 전에는 아니다 |
| `ABORTED` | 동시성 충돌 등으로 중단 | 409 | 더 위 단계에서 다시 시도 |
| `DEADLINE_EXCEEDED` | 기한 안에 끝나지 않음 | 504 | 작업이 실제로 끝났을 수 있다 |
| `UNAVAILABLE` | 일시적으로 쓸 수 없음 | 503 | 보통 일시적인 오류라 멱등한 메서드만 백오프로 다시 보낸다 |
| `INTERNAL` | 내부 불변식이 깨진 심각한 오류 | 500 | 처리됐는지 알 수 없다 |

반대 방향 표도 있지만 용도가 다르다. `grpc-status`가 없는 응답, 보통 프록시나 게이트웨이가 만든 응답을 받은 gRPC 클라이언트가 쓰는 표다. 여기서 400은 `INTERNAL`, 404는 `UNIMPLEMENTED`, 429와 502, 503, 504는 `UNAVAILABLE`이 된다. 일대일도 대칭도 아니라서 서버가 이 표로 HTTP 상태를 고르면 안 된다.

`INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION` 같은 코드는 gRPC 라이브러리가 스스로 만들지 않는다. 클라이언트가 이 코드를 받았다면 애플리케이션이 판단해서 돌려준 것이다. 예를 들어 gRPC 입구는 잘못된 인자와 외부 API의 4xx를 `INVALID_ARGUMENT`로, 나머지를 `INTERNAL`로 돌려주고, REST 입구는 같은 잘못된 인자를 400으로 돌려줄 수 있다. 호출하는 쪽이 고칠 문제와 서버 쪽 문제를 입구마다 그 프로토콜의 말로 나누는 셈이다.

## 데드라인은 남은 시간으로 넘어간다

gRPC는 기본 데드라인이 없다. 클라이언트가 정하지 않으면 끝없이 기다릴 수 있다. 데드라인은 넘지 말아야 할 시각이고, 타임아웃은 걸려도 되는 시간이다. gRPC는 시계 차이를 피하려고 `grpc-timeout` 헤더에 남은 시간을 실어 보낸다.

서버가 다른 서비스를 부를 때 원래 클라이언트의 데드라인을 이어 줄 수 있다. Java와 Go는 기본으로 이어 주고 C++은 켜야 한다. 데드라인이 지나면 서버 라이브러리가 그 호출을 `CANCELLED`로 취소하므로, 오래 걸리는 작업은 중간중간 취소 여부를 보고 멈춰야 한다.

외부 서버를 부르는 클라이언트에 연결, 응답, 읽기, 쓰기 타임아웃을 따로 두는 방식은 홉 하나의 단계마다 상한을 두는 것이다. gRPC 데드라인은 호출 전체에 걸리고 다음 홉으로도 이어진다. 앞의 것은 어느 단계가 느린지 따로 막을 수 있고, 뒤의 것은 여러 홉을 거쳐도 처음 호출자의 기한을 넘기지 않는다.

## 다시 보내도 되는 실패

gRPC 재시도는 서비스 설정의 `retryPolicy`로 메서드마다 건다.

```json
{
  "methodConfig": [{
    "name": [{"service": "example.ad.AdService"}],
    "retryPolicy": {
      "maxAttempts": 4,
      "initialBackoff": "0.1s",
      "maxBackoff": "1s",
      "backoffMultiplier": 2,
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}
```

`maxAttempts`는 5를 넘으면 5로 취급하고, 백오프에는 ±20% 지터가 붙는다. 데드라인은 시도마다 새로 주어지지 않고 모든 시도를 합친 시간에 걸린다. 클라이언트가 응답 헤더를 받으면 그 RPC는 확정되고 더는 재시도하지 않는다. `UNAVAILABLE`은 보통 일시적인 오류이고, 멱등한 메서드만 백오프로 재시도한다. `DEADLINE_EXCEEDED`는 작업이 끝난 뒤에도 올 수 있어서 멱등이 아닌 메서드에 넣으면 같은 작업이 두 번 일어날 수 있다.

HTTP는 메서드가 멱등성을 말해 준다. RFC 9110에서 GET, HEAD, OPTIONS, TRACE 같은 안전한 메서드와 PUT, DELETE는 멱등이고 POST는 아니다. 클라이언트는 멱등이 아닌 요청을 자동으로 다시 보내지 않아야 하지만, 그 리소스에서 요청이 안전하다는 것을 설계로 알고 있으면 POST도 다시 보낼 수 있다.

외부 호출에서 흔한 방식은 `Retry.backoff`에 필터를 달아 5xx만 재시도하고 4xx는 바로 잘못된 요청 예외로 바꾸는 것이다. gRPC 설정으로 옮기면 `retryableStatusCodes`를 `UNAVAILABLE`로 두고 `INVALID_ARGUMENT`는 빼는 것과 같은 모양이다.

## 한 서비스에서 둘 다 여는 방법

gRPC 스텁 구현과 REST 컨트롤러가 같은 애플리케이션 서비스를 부르는 어댑터 방식이 한 가지 길이다. REST 쪽은 사용자와 기기 정보를 헤더로, 조회 조건을 쿼리 파라미터로 받을 수 있다.

다른 방법은 grpc-gateway나 Envoy gRPC-JSON transcoder 같은 트랜스코딩 프록시를 앞에 두는 것이다. REST 모양은 proto의 `google.api.http` 규칙을 따르고 프록시가 gRPC 코드를 HTTP 상태로 바꿔 주므로, 맞춰야 할 것은 proto 하나와 그 HTTP 규칙으로 준다.

어댑터 방식은 REST 쪽을 앱이 쓰기 편한 모양으로 열 수 있고, 대가는 같은 요청을 두 번 변환하는 코드와 입구별 오류 표를 같이 고쳐야 한다는 점이다.

## 참고 자료

- [Protocol Buffers Language Guide (proto3)](https://protobuf.dev/programming-guides/proto3/), [Proto Best Practices](https://protobuf.dev/best-practices/dos-donts/)
- [gRPC Core concepts](https://grpc.io/docs/what-is-grpc/core-concepts/), [gRPC over HTTP2](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md), [gRPC Load Balancing](https://grpc.io/blog/grpc-load-balancing/)
- [gRPC Status Codes](https://grpc.io/docs/guides/status-codes/), [HTTP to gRPC Status Code Mapping](https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md), [google.rpc.Code](https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto)
- [gRPC Deadlines](https://grpc.io/docs/guides/deadlines/), [gRPC Retry](https://grpc.io/docs/guides/retry/), [gRFC A6 Client Retries](https://github.com/grpc/proposal/blob/master/A6-client-retries.md)
- [RFC 9110 Section 9.2 Common Method Properties](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2)
- [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway), [Envoy gRPC-JSON transcoder](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/grpc_json_transcoder_filter)

## English summary

gRPC sends every call as an HTTP/2 POST with protobuf fields identified by number, puts the result in trailers, and gives each RPC its own stream on a shared connection. When one application service is served over both gRPC and REST, each entry point maps errors in its own terms: for example, on gRPC invalid arguments and external 4xx can become INVALID_ARGUMENT and other failures INTERNAL, while on REST invalid arguments become HTTP 400. The gRPC-to-HTTP and HTTP-to-gRPC tables are not symmetric, and UNAVAILABLE is usually transient, so only idempotent methods should retry it with backoff. A common setup retries the external call only on 5xx and sets separate connect, response, read, and write timeouts, while a gRPC deadline would cover the whole call across hops and retries. Two adapters let the REST side keep headers and query parameters that fit the app, at the cost of keeping two request and error mappings in sync.
