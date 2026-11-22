---
title: "배치가 채우는 읽기 API 캐시는 언제 버려야 할까"
date: 2026-11-20 05:14:00 +0900
categories: [BI & Serving, Caching]
tags: [caching, http, spring, django, redis, concept]
description: "새벽 배치가 지표 테이블을 다시 썼는데 대시보드는 어제 숫자를 보여 주는 문제를 계기로, 읽기 API 캐시를 둘 자리와 언제 버릴지를 정리한다."
---

새벽 배치가 지표 테이블을 다시 썼는데 대시보드는 어제 숫자를 보여 준다. 캐시를 둔 읽기 API에서 자주 나오는 장면이다. 캐시를 어디에 둘지는 고르기 쉽지만, 배치가 원천을 통째로 바꾸는 순간 그 캐시를 언제 버릴지는 따로 설계해야 한다.

[지표 대시보드 API 글](/posts/metrics-dashboard-api/)은 서버 캐시 없이 브라우저에서 GET 응답을 캐시했고, [연말 결산 Spark 잡 글](/posts/year-end-recap-spark/)과 [HBase 행 키 글](/posts/hbase-serving-row-keys/)은 결과를 미리 계산해 두고 모든 사용자가 같이 읽는 공용 행을 Spring `@Cacheable`에 담았다.

버전은 RFC 9110·9111, Spring Framework 7.0, Spring Boot 4.1, Django 5.2, Redis 8.0 기준이다.

## 캐시를 둘 수 있는 네 자리

| 자리 | 예 | 같이 쓰는 범위 | 버리는 방법 |
|---|---|---|---|
| 클라이언트 메모리 | axios 어댑터의 LRU | 탭 하나 | 새로고침, 항목 수·유효 시간 |
| HTTP 캐시 | 브라우저, CDN, 리버스 프록시 | private은 사용자 한 명, shared는 여러 사용자 | `max-age` 만료, ETag 재검증 |
| 애플리케이션 프로세스 | `@Cacheable` + Caffeine, Django 로컬 메모리 캐시 | 프로세스 하나 | provider의 만료, `@CacheEvict`, 재시작 |
| 공유 캐시 | Redis | 모든 서버 노드 | `EXPIRE`, 키 삭제, 키 버전 |

지표 대시보드는 첫 줄을 골랐다. axios 인스턴스에 `axios-extensions`의 `cacheAdapterEnhancer`를 붙여 GET 응답을 URL과 파라미터 기준 메모리 LRU에 담았다. 한 화면의 여러 차트가 같은 조건으로 부르면 서버까지 가지 않고, 새로고침하면 캐시가 사라진다. 배치가 새 날짜를 넣은 뒤 서버 쪽에서 비울 것이 없다. 대신 사용자마다, 탭마다 처음 한 번은 서버를 거쳐 DB를 읽게 된다(캐시 위치로 본 추론).

## HTTP 캐시는 신선도와 재검증으로 움직인다

저장한 응답이 아직 신선한지, 아니라면 서버에 바뀌었는지 물어볼 수 있는지가 HTTP 캐시의 두 질문이다.

| 지시어 | 동작 |
|---|---|
| `max-age=N` | 응답 나이가 N초를 넘으면 stale |
| `s-maxage=N` | shared 캐시에서는 `max-age`보다 우선 |
| `no-cache` | 저장은 하되 쓰기 전에 매번 서버에 재검증. 캐시 금지가 아니다 |
| `no-store` | 요청과 응답 어느 부분도 저장하지 않음. 아예 남기지 않을 때 쓴다 |
| `private` | shared 캐시는 저장하지 않음 |

하루 단위로 바뀌는 데이터라면 지시어를 명시해야 동작을 예측할 수 있다.

재검증은 ETag로 한다. 서버가 응답에 `ETag`를 붙이면 클라이언트는 다음 요청에 `If-None-Match`로 그 값을 보내고, 같으면 서버는 본문 없이 `304 Not Modified`를 돌려준다. 왕복은 그대로지만 본문 전송을 아끼고, ETag를 싸게 계산할 수 있으면 쿼리도 아낀다. 배치가 채우는 테이블에는 ETag로 쓰기 좋은 값이 이미 있다. 마지막으로 끝난 배치의 실행 ID다. Django `condition` 데코레이터는 ETag 함수를 먼저 돌리고, 값이 `If-None-Match`와 맞으면 뷰 본문을 부르지 않고 304를 돌려준다.

```python
# 구조만 보여 주는 스케치 (BatchRun, play_metric_list는 예로 지은 이름)
from django.views.decorators.cache import cache_control
from django.views.decorators.http import condition

def batch_etag(request, *args, **kwargs):
    run = BatchRun.objects.filter(table="play_metric").latest("finished_at")
    return str(run.run_id)

@cache_control(private=True, max_age=300)
@condition(etag_func=batch_etag)
def play_metric_list(request):
    ...
```

인증이 걸린 응답은 shared 캐시를 조심해야 한다. RFC 9111은 `Authorization` 헤더가 있는 요청의 응답을 shared 캐시가 다른 요청에 쓰지 못하게 하고, `public`, `s-maxage`, `must-revalidate`가 있을 때만 예외를 둔다. 사용자마다 다른 응답에 `public`을 붙이면 그 예외가 남의 데이터를 내주는 길이 된다.

## 애플리케이션 캐시는 메서드 인자가 키다

Spring 캐시 추상화는 메서드 인자를 키로 결과를 담는다. 인자가 하나면 그 값, 여럿이면 모두 담은 `SimpleKey`, 없으면 `SimpleKey.EMPTY`가 키다. 추상화는 저장소가 아니라서 만료 시간과 크기 제한은 Caffeine이나 Redis 같은 provider에서 정한다. Spring Boot에서 Redis를 쓰면 `spring.cache.redis.time-to-live`로 기본 TTL을 준다.

```java
// 구조만 보여 주는 스케치. sync와 @CacheEvict는 이 글에서 덧붙였다
@Cacheable(cacheNames = "recapDefault", key = "#year", sync = true)
public Map<String, String> defaultRow(String year) {
    return hbase.readColumnsWithPrefix(DEFAULT_ROW_KEY, year + "_");
}

@CacheEvict(cacheNames = "recapDefault", allEntries = true)
public void evictDefaultRow() { }
```

- 기본 proxy 모드는 프록시를 거친 외부 호출만 가로챈다. 같은 클래스 안에서 `defaultRow()`를 부르면 캐시를 타지 않는다.
- `sync = true`는 같은 키를 계산하는 동안 다른 스레드를 기다리게 한다. 추상화는 멀티 프로세스를 따로 다루지 않으므로, 로컬 provider로 노드가 여럿이면 노드마다 처음 한 번씩 원천을 읽는다.
- 로컬 provider에서 `@CacheEvict`는 그 호출을 받은 노드만 비운다. 모든 노드를 비우려면 공유 캐시나 provider의 전파 기능이 필요하다.

결산 API는 연도를 키로 공용 행을 캐시하므로, 같은 연도의 공용 행을 다시 쓰면 provider의 만료나 재시작 전까지 캐시에 있던 값이 남는다(캐시 동작으로 본 추론). 읽기 부하를 리전 하나에서 떼어 내는 대신 다시 쓸 때 비우는 단계가 하나 생기는 구조다.

## 캐시 어사이드와 미리 계산하기

캐시 어사이드는 읽을 때 캐시를 보고, 없으면 원천을 읽어 채운다. `@Cacheable`과 Django `cache.get_or_set()`이 이 방식이다. 미리 계산하기는 원천 비용을 배치로 옮긴다. 결산 잡은 1년 치 재생 로그를 사용자별로 집계해 HBase 사용자 행에 넣었고, API는 행 키로 읽고 쉼표 문자열을 쪼개기만 한다.

| | 캐시 어사이드 | 미리 계산 |
|---|---|---|
| 첫 요청 | 원천 비용을 그대로 치른다 | 키 하나 조회 |
| 새 값이 보이는 때 | TTL 만료나 삭제 뒤 다음 요청 | 배치가 다시 쓴 직후 |
| 새 질문 | 쿼리만 바꾸면 된다 | 배치와 저장 모양을 바꿔야 한다 |

미리 계산한 결과 테이블은 그 자체가 캐시라서, 배치가 다시 쓰는 것으로 무효화가 끝난다. 그 앞에 캐시를 하나 더 얹을 때부터 무효화 문제가 다시 생긴다.

## 배치가 테이블을 다시 쓸 때

캐시 어사이드를 배치 테이블 위에 얹으면, 배치가 끝난 뒤 TTL이 다할 때까지 캐시는 옛 값을 낸다. 이 구간을 줄이는 방법은 두 가지다.

TTL을 배치 일정에 맞추는 방법은 `EXPIREAT`으로 다음 배치가 끝날 예상 시각에 키가 사라지게 한다. 배치가 늦어지면 만료 직후 요청이 아직 바뀌지 않은 테이블을 읽어 옛 값을 다시 채운다. 모든 키가 같은 순간에 사라지므로 그 직후 요청이 한꺼번에 원천으로 간다.

키에 배치 버전을 넣는 방법은 배치가 끝날 때 버전 포인터 하나만 바꾼다. 읽는 쪽은 포인터를 읽어 키를 만들고, 옛 버전 키는 아무도 읽지 않다가 TTL로 사라진다. 대신 요청마다 포인터를 읽는 왕복이 한 번 늘고, 포인터가 바뀌는 순간 그 테이블의 캐시가 전부 비어 첫 요청들이 DB로 간다.

```text
# 배치가 끝나면 포인터만 바꾼다 (키 이름은 예로 지었다)
SET play_metric:version 20261120

# 읽기 API: 포인터로 키를 만들고, 없으면 DB를 읽어 TTL과 함께 넣는다
GET play_metric:version                          -> "20261120"
GET play_metric:20261120:2026-11-01:2026-11-19   -> (nil)
SET play_metric:20261120:2026-11-01:2026-11-19 "<json>" EX 172800
```

Redis 키는 만료를 걸지 않으면 지우거나 메모리 정책으로 밀려날 때까지 남으므로 결과 키에는 TTL을 준다. `SET`으로 덮어쓰면 기존 TTL이 지워지니 덮어쓸 때마다 `EX`를 다시 준다. `INCR`이나 `HSET`처럼 값을 고치기만 하는 명령은 TTL을 그대로 둔다. Django에서는 `cache.get(key, version=run_id)`처럼 version 인자에 배치 버전을 넘기면 같은 구조가 된다. `incr_version()`은 값을 새 버전으로 옮기는 함수라 무효화에는 맞지 않는다.

## 참고 자료

- [RFC 9111: HTTP Caching](https://www.rfc-editor.org/rfc/rfc9111.html)
- [RFC 9110: HTTP Semantics, Conditional Requests](https://www.rfc-editor.org/rfc/rfc9110.html#name-conditional-requests)
- [MDN: HTTP caching](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Caching)
- [Spring Framework: Cache Abstraction](https://docs.spring.io/spring-framework/reference/integration/cache/strategies.html)
- [Spring Framework: Declarative Annotation-based Caching](https://docs.spring.io/spring-framework/reference/integration/cache/annotations.html)
- [Spring Framework: How to Set the TTL/TTI/Eviction policy](https://docs.spring.io/spring-framework/reference/integration/cache/specific-config.html)
- [Spring Boot: Caching](https://docs.spring.io/spring-boot/reference/io/caching.html)
- [Django 5.2: Django's cache framework](https://docs.djangoproject.com/en/5.2/topics/cache/)
- [Django 5.2: Conditional View Processing](https://docs.djangoproject.com/en/5.2/topics/conditional-view-processing/)
- [Redis: EXPIRE](https://redis.io/docs/latest/commands/expire/)

## English summary

A read API over batch-built tables can cache in the client, in HTTP caches, inside the app process, or in a shared store like Redis, and the hard part is dropping the cache when the batch rewrites the table. In my dashboard API I cached GET responses in browser memory and kept no server cache, and in my year-end recap jobs I precomputed every result in HBase so the API cached the shared row with Spring `@Cacheable`. HTTP caching works through freshness (`max-age`) and revalidation (`ETag` with a 304 reply), and the last batch run ID makes a cheap ETag. On top of a batch table, a TTL aligned with the schedule is simple but serves stale data when the batch runs late, while a version in the key switches cleanly at the cost of one extra lookup per request.
