---
title: "스트림 옆 Redis로 중복 거르고 윈도 세기"
date: 2026-12-18 09:10:00 +0900
categories: [Streaming, Flink]
tags: [redis, flink, kafka, deduplication, rate-limiting, concept]
---

같은 클릭이 두 번 들어왔는지, 한 IP가 1분 동안 몇 번 눌렀는지를 스트림 처리기 옆의 Redis에 묻는 구조는 흔하다. 붙이기는 쉽다. 어려운 건 Redis의 키가 만료, 메모리 축출, 장애 세 경로로 사라지고, 사라질 때마다 중복 판정과 카운트가 조용히 바뀐다는 점이다.

이 글에서는 광고 클릭 중복을 거르는 가상의 구조를 예로 든다. 1단계 스트림 앱은 클릭에서 만든 검증용 키가 Redis에 있는지 보고, 없으면 저장한 뒤 다음 토픽으로 보낸다. 2단계 Flink 잡은 IP와 기기 ID별로 슬라이딩 처리 시간 윈도에서 클릭 수를 센다.

버전은 Redis 8.10, Flink 2.3 기준이다.

## 확인과 저장은 명령 하나로 묶는다

`GET`으로 확인하고 `SET`으로 저장하면 두 컨슈머가 둘 다 nil을 보고 통과시킨다. `SET`의 `NX`는 키가 없을 때만 저장해 이 틈을 없앤다.

```text
SET dedup:<click-key> 1 NX EX 86400
OK        # 처음 본 클릭
(nil)     # 이미 본 클릭
```

키를 언제 쓰느냐(토픽 전송 전이냐 후냐)에 따라 유실과 중복 중 어느 쪽으로 기우는지는 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)에서 다뤘다.

`EX`가 곧 중복을 기억하는 기간이다. 같은 이벤트가 TTL보다 늦게 다시 오면 처음 보는 이벤트가 된다. 컨슈머가 멈췄다가 밀린 오프셋을 다시 읽는 시간, 클라이언트가 재전송하는 시간을 합친 것보다 TTL이 짧으면 그 재전달은 걸러지지 않는다. TTL을 늘리면 그만큼 키가 메모리에 오래 남는다.

## 고정 윈도는 INCR, 슬라이딩 윈도는 sorted set

고정 윈도는 윈도마다 키를 하나 만들고 `INCR`로 센다. 키 이름에 분 단위 시각을 넣으면 분이 바뀔 때 새 키가 생긴다.

```text
MULTI
INCR   cnt:ip:203.0.113.7:202612180910
EXPIRE cnt:ip:203.0.113.7:202612180910 120 NX
EXEC
```

`INCR`과 `EXPIRE`를 따로 보내면, 그 사이에 클라이언트가 죽었을 때 TTL 없는 키가 남는다. `MULTI`/`EXEC`로 묶으면 두 명령이 다른 클라이언트의 명령에 끼이지 않고 연달아 실행된다. `EXPIRE ... NX`(7.0부터)는 TTL이 없을 때만 걸어서, 증가할 때마다 만료 시각이 밀리지 않는다.

`MULTI`/`EXEC`에는 롤백이 없다. 큐에 들어간 명령 하나가 `EXEC` 뒤에 실패해도 나머지는 실행된다. 앞 명령의 결과를 보고 다음 명령을 고르는 일도 할 수 없다. 그게 필요하면 Lua 스크립트나 Redis Functions(7.0부터)를 쓴다. 스크립트가 도는 동안 서버는 다른 요청을 처리하지 않으므로 스크립트는 짧아야 한다.

고정 윈도는 경계에 약하다. 한도가 분당 10이면 09:10:59에 10번, 09:11:00에 10번을 눌러도 두 키 모두 한도 안이다. 2초가 안 되는 사이에 20번이 지나간다.

슬라이딩 윈도는 sorted set에 이벤트 시각을 점수로 넣고, 윈도 밖의 멤버를 지운 뒤 개수를 센다.

```lua
-- 구조만 보여 주는 스케치
-- KEYS[1] = 윈도 키, ARGV[1] = 현재 ms, ARGV[2] = 현재 - 윈도 ms, ARGV[3] = 이벤트 ID, ARGV[4] = 윈도 ms
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return redis.call('ZCARD', KEYS[1])
```

멤버는 이벤트 ID여야 한다. 시각을 멤버로 쓰면 같은 밀리초에 들어온 이벤트가 하나로 합쳐진다. 거꾸로 클릭 키를 멤버로 쓰면 같은 클릭이 두 번 와도 한 번만 세므로, 윈도 안의 중복 제거가 덤으로 따라온다. 시각은 호출하는 쪽이 넘기므로, 컨슈머가 여럿이면 각자의 시계가 점수가 된다.

| | INCR 고정 윈도 | sorted set 슬라이딩 | Flink 슬라이딩 윈도 |
|---|---|---|---|
| 상태 크기 | 키와 윈도마다 정수 하나 | 이벤트마다 멤버 하나 | 키마다 겹치는 윈도 수만큼 집계값 |
| 판정 시점 | 이벤트마다 즉시 | 이벤트마다 즉시 | 윈도가 닫힐 때 |
| 경계 | 경계 양쪽으로 몰리면 한도의 두 배까지 통과 | 경계 없음 | slide 간격마다 다시 계산 |
| 원자성 | `MULTI`/`EXEC` | Lua 스크립트 | 키 하나를 태스크 하나가 처리 |

앞의 예시에서 2단계는 셋째 열이다. 슬라이딩 윈도가 겹치기 때문에 같은 키가 slide 간격마다 다시 나오므로, 결과를 MySQL 후보 테이블에 쓴다면 이미 있는 후보는 갱신 시각만 바꿔야 한다. Redis 방식은 이벤트가 올 때마다 바로 숫자를 돌려주는 대신, 이벤트 수만큼 메모리를 쓰고 이벤트마다 네트워크 왕복이 한 번 생긴다.

## 키가 사라지는 세 경로

첫째는 만료다. Redis는 두 방식으로 키를 지운다. 클라이언트가 만료된 키에 접근하면 그때 지우고, 주기적으로 만료가 걸린 키를 무작위로 골라 이미 지난 것을 지운다. 그래서 만료된 키가 메모리에는 잠시 남아도 읽기에는 보이지 않는다. 만료 시각은 절대 Unix 시각으로 저장되므로 서버 시계가 앞으로 뛰면 키가 한꺼번에 사라진다.

둘째는 메모리 축출이다. `maxmemory`를 넘으면 `maxmemory-policy`에 따라 키를 지운다.

| 정책 | 중복 키에 생기는 일 |
|---|---|
| `allkeys-lru`, `allkeys-lrm`, `allkeys-lfu`, `allkeys-random` | TTL이 남은 중복 키도 지워진다. 다음에 온 중복은 처음 보는 이벤트로 통과한다 |
| `volatile-*` | TTL이 걸린 키 중에서 지운다. 중복 키는 모두 TTL이 있으니 그대로 후보가 된다 |
| `noeviction` | 아무것도 지우지 않고 새 데이터를 쓰는 명령에 오류를 낸다. `SET NX`가 실패하므로 앱이 통과시킬지 막을지 정해야 한다 |

캐시 키와 중복 키를 한 인스턴스에 두면, 캐시가 커질 때 중복 키가 같이 밀려난다. 캐시와 지워지면 안 되는 키를 함께 쓰는 경우라면 인스턴스를 나누는 쪽이 낫다. `INFO stats`의 `evicted_keys`가 늘고 있으면 축출이 일어나는 중이고, 중복 키도 그 대상이 될 수 있다.

셋째는 장애다. RDB 스냅숏만 쓰면 마지막 스냅숏 뒤의 몇 분이, AOF를 기본값 `appendfsync everysec`로 쓰면 최대 1초가 사라질 수 있다. 영속화를 끈 마스터가 자동 재시작하면 빈 데이터로 뜨고, 레플리카가 그 빈 마스터를 따라 자기 데이터까지 비운다. 복제는 비동기라서 마스터가 OK를 돌려준 쓰기도 장애 조치 때 잃을 수 있다. `WAIT`는 그 확률을 줄이지만 강한 일관성을 만들지는 않는다.

중복 키를 잃으면 중복이 통과하고, 카운터 증가분을 잃으면 덜 센다. 앞의 예시에서 Redis가 맡은 일은 1단계 중복 판정이라, 키를 잃으면 그 클릭은 다음 토픽으로 넘어간다. 2단계 결과를 바로 차단하지 않고 후보로 쌓아 사람이 확정하게 하면, 이런 오차가 바로 차단으로 이어지지 않는다. 후보와 차단의 중복은 MySQL 쪽 유니크 제약으로 막을 수 있다.

## 클러스터에서는 같이 쓰는 키를 한 슬롯에 둔다

Redis Cluster는 키 공간을 16384개 슬롯으로 나누고, `CRC16(key) mod 16384`로 슬롯을 정한다. `MULTI`/`EXEC`나 스크립트처럼 여러 키를 한 번에 다루려면 그 키들이 같은 슬롯에 있어야 한다. 키 이름에 `{...}`를 넣으면 중괄호 안의 문자열만 해시한다.

```text
cnt:{ip:203.0.113.7}:202612180910    # 고정 윈도 카운터
zwin:{ip:203.0.113.7}                 # 슬라이딩 윈도
```

두 키는 같은 슬롯에 들어간다. 해시 태그를 너무 넓게 잡으면(예: 날짜만 태그로) 그날 키가 전부 한 노드로 몰린다. 스크립트가 쓰는 키는 이름을 스크립트 안에서 만들지 말고 모두 `KEYS`로 넘겨야 클러스터가 어느 노드로 보낼지 안다. 클러스터도 장애 조치는 마지막으로 선출된 마스터의 데이터가 이기는 방식이라, 앞의 비동기 복제 문제가 슬롯마다 그대로 있다.

## 상태를 Flink에 둘 때와 Redis에 둘 때

Flink에서는 `keyBy` 뒤의 키 상태가 그 키를 맡은 태스크 안에 있다. 체크포인트는 키 상태와 소스의 읽기 위치(Kafka라면 오프셋)를 같은 시점으로 함께 저장하고, 복구하면 둘이 같이 되감긴다. Redis 중복 키는 재시작해도 되감기지 않으므로, 오프셋을 되감아 다시 읽은 이벤트가 "이미 봤다"로 판정되어 버려질 수 있다.

```java
// 구조만 보여 주는 스케치
StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
ValueStateDescriptor<Boolean> seen = new ValueStateDescriptor<>("seen", Boolean.class);
seen.enableTimeToLive(ttl);
```

Flink 상태 TTL은 처리 시간 기준만 지원한다. 위 두 설정은 기본값이고, 만료된 값은 읽을 때 지워지며 RocksDB 백엔드에서는 컴팩션 필터가 백그라운드로 정리한다.

Redis가 맞는 경우도 분명하다. 판정하는 쪽이 Flink 잡이 아닐 때다. 앞의 예시처럼 1단계가 Spring Boot 같은 일반 앱이면 체크포인트되는 키 상태가 없고, 여러 인스턴스가 같은 기억을 보려면 바깥 저장소가 필요하다. 다른 서비스가 같은 카운트를 읽어야 할 때, 잡을 다시 배포해도 기억이 남아야 할 때도 Redis 쪽이 단순하다. 대신 이벤트마다 네트워크 왕복이 생기고, 앞에서 본 세 경로로 기억을 잃는다.

## 참고 자료

- [Redis: SET](https://redis.io/docs/latest/commands/set/)
- [Redis: EXPIRE (How Redis expires keys)](https://redis.io/docs/latest/commands/expire/)
- [Redis: INCR (rate limiter pattern)](https://redis.io/docs/latest/commands/incr/)
- [Redis: Transactions](https://redis.io/docs/latest/develop/using-commands/transactions/)
- [Redis: Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- [Redis: Key eviction](https://redis.io/docs/latest/develop/reference/eviction/)
- [Redis: Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis: Replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/)
- [Redis: Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Flink 2.3: Working with State (State TTL)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/)
- [Flink 2.3: Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/checkpointing/)

## English summary

Redis next to a stream processor is a quick way to drop repeated events with `SET NX EX` and to count events per key with `INCR` fixed windows or sorted-set sliding windows. Its keys can disappear through expiry, memory eviction, or failover, and each loss lets a duplicate through or undercounts a window. In an example two-stage ad click setup, a stream app checks a Redis key for repeated clicks, a Flink job counts clicks in sliding windows, and the results become block candidates that a person confirms, with unique keys in MySQL preventing duplicate blocks. Flink keyed state rewinds together with Kafka offsets on recovery, while Redis is easier to share across processes, so the choice comes down to replay consistency versus shared access.
