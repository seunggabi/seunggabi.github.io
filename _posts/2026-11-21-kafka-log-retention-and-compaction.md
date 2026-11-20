---
title: "Kafka는 세그먼트 단위로 지우고 키 단위로 압축한다"
date: 2026-11-21 05:13:00 +0900
categories: [Streaming, Kafka]
tags: [kafka, log-segment, retention, log-compaction, tiered-storage, concept]
---

트래픽이 적은 토픽의 `retention.ms`를 1일로 줄였다. 그런데 사흘 전에 들어온 레코드가 컨슈머에서 아직 읽힌다. 설정이 늦게 먹은 것이 아니다. Kafka는 레코드 하나하나에 만료를 걸지 않고, 파티션을 이루는 세그먼트 파일을 통째로 지운다. 세그먼트 안의 가장 새 레코드가 보존 기간을 넘겨야 그 파일이 지워진다.

compaction도 같은 세그먼트 위에서 돈다. 다만 시간 대신 키를 본다.

버전은 Kafka 4.4 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 세그먼트(segment) | 파티션 로그를 나눈 파일 묶음. 첫 오프셋이 파일 이름이 된다 |
| active segment | 지금 쓰고 있는 마지막 세그먼트. compaction 대상이 아니다 |
| `.index` / `.timeindex` | 오프셋→파일 위치, 타임스탬프→오프셋을 듬성듬성 적은 인덱스 |
| `segment.bytes` / `segment.ms` | 세그먼트를 새로 여는 크기(1GiB)와 시간(7일) 기준 |
| `retention.ms` / `retention.bytes` | 시간(7일), 파티션당 크기(무제한) 보존 기준. 세그먼트째 지운다 |
| `cleanup.policy` | `delete`(기본), `compact`, 또는 `compact,delete` |
| log cleaner | compaction을 하는 브로커의 백그라운드 스레드(기본 1개) |
| dirty ratio | 아직 정리하지 않은 바이트의 비율. `min.cleanable.dirty.ratio`(0.5)를 넘어야 청소 후보가 된다 |
| tombstone | 값이 `null`인 레코드. compacted 토픽에서 그 키의 삭제 표시 |
| `delete.retention.ms` | tombstone을 남겨 두는 시간(1일) |
| `min.compaction.lag.ms` | 이 시간 동안은 압축하지 않고 원본을 남긴다(기본 0) |
| tiered storage | 닫힌 세그먼트를 원격 저장소로 올리는 기능(KIP-405) |

## 파티션은 디렉터리, 세그먼트는 파일 세 개 이상이다

파티션 `orders-0`은 `log.dirs` 아래 `orders-0` 디렉터리 하나다. 그 안에 세그먼트마다 같은 이름의 파일이 여러 개 생긴다. 이름은 그 세그먼트의 첫 오프셋을 20자리로 채운 숫자다.

| 파일 | 담는 것 |
|---|---|
| `00000000000000000000.log` | 레코드 배치 자체. 프로듀서가 보낸 형식 그대로 이어 붙인다 |
| `00000000000000000000.index` | 상대 오프셋 4바이트 + 파일 위치 4바이트. 약 `index.interval.bytes`(4096바이트)마다 한 줄 |
| `00000000000000000000.timeindex` | 타임스탬프 8바이트 + 상대 오프셋 4바이트 |
| `.txnindex`, `.snapshot` | 중단된 트랜잭션 목록, 프로듀서 상태 스냅샷 |

컨슈머가 오프셋 N을 달라고 하면 브로커는 먼저 N을 품은 세그먼트를 찾고, `.index`를 이진 탐색해 N 이하의 가장 가까운 위치로 간 뒤 `.log`를 앞으로 읽는다. 인덱스가 모든 레코드를 가리키지 않아도 되는 이유다. `offsetsForTimes()`처럼 시각으로 찾는 요청은 `.timeindex`에서 오프셋을 얻고 같은 길을 탄다. 인덱스 파일은 `segment.index.bytes`(10MiB)만큼 미리 잡아 두고 메모리 맵으로 읽는다.

## 세그먼트를 닫는 기준과 지우는 기준은 따로 있다

쓰기는 언제나 마지막 세그먼트, 곧 active segment에만 붙는다. 브로커는 새 배치를 붙이기 직전에 굴릴지를 따진다.

- 붙이면 `segment.bytes`(기본 1GiB)를 넘는다.
- 세그먼트 첫 배치의 타임스탬프와 새 배치의 타임스탬프 차이가 `segment.ms`(기본 7일)에서 무작위 지터(최대 `segment.jitter.ms`, 기본 0)를 뺀 값을 넘는다.
- `.index`나 `.timeindex`가 가득 찼다.

시간 기준도 새 레코드가 들어올 때 따진다. 아무것도 들어오지 않는 파티션은 시간 때문에 굴러가지 않는다. retention이 세그먼트를 모두 지워야 할 때만 브로커가 빈 세그먼트를 새로 연다.

지우는 일은 브로커가 `log.retention.check.interval.ms`(5분)마다 한다. 오래된 세그먼트부터 차례로 보며, 세그먼트의 가장 큰 레코드 타임스탬프가 지금보다 `retention.ms`를 넘겨 오래됐으면 지운다. `retention.bytes`는 파티션 크기가 이 값을 넘는 만큼 앞에서부터 세그먼트를 지운다. 파티션당 값이라서 토픽 전체 크기는 이 값에 파티션 수를 곱해야 나온다. 파티션 수가 lag와 병렬성에 미치는 영향은 [Kafka 파티션과 lag 글](/posts/kafka-partitions-and-lag/)에 있다.

처음 예시가 여기서 풀린다. 하루에 몇 MB만 들어오는 토픽이면 한 세그먼트가 1GiB를 채우지 못하고 `segment.ms`인 7일 가까이 열려 있다. 그 세그먼트는 마지막 레코드가 하루를 넘겨야 지워지므로, 첫 레코드는 최대 8일쯤 남는다. 보존을 촘촘하게 맞추려면 `segment.ms`도 같이 줄여야 하고, 그만큼 파일 수와 열린 파일 핸들이 늘어난다.

지울 세그먼트는 바로 사라지지 않는다. 먼저 `.deleted`를 붙여 이름을 바꾸고 `file.delete.delay.ms`(1분) 뒤에 파일을 지운다. 로그 시작 오프셋은 그 전에 다음 세그먼트의 첫 오프셋으로 올라간다. 지워진 범위를 요청한 컨슈머는 `auto.offset.reset`에 따라 처음이나 끝으로 간다.

[Spark Streaming 적재 글](/posts/spark-streaming-to-hive/)의 복구 모드는 다른 컨슈머 그룹으로 토픽을 처음부터 다시 읽었다. 그 "처음"은 retention이 남겨 둔 가장 오래된 세그먼트다. [로그 수집 API](/posts/log-collection-api/)가 스키마 버전마다 새 토픽을 만들고 옛 토픽을 남겨 둔 구조도, 옛 토픽에 무엇이 얼마나 남는지는 그 토픽의 retention이 정한다.

## compaction은 키마다 마지막 값을 남긴다

`cleanup.policy=compact`인 토픽은 시간이 지나도 키마다 최신 값 하나는 남는다. 오프셋은 바뀌지 않고 순서도 그대로다. 지워진 오프셋은 빈자리로 남아, 컨슈머가 받는 오프셋이 건너뛸 뿐이다.

로그 클리너가 하는 일을 순서대로 적으면 이렇다.

1. 마지막으로 정리한 위치(체크포인트) 뒤쪽, dirty 구간의 바이트 비율이 가장 높은 파티션을 고른다. 비율이 `min.cleanable.dirty.ratio`(0.5)를 넘어야 후보가 된다.
2. dirty 구간을 읽어 키 해시 → 마지막 오프셋 맵을 만든다. 맵은 `log.cleaner.dedupe.buffer.size`(128MiB)를 클리너 스레드 수로 나눈 메모리에 들어간다.
3. 앞쪽 세그먼트부터 다시 복사하면서, 맵에 더 뒤의 오프셋이 있는 레코드를 버린다. 복사본(`.cleaned`)을 `.swap`으로 바꿔 원본과 교체한다.

정리할 수 있는 끝은 셋 가운데 가장 앞이다. active segment의 시작, last stable offset(아직 끝나지 않은 트랜잭션 앞), 그리고 `min.compaction.lag.ms`가 지나지 않은 레코드가 있는 세그먼트. 그래서 active segment 안의 중복은 세그먼트가 굴러가기 전까지 그대로 남는다. 키 몇 개만 계속 바뀌는 작은 토픽에서 compaction이 안 도는 것처럼 보이는 흔한 이유다.

`max.compaction.lag.ms`를 주면 이 상한을 넘긴 레코드가 있는 파티션은 dirty ratio와 상관없이 청소 대상이 되고, active segment를 굴리는 시간 기준도 `segment.ms`와 이 값 가운데 작은 쪽이 된다. 다만 클리너가 밀리면 이 상한은 지켜지지 않을 수 있다.

## tombstone은 언제 사라지나

값이 `null`인 레코드가 tombstone이다. 클리너는 tombstone보다 앞선 같은 키의 값을 지우고, tombstone 자체는 바로 지우지 않는다. 클리너가 tombstone이 든 배치를 처음 정리할 때 배치에 삭제 기한(그때 시각 + `delete.retention.ms`)을 적고, 이후 정리에서 그 기한이 지났으면 tombstone을 버린다.

그 결과 처음부터 읽는 컨슈머가 이 시간 안에 끝까지 따라잡으면 삭제까지 보고, 더 늦으면 삭제된 키를 아예 모른다. Debezium이 보내는 tombstone과 이 시간 제약은 [CDC 개념 글](/posts/change-data-capture/)에 있다.

## 정책 세 가지와 어울리는 토픽

| `cleanup.policy` | 남는 것 | 어울리는 토픽 |
|---|---|---|
| `delete` | 보존 기간·크기 안의 모든 레코드 | 클릭·재생 로그처럼 사건 하나하나가 의미 있는 토픽 |
| `compact` | 키마다 최신 값(과 아직 기한이 남은 tombstone) | CDC 변경 로그, 설정·상태 저장 토픽 |
| `compact,delete` | 보존 기간 안에서 키마다 최신 값 | 오래된 키는 버려도 되는 상태 토픽 |

CDC 토픽에 `compact`가 맞는 이유는 키가 원천 테이블의 기본 키이기 때문이다. 키마다 마지막 행만 있으면 새 컨슈머가 처음부터 읽어 테이블 전체를 다시 만들 수 있고, 토픽은 행 수에 비례해서만 자란다. `delete`였다면 보존 기간이 지난 행은 토픽에서 사라져 처음부터 읽어도 테이블이 완성되지 않는다.

Kafka Connect가 분산 모드에서 쓰는 config, offset, status 내부 토픽도 compacted 토픽이다. 커넥터 이름이나 source 파티션이 키이고 필요한 것은 그 키의 최신 값뿐이다. 세 토픽의 역할은 [Kafka Connect 워커와 태스크 글](/posts/kafka-connect-workers-tasks/)에 있다.

`compact,delete`에서는 retention이 키를 가리지 않는다. 오래 바뀌지 않은 키는 최신 값까지 세그먼트와 함께 지워진다.

로컬에서 compaction을 확인하는 순서다(Kafka 4.4, 단일 노드 quickstart 기준).

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic users \
  --config cleanup.policy=compact --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01

# u1:a, u2:b, u1:c 를 보내고 10초 넘게 기다린 뒤 u3:d 를 보내면 세그먼트가 굴러간다
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic users \
  --reader-property parse.key=true --reader-property key.separator=:

ls /tmp/kraft-combined-logs/users-0/
bin/kafka-dump-log.sh --print-data-log \
  --files /tmp/kraft-combined-logs/users-0/00000000000000000000.log
```

클리너가 돌고 나면(쉬는 간격 `log.cleaner.backoff.ms` 15초) 첫 세그먼트에는 `u2:b`, `u1:c`만 남고 `u1:a`의 오프셋 0은 비어 있을 것이다. `--reader-property null.marker=NULL`을 주고 `u1:NULL`을 보내면 tombstone을 만들 수 있다.

## 빠른 이유와 tiered storage

브로커는 레코드를 자기 메모리에 캐시하지 않고 파일에 쓴 뒤 OS 페이지 캐시에 맡긴다. 쓰기마다 fsync하지도 않는다. 내구성은 복제로 얻는다. 따라잡은 컨슈머는 방금 쓴 데이터를 페이지 캐시에서 읽고, 브로커를 재시작해도 캐시는 살아 있다.

읽기는 `FileChannel.transferTo`, 곧 리눅스 `sendfile`로 페이지 캐시에서 소켓으로 바로 보낸다. 사용자 영역으로 복사하지 않아서 zero-copy라고 부른다. 리스너에 TLS를 켜면 브로커가 암호화를 해야 하므로 이 경로를 쓰지 않는다. 오래된 오프셋을 읽는 컨슈머는 페이지 캐시에 없는 세그먼트를 디스크에서 읽게 되어, 따라잡은 컨슈머의 캐시까지 밀어낼 수 있다.

tiered storage(KIP-405)는 3.9에서 운영 환경용으로 선언됐다. 브로커에 `remote.log.storage.system.enable=true`와 `RemoteStorageManager` 구현을 걸고, 토픽에 `remote.storage.enable=true`를 준다. 닫힌 세그먼트가 원격 저장소로 올라가고, 로컬에는 `local.retention.ms`/`local.retention.bytes`만큼만 남는다. 기본값 `-2`는 전체 retention과 같다는 뜻이다. 전체 보존은 여전히 `retention.ms`가 정한다.

제약도 분명하다. Apache Kafka는 S3 같은 저장소용 `RemoteStorageManager`를 함께 주지 않아 별도 플러그인이 필요하다. compacted 토픽은 지원하지 않는다. 그래서 CDC 토픽과 Connect 내부 토픽은 tiered storage 밖에서 로컬 디스크에 남는다.

## 참고 자료

- [Apache Kafka 4.4 Topic Configs](https://kafka.apache.org/44/configuration/topic-configs/)
- [Apache Kafka 4.4 Broker Configs](https://kafka.apache.org/44/configuration/broker-configs/)
- [Apache Kafka 4.4 Design (persistence, efficiency, log compaction)](https://kafka.apache.org/44/design/design/)
- [Apache Kafka 4.4 Implementation: Log](https://kafka.apache.org/44/implementation/log/)
- [Apache Kafka 4.4 Tiered Storage](https://kafka.apache.org/44/operations/tiered-storage/)
- [UnifiedLog.java (4.4.0, 세그먼트 삭제)](https://github.com/apache/kafka/blob/4.4.0/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java)
- [LogSegment.java (4.4.0, 롤링 조건)](https://github.com/apache/kafka/blob/4.4.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java)
- [LogCleanerManager.java (4.4.0, 정리 가능 구간)](https://github.com/apache/kafka/blob/4.4.0/storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleanerManager.java)
- [MemoryRecords.java (4.4.0, tombstone 삭제 기한)](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/record/internal/MemoryRecords.java)
- [Apache Kafka 3.9.0 Release Announcement](https://kafka.apache.org/blog/2024/11/06/apache-kafka-3.9.0-release-announcement/)
- [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)

## English summary

Kafka stores each partition as a directory of segments, each with a `.log` file and sparse `.index` and `.timeindex` files, and it only ever appends to the active segment. Retention deletes whole segments once their newest record is older than `retention.ms`, so on a quiet topic data can outlive the setting by up to `segment.ms`. Compaction keeps the latest value per key, never touches the active segment, and drops a tombstone only after `delete.retention.ms` has passed since the cleaner first saw it. That is why CDC topics and Kafka Connect's internal topics use compaction, while event-log topics fit time-based deletion, and a replay like the recovery mode of my Spark Streaming job can reach back only as far as retention allows. Tiered storage has been production-ready since 3.9, but it does not support compacted topics, so the trade-off is cheap long retention for event logs against local disk for changelogs.
