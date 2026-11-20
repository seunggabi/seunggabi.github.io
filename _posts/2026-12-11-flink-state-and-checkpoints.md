---
title: "Flink 잡은 재시작할 때 무엇을 들고 돌아오나"
date: 2026-12-11 09:10:00 +0900
categories: [Streaming, Flink]
tags: [flink, checkpoint, savepoint, state-backend, rocksdb, kafka, exactly-once, concept]
---

Flink 잡이 죽었다 다시 뜨면 세 가지를 맞춰야 한다. 연산자가 들고 있던 상태, source가 어디까지 읽었는지, sink가 이미 밖으로 내보낸 결과다. 앞의 둘은 체크포인트 하나로 같이 되돌아간다. 세 번째는 sink가 쓰는 방식에 따라 중복이 되기도 하고 그대로 맞기도 한다. 광고 클릭을 세어 MySQL에 차단 후보를 쌓는 잡과 외부 API를 부르는 매칭 잡, 두 가상의 잡을 예로 들어 이 세 번째를 어떻게 다루는지 비교한다.

버전은 Flink 2.3, Kafka 커넥터 예시는 5.0.0(Flink 2.2용) 기준이다.

## keyed state와 operator state

| | keyed state | operator state |
|---|---|---|
| 범위 | 지금 처리 중인 레코드의 키 | 병렬 인스턴스 하나 |
| 주로 쓰는 곳 | `keyBy` 뒤의 윈도, 집계, `KeyedProcessFunction` | source와 sink |
| 타입 | `ValueState`, `ListState`, `MapState`, `ReducingState`, `AggregatingState` | `ListState`(균등 분할 또는 union), broadcast state |
| 병렬도를 바꿀 때 | key group 단위로 옮겨짐 | 리스트 원소를 나눠 주거나(기본), 전체를 모두에게 줌(union) |

keyed state는 key group으로 묶여 저장된다. key group 수는 최대 병렬도와 같고, 병렬 인스턴스 하나가 key group 여러 개를 맡는다. 병렬도를 바꾸면 key group이 인스턴스 사이로 옮겨 갈 뿐 키가 다시 해시되지는 않는다. 그래서 최대 병렬도는 그 연산자의 상태를 버리지 않고는 바꿀 수 없다. 지정하지 않으면 병렬도 128 이하에서 128로 잡힌다.

예시의 클릭 잡이 IP나 기기 ID로 `keyBy`한 뒤 슬라이딩 처리 시간 윈도로 클릭 수를 센다고 하자. 이 윈도 내용이 keyed state다. `sum`은 레코드를 쌓아 두지 않고 윈도마다 누적값 하나만 갱신하므로, 키 하나가 차지하는 상태는 동시에 열린 윈도 수(size / slide)에 비례한다. 반면 Kafka source는 split마다 현재 오프셋을 들고 있다가 체크포인트 때 split 상태로 저장한다.

## 상태가 사는 곳

| state backend | `state.backend.type` | 상태 위치 | 체크포인트 |
|---|---|---|---|
| HashMapStateBackend (기본값) | `hashmap` | TaskManager JVM 힙의 객체 | 매번 전체 |
| EmbeddedRocksDBStateBackend | `rocksdb` | TaskManager 로컬 디스크의 RocksDB, 직렬화된 바이트 | 전체 또는 증분 |
| ForStStateBackend | `forst` | 원격 파일 시스템(S3, HDFS)의 SST 파일, 로컬 디스크는 캐시 | 항상 비동기 증분, 2.3에서도 실험 단계 |

HashMap은 상태를 객체 그대로 다뤄 빠르지만 상태 전체가 힙에 들어가야 한다. RocksDB는 읽고 쓸 때마다 직렬화 비용을 내는 대신 로컬 디스크만큼 상태를 들 수 있다. 증분 체크포인트를 켜면 지난 완료 체크포인트 이후 바뀐 부분만 올리고, 복구할 때는 그 체크포인트가 기대는 앞선 체크포인트들의 파일까지 내려받아 상태를 다시 만든다. 설정은 `state.backend.type: rocksdb`와 `execution.checkpointing.incremental: true`다.

## 배리어가 스냅숏의 경계를 긋는다

체크포인트는 source에 배리어를 끼워 넣으며 시작한다. 배리어는 레코드와 함께 흐르고 레코드를 앞지르지 않는다. 배리어 n 앞의 레코드는 스냅숏 n에, 뒤의 레코드는 n+1에 속한다. 입력이 여럿인 연산자는 배리어 n을 먼저 받은 입력을 멈추고, 나머지 입력에서 배리어 n이 올 때까지 기다린다. 다 모이면 상태를 스냅숏하고 배리어를 내보낸다. 이것이 aligned 체크포인트다. 모든 sink가 배리어를 받았다고 알리면 체크포인트 n이 완료된다.

backpressure가 심하면 배리어가 밀린 네트워크 버퍼 뒤에서 기다리느라 체크포인트가 늦어진다. unaligned 체크포인트는 연산자가 첫 배리어를 바로 내보내고, 앞지른 버퍼의 레코드를 상태에 함께 저장한다. `execution.checkpointing.unaligned.enabled: true`와 함께 `execution.checkpointing.aligned-checkpoint-timeout`을 주면 aligned로 시작했다가 시간을 넘길 때 unaligned로 바뀐다. 대가는 체크포인트 크기와 I/O다. 동시에 여러 개 돌 수 없고, 복구 때 버퍼를 되살린 뒤에 워터마크를 다시 만들기 때문에 워터마크에 기대는 연산자의 결과가 달라질 수 있다. `AT_LEAST_ONCE` 모드는 정렬을 건너뛴다. 배리어 n이 온 입력도 계속 처리하니 n+1에 속할 레코드가 스냅숏 n에 섞이고, 복구 때 그 레코드가 다시 들어와 중복이 된다.

## 복구할 때 Kafka 오프셋은 체크포인트에서 온다

장애가 나면 Flink는 마지막으로 완료된 체크포인트를 골라 모든 연산자의 상태를 되돌리고, source를 그 체크포인트에 기록된 위치로 되돌린다. Kafka source가 체크포인트 완료 때 브로커에 커밋하는 오프셋은 컨슈머 그룹의 진행을 보여 주는 모니터링 값이고, 장애 복구에는 쓰이지 않는다. 시작 위치를 "커밋된 오프셋부터, 없으면 최신부터"로 잡았다면, 이 설정은 체크포인트 없이 새로 뜰 때 쓰이고, 체크포인트나 세이브포인트에서 복구할 때는 상태에 든 오프셋이 우선한다.

복구 지점부터 장애 시점까지 처리했던 레코드는 다시 처리된다. Flink 안의 상태는 그 레코드를 처음 보는 시점으로 돌아가 있으니 계산이 겹치지 않는다. 그 사이 sink가 밖에 쓴 결과는 되돌아가지 않는다. 예시의 클릭 잡이라면 MySQL sink가 이미 있는 후보는 새로 넣지 않고 갱신 시각만 바꾸며, 알림은 새 후보에만 보내게 할 수 있다. 슬라이딩 윈도가 slide마다 같은 키를 다시 내보내는 것에 맞춘 처리인데, 재처리로 같은 키가 다시 와도 행과 알림이 늘지 않는다. 처리 시간 윈도가 재시작 직후 백로그를 어떻게 받는지는 [Flink 시간과 윈도](/posts/flink-time-and-windows/) 글에 적었다.

## Async I/O의 진행 중 요청은 다시 나간다

Async I/O 연산자는 응답을 아직 받지 못한 요청의 입력 레코드를 체크포인트에 저장하고, 복구할 때 그 요청을 다시 보낸다. 연산자 상태는 exactly-once로 맞춰지지만, 외부 API는 같은 요청을 한 번 더 받는다. 체크포인트 이후 Kafka에서 다시 읽힌 레코드도 API를 다시 부른다. 예를 들어 장소 검색 API를 부르는 매칭 잡이라면 호출을 `unorderedWait`로 감싸고, `timeout()`을 직접 구현해 느린 요청을 잡 재시작 대신 실패 토픽으로 보낼 수 있다. 재시작 한 번은 API 호출 몇 건을 더 만든다. sink가 장소 ID 기준 업서트라면 같은 행을 덮어쓰고, 완료 이벤트를 커밋 뒤에 내보내면 복구 뒤에도 "완료 이벤트가 왔으면 행이 있다"는 순서는 그대로 지켜진다.

## Kafka sink의 두 단계 커밋

sink까지 exactly-once가 되려면 외부 쓰기가 체크포인트와 함께 확정돼야 한다. `KafkaSink`의 `EXACTLY_ONCE`가 그 방식이다. 체크포인트 사이의 레코드를 Kafka 트랜잭션 하나에 쓰고, 체크포인트가 완료되면 커밋한다. Kafka 트랜잭션과 `read_committed`는 [Kafka 전달 보장](/posts/kafka-delivery-guarantees/) 글에서 다뤘다.

```java
// Flink 2.2 + flink-connector-kafka 5.0.0-2.2 기준 스케치
env.enableCheckpointing(60_000);   // 기본 모드는 EXACTLY_ONCE

KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("block-candidates")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("click-candidates")   // 클러스터 안에서 잡마다 달라야 함
        .setProperty("transaction.timeout.ms", "900000")      // 브로커 transaction.max.timeout.ms 이하
        .build();

stream.sinkTo(sink).uid("candidate-kafka-sink");
```

트랜잭션 timeout은 기본값끼리 맞지 않는다. `KafkaSinkBuilder`는 프로듀서의 `transaction.timeout.ms`를 1시간으로 넣는다. 브로커의 `transaction.max.timeout.ms` 기본값은 15분이고, 프로듀서가 이보다 긴 값을 요청하면 브로커가 `InitProducerId` 요청에 오류를 돌려준다. 그래서 브로커 최대값을 올리거나 프로듀서 값을 내려야 한다. 반대로 너무 짧으면 체크포인트와 재시작이 길어지는 동안 브로커가 미커밋 트랜잭션을 만료시켜 데이터가 사라진다. 기준은 "가장 긴 체크포인트 시간 + 가장 긴 재시작 시간"보다 길게다.

나머지 조건도 있다. 레코드는 체크포인트가 끝나야 보이므로 체크포인트 간격이 곧 지연이다. 트랜잭션 이름은 기본(`INCREMENTING`)이면 체크포인트마다 새로 생기고, 브로커는 이름마다 메타데이터를 7일 동안 들고 있다. 커넥터 4.x부터 이름을 재사용하는 `POOLING`이 있고, Kafka 3.0 이상과 대상 토픽 읽기 권한이 필요하다.

## 체크포인트와 세이브포인트

| | 체크포인트 | 세이브포인트 |
|---|---|---|
| 목적 | 예상하지 못한 장애 복구 | 계획한 작업: 코드 변경, Flink 업그레이드, 병렬도 변경 |
| 소유 | Flink가 만들고 지움, 잡이 끝나면 기본으로 삭제 | 사용자가 만들고 지움 |
| 형식 | state backend 고유 형식, 증분 가능 | 기본은 backend와 무관한 canonical, native도 가능 |
| state backend 바꾸기 | 안 됨 | canonical만 됨 |
| Flink 마이너 버전 업그레이드 | aligned만 됨 | 됨 |
| 병렬도 변경 | 됨 | 됨 |

`flink stop --savepointPath`로 세이브포인트를 남기고 멈춘 뒤 `flink run --fromSavepoint`로 다시 띄운다. 다시 이어서 돌릴 잡에는 이벤트 시간 윈도와 타이머를 모두 비우는 `--drain`을 붙이지 않는다. 세이브포인트의 상태는 연산자 ID로 연산자에 붙는다. `uid()`를 주지 않으면 ID가 잡 그래프에서 계산되므로, 연산자 하나를 끼워 넣기만 해도 ID가 바뀌어 상태를 찾지 못한다. 기본 동작은 세이브포인트의 모든 상태가 연산자에 짝지어져야 잡이 뜨는 것이다. `--allowNonRestoredState`로 짝 없는 상태를 버리고 띄울 수 있지만, 버려진 것이 Kafka source 상태면 source는 설정된 시작 위치부터 다시 읽는다.

## state TTL로 상태 크기 묶기

같은 클릭의 반복은 흔히 스트림 앱 옆의 Redis 키로 막는다. 이 확인을 Flink keyed state로 옮긴다면 키마다 "본 적 있음"을 들고 있어야 하고, 지우지 않으면 상태가 끝없이 자란다. 아래는 그 가정의 스케치다.

```java
// Flink 2.3 기준 스케치
StateTtlConfig ttl = StateTtlConfig
        .newBuilder(Duration.ofMinutes(10))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();

ValueStateDescriptor<Boolean> seen = new ValueStateDescriptor<>("seen-click", Types.BOOLEAN);
seen.enableTimeToLive(ttl);
```

TTL은 처리 시간만 지원한다. 마지막 쓰기 시각을 기계 시각으로 재므로, 오래 멈췄던 잡을 복구하면 멈춘 시간도 지난 시간으로 계산된다. 만료된 값은 읽을 때 지워지고, 백그라운드 정리는 backend마다 다르다. 힙 backend는 상태에 접근할 때마다 몇 개씩 훑고, RocksDB는 compaction 필터에서 지운다. 스냅숏을 만들 때 만료 값을 빼는 정리는 RocksDB 증분 체크포인트에는 적용되지 않는다. TTL 설정은 체크포인트와 세이브포인트에 저장되지 않는다. 2.2.0부터는 복구할 때 TTL을 켜거나 꺼도 오류가 나지 않는다. 다만 복구 때 TTL을 짧은 값에서 긴 값으로 늘리는 것은 데이터 오류가 날 수 있어 권장되지 않는다.

## 참고 자료

- [Stateful Stream Processing (Flink 2.3)](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)
- [Working with State](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/)
- [State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
- [Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Checkpointing under backpressure](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing_under_backpressure/)
- [Checkpoints vs. Savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints_vs_savepoints/)
- [Upgrading Applications and Flink Versions](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/upgrading/)
- [Production Readiness Checklist](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/production_ready/)
- [Command-Line Interface](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/)
- [Async I/O](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/asyncio/)
- [Kafka Connector (Flink 2.2, connector 5.0.0)](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/)
- [Apache Flink Downloads: Kafka connector 호환 버전](https://flink.apache.org/downloads/)
- [KafkaSinkBuilder.java, TransactionNamingStrategy.java (v5.0.0)](https://github.com/apache/flink-connector-kafka/tree/v5.0.0/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink)
- [Apache Kafka 4.4 Broker Configs: transaction.max.timeout.ms](https://kafka.apache.org/44/configuration/broker-configs/)

## English summary

A Flink restart has to line up three things: operator state, source positions, and what the sinks already wrote. Checkpoints restore the first two together, and the Kafka source recovers its offsets from checkpoint state, not from offsets committed to the broker. A click-counting job whose MySQL sink only refreshes a timestamp for an existing candidate adds no rows or alerts on replay, and in a matching job that calls an external API, Async I/O re-sends in-flight requests on recovery while a sink that upserts by place ID keeps the rows correct. A two-phase-commit Kafka sink gives exactly-once output but needs a transaction timeout that fits the broker limit and shows records only after each checkpoint. Idempotent writes show results right away, while two-phase commit trades that latency for output that never repeats.
