---
title: "재시작 뒤 Flink 윈도 결과가 시간 기준마다 달라지는 이유"
date: 2026-11-20 04:40:00 +0900
categories: [Streaming, Flink]
tags: [flink, streaming, watermark, windowing, concept]
---

광고 클릭을 IP별로 세어 짧은 시간에 클릭이 몰린 키를 차단 후보로 쌓는 Flink 잡을 예로 든다. 이런 잡은 워터마크 없이 슬라이딩 처리 시간 윈도로 시작하기 쉽다. 설정이 단순한 대신 재시작 때 백로그가 현재 윈도에 몰린다. 이 선택이 무엇을 얻고 무엇을 내주는지 Flink의 시간 개념으로 풀어 본다.

버전은 Flink 2.3 기준이다.

## 처리 시간과 이벤트 시간

처리 시간은 연산을 실행하는 기계의 시스템 시각이다. 이벤트 시간은 이벤트가 실제로 일어난 시각으로, 보통 레코드 안에 들어 있다.

| | 처리 시간 | 이벤트 시간 |
|---|---|---|
| 기준 시각 | 연산자가 돌고 있는 기계의 현재 시각 | 레코드의 타임스탬프 |
| 필요한 설정 | 없음 | 타임스탬프 추출, 워터마크 전략 |
| 결과의 결정성 | 레코드가 들어오는 속도에 따라 달라짐 | 같은 입력이면 같은 윈도에 들어감 |
| 지연 | 가장 낮음 | 순서가 뒤바뀐 이벤트를 기다리는 만큼 늦음 |
| 늦게 온 데이터 | 개념이 없음, 도착한 시각의 윈도에 들어감 | 워터마크보다 늦으면 따로 처리해야 함 |

처리 시간은 분산 환경에서 결정성을 주지 않는다. 한 시간짜리 처리 시간 윈도에는 그 시간 동안 연산자에 "도착한" 레코드가 들어간다. 언제 일어났는지는 상관없다.

이런 잡이 처리 시간을 고르는 이유도 이 표 안에 있다. 이벤트 시간으로 가려면 로그 발생 시각을 믿을 수 있어야 하고 늦은 로그를 얼마나 기다릴지 정해야 한다. 처리 시간은 그런 설정 없이 돈다.

## 워터마크는 이벤트 시간의 시계다

이벤트 시간 윈도는 "이 윈도에 들어올 이벤트가 다 왔다"는 신호가 있어야 닫힌다. 그 신호가 워터마크다. `Watermark(t)`는 이 스트림의 이벤트 시간이 t에 도달했으니, 이제 타임스탬프가 t 이하인 레코드는 더 오지 않을 것이라는 선언이다.

가장 많이 쓰는 전략은 `forBoundedOutOfOrderness`다. 지금까지 본 가장 큰 타임스탬프에서 허용할 순서 뒤바뀜 폭을 뺀 값을 워터마크로 낸다. 폭을 30초로 두면 "30초까지는 늦게 와도 제자리에 넣어 준다"는 뜻이 된다.

연산자가 여러 입력을 받으면 현재 이벤트 시간은 입력들의 워터마크 중 가장 작은 값이다. Kafka 소스는 파티션마다 워터마크를 만들고 이걸 합친다. 그래서 파티션 하나가 조용하면 전체 워터마크가 그 자리에 멈춘다. Kafka 소스의 병렬도가 파티션 수보다 크면 남는 소스가 자동으로 idle 상태가 되지 않는다. 이때는 병렬도를 줄이거나 워터마크 전략에 idle 타임아웃을 넣어야 한다.

## 텀블링, 슬라이딩, 세션 윈도

세 윈도 모두 처리 시간 버전과 이벤트 시간 버전이 따로 있다.

| 윈도 | 이벤트 시간 할당자 | 모양 | 한 레코드가 속하는 윈도 수 |
|---|---|---|---|
| 텀블링 | `TumblingEventTimeWindows.of(size)` | 크기가 같고 겹치지 않음 | 1 |
| 슬라이딩 | `SlidingEventTimeWindows.of(size, slide)` | 크기는 같고 slide마다 새로 시작, 겹침 | size / slide |
| 세션 | `EventTimeSessionWindows.withGap(gap)` | 고정 크기 없음, gap보다 오래 비면 끊김 | 1 (합쳐진 뒤) |

슬라이딩 윈도는 겹치기 때문에 같은 키가 slide마다 결과에 다시 나온다. 결과를 후보 테이블에 쓴다면 이미 있는 후보는 새로 넣지 않고 갱신 시각만 바꾸고, 알림은 새 후보에만 보내야 하는 이유가 이것이다.

세션 윈도는 레코드마다 윈도를 하나 만들고, gap보다 가까운 윈도끼리 합친다. 그래서 윈도 함수로 `ReduceFunction`, `AggregateFunction`, `ProcessWindowFunction`처럼 합칠 수 있는 것을 써야 한다. 크기를 미리 정하지 않아도 되니 "쉬지 않고 이어진 클릭 덩어리"를 보고 싶을 때 후보가 된다.

시간 윈도의 시작점은 epoch에 맞춰진다. 1시간 텀블링 윈도는 `1:00:00.000 - 1:59:59.999`처럼 잘린다. 한국 시간 기준으로 하루 윈도를 자르려면 offset을 `Duration.ofHours(-9)`로 줘야 한다.

## 늦은 데이터가 가는 곳

워터마크가 윈도 끝을 지나면 윈도가 발화한다. 그 뒤에 그 윈도에 속하는 레코드가 오면 늦은 데이터다. 기본 allowed lateness는 0이라서, 아무 설정이 없으면 늦은 데이터는 버려진다.

`allowedLateness`를 주면 윈도 상태를 그만큼 더 들고 있다가, 늦은 레코드가 오면 윈도를 다시 발화한다. 늦은 발화의 결과는 앞선 계산의 갱신으로 다뤄야 하고, 다운스트림에서는 중복처럼 보일 수 있다. 윈도 상태는 워터마크가 "윈도 끝 + allowed lateness"를 지날 때 지워진다. 5분 텀블링 윈도 12:00-12:05에 lateness 1분이면 워터마크가 12:06을 지날 때다.

그보다도 늦은 레코드는 `sideOutputLateData`로 따로 빼낼 수 있다. 버리지 않고 로그로 남기거나 다른 저장소로 보낼 수 있다.

## 재시작과 처리 시간 윈도

Flink는 윈도 상태와 타이머를 체크포인트에 함께 저장한다. 멈춘 동안 닫혀야 했던 처리 시간 윈도는 복구 직후 체크포인트 시점까지 모인 레코드로 한꺼번에 발화한다고 볼 수 있다. 문서에 적힌 규칙은 밀린 타이머가 복구 직후 바로 실행된다는 것이다.

그다음 Kafka에서 밀린 레코드를 읽기 시작한다. 처리 시간 윈도는 레코드를 "지금" 시각의 윈도에 넣는다. 몇 분, 몇 시간 동안 쌓인 로그가 지금 열린 윈도 몇 개에 몰리고, 평소엔 기준 아래였던 키가 기준을 넘는다. 결과를 바로 차단하지 않고 후보로만 쌓으면 이 오탐을 사람이 확인하는 단계에서 받아낼 수 있다.

| 재시작 후 | 처리 시간 윈도 | 이벤트 시간 윈도 |
|---|---|---|
| 밀린 레코드가 들어가는 윈도 | 다시 읽은 시각의 윈도 | 레코드 타임스탬프의 윈도 |
| 윈도가 닫히는 시점 | 기계 시각이 윈도 끝을 지날 때 | 워터마크가 윈도 끝을 지날 때 |
| 백로그 처리 중 키별 개수 | 부풀 수 있음 | 원래 시간대별로 나뉨 |

이벤트 시간에서는 백로그를 빨리 읽으면 워터마크도 빨리 전진할 뿐, 레코드는 원래 윈도로 간다. 대신 지연 폭을 잘못 잡으면 정상 로그가 늦은 데이터로 버려진다.

## 이벤트 시간 버전으로 바꾼다면

아래는 앞의 예시 잡을 이벤트 시간으로 바꾼 모습이다. 크기와 지연 폭 숫자는 예시다. `Click`은 `ip`, `clickedAt`(epoch 밀리초), `count` 필드를 가진 POJO라고 가정했다.

```java
// 구조만 보여 주는 스케치, Flink 2.3 DataStream API
WatermarkStrategy<Click> watermarks = WatermarkStrategy
        .<Click>forBoundedOutOfOrderness(Duration.ofSeconds(30))
        .withTimestampAssigner((click, kafkaTimestamp) -> click.getClickedAt())
        .withIdleness(Duration.ofMinutes(1));

OutputTag<Click> lateTag = new OutputTag<Click>("late-clicks") {};

SingleOutputStreamOperator<Click> counts = env
        .fromSource(kafkaSource, watermarks, "clicks")   // KafkaSource<Click>
        .keyBy(Click::getIp)
        .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofMinutes(1)))
        .allowedLateness(Duration.ofMinutes(2))
        .sideOutputLateData(lateTag)
        .sum("count");

DataStream<Click> lateClicks = counts.getSideOutput(lateTag);
```

Flink 2.0에서 `org.apache.flink.streaming.api.windowing.time.Time` 클래스가 지워졌고, 윈도 API는 `java.time.Duration`을 받는다. 예전 글에 많은 `Time.seconds(5)` 예제는 2.x에서 그대로 컴파일되지 않는다.

`withTimestampAssigner`를 빼면 Kafka 소스는 `ConsumerRecord`에 붙은 Kafka 타임스탬프를 이벤트 시간으로 쓴다. 앱에서 찍은 클릭 시각과 Kafka에 쓰인 시각은 다를 수 있어서, 어떤 시각을 기준으로 할지는 직접 골라야 한다. 이 바꾼 잡에서도 늦은 발화가 같은 키를 다시 내보내므로, 싱크의 "있으면 갱신 시각만 바꾼다"는 처리가 그대로 필요하다.

## 자주 틀리는 곳

이벤트 시간 윈도에 `WatermarkStrategy.noWatermarks()`를 주면 워터마크가 전진하지 않아 윈도가 끝나지 않는다. 처리 시간 윈도는 워터마크가 필요 없으니 이 문제가 없다.

allowed lateness를 늘리면 윈도 상태를 그만큼 오래 들고 있고, 늦은 발화로 같은 윈도 결과가 여러 번 나온다. 다운스트림이 갱신을 받을 수 있어야 한다.

워터마크는 입력 중 가장 느린 쪽을 따라간다. 파티션 하나가 비면 idle 설정이 없는 한 잡 전체의 윈도가 멈춘다.

백로그를 따라잡을 때 파티션마다 읽는 속도가 벌어지면 앞선 쪽 레코드가 연산자 상태에 쌓인다. `WatermarkStrategy.withWatermarkAlignment(그룹, 최대 drift, 갱신 주기)`를 주면 같은 그룹의 최소 워터마크보다 drift 이상 앞선 소스나 split의 읽기를 잠시 멈추며, FLIP-27 소스에서만 동작한다.

## 참고 자료

- [Timely Stream Processing (Flink 2.3)](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/)
- [Generating Watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)
- [Windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)
- [Process Function: Timers](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/)
- [Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [WindowedStream Javadoc](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/datastream/WindowedStream.html)
- [Release Notes: Flink 2.0](https://nightlies.apache.org/flink/flink-docs-master/release-notes/flink-2.0/)

## English summary

A job that counts ad clicks per IP is easy to start with sliding processing-time windows, which run without timestamp extraction or watermark settings. The cost shows up on restart: the Kafka backlog lands in the windows that are open now and can inflate per-key counts. Event time puts each record back in the window of its own timestamp and uses watermarks to decide when a window is complete, with allowed lateness and side outputs for records that come too late. Processing time is simpler but its results depend on arrival time, while event time gives repeatable windows at the cost of choosing a lateness bound and holding more state.
