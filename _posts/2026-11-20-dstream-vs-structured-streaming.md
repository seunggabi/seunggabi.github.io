---
title: "Structured Streaming은 Kafka 오프셋을 어디 둘까"
date: 2026-11-20 04:53:00 +0900
categories: [Streaming, Spark Streaming]
tags: [spark-streaming, structured-streaming, kafka, checkpoint, watermark, concept]
description: "Spark Streaming(DStream)과 Structured Streaming이 Kafka 오프셋을 어디에 기억하고 출력이 몇 번 쓰이는지 어떻게 다른지 비교한다."
---

Spark에는 스트리밍 API가 두 개 있다. RDD 위에서 도는 Spark Streaming(DStream)과 DataFrame 위에서 도는 Structured Streaming이다. 둘 다 마이크로배치로 돌지만, Kafka를 어디까지 읽었는지 기억하는 곳과 출력이 몇 번 쓰이는지가 다르다. [Spark Streaming으로 Hive에 쌓는 글](/posts/spark-streaming-to-hive/)의 DStream 잡을 예로 두고, 그 잡을 Structured Streaming으로 옮긴다고 가정해 무엇을 얻고 무엇을 내주는지 정리한다. 실제로 옮긴 잡은 아니고, 아래의 전환은 모두 가정이다. 버전은 Spark 4.3 기준이다.

## 두 API를 한 표로

| | DStream (Kafka 0-10 direct stream) | Structured Streaming |
|---|---|---|
| 실행 단위 | batch interval마다 RDD 하나 | 트리거마다 마이크로배치 하나, 결과 테이블을 증분으로 갱신 |
| 코드 | `foreachRDD` 안에서 RDD를 직접 다룸 | DataFrame 연산과 `writeStream` |
| Kafka 오프셋 | 체크포인트, Kafka 커밋, 외부 저장소 중에서 고름 | 체크포인트의 오프셋 로그. Kafka에는 커밋하지 않음 |
| 컨슈머 그룹 | 직접 지정 | 쿼리마다 고유한 그룹을 자동으로 만듦 |
| 파일 출력 | at-least-once, 멱등하게 만드는 건 사용자 몫 | 파일 싱크는 exactly-once |
| 이벤트 시간 | 직접 구현 | `withWatermark`, `window` |
| 현재 상태 | 업데이트가 끝난 legacy 프로젝트 | Spark의 현재 스트리밍 엔진 |

DStream은 Spark 2.0에서 Structured Streaming이 나오기 전부터 쓰이던 API다. Kafka direct stream은 Kafka 파티션 하나를 RDD 파티션 하나로 대응시키는 단순한 구조이고, 그 잡도 20초 batch interval로 토픽을 읽어 배치마다 HDFS 파일에 붙여 썼다.

## 오프셋은 어디에 남는가

DStream의 Kafka 0-10 연동은 오프셋을 체크포인트, Kafka 커밋, 결과와 같은 트랜잭션 저장소 중 어디에 둘지 사용자에게 맡기고, Hive 적재 글의 잡이 자동 커밋에 일 배치 `dropDuplicates()`와 복구 모드를 묶어 스트리밍 쪽을 단순하게 둔 구조는 [Kafka 전달 보장 글](/posts/kafka-delivery-guarantees/)에 정리했다.

Structured Streaming의 Kafka 소스는 이 선택지를 없앴다. `auto.offset.reset`, `enable.auto.commit`은 예외가 나고, `kafka.group.id`는 경고와 함께 허용되지만 그 그룹의 커밋 오프셋은 읽지 않는다. 쿼리는 트리거마다 처리한 오프셋 범위를 체크포인트 디렉터리에 먼저 기록하고, 재시작하면 거기서 이어 읽는다. `startingOffsets`(스트리밍 기본값 `latest`)는 체크포인트가 없는 새 쿼리를 시작할 때만 쓰인다.

옮길 때 놓치기 쉬운 부분이 여기다. 새 쿼리는 예전 컨슈머 그룹의 커밋 오프셋을 보지 않는다. 예전 잡이 멈춘 자리부터 읽으려면 그 그룹의 오프셋을 조회해 `startingOffsets`에 파티션별 JSON(`{"logs":{"0":1234,"1":5678}}`)으로 넣어야 한다. 그냥 띄우면 기본값 `latest`라서 두 잡 사이에 들어온 메시지를 건너뛴다.

## 출력 모드와 파일 싱크의 exactly-once

Structured Streaming은 입력을 계속 늘어나는 테이블로 보고, 트리거마다 결과 테이블에서 무엇을 내보낼지를 출력 모드로 정한다.

| 출력 모드 | 내보내는 것 | 쓸 수 있는 쿼리 |
|---|---|---|
| append (기본) | 새로 추가된 행만, 한 번씩 | 이미 나간 행이 바뀌지 않는 쿼리. 집계라면 이벤트 시간 워터마크가 있어야 한다 |
| update | 지난 트리거 뒤 바뀐 행 | 집계, `mapGroupsWithState`, 집계 없는 쿼리 |
| complete | 결과 테이블 전체 | 집계 쿼리 |

파일 싱크는 append만 받고, 장애가 나도 exactly-once다. 출력 경로 아래 `_spark_metadata` 디렉터리에 배치마다 어떤 파일을 커밋했는지 로그를 남기고, 재시작 뒤 이미 커밋된 배치 ID가 다시 오면 쓰지 않고 건너뛴다. Kafka처럼 다시 읽을 수 있는 소스와 이 싱크를 묶으면 Kafka 재전달로 생기는 중복 줄이 파일에 남지 않는다.

이 보장에는 범위가 있다. `_spark_metadata`를 읽는 건 Spark의 파일 소스이고, 그것도 출력 루트 경로 하나를 줄 때다. `dt=2026-11-19` 같은 파티션 하위 폴더를 직접 읽거나, Hive처럼 폴더의 파일을 그대로 나열하는 쪽은 이 로그를 보지 않는다. 실패한 배치가 남긴 파일까지 읽을 수 있다는 뜻이다. 일 배치가 이 출력을 읽는다면 루트 경로를 읽고 `dt`로 거르는 편이 맞다.

같은 출력 경로에 체크포인트만 새로 만들어 붙이는 것도 위험하다. 새 쿼리의 배치 ID는 0부터 다시 시작하는데, 싱크는 `_spark_metadata`에 기록된 마지막 배치 ID 이하를 이미 커밋된 것으로 보고 건너뛴다. 에러 없이 데이터만 빠진다. 파일 싱크의 출력 경로를 바꾸는 변경도 재시작 시 허용되지 않는다.

## 이벤트 시간과 워터마크

DStream 잡은 메시지의 첫 이벤트 시각을 꺼내 분 단위 키로 만들고, 그 키의 파일에 붙여 썼다. 늦게 온 로그도 자기 시각의 파일로 간다. Structured Streaming에서는 이벤트 시각으로 파티션 컬럼을 만들고 `partitionBy`로 쓰면 같은 효과가 난다. 집계가 없는 쿼리라 워터마크는 필요 없다.

워터마크는 상태를 들고 있는 연산에서 필요하다. `withWatermark("event_ts", "2 hours")`는 지금까지 본 가장 큰 이벤트 시각에서 2시간을 뺀 값을 기준으로, 그보다 늦지 않은 데이터는 버리지 않겠다는 약속이다. 2시간보다 늦은 데이터는 반영될 수도 있고 버려질 수도 있다. Flink 워터마크는 그보다 이른 이벤트가 더 오지 않는다는 선언이지만, Spark 쪽은 지연보다 덜 늦은 이벤트를 버리지 않는다는 데까지만 보장한다. 워터마크가 있어야 지난 윈도와 중복 판단 상태를 지울 수 있고, append 모드 집계는 워터마크가 윈도 끝을 지난 뒤에야 그 윈도 결과를 내보낸다.

일 배치의 중복 제거를 스트림 안으로 옮기는 방법도 있다. `dropDuplicatesWithinWatermark(["line"])`은 워터마크 지연 안에서 같은 값을 걸러 내고, 워터마크보다 늦게 온 행은 중복이 아니어도 통째로 버린다. 그 잡은 7일까지 늦게 오는 로그를 받았으니, 늦은 로그를 잃지 않으려면 지연을 7일로 두고 7일치 줄을 상태로 들고 있어야 한다. 일 배치는 이런 상태 없이 하루 폴더를 한 번 읽어 같은 줄을 지운다. 대신 일 배치가 돈 뒤에 들어온 늦은 로그는 그 정리에서 빠진다.

## 트리거: 20초 간격과 availableNow

| 트리거 | 동작 |
|---|---|
| 지정 안 함 | 앞 배치가 끝나자마자 다음 배치 |
| `processingTime="20 seconds"` | 20초 경계마다 배치. 앞 배치가 20초를 넘기면 끝나자마자 바로 다음 배치. 새 데이터가 없으면 배치를 만들지 않음 |
| `availableNow=True` | 시작 시점까지 쌓인 데이터를 소스 옵션에 맞춰 여러 배치로 나눠 처리하고 멈춤 |
| `once=True` | 한 배치로 처리하고 멈춤. deprecated, availableNow로 대체 |

예전 잡의 20초 batch interval은 `processingTime`으로 옮기면 된다. 복구 모드는 `availableNow`와 잘 맞는다. 새 체크포인트, 별도 출력 경로, `startingOffsets="earliest"`로 쿼리를 띄우면 그 시점까지의 메시지를 다 읽고 스스로 끝난다. 출력 경로를 따로 두는 이유는 앞 절의 배치 ID 문제 때문이다.

## 옮긴다면

```python
# 구조만 보여 주는 스케치, Spark 4.3 PySpark. 토픽, 경로, JSON 필드는 예시다
from pyspark.sql import functions as F

lines = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "broker:9092")
         .option("subscribe", "logs")
         .option("startingOffsets", '{"logs":{"0":1234,"1":5678}}')  # 새 체크포인트일 때만 쓰인다
         .load()
         .selectExpr("CAST(value AS STRING) AS line"))

event_ts = F.coalesce(
    F.try_to_timestamp(F.get_json_object("line", "$.events[0].time")),
    F.current_timestamp())  # 시각을 못 읽으면 현재 시각, 예전 잡과 같은 처리

query = (lines
         .withColumn("dt", F.date_format(event_ts, "yyyy-MM-dd"))
         .withColumn("hr", F.date_format(event_ts, "HH"))
         .writeStream.format("text")
         .partitionBy("dt", "hr")
         .option("path", "hdfs:///logs/raw")
         .option("checkpointLocation", "hdfs:///checkpoints/logs-raw")
         .trigger(processingTime="20 seconds")
         .start())
```

이렇게 옮기면 오프셋은 컨슈머 그룹 대신 체크포인트에 남고, 이미 커밋된 배치를 다시 쓰는 일은 `_spark_metadata`가 거른다. 드라이버로 모아 쓰던 일은 executor가 파일을 직접 쓰는 일로 바뀌어 HDFS append가 필요 없다. 대신 배치마다 파티션마다 새 파일이 생기니 작은 파일은 오히려 늘 수 있어, 파일 합치는 잡은 계속 필요하다. 진행 위치도 체크포인트 디렉터리 하나에 모여서, 그 디렉터리를 잃으면 Kafka 쪽에 남은 위치가 없다. 예전 잡은 위치를 컨슈머 그룹에 두고 스트리밍을 단순하게 유지한 채, 겹친 줄은 뒤의 일 배치가 정리하는 쪽이었다.

## 참고 자료

- [Spark 4.3.0 Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html)
- [Spark 4.3.0 Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html)
- [Spark 4.3.0 Spark Streaming Programming Guide (DStream)](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Spark 4.3.0 Spark Streaming + Kafka Integration Guide (0-10)](https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html)
- [FileStreamSink.scala (apache/spark v4.3.0)](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sinks/FileStreamSink.scala)

## English summary

DStream and Structured Streaming both run micro-batches, but they keep Kafka progress in different places. My DStream job left offsets to consumer auto commit and kept the streaming side simple, with a daily `dropDuplicates()` batch and a recovery mode on a separate consumer group handling overlaps and gaps. Structured Streaming does not commit to Kafka; it writes processed offset ranges to its checkpoint, and its file sink skips batches already logged in `_spark_metadata`. Moving that job over is a hypothetical here. It would put all progress in a checkpoint and a metadata log, which become the only record of where the job is, and which readers that list files directly, like Hive, do not see.
