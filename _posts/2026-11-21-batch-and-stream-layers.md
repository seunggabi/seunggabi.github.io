---
title: "실시간 테이블과 일별 테이블 중 어느 쪽을 믿을까"
date: 2026-11-21 03:22:00 +0900
categories: [Streaming, Spark Streaming]
tags: [lambda-architecture, kappa-architecture, kafka, spark, flink, iceberg, late-data, concept]
---

같은 로그를 실시간 테이블과 일별 테이블에 함께 쌓으면, 같은 날짜의 건수가 두 테이블에서 조금씩 다르게 나온다. 예를 들어 앱 이벤트 로그를 Flink로 실시간 Iceberg 테이블에 넣고, S3 sink를 거쳐 Spark로 일별 테이블도 만든다고 하자. 실시간 테이블은 서버 수신 시각으로, 일별 테이블은 sink가 레코드를 처리한 시각으로 날짜를 자르면 자정 근처에 받은 로그는 두 테이블에서 다른 날짜에 들어갈 수 있다. 그러면 대시보드와 경고는 어느 쪽 숫자를 믿어야 하고, 로직이 바뀌면 과거는 어디서 다시 만들어야 할까.

이 질문에 대한 오래된 답이 Lambda와 Kappa 아키텍처다.

버전은 Spark 4.3, Flink 2.3, Iceberg 1.12 기준이다.

## 배치가 덮어쓰는 Lambda

Nathan Marz는 2011년 글 "How to beat the CAP theorem"에서 시스템을 두 층으로 나눴다. 배치 층은 지우거나 고치지 않고 쌓기만 하는 원본 데이터 전체로 뷰를 미리 계산한다. 실시간 층은 배치가 아직 따라잡지 못한 최근 몇 시간만 증분으로 계산한다. 쿼리는 두 뷰를 읽어 합치고, 배치가 그 구간을 다시 계산하면 실시간 결과는 덮어써진다. Lambda Architecture라는 이름은 그 뒤 Marz가 붙였고, James Warren과 함께 쓴 책 『Big Data』(Manning, 2015)에 정리돼 있다.

이 구조가 노린 건 사람의 실수에 대한 내성이다. 원본은 불변이니, 쿼리 로직에 버그가 있으면 고친 뒤 원본에서 다시 계산하면 된다. 실시간 층의 오차도 배치가 그 구간을 덮을 때까지만 남는다.

대가는 같은 변환을 두 시스템에 두 번 짜는 것이다. Jay Kreps는 2014년 O'Reilly 글 "Questioning the Lambda Architecture"에서 이 점을 짚었다. 서로 다른 두 분산 시스템에서 같은 결과를 내는 코드를 계속 맞춰 두기가 매우 어렵고, 한 코드를 두 엔진으로 컴파일하는 추상화도 결국 새어 나온다는 것이다.

## 스트림 잡을 한 벌 더 띄우는 Kappa

Kreps의 대안은 재처리도 스트림 잡으로 하는 것이다.

1. Kafka처럼 여러 구독자를 받는 로그에 입력을 순서대로 보관한다.
2. 다시 처리할 일이 생기면 같은 스트림 잡을 하나 더 띄워 보관된 처음부터 읽고, 새 출력 테이블에 쓴다.
3. 새 잡이 따라잡으면 애플리케이션이 새 테이블을 읽게 바꾼다.
4. 옛 잡을 멈추고 옛 출력 테이블을 지운다.

Kreps는 이 방식을 Kappa Architecture라고 불러도 되겠다고 적으면서, 그리스 문자를 붙일 만큼 거창한 생각은 아닐 수 있다고 덧붙였다. 대가도 같이 적었다. 재처리하는 동안 출력이 두 벌 필요하고, 출력 저장소가 대량 쓰기를 견뎌야 한다. 그의 권고는 단순하다. 지연에 민감하지 않으면 배치, 민감하면 스트림으로 하고, 꼭 필요하지 않으면 둘을 같이 두지 않는다.

| | Lambda | Kappa |
|---|---|---|
| 변환 코드 | 배치와 스트림에 두 벌 | 스트림 하나 |
| 재처리 원천 | 레이크의 불변 원본 | Kafka에 보관된 로그 |
| 재처리할 수 있는 범위 | 원본을 보관한 전체 기간 | 토픽 보관 기간 안 |
| 정정 방식 | 배치가 실시간 결과를 덮어씀 | 새 출력을 만든 뒤 읽는 쪽을 바꿈 |
| 늘어나는 비용 | 두 코드의 일치 검증 | 재처리 중 출력 두 벌, 긴 토픽 보관 |

## Kafka를 다시 읽을까, 레이크에서 다시 계산할까

Kappa의 재처리 범위는 토픽 보관 기간이 정한다. `retention.ms` 기본값은 7일이고, `-1`이면 시간 제한이 없다. 30일을 다시 처리하고 싶으면 30일을 보관해야 한다. 레이크는 이 제한이 없다. 앞의 예에서도 S3 sink가 Kafka 메시지를 시간별 경로로 남기고, 일별 Spark 잡은 그 경로를 읽는다.

[Spark Streaming으로 Hive에 쌓는 글](/posts/spark-streaming-to-hive/)의 잡에는 두 방식이 다 있었다. 복구 모드 인자를 주면 잡 이름과 컨슈머 그룹 이름이 바뀌어, 평소 잡과 따로 토픽을 처음부터 다시 읽는다. Kafka를 다시 읽는 재처리다. 다만 Kreps처럼 새 출력 테이블에 쓰고 바꿔 끼우는 대신, 다시 읽은 줄은 같은 날짜 폴더에 겹쳐 들어가고 매일 도는 배치가 `dropDuplicates()`로 정리했다. Hive 적재 배치는 시작일과 종료일을 받아 하루씩 돌아서, HDFS에 쌓인 텍스트로 과거 날짜를 다시 만드는 데 그대로 썼다. 스트리밍 쪽은 단순하게 두고, 중복과 재적재는 배치가 맡는 구성이다. 이 잡을 Structured Streaming의 `availableNow` 트리거로 옮겨 복구를 돌리는 가정은 [DStream과 Structured Streaming 글](/posts/dstream-vs-structured-streaming/)에 적었다.

로직이 바뀌어 지난 석 달을 다시 만들어야 한다면, 일주일치만 남는 토픽으로는 할 수 없다. 이때는 레이크에서 다시 계산하는 길밖에 없고, 스트림 코드와 배치 코드가 다르면 그 차이도 같이 검증해야 한다.

## 두 테이블 중 어느 쪽이 이기나

두 테이블을 같이 두면 날짜마다 정답 테이블을 하나로 정해야 한다. Lambda의 규칙은 배치가 이긴다는 것이다. 앞의 예에 경고를 붙인다면, 일별 정합성 경고는 일별 테이블로 전주와 몇 주 추이를 비교하고, 시간별 경고는 아직 일별 테이블에 없는 오늘 건수만 실시간 테이블에서 읽는 식으로 나눌 수 있다. 그러면 실시간 테이블은 며칠치만 남기고 오래된 날짜를 지워도 된다.

읽는 쪽에서 두 테이블을 합친다면 경계를 하나로 정해야 한다.

```sql
-- 구조만 보여 주는 스케치. 테이블 이름은 예시다
WITH cutoff AS (SELECT MAX(dt) AS last_dt FROM daily_events)
SELECT dt, COUNT(*) AS events FROM daily_events GROUP BY dt
UNION ALL
SELECT r.dt, COUNT(*) FROM realtime_events r, cutoff c
WHERE r.dt > c.last_dt
GROUP BY r.dt;
```

이 경계는 일별 파티션이 한 번에 완성된 모습으로 나타날 때만 맞다. 쓰다 만 파티션이 보이면 `MAX(dt)`가 반쯤 찬 날짜를 가리킨다. Iceberg의 덮어쓰기는 원자적으로 커밋되니 이 조건을 채운다. 그리고 두 테이블이 다른 시계로 날짜를 자르면, 경계를 넘는 순간 자정 근처 몇 건이 옮겨 다닌다. 두 테이블의 같은 날짜를 비교하는 검사라면 그 차이를 허용 범위로 둬야 한다.

## 늦은 데이터는 어느 파티션으로 가나

경계 문제는 결국 날짜를 무슨 시각으로 자르느냐에서 나온다.

| 파티션 시계 | 늦게 온 로그가 가는 곳 | 닫힌 날짜를 다시 써야 하나 | 날짜가 뜻하는 것 |
|---|---|---|---|
| 이벤트 시각 | 그 로그가 생긴 날짜 | 늦게 온 만큼 지난 파티션을 다시 써야 한다 | 사건이 일어난 날 |
| 서버 수신 시각, sink 처리 시각 | 오늘 | 아니다 | 우리가 받은 날 |

Hive 적재 글의 스트리밍 잡은 이벤트 시각으로 파일을 나눴다. 늦게 온 로그도 자기 날짜 폴더에 들어가서 일 배치는 그 폴더 하나만 읽으면 된다. 대신 7일까지 늦는 로그는 일 배치가 돈 뒤에 들어올 수 있다. 그 로그까지 반영하려면 지난 며칠의 파티션을 다시 만들어야 한다. 앞의 앱 이벤트 로그 예는 반대 쪽인 서버 수신 시각으로 자른다. 단말 시계로 자르면 시계가 틀린 단말 때문에 미래 날짜 파티션이 생기고, 보존 삭제가 그 파티션을 지우지 못할 수 있다. 닫힌 날짜가 다시 바뀌지 않고 보존 삭제가 믿을 수 있는 시계를 쓰는 대신, 날짜는 사건이 일어난 날이 아니라 받은 날이 된다. 이벤트 시각은 컬럼으로 따로 남는다.

스트림 쪽에도 같은 선택이 있다. Flink 이벤트 시간 윈도의 allowed lateness 기본값은 0이고, 늘리면 기본 이벤트 시간 트리거에서는 늦은 레코드가 올 때 윈도가 다시 발화해 앞선 결과의 갱신을 내보낸다. 그보다 늦은 레코드는 side output으로 빼낼 수 있다. 자세한 동작은 [Flink 시간과 윈도 글](/posts/flink-time-and-windows/)에 있다. 어느 쪽이든 늦은 데이터를 받아 주는 기간이 곧 다시 써야 하는 범위다.

## 날짜 파티션을 통째로 바꾸는 재처리

이벤트 시각으로 자른 테이블에서 지난 7일을 다시 만든다면, 그 7개 파티션만 바꾸고 나머지는 건드리지 않아야 한다. 같은 날짜를 두 번 돌려도 한 벌만 남아야 하는 건 retry와 백필에서도 마찬가지다.

```python
# 구조만 보여 주는 스케치, Spark 4.3 PySpark. 테이블 이름은 예시다
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sql("""
    INSERT OVERWRITE lake.db.daily_events
    SELECT * FROM staged_events
    WHERE dt BETWEEN '2026-12-14' AND '2026-12-20'
""")

# Iceberg DataFrameWriterV2로는 같은 동작이 이 한 줄이다
staged.writeTo("lake.db.daily_events").overwritePartitions()
```

Spark의 `partitionOverwriteMode` 기본값은 `STATIC`이다. static 모드는 `PARTITION` 절에 맞는 파티션을 먼저 지우고, Iceberg 테이블에 `PARTITION` 절 없이 쓰면 테이블 전체가 바뀐다. dynamic 모드는 쿼리 결과에 행이 있는 파티션만 바꾼다. Hive serde 테이블은 이 설정과 상관없이 늘 dynamic으로 덮어쓴다. 앞의 예에서 일별 Spark 잡을 dynamic 모드로 쓰면, 같은 날짜를 다시 돌려도 그 파티션만 바뀐다.

dynamic 모드에도 빈틈이 하나 있다. 다시 계산한 결과에 어떤 날짜의 행이 하나도 없으면 그 파티션은 지워지지 않고 옛 값이 남는다. 날짜를 DAG run에서 받아 그 파티션만 덮어쓰는 방법은 [멱등한 Airflow DAG 글](/posts/airflow-idempotent-dags/)에, 파티션 대신 unique 키로 지우고 넣을 때 남는 옛 키는 [지표 테이블 grain 글](/posts/metric-table-grain/)에 적었다. Hive 적재 글의 중복 제거 배치는 `_TMP` 경로에 쓴 뒤 원래 폴더를 지우고 이름을 바꿔 끼웠다. 쓰는 도중 실패해도 원래 데이터가 남는 방식이다.

## 스트리밍 하나로 끝나는 경우

광고 클릭 중복을 거르는 잡이라면 배치 층 없이 끝날 수 있다. IP나 기기별로 슬라이딩 처리 시간 윈도에서 클릭 수를 세고, 기준 이상인 키를 MySQL에 차단 후보로 넣는 구조를 생각해 보자. 잡이 재시작하면 밀린 로그가 한 윈도에 몰려 숫자가 부풀 수 있지만, 결과가 차단 후보로만 쌓이기 때문에 이런 오탐이 바로 차단으로 이어지지는 않는다. 날짜별로 확정해 두고 나중에 다시 계산할 숫자가 아니라 사람이 검토할 후보 목록이다.

스트리밍만으로 충분한지는 결과에 다음 질문을 던져 보면 갈린다.

| 질문 | 스트리밍만으로 된다 | 배치 층이 필요하다 |
|---|---|---|
| 결과가 무엇인가 | 경고, 후보, 최근 상태 | 정산, 지표처럼 날짜별로 확정되는 숫자 |
| 로직이 바뀌면 | 앞으로만 적용해도 된다 | 과거도 새 로직으로 다시 만들어야 한다 |
| 다시 만들 범위 | 토픽 보관 기간 안 | 보관 기간보다 길다 |

두 번째와 세 번째 질문에 오른쪽 답이 나오면, 재처리 원천은 레이크에 있어야 한다. 그 뒤에도 같은 변환을 한 벌로 둘지 두 벌로 둘지는 남는다. Spark처럼 배치와 스트림을 같은 DataFrame 코드로 돌리는 엔진에서는 한 벌로 줄일 수 있지만, 늦은 데이터를 받는 기간과 날짜를 자르는 시계까지 같아지는 건 아니다.

## 참고 자료

- [Nathan Marz, How to beat the CAP theorem (2011)](https://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html)
- [Nathan Marz, James Warren, Big Data (Manning, 2015)](https://www.manning.com/books/big-data)
- [Jay Kreps, Questioning the Lambda Architecture (O'Reilly, 2014)](https://www.oreilly.com/radar/questioning-the-lambda-architecture/)
- [Apache Kafka Topic Configs (retention.ms)](https://kafka.apache.org/44/configuration/topic-configs/)
- [Apache Spark 4.3 Configuration (partitionOverwriteMode)](https://spark.apache.org/docs/latest/configuration.html)
- [Apache Iceberg 1.12 Spark Writes (INSERT OVERWRITE, overwritePartitions)](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Apache Flink 2.3 Windows (allowed lateness, side output)](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)

## English summary

Lambda keeps a batch layer that recomputes from immutable raw data and overrides a speed layer, at the cost of writing the same logic twice; Kappa replays a retained Kafka log through a second copy of the stream job, at the cost of retention and double output during the switch. In an app event log pipeline with both shapes, alerts can read closed days from the daily table and only today from the realtime table, and a daily Spark job with dynamic partition overwrite replaces only the date it reruns. In my earlier Spark Streaming job, a recovery mode replayed Kafka on a separate consumer group and a daily batch removed the overlaps. A click-counting job whose output is only a list of block candidates for review can run on streaming alone. Partitioning by server time keeps closed dates stable and retention safe, while partitioning by event time puts late logs on the right date but forces you to rewrite past partitions.
