---
title: "KST 날짜 파티션이 9시간 밀리는 타임스탬프 타입"
date: 2026-11-21 06:12:00 +0900
categories: [Batch & Query, SQL]
tags: [timestamp, timezone, spark, trino, parquet, iceberg, hive, athena, mysql, debezium, concept]
description: "MySQL DATETIME의 한국 시각을 UTC로 읽어 Iceberg timestamptz에 쌓으면 KST 날짜 파티션이 9시간 밀린다. 엔진과 포맷별 타임스탬프 타입의 차이를 정리한다."
---

애플리케이션이 MySQL `DATETIME` 컬럼에 한국 시각 `2026-11-21 08:30:00`을 넣는다. Debezium은 이 값을 `1795249800000`으로 내보내는데, 이는 08:30을 UTC로 읽은 epoch 밀리초다. 이 값을 Iceberg `timestamptz`로 쌓고 세션 시간대가 `Asia/Seoul`인 Spark에서 읽으면 17:30이 나온다. 실제 순간인 UTC 11월 20일 23:30을 제대로 쌓은 테이블에서는 UTC 날짜로 자른 파티션 때문에 같은 주문이 전날로 들어간다. 두 오류 모두 9시간짜리이고, 원인은 저장 타입이 "순간"인지 "벽시계"인지를 각 단계가 다르게 가정했다는 데 있다.

버전은 Spark 4.3.0, Trino 484, Parquet format 2.14.0, Iceberg 1.12.0(테이블 스펙 v3), Hive 4.2.1, Athena 엔진 버전 3, MySQL 8.4, Debezium 3.7.1 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| instant | 시간축 위의 한 점. UTC epoch로 쓰면 어디서 읽어도 같은 순간이다 |
| local date-time | 연월일시분초만 있는 벽시계 값. 시간대를 붙이기 전에는 순간이 정해지지 않는다 |
| 세션 시간대 | 엔진이 시간대 없는 입력을 해석하고 순간을 화면에 보여 줄 때 쓰는 기본 시간대 |
| `TIMESTAMP_LTZ` | Spark의 기본 `TIMESTAMP`. 순간을 저장하고 세션 시간대로 보여 준다 |
| `TIMESTAMP_NTZ` | Spark의 시간대 없는 타입. 어떤 연산에도 시간대를 쓰지 않는다 |
| `isAdjustedToUTC` | Parquet TIMESTAMP 논리 타입의 플래그. true면 순간, false면 벽시계 |
| INT96 | Parquet의 옛 12바이트 타임스탬프. 스펙상 deprecated이고 쓰는 쪽마다 해석이 달랐다 |
| `timestamptz` | Iceberg의 순간 타입. UTC로 저장하고 원래 시간대는 남기지 않는다 |
| `timestamp_ns` | Iceberg v3에서 추가된 나노초 타입. `timestamptz_ns`와 짝이다 |
| `ZonedTimestamp` | Debezium이 MySQL `TIMESTAMP`를 UTC ISO 8601 문자열로 내보낼 때 쓰는 타입 |
| DST | 일광 절약 시간. 시행하는 지역에서는 하루가 23시간이나 25시간이 된다 |

## 같은 이름의 타입이 시스템마다 다른 쪽을 가리킨다

저장소와 엔진마다 순간 타입과 벽시계 타입이 한 쌍씩 있다. 이름만 보면 짝을 잘못 맞추기 쉽다.

| 시스템 | 순간 | 벽시계 |
|---|---|---|
| MySQL 8.4 | `TIMESTAMP` | `DATETIME` |
| Parquet | TIMESTAMP, `isAdjustedToUTC=true` | TIMESTAMP, `isAdjustedToUTC=false` |
| Iceberg | `timestamptz`, `timestamptz_ns`(v3) | `timestamp`, `timestamp_ns`(v3) |
| Spark 4.3 | `TIMESTAMP`(= `TIMESTAMP_LTZ`, 기본값) | `TIMESTAMP_NTZ` |
| Trino 484 | `timestamp(p) with time zone` | `timestamp(p)` |
| Hive 4.2 | `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMP` |

순간 쪽도 원래 시간대를 들고 있는 경우는 드물다. Iceberg `timestamptz`는 `2017-11-16 17:10:34 PST`를 `2017-11-17 01:10:34 UTC`로 저장하고 두 값을 같은 것으로 본다. MySQL `TIMESTAMP`는 현재 세션의 `time_zone`에서 UTC로 바꿔 저장하고 읽을 때 되돌린다. 값마다 시간대를 들고 다니는 건 Trino의 `timestamp with time zone`뿐이다.

Spark의 `TIMESTAMP`는 `spark.sql.timestampType`이 정하는 별칭이다. 기본값은 `TIMESTAMP_LTZ`이고, 3.4.0부터 `TIMESTAMP_NTZ`로 바꿀 수 있다. Spark와 Iceberg 사이에서는 Spark `TIMESTAMP`가 `timestamptz`로, `TIMESTAMP_NTZ`가 `timestamp`로 짝지어진다.

## 세션 시간대는 보여 주기와 날짜 자르기를 바꾼다

Spark `spark.sql.session.timeZone`의 기본값은 JVM 기본 시간대다. 같은 잡이라도 드라이버가 뜬 서버의 시간대에 따라 결과가 달라질 수 있다는 뜻이다. `TIMESTAMP_LTZ`를 `DATE`로 바꿀 때, `TIMESTAMP_NTZ`를 `TIMESTAMP_LTZ`로 바꿀 때, 시간대 없는 문자열 리터럴을 읽을 때 이 값을 쓴다. `TIMESTAMP_NTZ`를 `DATE`로 바꿀 때는 쓰지 않는다.

```sql
-- Spark 4.3.0 spark-sql에서 그대로 돌려 볼 수 있다
SET spark.sql.session.timeZone=UTC;
SELECT CAST(TIMESTAMP '2026-11-21 08:30:00+09:00' AS DATE);   -- 2026-11-20
SET spark.sql.session.timeZone=Asia/Seoul;
SELECT CAST(TIMESTAMP '2026-11-21 08:30:00+09:00' AS DATE);   -- 2026-11-21
SELECT CAST(TIMESTAMP_NTZ '2026-11-21 08:30:00' AS DATE);     -- 세션과 상관없이 2026-11-21
```

Trino의 세션 시간대는 클라이언트가 정한다. CLI는 `--timezone`을 주지 않으면 작업하는 컴퓨터의 시간대를 쓰고, 세션 안에서는 `SET TIME ZONE 'Asia/Seoul'`로 바꾼다. 다만 쓰이는 자리가 Spark보다 좁다. `timestamp`를 `timestamp with time zone`으로 캐스팅할 때는 세션 시간대를 붙이지만, 반대로 `timestamp with time zone`을 `date`나 `timestamp`로 캐스팅할 때는 값이 들고 있는 시간대를 쓴다. Iceberg 커넥터는 `timestamptz` 값을 UTC 시간대로 돌려주므로, `CAST(ts AS date)`는 세션을 `Asia/Seoul`로 바꿔도 UTC 날짜가 나온다.

```sql
-- Trino 484, 세션 시간대와 상관없이 결과가 같다
SELECT
  CAST(TIMESTAMP '2026-11-20 23:30:00 UTC' AS date)                            AS utc_date,  -- 2026-11-20
  CAST(TIMESTAMP '2026-11-20 23:30:00 UTC' AT TIME ZONE 'Asia/Seoul' AS date) AS kst_date;  -- 2026-11-21
```

Athena에서도 시간대는 쿼리 안에서 `AT TIME ZONE`, `with_timezone`, `at_timezone`으로 지정한다. Hive 테이블의 `TIMESTAMP` 컬럼은 밀리초 정밀도로 고정이라, 데이터가 마이크로초면 `SERIALIZATION_ERROR`가 난다. Iceberg 테이블에서 CTAS를 할 때는 `CAST(dt AS timestamp(6))`로 정밀도를 맞춰야 한다.

## Parquet INT96은 쓰는 쪽마다 의미가 달랐다

Parquet 스펙은 INT96을 deprecated로 두고 새 writer는 쓰지 말라고 한다. 표준은 INT64에 `isAdjustedToUTC`와 단위(`MILLIS`, `MICROS`, `NANOS`)를 붙인 TIMESTAMP 논리 타입이다. 그런데 기본값은 아직 INT96인 곳이 많다.

| writer | 기본 | 바꾸는 설정 |
|---|---|---|
| Spark 4.3 `TIMESTAMP` | INT96 | `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS`(`isAdjustedToUTC=true`) |
| Spark 4.3 `TIMESTAMP_NTZ` | INT64 `MICROS`, `isAdjustedToUTC=false` | 설정 없이 항상 이 형태 |
| Hive 4.2.1 | INT96 | `hive.parquet.write.int64.timestamp=true`, 단위는 `hive.parquet.timestamp.time.unit`(기본 `micros`) |
| Iceberg | INT64, `timestamp`는 false, `timestamptz`는 true | 스펙으로 고정 |

INT96 값은 Hive와 Spark가 UTC로 맞춰 쓰는 반면 Impala는 다른 오프셋으로 썼다. 그래서 Spark에는 Impala가 쓴 INT96을 보정하는 `spark.sql.parquet.int96TimestampConversion`(기본 false)이 있다. Trino Hive 커넥터는 Parquet 타임스탬프를 읽고 쓸 때 `hive.parquet.time-zone`(기본 JVM 시간대)을 쓰고, Hive 3.1 이후 버전이 Parquet에 쓴 `TIMESTAMP`는 Trino에서 Hive와 다른 값으로 읽힌다(HIVE-21002). 같은 파일을 여러 엔진이 읽는다면 INT96을 피하고 `isAdjustedToUTC`가 박힌 INT64로 쓰는 편이 해석이 하나로 모인다. Spark는 다른 도구가 쓴 파일에서 `isAdjustedToUTC=false`인 컬럼을 `TIMESTAMP_NTZ`로 추론한다(`spark.sql.parquet.inferTimestampNTZ.enabled`, 기본 true).

Iceberg 테이블이라면 이 고민이 대부분 사라진다. 스펙이 Parquet 표현을 정해 두었기 때문이다. `day` 변환이 받는 타입과 v3 나노초 타입의 호환성은 [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에 있다. 여기서 더할 점은 두 가지다. `day(ts)`는 저장값인 epoch 마이크로초를 그대로 하루 단위로 자르므로, `timestamptz` 컬럼이면 UTC 날짜로 나뉜다. 그리고 Iceberg 1.12.0의 Spark 연동은 나노초 타입을 지원하지 않고, Trino 484는 v3 테이블에서 `timestamp(9)`로 읽는다(Trino의 v3 지원은 실험 단계다).

## MySQL DATETIME은 Debezium을 지나며 UTC인 척한다

MySQL 8.4의 `DATETIME`은 넣은 값을 그대로 둔다. 범위는 `1000-01-01`부터 `9999-12-31`까지다. `TIMESTAMP`는 세션 `time_zone`에서 UTC로 바꿔 저장하므로 순간이 남지만, 범위가 UTC `2038-01-19 03:14:07`에서 끝난다.

Debezium MySQL 커넥터는 둘을 다르게 내보낸다. [CDC 글](/posts/change-data-capture/)의 파이프라인이 이 이벤트를 받는다.

| MySQL 타입 | Debezium 3.7 출력(기본 `adaptive_time_microseconds`) | 해석 |
|---|---|---|
| `DATETIME(0~3)` | INT64 `io.debezium.time.Timestamp`, epoch 밀리초 | 벽시계 값을 UTC로 읽어 만든 수 |
| `DATETIME(4~6)` | INT64 `io.debezium.time.MicroTimestamp`, epoch 마이크로초 | 위와 같고 단위만 다르다 |
| `TIMESTAMP` | STRING `io.debezium.time.ZonedTimestamp`, 예: `2018-06-20T13:37:03Z` | 서버나 세션 시간대 기준으로 UTC로 바꾼 순간 |

Debezium 문서의 예에서 `DATETIME` `2018-06-20 06:37:03`은 `1529476623000`이 되는데, 이 수는 정확히 `2018-06-20T06:37:03Z`다. Kafka Connect를 돌리는 JVM의 시간대는 이 변환에 영향을 주지 않는다. 그러니 KST 벽시계를 담은 `DATETIME`을 epoch로 받으면 실제 순간보다 9시간 뒤를 가리킨다. 이 수를 그대로 `timestamptz`에 넣으면 글머리의 17:30이 나온다. 벽시계 그대로 `timestamp`(NTZ)에 넣거나, 변환 단계에서 원천의 시간대를 명시해 순간으로 바꿔야 의미가 유지된다. 정밀도 모드에 따라 단위가 바뀌는 문제와 BI 도구가 마이크로초 정수를 다루는 방법은 [Superset epoch_us 글](/posts/superset-epoch-us/)에 있다.

## 하루의 경계는 시간대가 정한다

KST 날짜 `2026-11-21`은 UTC로 `2026-11-20 15:00`부터 `2026-11-21 15:00`까지다. UTC 날짜로 파티션을 나눈 테이블에서 KST 하루를 읽으면 파티션 두 개를 걸쳐 읽어야 하고, KST 0시~9시의 이벤트는 UTC 전날 파티션에 있다. 날짜 하나만 조건으로 주면 9시간이 빠지고, 다른 날의 9시간이 섞인다.

```sql
-- Trino 484. UTC 날짜 파티션(dt)과 epoch 마이크로초 컬럼(created_us)이 있는 테이블에서 KST 하루 읽기
SELECT count(*)
FROM events
WHERE dt IN ('2026-11-20', '2026-11-21')
  AND created_us >= 1795186800000000   -- 2026-11-21 00:00 KST
  AND created_us <  1795273200000000;  -- 2026-11-22 00:00 KST
```

Hive 방식 테이블에서는 엔진이 파티션 키와 시각 컬럼의 관계를 모르기 때문에, Athena에서도 두 조건을 같이 써야 한다. 파티션 값은 디렉터리 이름 문자열일 뿐이고 어느 시간대의 날짜인지는 어디에도 기록되지 않는다. 파티션과 메타스토어의 관계는 [Hive 파티션 글](/posts/hive-partitions-and-metastore/)에 있다. [Spark Streaming 수집 글](/posts/spark-streaming-to-hive/)의 잡은 받은 시각 대신 이벤트 시각으로 날짜 폴더를 나눴고, 이런 구조에서는 그 날짜를 어느 시간대로 자를지가 테이블 정의의 일부가 된다. 스케줄러 쪽도 같다. [Airflow 멱등 DAG 글](/posts/airflow-idempotent-dags/)처럼 run의 data interval로 날짜를 받을 때 timetable 시간대가 UTC면 그 구간은 UTC 하루다.

`Asia/Seoul`은 tz database 기준으로 1988년을 마지막으로 DST를 쓰지 않아서 하루는 늘 24시간이고 오프셋은 +09:00으로 고정이다. 다른 지역은 다르다. `America/Los_Angeles`는 2026-03-08이 23시간, 2026-11-01이 25시간이고, 11월 1일 01:30은 -07:00과 -08:00으로 두 번 온다. "UTC에 고정 오프셋을 더해 날짜를 만든다"는 코드는 한국에서는 맞지만 이런 지역에서는 하루에 1시간이 틀린다. Spark와 Trino 모두 세션 시간대에 `+09:00` 같은 오프셋과 `area/city` 형태의 지역 ID를 받는데, 여러 지역을 다루면 지역 ID를 써야 DST가 반영된다.

## epoch는 단위를 이름에 적어 둔다

| 단위 | 2026-11-21 08:30 KST | 자릿수 | Spark 4.3 | Trino 484 |
|---|---|---|---|---|
| 초 | `1795217400` | 10 | `timestamp_seconds` | `from_unixtime` |
| 밀리초 | `1795217400000` | 13 | `timestamp_millis` | `from_unixtime(x / 1e3)` |
| 마이크로초 | `1795217400000000` | 16 | `timestamp_micros` | `from_unixtime_nanos(x * 1000)` |
| 나노초 | `1795217400000000000` | 19 | 없음 | `from_unixtime_nanos` |

epoch 정수는 그 자체로 UTC 기준 순간이라 시간대 문제는 없다. 대신 단위가 값에 남지 않는다. 밀리초를 초로 읽으면 5만 년도 더 뒤의 날짜가 되고, 반대로 초를 밀리초로 읽으면 1970년 1월에 머문다. Trino의 `from_unixtime`은 초를 받아 `timestamp(3) with time zone`을 돌려주므로 마이크로초 정밀도는 거기서 잘린다. 컬럼 이름에 `_ms`, `_us`처럼 단위를 붙여 두는 습관이 타입 시스템이 대신해 주지 못하는 부분을 채운다. 벽시계로 쓴 `DATETIME`이 Debezium을 지나 epoch가 되면 이 표 어느 칸에 들어가도 9시간이 어긋나 있다는 점은 단위를 붙여도 해결되지 않는다.

## 참고 자료

- Spark 4.3.0: [Data Types](https://spark.apache.org/docs/4.3.0/sql-ref-datatypes.html), [Literals](https://spark.apache.org/docs/4.3.0/sql-ref-literals.html), 소스 [SQLConf.scala](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala), [ParquetSchemaConverter.scala](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetSchemaConverter.scala), [Cast.scala](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala)
- Trino 484: [Data types](https://trino.io/docs/484/language/types.html), [SET TIME ZONE](https://trino.io/docs/484/sql/set-time-zone.html), [Date and time functions](https://trino.io/docs/484/functions/datetime.html), [CLI](https://trino.io/docs/484/client/cli.html), [Hive connector](https://trino.io/docs/484/connector/hive.html), [Iceberg connector](https://trino.io/docs/484/connector/iceberg.html), 소스 [TimestampWithTimeZoneToDateCast.java](https://github.com/trinodb/trino/blob/484/core/trino-main/src/main/java/io/trino/operator/scalar/timestamptz/TimestampWithTimeZoneToDateCast.java)
- Parquet format: [LogicalTypes.md](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md), [parquet.thrift](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
- Iceberg 1.12.0: [테이블 스펙](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md), [Spark 타입 매핑](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/docs/docs/spark-getting-started.md)
- Hive 4.2.1 소스: [HiveConf.java](https://github.com/apache/hive/blob/rel/release-4.2.1/common/src/java/org/apache/hadoop/hive/conf/HiveConf.java)
- Athena: [Data types](https://docs.aws.amazon.com/athena/latest/ug/data-types.html), [Work with timestamp data](https://docs.aws.amazon.com/athena/latest/ug/data-types-timestamps.html), [Athena engine version 3](https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html), [Supported time zones](https://docs.aws.amazon.com/athena/latest/ug/athena-supported-time-zones.html)
- [MySQL 8.4, The DATE, DATETIME, and TIMESTAMP Types](https://dev.mysql.com/doc/refman/8.4/en/datetime.html)
- [Debezium 3.7, MySQL connector, Temporal types](https://debezium.io/documentation/reference/3.7/connectors/mysql.html)
- tz database: [asia](https://github.com/eggert/tz/blob/main/asia) (Rule ROK, Zone Asia/Seoul)

## English summary

A timestamp is either an instant or a local date-time, and each layer names the pair differently: MySQL TIMESTAMP and DATETIME, Parquet isAdjustedToUTC true and false, Iceberg timestamptz and timestamp, Spark TIMESTAMP_LTZ and TIMESTAMP_NTZ, Trino timestamp with and without time zone. Spark applies its session time zone when it casts an instant to a date, while Trino casts a timestamptz to a date in the value's own zone, which is UTC for Iceberg columns. Debezium turns a MySQL DATETIME into an epoch as if it were UTC, so a KST wall-clock value ends up nine hours ahead when it lands in an instant column. In my log pipeline for music and audio services, the streaming job split files into date folders by event time, and that design works well as long as the table definition also says which time zone the date belongs to.
