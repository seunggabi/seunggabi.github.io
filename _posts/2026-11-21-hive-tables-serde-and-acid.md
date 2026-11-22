---
title: "같은 Hive 테이블을 엔진마다 다르게 읽는 지점"
date: 2026-11-21 05:15:00 +0900
categories: [Lakehouse & Storage, Hive]
tags: [hive, hive-metastore, serde, orc, bucketing, acid, spark, trino, concept]
description: "Hive 메타스토어의 테이블 종류, SerDe, 버킷, transactional 속성을 엔진마다 다르게 해석해 생기는 DROP, 버킷, ACID 문제를 설명한다."
---

외부 테이블인 줄 알고 `DROP TABLE`을 돌렸는데 데이터 디렉터리까지 사라진다. Spark로 버킷 테이블을 만들었는데 Hive와 Trino는 그 버킷을 쓰지 못한다. Hive에서 `UPDATE`한 테이블을 Spark로 읽을 방법이 마땅치 않다. 세 문제는 같은 곳에서 나온다. 메타스토어의 테이블 항목에는 컬럼과 파티션 말고도 테이블 종류, SerDe와 InputFormat, 버킷 정의, `transactional` 속성이 들어 있고, 이 값을 해석하는 일은 읽고 쓰는 엔진마다 따로 한다.

파티션 등록과 메타스토어가 어긋날 때의 이야기는 [Hive 파티션 글](/posts/hive-partitions-and-metastore/)에 적었다. 여기서는 파티션 바깥의 테이블 정의를 다룬다.

버전은 Hive 4.2.1, Spark 4.3.0, Trino 484 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| managed 테이블 | Hive가 데이터 수명까지 맡는 테이블, DROP하면 데이터도 지운다 |
| external 테이블 | 메타데이터만 Hive가 맡는다, DROP해도 파일은 남는다 |
| InputFormat | 파일을 split으로 나누고 레코드 단위로 꺼내는 Hadoop 클래스 |
| SerDe | 레코드를 행 객체로 바꾸고(deserialize) 다시 바이트로 바꾸는(serialize) 클래스 |
| `STORED AS` | InputFormat, OutputFormat, SerDe 세 클래스를 한 번에 정하는 약칭 |
| stripe | ORC 파일 안의 큰 묶음, Hive 기본 64MB |
| 버킷 | `hash(컬럼) mod 버킷 수`로 정해지는 파일 단위 묶음 |
| `bucketing_version` | 버킷 해시 방식, 1은 옛 Java식 해시, 2는 murmur 해시 |
| ACID 테이블 | `transactional=true`, base와 delta 디렉터리로 버전을 쌓는다 |
| compaction | delta를 합치는 작업, minor는 delta끼리, major는 base까지 다시 쓴다 |
| LLAP | 캐시와 짧은 쿼리 처리를 맡는 상주 데몬, 실행은 Tez가 지휘한다 |

## DROP이 지우는 범위는 테이블 종류가 정한다

managed 테이블은 `DROP TABLE` 때 메타데이터와 데이터를 함께 지운다. HDFS 휴지통이 켜져 있으면 데이터는 `.Trash`로 옮겨지고, `PURGE`를 붙이면 휴지통을 거치지 않아 되살릴 수 없다. external 테이블은 메타데이터만 지운다. Hive 4.0부터는 external 테이블에 `'external.table.purge'='true'`를 주면 DROP이 데이터까지 지운다. `hive.external.table.purge.default`를 켜면 새로 만드는 external 테이블에 이 속성이 기본으로 붙으니, 공용 설정을 바꿨다면 external이라는 이유만으로 안전하다고 볼 수 없다.

managed 테이블에서만 되는 기능도 있다. ACID, `MERGE`, `CONCATENATE`, 쿼리 결과 캐시가 그렇다. `TRUNCATE`는 external이라도 `external.table.purge=true`면 된다. external 테이블이 ACID가 될 수 없는 이유는 compaction에 있다. 파일이 Hive 밖에서 바뀌면 compactor가 그 변화를 관리할 수 없다. Apache Hive 4.2.1의 기본값은 `hive.create.as.acid=false`, `hive.create.as.insert.only=false`, `hive.strict.managed.tables=false`다. 설정을 바꾸지 않은 클러스터에서는 그냥 `CREATE TABLE`이 ACID가 아닌 managed 테이블을 만든다.

## 행 하나는 InputFormat과 SerDe를 거쳐 나온다

Hive가 행을 읽는 순서는 이렇다.

```text
HDFS/S3 파일 → InputFormat(split, RecordReader) → 레코드(value)
→ SerDe.deserialize() → 행 객체 + ObjectInspector → 연산자
쓰기는 반대로: 행 객체 + ObjectInspector → SerDe.serialize() → OutputFormat
```

`STORED AS`는 세 클래스를 한 번에 정하는 약칭이다. 기본인 TEXTFILE을 풀어 쓰면 이렇다.

```sql
-- Hive 4.2: STORED AS TEXTFILE과 같은 정의
CREATE EXTERNAL TABLE raw_logs (event_time string, user_id bigint, body string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/raw_logs';
```

`STORED AS ORC`는 `OrcSerde`와 `OrcInputFormat`, `STORED AS PARQUET`는 `ParquetHiveSerDe`와 `MapredParquetInputFormat`을 고른다. SerDe는 읽을 때 일한다. 텍스트 테이블은 쓸 때 형식을 검사하지 않으니, 구분자가 다르거나 `user_id` 자리에 숫자가 아닌 값이 오면 에러 대신 NULL이 나온다. `LazyLong` 같은 필드 객체가 `NumberFormatException`을 잡아 NULL로 둔다. `LazySimpleSerDe`는 필드를 실제로 접근할 때 파싱한다.

ORC와 Parquet는 SerDe가 컬럼 단위 리더와 묶여 있어서, 필요한 컬럼만 읽고 stripe나 row group 통계로 건너뛴다. Hive의 ORC 기본값은 stripe 64MB, row index 간격 10,000행이고, 컬럼 통계로 건너뛰는 원리는 [컬럼 파일과 프루닝 글](/posts/columnar-files-and-pruning/)에 적었다. [Spark Streaming 글](/posts/spark-streaming-to-hive/)의 적재 흐름에도 텍스트 테이블을 ORC로 바꾸는 잡이 따로 있었다. 텍스트 SerDe에는 이런 통계가 없어서, ORC로 바꾼 뒤에야 위의 건너뛰기를 쓸 수 있다.

## 버킷은 해시 함수까지 맞아야 쓸모가 있다

```sql
CREATE TABLE plays (user_id bigint, track_id bigint, played_at timestamp)
CLUSTERED BY (user_id) SORTED BY (user_id) INTO 64 BUCKETS
STORED AS ORC;
```

행이 들어갈 버킷은 `hash(user_id) mod 64`다. Hive 2.0부터는 `hive.enforce.bucketing` 없이도 INSERT가 이 규칙대로 파일을 나눈다. 해시 함수는 테이블 속성 `bucketing_version`이 정한다. Hive 3.0에서 murmur 해시로 옮겼고(HIVE-18910), 4.2.1도 새 테이블에 `bucketing_version=2`를 붙인다. 버전 1 테이블은 옛 해시를 그대로 쓴다.

두 테이블이 조인 키로 버킷팅되어 있고 한쪽 버킷 수가 다른 쪽의 배수면, bucket map join은 작은 테이블 전체 대신 대응하는 버킷만 각 태스크로 보낸다. 양쪽이 같은 키로 정렬까지 되어 있고 버킷 수도 같으면 sort-merge bucket join이 버킷끼리 병합만 한다. MapReduce 계획에서는 `hive.optimize.bucketmapjoin`(기본 false)을 켜야 하고, Tez에서는 `hive.convert.join.bucket.mapjoin.tez`(기본 true)가 자동 변환을 맡는다.

Spark의 버킷은 같은 문법을 써도 다른 물건이다. Spark 데이터 소스 테이블(`USING parquet ... CLUSTERED BY`)은 버킷 번호를 `pmod(murmur3(키), 버킷 수)`로 매긴다. 셔플의 `HashPartitioning`과 같은 식이어서 Spark끼리는 셔플을 줄이지만, Hive의 해시와 다르고 파일 이름 규칙도 달라서 Hive와 Trino는 이 테이블을 버킷 테이블로 쓰지 못한다. Spark 안에서 버킷 조인이 계획에 어떻게 반영되는지는 [Spark 조인 전략 글](/posts/spark-join-strategies/)에 있다. 거꾸로 Spark가 Hive SerDe 버킷 테이블에 쓰는 경우, Spark 3.3부터 `HiveHash`와 Hive식 파일 이름(`00000_0_...`)을 쓴다(SPARK-32709, Hive 1·2 호환). Spark 4.3.0 소스의 이 경로는 `bucketing_version`을 보지 않고 Hive 3용 murmur 쓰기(SPARK-32711)도 아직 열려 있으므로, 코드로 보면 버전 2 테이블에 Spark가 쓴 파일은 Hive가 기대하는 버킷 번호와 어긋날 수 있다. Trino는 읽을 때 `hive.validate-bucketing`(기본 true)으로 행이 맞는 버킷에 있는지 검사한다.

## ACID 테이블은 디렉터리를 지우지 않고 쌓는다

```sql
-- 세션: hive.support.concurrency=true, hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager
CREATE TABLE orders (id bigint, status string)
STORED AS ORC TBLPROPERTIES ('transactional'='true');

UPDATE orders SET status = 'paid' WHERE id = 1;
ALTER TABLE orders COMPACT 'major';
```

full ACID 테이블은 ORC여야 하고, Hive 3.0부터는 버킷팅이 필요 없다(HIVE-17205). `'transactional_properties'='insert_only'`를 주면 INSERT만 되는 대신 ORC가 아닌 형식도 쓸 수 있다. 한 번 `transactional=true`로 만든 테이블은 false로 되돌릴 수 없다. 쓰기마다 테이블별 쓰기 ID를 받아 새 디렉터리를 만든다.

```text
orders/
  base_0000005_v0000012/bucket_00000     ← major compaction 결과(_v 뒤는 compactor 트랜잭션 ID)
  delta_0000006_0000006_0000/bucket_00000
  delete_delta_0000007_0000007_0000/bucket_00000
  delta_0000007_0000007_0000/bucket_00000  ← UPDATE는 delete + insert로 나뉜다
```

읽는 쪽은 자기 스냅샷에서 유효한 쓰기 ID 목록을 받아 base와 delta를 합치고, delete_delta에 있는 행을 뺀다. delta가 쌓일수록 읽기마다 합칠 파일이 늘어서 compaction이 필요하다. minor는 delta들을 delta 하나로, major는 base와 delta를 새 base 하나로 다시 쓴다. 메타스토어의 initiator는 delta 디렉터리가 10개(`hive.compactor.delta.num.threshold`)를 넘으면 minor를, delta 크기가 base의 10%(`delta.pct.threshold`)를 넘으면 major를 요청한다. 실제로 돌리는 worker 스레드 수는 기본 0이라, 어느 메타스토어에서도 늘리지 않으면 요청은 큐에만 쌓인다. 옛 디렉터리는 그것을 읽던 쿼리가 끝난 뒤 cleaner가 지운다.

이 구조는 Hive 3 ACID 형식을 아는 엔진만 읽을 수 있다. Trino는 메타스토어 3.x에 붙으면 insert-only와 ACID 테이블을 읽고 쓰며, `UPDATE`는 ORC ACID 테이블에서만 된다. Spark에는 Hive ACID 지원이 없다. SPARK-15348은 구현 없이 닫혔다. 여러 엔진이 같은 테이블을 고쳐 써야 한다면 이 자리는 Iceberg 같은 테이블 포맷이 맡는다.

## 실행 엔진은 MapReduce에서 Tez로 옮겨 갔다

Hive 4.2.1의 `hive.execution.engine` 기본값은 `tez`이고, 선택지는 `tez`와 `mr`뿐이다. MapReduce는 Hive 2부터 deprecated이고, Hive on Spark는 4.0에서 제거됐다. 4.0은 Hive CLI도 deprecated로 돌렸다. Iceberg 테이블에 대한 INSERT, UPDATE, DELETE, MERGE와 브랜치·태그 지원, ACID의 잠금 없는 읽기도 4.0에 들어왔다. 3.x 라인은 2024년 10월에 EOL이 선언됐다.

LLAP는 별도의 실행 엔진이 아니다. 노드마다 상주하는 데몬이 컬럼 청크를 off-heap에 캐시하고 짧은 쿼리 조각을 직접 처리하며, 전체 계획은 Tez가 LLAP 데몬과 일반 YARN 컨테이너에 나눠 맡긴다. `hive.execution.mode=llap`으로 쓴다. 큰 셔플 같은 무거운 일은 여전히 컨테이너로 간다.

## Spark와 Trino는 메타스토어만 빌려 쓴다

Spark는 `enableHiveSupport()`로 메타스토어에 붙는다. 기본 클라이언트는 내장 Hive 2.3.10이고, Spark 4.3이 맞춰 쓸 수 있는 메타스토어 버전은 4.1.0까지다. ORC와 Parquet Hive 테이블은 `spark.sql.hive.convertMetastoreOrc`, `convertMetastoreParquet`(둘 다 기본 true) 때문에 Hive SerDe 대신 Spark 자체 리더로 읽는다. 텍스트처럼 다른 SerDe를 쓰는 테이블은 내장 Hive SerDe 클래스로 읽는다. Trino는 메타스토어의 메타데이터와 파일만 쓰고 HiveQL이나 Hive 실행 환경은 쓰지 않는다. 그래서 SerDe도 Trino가 직접 구현한 것만 된다. ORC, Parquet, Avro와 정해진 텍스트 계열(TextFile, CSV, JSON, SequenceFile 등) 밖의 사용자 정의 SerDe 테이블은 Hive에서는 읽혀도 Trino에서는 읽히지 않는다.

[연말 결산 글](/posts/year-end-recap-spark/)의 Spark 잡은 Hive 중간 테이블의 `type`, `dt` 파티션에 `INSERT OVERWRITE`로 쓰고 다음 단계가 그 파티션을 읽었다. ACID도 버킷도 쓰지 않는 파티션 단위 덮어쓰기라서, 위에서 본 버킷 해시와 ACID 디렉터리의 엔진 간 차이에는 걸리지 않는다. 대신 행 단위 수정은 파티션 하나를 통째로 다시 쓰는 비용으로 치른다.

## 참고 자료

- [Apache Hive: Managed vs. External Tables](https://hive.apache.org/docs/latest/language/managed-vs--external-tables/)
- [Apache Hive: LanguageManual DDL](https://hive.apache.org/docs/latest/language/languagemanual-ddl/)
- [Apache Hive: SerDe](https://hive.apache.org/docs/latest/user/serde/)
- [Apache Hive: Bucketed Tables](https://hive.apache.org/docs/latest/language/languagemanual-ddl-bucketedtables/)
- [Apache Hive: Joins (bucket map join, sort-merge join)](https://hive.apache.org/docs/latest/language/languagemanual-joins/)
- [Apache Hive: Hive Transactions](https://hive.apache.org/docs/latest/user/hive-transactions/)
- [Apache Hive: Hive 4.0 Overview of Major Changes](https://hive.apache.org/docs/latest/overview-of-major-changes/)
- [Apache Hive: LLAP design document](https://hive.apache.org/development/designdocs/llap)
- [Apache Hive 4.2.1 source: HiveConf.java, AcidUtils.java, Table.java, LazyLong.java](https://github.com/apache/hive/tree/rel/release-4.2.1)
- [HIVE-18910: Migrate to Murmur hash for shuffle and bucketing](https://issues.apache.org/jira/browse/HIVE-18910)
- [HIVE-17205: add functional support for unbucketed tables](https://issues.apache.org/jira/browse/HIVE-17205)
- [Spark: Hive Tables](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
- [Spark: ORC Files (convertMetastoreOrc)](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
- [SPARK-32709: Write Hive ORC/Parquet bucketed table with hivehash](https://issues.apache.org/jira/browse/SPARK-32709)
- [SPARK-15348: Hive ACID](https://issues.apache.org/jira/browse/SPARK-15348)
- [Trino: Hive connector](https://trino.io/docs/current/connector/hive.html)

## English summary

A Hive table's metastore entry holds more than columns and partitions: whether it is managed or external (which decides what DROP deletes), the SerDe and InputFormat that turn bytes into rows, the bucket spec and its hash version, and the transactional flag. Each engine interprets these on its own, so Spark's Murmur3 bucketing is not Hive bucketing, Hive 3+ ACID tables with base and delta directories are readable by Hive and Trino but not by Spark, and Trino only supports the SerDes it implements. Hive 4.2 runs on Tez by default, with LLAP as a caching layer rather than a separate engine. In my music service batches, Spark wrote Hive intermediate tables with partition-level INSERT OVERWRITE, which avoids these cross-engine gaps at the cost of rewriting a whole partition for any row change.
