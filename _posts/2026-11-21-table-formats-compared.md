---
title: "Iceberg, Delta, Hudi는 커밋을 어디에 적는가"
date: 2026-11-21 05:01:00 +0900
categories: [Lakehouse & Storage, Iceberg]
tags: [iceberg, delta-lake, hudi, hive, lakehouse, catalog, concept]
---

CDC upsert가 계속 들어오는 테이블을 Iceberg, Delta Lake, Hudi 중 무엇으로 만들어도 ACID 커밋과 시간 여행은 된다. 세 포맷이 갈리는 지점은 "지금 이 테이블을 이루는 파일은 무엇인가"를 어디에 적는지, 그리고 두 커밋이 부딪힐 때 누가 판정하는지다. 행 단위 변경 비용, 파티션을 바꾸는 방법, 붙일 수 있는 엔진이 모두 이 선택을 따라간다.

Hive 스타일 테이블에는 그 기록이 없다. 디렉터리와 메타스토어의 파티션 항목이 곧 상태이고, 둘이 어긋나면 에러 없이 빈 결과가 나온다는 이야기는 [Hive 파티션 글](/posts/hive-partitions-and-metastore/)에 적었다.

버전은 Iceberg 1.12, Delta Lake 4.4, Hudi 1.2 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 매니페스트 트리 | Iceberg. metadata.json, 매니페스트 리스트, 매니페스트, 데이터 파일로 내려가는 메타데이터 |
| 트랜잭션 로그 | Delta. `_delta_log/`에 버전 번호로 쌓이는 JSON 커밋 파일과 checkpoint |
| 타임라인 | Hudi. `.hoodie/timeline`에 남는 instant(요청, 진행, 완료)의 기록 |
| 낙관적 동시성 제어 | 일단 쓰고, 커밋할 때 그사이 다른 커밋과 충돌했는지 검사 |
| copy-on-write | 바뀐 행이 든 파일을 통째로 다시 쓴다 |
| merge-on-read | 변경만 따로 적고 읽을 때 합친다 |
| deletion vector | 데이터 파일 하나에서 지운 행 위치를 담은 비트맵. Iceberg v3와 Delta |
| file group, file slice | Hudi에서 같은 키 묶음을 담는 파일의 버전 집합과 그 한 버전 |
| hidden partitioning | Iceberg가 원본 컬럼에 변환을 걸어 파티션 값을 계산 |
| liquid clustering | Delta에서 파티션 대신 쓰고 나중에 키를 바꿀 수 있는 클러스터링 |
| column mapping, 필드 ID | 컬럼을 이름 대신 고정 ID나 물리 이름으로 찾는 장치 |
| UniForm | Delta 커밋 뒤 Iceberg, Hudi 메타데이터를 만들어 읽기 전용으로 여는 기능 |

## 상태를 적는 방식이 셋으로 갈린다

| | Hive 스타일 | Iceberg | Delta Lake | Hudi |
|---|---|---|---|---|
| 상태가 있는 곳 | 디렉터리와 메타스토어 파티션 | 메타데이터 파일 트리, 카탈로그에는 현재 metadata.json 위치 | `_delta_log/`의 JSON 커밋과 Parquet checkpoint | `.hoodie/timeline`의 instant, 오래된 것은 LSM history |
| 버전 단위 | 없음 | 스냅샷 | 연속 정수 버전 | instant (요청 시각이 트랜잭션 ID) |
| 파일 목록 | 디렉터리 나열 | 매니페스트에서 읽는다 | 최신 checkpoint부터 로그 재생 | 타임라인과 메타데이터 테이블 `files` 파티션 |
| 원자성 | 테이블 단위 커밋 없음 | 카탈로그 포인터 교체 | 다음 버전 파일을 한 writer만 만든다 | instant 완료 기록, 다중 writer는 락 |

Iceberg의 트리와 필터링 순서는 [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에 있다. 이 트리는 루트 위치를 카탈로그가 쥐고 있어서, 카탈로그 없이는 현재 버전을 모른다.

Delta는 커밋 하나가 `_delta_log/00000000000000000005.json` 같은 파일 하나다. 그 안의 `add`, `remove` 액션을 이전 버전에 더하면 다음 스냅샷이 된다. checkpoint는 그 버전까지의 액션을 모두 재생해 둔 Parquet이라, 리더는 처음부터 로그를 읽지 않는다. 테이블 경로만 알면 메타스토어 없이 읽을 수 있게 설계됐다.

Hudi는 액션 종류(`commit`, `deltacommit`, `replacecommit`, `compaction`, `clean`, `rollback` 등)와 상태를 파일 이름에 담아 타임라인에 쌓는다. 데이터는 file group 단위로 버전이 쌓이고, 쿼리는 각 file group에서 완료된 마지막 file slice만 읽는다. 실패한 쓰기는 완료 기록이 없으니 보이지 않고, `rollback` 액션이 남은 파일을 치운다.

## 커밋 경합을 판정하는 주체가 다르다

Iceberg는 카탈로그가 포인터를 비교 후 교체하고, 진 writer는 새 상태 위에서 검증한 뒤 다시 시도한다. 카탈로그별 장치와 재시도 규칙은 [Iceberg 카탈로그 글](/posts/iceberg-catalogs-and-commits/)에 정리했다.

Delta는 저장소가 "같은 버전 파일은 한 번만 만들어진다"를 보장해야 한다. S3 기본 설정에서는 그 보장이 없어서 쓰기를 Spark 드라이버 하나로 제한한다. 여러 클러스터에서 쓰려면 실험 기능인 `S3DynamoDBLogStore`로 DynamoDB에 상호 배제를 맡기고, 모든 writer가 같은 설정을 써야 한다. 섞으면 데이터를 잃을 수 있다. 프로토콜에 들어온 `catalogManaged` 기능은 커밋 성공 여부의 판정을 카탈로그로 옮긴다. Iceberg REST 카탈로그와 같은 방향이다.

충돌 검사는 Delta도 파일 단위다. `INSERT`끼리는 충돌하지 않고, `dataChange=false`로 쓰는 compaction도 `INSERT`와는 부딪히지 않는다. `UPDATE`, `DELETE`, `MERGE`는 읽은 파일이나 파티션에 동시 변경이 있으면 `ConcurrentAppendException`, `ConcurrentDeleteReadException` 같은 예외로 실패한다.

Hudi의 기본은 `SINGLE_WRITER`다. writer 하나와 compaction, cleaning 같은 테이블 서비스가 MVCC로 서로를 막지 않는다. writer가 여럿이면 `hoodie.write.concurrency.mode=optimistic_concurrency_control`과 락 제공자(ZooKeeper, Hive Metastore, DynamoDB 등)를 설정하고, 충돌은 파일 단위로 본다. `NON_BLOCKING_CONCURRENCY_CONTROL`은 writer들이 같은 file group에 쓰게 두고 읽기와 compaction 때 정리하는데, 지금은 simple bucket 또는 partition-level bucket index를 쓰는 MOR 테이블에서만 된다.

## 행 단위 변경은 셋 다 COW와 MOR 사이에서 고른다

| | copy-on-write | merge-on-read 기록 | 정리 작업 |
|---|---|---|---|
| Iceberg | 명령별 기본값 | position delete, equality delete, v3 deletion vector | `rewrite_data_files` |
| Delta | deletion vector를 끄면 | deletion vector (DELETE 2.4, UPDATE 3.0, MERGE 3.1부터) | `OPTIMIZE`, `REORG TABLE ... APPLY (PURGE)` |
| Hudi | `COPY_ON_WRITE` 테이블 | `MERGE_ON_READ` 테이블의 로그 파일 | compaction, log compaction, clustering |

Iceberg 쪽 비용 구조는 [행 단위 삭제 글](/posts/iceberg-row-level-deletes/)에 있다. 세 포맷의 차이는 쓰는 순간 이전 행의 위치를 알아야 하느냐에서 난다.

Delta의 deletion vector와 Iceberg의 position delete는 행 위치를 적으니, writer가 지울 행을 먼저 찾아야 한다. Iceberg의 equality delete는 키 값만 적어서 찾지 않고 흘려보낼 수 있고, Flink upsert sink가 이 방식을 쓴다([Flink Iceberg sink 글](/posts/flink-iceberg-sink/)). 대신 읽는 쪽이 키를 대조한다.

Hudi는 인덱스로 레코드 키가 든 file group을 먼저 찾고 그 file group에 쓴다. Spark의 기본 인덱스는 `SIMPLE`이고, bloom, bucket, 메타데이터 테이블의 record-level index 중에서 고른다. 쓰기 때 키 조회 비용을 내는 대신 읽기는 file slice 하나의 base 파일과 로그만 합치면 된다. MOR 테이블은 read optimized 쿼리로 compaction된 base 파일만 읽어 빠르게 볼 수도 있고, 이때는 최근 delta commit이 빠진다.

## 파티션과 스키마를 바꾸는 비용

| | 파티션 변경 | 함수 조건으로 거르기 | 이름 변경, 삭제 |
|---|---|---|---|
| Iceberg | 스펙 추가, 기존 파일은 옛 스펙 그대로 | hidden partitioning 변환 | 필드 ID라 기본 지원 |
| Delta | `overwriteSchema`로 다시 쓰기, liquid clustering은 `ALTER TABLE ... CLUSTER BY` | generated column에서 파티션 필터 도출 | column mapping `name`이나 `id` 모드 |
| Hudi | 키 생성기 설정(`partitionpath.field`)이 경로를 정한다 | 1.0부터 expression index | schema-on-read(실험 기능) |

Iceberg 쪽 규칙은 [스키마와 파티션 진화 글](/posts/iceberg-schema-and-partition-evolution/)에 있다.

Delta에서 파티션 컬럼을 바꾸려면 테이블을 다시 쓴다. liquid clustering은 키를 바꿔도 기존 데이터를 다시 쓰지 않고 이후 `OPTIMIZE`와 쓰기부터 새 키를 쓴다. 다만 파티션과 함께 쓸 수 없다. column mapping을 켜면 컬럼마다 물리 이름과 ID가 생기고 Parquet `field_id`에도 기록되어, Iceberg처럼 이름 변경과 삭제가 파일을 건드리지 않는다. 켜는 순간 프로토콜이 올라가 이 기능을 모르는 리더는 테이블을 못 읽는다.

Hudi는 기본으로 끝에 nullable 컬럼 추가와 `int`에서 `long` 같은 승격만 받는다. 이름 변경, 삭제, 순서 변경은 `hoodie.schema.on.read.enable=true`가 필요하고, 한 번 켜면 끌 수 없다.

이름을 ID로 바꾸는 경계는 어느 포맷에나 남는다. [Kafka Connect 대소문자 글](/posts/iceberg-kafka-connect-mixed-case/)에서 고친 NPE가 설정의 컬럼 이름을 필드 ID로 바꾸는 자리에서 났다.

## 시간 여행 범위는 정리 작업이 정한다

| | 기록 보존 | 파일 삭제 | 기본값 |
|---|---|---|---|
| Iceberg | `expire_snapshots` | 만료된 스냅샷만 쓰던 파일 | 스냅샷 5일 |
| Delta | `delta.logRetentionDuration` | `VACUUM`, `delta.deletedFileRetentionDuration` | 로그 30일, 파일 7일 |
| Hudi | 오래된 instant는 LSM history로 | cleaner `KEEP_LATEST_COMMITS` | 커밋 10개, 커밋마다 자동 |

Delta는 로그가 30일 남아도 `VACUUM`이 7일 지난 파일을 지우면 그 이전 버전은 읽을 수 없다. Hudi에서는 cleaner가 남긴 커밋 수가 곧 증분 쿼리로 거슬러 갈 수 있는 범위다. 오래 보관할 시점은 Iceberg 태그, Hudi savepoint로 고정한다. Iceberg 브랜치와 태그는 [스냅샷과 브랜치 글](/posts/iceberg-snapshots-and-branches/)에 있다.

## 엔진과 카탈로그가 선택지를 줄인다

| | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| Athena | 읽기, 쓰기, 시간 여행 (v2 테이블, Glue 카탈로그) | 읽기만, 시간 여행 없음 | 읽기만 (snapshot, read optimized), 커넥터 0.14, 0.15라 1.x 테이블은 호환 보장 없음 |
| 카탈로그 역할 | 현재 metadata.json 위치를 쥔다 (REST, Glue) | 등록만, `catalogManaged`면 커밋 판정 (Unity Catalog) | meta sync로 등록만 (Hive Metastore, Glue) |

Unity Catalog OSS는 Delta용 REST API와 함께 Iceberg REST API도 연다. Delta UniForm을 켜면 Delta 커밋 뒤 Iceberg 메타데이터가 비동기로 생기지만 Iceberg 쪽에서는 읽기만 되고, deletion vector를 켠 테이블에서는 동작하지 않는다.

고를 때는 누가 쓰고 누가 읽느냐가 먼저다.

- Spark 중심이고 Unity Catalog를 쓴다면 Delta다. Unity Catalog OSS는 Delta 전용 REST API를 따로 둔다.
- Flink 스트리밍 upsert와 쓰기 중 키 조회 비용을 감수할 수 있으면 Hudi MOR의 인덱스와 테이블 서비스가 맞는다.
- Athena에서도 쓰고 Flink, Spark도 같은 테이블에 써야 하면 Iceberg다. Athena에서 쓰기까지 되는 포맷은 Iceberg뿐이다.

[Spark Streaming 글](/posts/spark-streaming-to-hive/)의 일 배치와 [광고 리포트 배치](/posts/ad-report-batch/)의 S3 복사 잡은 날짜 파티션 단위로 쓰고 다시 돌리는 Hive 스타일이었다. 임시 경로 교체나 `MSCK REPAIR TABLE` 같은 단계를 잡이 직접 맡는 대신 어떤 엔진에서도 읽힌다. 어느 테이블 포맷으로 옮겨도 그 단계는 커밋 한 번으로 흡수되고, 남는 차이는 위의 표들이다.

```sql
-- Spark SQL, 포맷마다 런타임과 spark_catalog를 따로 설정한 세션 (Iceberg 1.12, Delta 4.4, Hudi 1.2)
CREATE TABLE ice.db.orders (id bigint, status string, ts timestamp) USING iceberg
PARTITIONED BY (days(ts))
TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read');

CREATE TABLE orders_delta (id bigint, status string, ts timestamp) USING delta
CLUSTER BY (ts)
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.columnMapping.mode' = 'name');

CREATE TABLE orders_hudi (id bigint, status string, updated_at bigint, dt string) USING hudi
PARTITIONED BY (dt)
TBLPROPERTIES (type = 'mor', primaryKey = 'id', orderingFields = 'updated_at');
```

세 테이블에 같은 `MERGE INTO`를 돌리면 바뀐 행이 각각 position delete, deletion vector, 로그 파일로 남는다. Hudi 테이블에는 `primaryKey`를 두었고, Hudi는 그 키로 file group을 찾는다.

## 참고 자료

- Iceberg: [Table Spec](https://iceberg.apache.org/spec/), [Configuration](https://iceberg.apache.org/docs/latest/configuration/), [Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
- Delta Lake: [PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md), [Storage configuration](https://docs.delta.io/latest/delta-storage.html), [Concurrency control](https://docs.delta.io/latest/concurrency-control.html), [Deletion vectors](https://docs.delta.io/latest/delta-deletion-vectors.html), [Liquid clustering](https://docs.delta.io/latest/delta-clustering.html), [Batch reads and writes](https://docs.delta.io/latest/delta-batch.html), [Table properties](https://docs.delta.io/latest/table-properties.html), [UniForm](https://docs.delta.io/latest/delta-uniform.html), [Releases](https://github.com/delta-io/delta/releases)
- Hudi 1.2.1: [Timeline](https://hudi.apache.org/docs/timeline), [Table types](https://hudi.apache.org/docs/table_types), [Concurrency control](https://hudi.apache.org/docs/concurrency_control), [Indexes](https://hudi.apache.org/docs/indexes), [Metadata table](https://hudi.apache.org/docs/metadata), [Key generation](https://hudi.apache.org/docs/key_generation), [Schema evolution](https://hudi.apache.org/docs/schema_evolution), [Cleaning](https://hudi.apache.org/docs/cleaning), [SQL DDL](https://hudi.apache.org/docs/sql_ddl)
- Athena: [Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html), [Delta Lake](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html), [Hudi limitations](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi-in-athena-considerations-and-limitations.html)
- [Unity Catalog OSS](https://unitycatalog.io/docs/)

## English summary

Iceberg, Delta Lake and Hudi all add ACID commits on top of files, but they record table state differently: Iceberg keeps a manifest tree whose root pointer lives in a catalog, Delta appends numbered JSON commits to a log, and Hudi records actions on a timeline over file groups. Delta needs storage that writes each version file only once, so plain S3 limits it to one writing driver, while Hudi defaults to a single writer and needs a lock provider for more. All three choose between copy-on-write and merge-on-read, and the real split is whether the writer must find old rows first: Hudi looks keys up through an index, while Iceberg equality deletes skip the lookup and push the cost to readers. My Hive-style daily batches traded a separate partition registration step for being readable by any engine.
