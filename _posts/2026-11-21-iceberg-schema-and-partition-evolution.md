---
title: "Iceberg 테이블을 다시 쓰지 않고 스키마와 파티션 바꾸기"
date: 2026-11-21 03:38:00 +0900
categories: [Lakehouse & Storage, Iceberg]
tags: [iceberg, schema-evolution, partitioning, spark, hive, cdc, concept]
---

컬럼 타입을 정수에서 문자열로 바꾸려면 Iceberg 테이블도 다시 만들어야 한다. Iceberg 테이블에서 컬럼 이름을 바꾸거나 순서를 바꾸는 일은 파일 하나 건드리지 않고 끝난다. 일 단위 파티션을 시간 단위로 바꾸는 일도 메타데이터 커밋 한 번이다.

어떤 변경은 공짜고 어떤 변경은 테이블을 다시 써야 하는지는 스펙의 규칙 몇 개로 갈린다.

버전은 Iceberg 1.12.0, 테이블 스펙 v2·v3, Spark 4.0 기준이다.

## 이름과 순서는 메타데이터라서 바꿔도 파일은 그대로다

Iceberg는 데이터 파일에서 컬럼을 필드 ID로 고른다. 이름과 순서는 테이블 스키마에만 있고, 바꾸면 새 스키마 ID가 붙은 스키마가 스키마 목록에 추가될 뿐이다. ID로 읽는 예시는 [Iceberg 테이블 포맷 글](/posts/iceberg-table-format/)에 적었다.

Hive 스타일 테이블은 메타스토어의 컬럼을 파일의 컬럼에 이름이나 위치로 맞춘다. Athena 기준으로 Parquet은 이름, ORC는 위치가 기본이고, CSV·TSV는 위치로 읽는다.

| 변경 | 이름으로 읽는 Hive 테이블 | 위치로 읽는 Hive 테이블 | Iceberg |
|---|---|---|---|
| 이름 변경 | 옛 파일의 그 컬럼이 null로 읽힌다 | 된다 | 된다 |
| 중간에 추가, 순서 변경 | 된다 | 뒤 컬럼 값이 밀린다 | 된다 |
| 삭제 | 된다 | 뒤 컬럼 값이 밀린다 | 된다 |
| 지운 이름으로 다시 추가 | 옛 값이 되살아난다 | 위치에 따라 다르다 | 새 ID라 null로 시작한다 |

이름으로 맞추면 지웠던 컬럼이 되살아나고, 위치로 맞추면 컬럼 하나를 지울 때 다른 컬럼의 값이 바뀐다. 새 컬럼에 늘 새 ID를 주는 방식은 둘 다 막는다.

ID가 없는 파일을 Iceberg 테이블에 붙이는 경우도 있다. Hive 테이블을 파일 이동 없이 옮길 때가 그렇다. 이때는 테이블 속성 `schema.name-mapping.default`에 이름과 ID의 대응을 적어 두고, 리더가 ID 대신 이 매핑으로 컬럼을 찾는다.

경계는 두 군데 남는다. 하나는 쓰는 SQL이다. Spark의 `INSERT INTO t SELECT ...`는 위치로 맞추므로 컬럼 순서를 바꾼 뒤에는 `INSERT INTO t BY NAME`을 쓰거나 SELECT 순서를 같이 고쳐야 한다. 다른 하나는 Hive Metastore를 카탈로그로 쓰는 경우다. HMS는 스키마 변경을 위치 기준으로 검사해서(`hive.metastore.disallow.incompatible.col.type.changes`, 기본 true) `FIRST`/`AFTER`나 중간 컬럼 삭제가 HMS 쪽에서 거절될 수 있다.

## 타입은 넓히는 방향만 된다

| 원래 타입 | v1, v2 | v3 |
|---|---|---|
| `int` | `long` | `long` |
| `float` | `double` | `double` |
| `decimal(P, S)` | `decimal(P', S)`, P' > P | 같음 |
| `date` | 없음 | `timestamp`, `timestamp_ns` |
| `unknown` | 없음 | 아무 타입 |

나머지는 전부 안 된다. scale을 바꾸는 decimal, `date`에서 `timestamptz`, 숫자에서 문자열이 다 여기에 걸린다.

승격은 기존 파일을 다시 쓰지 않는다. 매니페스트의 min/max도 그대로라서 `long` 컬럼에 4바이트 bound가 있으면 쓸 때 `int`였다고 보고 읽는다. 파티션 원본 컬럼에는 조건이 하나 더 붙는다. 승격 뒤에 변환 결과가 달라지면 안 된다. `bucket`은 `34`와 `34L`에 같은 해시를 내니 `int`에서 `long`은 되지만, `date`를 원본으로 쓰는 파티션 필드가 있으면 `date`에서 timestamp로의 승격이 막힐 수 있다.

정수 컬럼을 boolean으로 바꾸려 해도 같다. 정수와 boolean 사이에는 승격 규칙이 없다. 허용 목록 밖의 변경은 새 컬럼을 추가해 값을 채우고 옛 컬럼을 지우거나, 테이블을 다시 만드는 길만 남는다. 앞의 방법은 옛 컬럼을 지운 뒤 새 컬럼을 옛 이름으로 바꾸면 쿼리를 그대로 둘 수 있지만, 값을 채우는 동안 쓰는 쪽이 두 컬럼에 같이 써야 한다.

## 필수 컬럼은 기본값이 있어야 추가된다

옵셔널 컬럼은 언제든 추가된다. 옛 파일에는 그 ID가 없으니 null로 읽힌다. 필수(required) 컬럼은 옛 행에 채울 값이 없어서, Java API의 `addRequiredColumn`은 기본값 없이 부르면 `Incompatible change: cannot add required column without a default value`로 실패한다.

v3는 필드마다 기본값 두 개를 둔다.

| 기본값 | 쓰이는 곳 | 바꿀 수 있나 |
|---|---|---|
| `initial-default` | 컬럼 추가 전에 쓴 행을 읽을 때 | 안 된다 |
| `write-default` | 추가 후 쓰는 쪽이 값을 안 줬을 때 | 된다 |

필수 컬럼을 추가하려면 두 값이 모두 null이 아니어야 하고, 파일을 다시 쓰지 않고 SQL `DEFAULT`처럼 동작한다. null이 아닌 기본값은 v3 테이블에서만 허용된다. Iceberg 1.12에서 새 테이블의 기본 포맷 버전은 2라서 `format-version`을 3으로 올려야 쓸 수 있다. [Avro 스키마 진화 글](/posts/schema-evolution/)의 Avro default는 읽을 때만 쓰이고, Iceberg는 쓰기용 기본값을 따로 둔다는 점이 다르다.

반대 방향도 있다. 필수 컬럼을 옵셔널로 바꾸는 건 되지만(`ALTER COLUMN id DROP NOT NULL`), 옵셔널을 필수로 바꾸는 `SET NOT NULL`은 Spark에서 막혀 있다. 기존 파일에 null이 있는지 Iceberg가 알 수 없기 때문이다.

## 파티션 스펙은 쌓이고 옛 파일은 옛 스펙에 남는다

[Hive 파티션 글](/posts/hive-partitions-and-metastore/)에서 본 Hive 파티션은 디렉터리 경로이고, 쿼리도 그 파티션 컬럼을 조건으로 쓴다. 일 단위를 시간 단위로 바꾸려면 새 테이블을 만들고 쿼리도 고쳐야 한다.

Iceberg에서 파티션 스펙을 바꾸면 새 spec ID를 가진 스펙이 테이블의 스펙 목록에 더해지고 기본 스펙이 된다. 이미 쓴 파일은 그대로 옛 스펙에 남는다. v3까지의 매니페스트는 스펙 하나의 파일만 담고, 매니페스트마다 어느 스펙으로 썼는지 기록된다. 쿼리 계획은 매니페스트별로 그 스펙의 변환을 써서 `ts` 조건을 파티션 조건으로 바꾼다. 이를 split planning이라고 한다. 2008년은 월 파티션, 2009년부터는 일 파티션인 테이블에서 `ts` 범위 조건 하나로 양쪽 다 건너뛸 수 있다.

파티션 필드에도 ID가 있다. v2부터는 테이블 메타데이터의 마지막 파티션 필드 ID에서 이어 붙이고, 같은 필드가 다시 나오면 옛 ID를 재사용한다. v1은 ID를 저장하지 않아서 필드를 지우는 대신 `void` 변환으로 바꾸고, 순서를 바꾸지 않고, 끝에만 추가하는 규칙을 권한다.

바꾸고 나서 달라지는 것이 셋 있다.

- dynamic partition overwrite는 새 스펙의 파티션 단위로 지운다. 일에서 시간으로 바꾸면 `INSERT OVERWRITE`가 하루가 아니라 한 시간을 덮어쓴다.
- `files` 같은 메타데이터 테이블의 `partition` 구조체에 모든 스펙의 필드가 들어가고, 옛 파일의 새 필드는 null이다. 일 필드를 지우지 않고 시간 필드만 더하면 기존 메타데이터 쿼리가 그대로 돈다.
- equality delete는 데이터 파일과 spec ID와 파티션 값이 같을 때만 붙는다. delete 파일이 파티션 없는 스펙으로 쓰였으면 예외다. [행 단위 삭제 글](/posts/iceberg-row-level-deletes/)의 그 규칙을 따라가면, upsert 테이블의 스펙을 바꾼 뒤 새 스펙으로 쓴 equality delete는 옛 스펙 파일의 이전 행에 닿지 않는다. 스펙 규칙에서 나온 추론이라, upsert 테이블은 스펙을 바꾸기 전에 실제 엔진으로 확인해야 한다.

## 변환은 조회 조건과 파일 수로 고른다

변환을 고르는 기준은 이렇다.

| 변환 | 맞는 경우 | 조심할 점 |
|---|---|---|
| `identity` | 값 종류가 적고 조건에 자주 오는 컬럼 (국가, 이벤트 종류) | 값 종류가 많으면 작은 파일이 폭증한다 |
| `day`, `hour` | 시간 범위 조회 | `hour`는 시간당 데이터가 파일 몇 개를 채울 만큼일 때만 |
| `bucket[N]` | id 같은 고카디널리티 키의 등치 조회, 쓰기 분산 | 범위 조건으로는 못 거른다. N을 바꾸면 새 스펙이다 |
| `truncate[W]` | 문자열 접두사, 정수 구간 | 정수는 `v - (v % W)`라 음수는 아래 구간으로 간다 |

## 다시 쓰는 건 필요한 범위만

`rewrite_data_files`는 결과를 기본적으로 현재 스펙(`output-spec-id`)으로 쓰고, `strategy => 'sort'`를 주면 테이블 정렬 순서로 정렬까지 한다. 다만 어느 파일을 고를지는 크기와 개수 기준으로 정해서, 크기가 적당한 옛 스펙 파일은 그대로 남을 수 있다. 옛 파일을 새 레이아웃으로 옮기려면 `where`로 범위를 좁히고 `rewrite-all`을 켠다. 아래 예제 테이블이라면 이렇다.

```sql
CALL local.system.rewrite_data_files(
  table => 'db.events',
  where => 'ts < TIMESTAMP ''2027-01-01 00:00:00''',
  options => map('rewrite-all', 'true')
);
```

옛 스펙 구간이 거의 조회되지 않는다면 그대로 두는 편이 싸다. 다시 쓸 이유는 그 구간을 자주 읽어서 파일 수나 정렬이 문제가 될 때, upsert 테이블에서 delete 적용 범위를 맞춰야 할 때 정도다.

## 로컬에서 해 보기

Spark 4.0과 Iceberg 1.12.0, 로컬 디렉터리를 쓰는 hadoop 카탈로그로 돌린다.

```bash
spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-wh
```

```sql
CREATE TABLE local.db.events (id bigint, user_id int, amount float, ts timestamp)
USING iceberg PARTITIONED BY (months(ts));
INSERT INTO local.db.events VALUES (1, 10, 1.5, TIMESTAMP '2026-12-15 12:00:00');

-- 스키마: 모두 메타데이터 커밋
ALTER TABLE local.db.events RENAME COLUMN amount TO price;
ALTER TABLE local.db.events ALTER COLUMN user_id TYPE bigint;
ALTER TABLE local.db.events ALTER COLUMN price TYPE double;
ALTER TABLE local.db.events ADD COLUMN country string AFTER user_id;

-- 파티션: 월에서 일로
ALTER TABLE local.db.events REPLACE PARTITION FIELD ts_month WITH days(ts);
INSERT INTO local.db.events BY NAME
SELECT 2L AS id, 11L AS user_id, 'KR' AS country, 9.9D AS price,
       TIMESTAMP '2027-01-08 12:00:00' AS ts;

SELECT spec_id, partition, record_count FROM local.db.events.files;
SELECT * FROM local.db.events;
```

`files`에는 spec 0인 파일과 spec 1인 파일이 하나씩 나오고, 각자 상대 스펙의 파티션 필드는 null이다. 첫 행은 `price`가 1.5, `country`가 null로 읽힌다. 파일은 처음 쓴 그대로다.

## 참고 자료

- [Apache Iceberg Table Spec (Schema Evolution, Default values, Column Projection, Partitioning, Sorting, Scan Planning)](https://iceberg.apache.org/spec/)
- [Iceberg Evolution](https://iceberg.apache.org/docs/latest/evolution/)
- [Iceberg Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/)
- [Iceberg Spark Procedures (rewrite_data_files)](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [apache/iceberg 1.12.0 SchemaUpdate.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/SchemaUpdate.java)
- [apache/iceberg 1.12.0 IcebergSqlExtensionsAstBuilder.scala](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.0/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/parser/extensions/IcebergSqlExtensionsAstBuilder.scala)
- [Amazon Athena: Handle schema updates](https://docs.aws.amazon.com/athena/latest/ug/handling-schema-updates-chapter.html)
- [Spark SQL: INSERT TABLE (BY NAME)](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-table.html)

## English summary

Iceberg reads columns by field ID, so renaming, reordering and dropping columns are metadata commits that never rewrite files, while Hive-style tables that match by name or by position can bring back dropped values or shift columns. Types can only widen, so changing an integer column to a string still means rebuilding the table. A required column needs v3 default values, and a partition change adds a new spec while old files keep the old one, so each manifest is planned with its own spec. rewrite_data_files moves old files to the new layout only where asked. Evolving in place keeps writes cheap, but old and new layouts live side by side until you pay for a rewrite.
