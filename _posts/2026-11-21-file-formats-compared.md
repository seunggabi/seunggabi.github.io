---
title: "CSV, Avro, Parquet, ORC는 파일에 무엇을 남기나"
date: 2026-11-21 06:05:00 +0900
categories: [Lakehouse & Storage, Parquet]
tags: [csv, json, avro, parquet, orc, compression, schema-evolution, concept]
description: "CSV, JSON, Avro, Parquet, ORC가 파일 안에 남기는 레코드 경계, 스키마, 컬럼 통계가 분할, 건너뛰기, 스키마 변경을 어떻게 정하는지 비교한다."
---

gzip으로 묶은 CSV 파일 하나는 몇 GB든 Spark 태스크 하나가 처음부터 끝까지 푼다. 같은 데이터를 snappy Avro로 두면 여러 태스크가 나눠 읽고, Parquet으로 두면 필요한 컬럼만 읽는다. 차이는 포맷이 파일 안에 무엇을 남기느냐에서 온다. 레코드 경계 표시, 스키마, 컬럼별 통계와 인덱스가 각각 어느 포맷에 들어 있는지가 분할, 건너뛰기, 스키마 변경을 정한다.

[Spark Streaming 글](/posts/spark-streaming-to-hive/)의 로그도 JSON 텍스트로 HDFS에 쌓인 뒤 ORC로 바꾸는 잡을 따로 거쳤다. 들어오는 쪽은 행 단위로 쓰기 쉬운 포맷, 쌓아 두고 조회하는 쪽은 컬럼 포맷이라는 분담이다.

버전은 Avro 1.12 스펙(최신 릴리스 1.12.2), Parquet format 2.14.0, ORC 2.3.1, Spark 4.3.0, Iceberg 1.12.0 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 분할 가능(splittable) | 파일 중간에서 읽기를 시작할 수 있어 여러 태스크가 나눠 읽는 성질 |
| JSON Lines | 한 줄에 JSON 값 하나, 줄바꿈이 레코드 경계 |
| Avro object container file | 헤더에 스키마와 코덱을 적고 블록마다 sync marker를 붙인 Avro 파일 |
| sync marker | 파일마다 무작위로 만든 16바이트, 블록 끝마다 반복된다 |
| row group, column chunk, page | Parquet의 행 묶음, 그 안의 컬럼별 조각, 그 안의 압축·인코딩 단위 |
| dictionary + RLE | 고유값 사전을 따로 두고 값 자리에 사전 번호를 run-length로 적는 인코딩 |
| page index | Parquet page별 min/max(ColumnIndex)와 위치(OffsetIndex) |
| stripe, row index | ORC의 큰 묶음(기본 64MB)과 그 안의 10,000행 단위 통계·위치 |
| bloom filter | "이 값은 확실히 없다"를 답하는 비트 집합, 같음 조건에만 쓴다 |
| definition/repetition level | Parquet이 중첩·반복 필드를 평평한 컬럼으로 펴면서 남기는 숫자 |
| 코덱 | Snappy, ZSTD, GZIP 같은 압축 방식, 포맷 안에서는 블록·page·chunk 단위로 걸린다 |

## 행 포맷은 레코드 경계를 어떻게 찾느냐가 갈린다

CSV와 JSON Lines는 파일에 스키마가 없다. 컬럼 이름은 헤더 한 줄이거나 아예 없고, 타입은 읽는 쪽이 추론하거나 따로 받는다. 대신 줄바꿈이 레코드 경계라서, 파일 중간 아무 데서나 다음 줄바꿈까지 건너뛰고 읽기 시작할 수 있다. Spark도 기본 모드에서는 CSV와 JSON 파일을 나눠 읽는다.

이 성질은 두 경우에 깨진다. 따옴표 안에 줄바꿈이 든 CSV를 `multiLine` 옵션으로 읽으면 Spark는 파일을 나누지 않는다. 그리고 압축이 걸리면 코덱이 분할 여부를 정한다. Spark 4.3.0의 텍스트 계열 소스는 코덱이 없거나 Hadoop `SplittableCompressionCodec`을 구현할 때만 파일을 나눈다. 기본 코덱 중 여기에 드는 것은 bzip2다. gzip과 Hadoop snappy로 압축한 텍스트 파일은 크기와 상관없이 태스크 하나가 맡는다.

Avro 파일은 스키마를 파일에 싣고, 경계 표시를 따로 둔다.

```text
"Obj" 0x01                       매직 4바이트
file metadata (map<string,bytes>)
  avro.schema = 라이터 스키마 JSON  (필수)
  avro.codec  = null | deflate | snappy | bzip2 | xz | zstandard
sync marker (16바이트, 파일마다 무작위)
block (반복): 객체 수(long), 압축 후 바이트 수(long), 직렬화된 객체들, sync marker
```

압축은 블록 단위로 걸린다. 리더는 파일 중간에서 sync marker 16바이트를 찾아 다음 블록부터 읽으면 되니, snappy든 deflate든 Avro 파일은 나눠 읽힌다. 블록 크기 값이 있어서 풀지 않고 건너뛸 수도 있다. 코덱 중 `null`과 `deflate`만 필수이고, snappy는 블록 뒤에 원본의 CRC32 4바이트를 붙인다. Spark의 Avro 쓰기 기본 코덱은 snappy다. Kafka 메시지에 쓰는 Avro는 이 컨테이너 파일이 아니다. 메시지마다 스키마 JSON을 넣으면 스키마가 메시지보다 커서 스키마 레지스트리 ID만 붙이는데, 이 형식과 리더·라이터 스키마 해석은 [Avro 스키마 진화 글](/posts/schema-evolution/)에 적었다.

## Parquet은 page 안에서 인코딩하고 압축한다

Parquet 파일은 row group 여러 개와 끝의 footer로 이뤄진다. row group 안에서 컬럼마다 column chunk가 하나씩 있고, column chunk는 page로 나뉜다. 파일·row group·page 단위 통계로 건너뛰는 단계는 [컬럼 파일과 프루닝 글](/posts/columnar-files-and-pruning/)에 적었으니, 여기서는 page 안쪽을 본다. 인코딩은 압축 전에 값의 모양을 바꾼다.

| 인코딩 | 대상 | 하는 일 |
|---|---|---|
| `RLE_DICTIONARY` | 모든 타입 | column chunk마다 dictionary page 하나에 고유값을 두고, data page에는 사전 번호만 적는다 |
| RLE/bit-packing hybrid | 레벨, 사전 번호, boolean | 같은 값 반복은 run-length로, 나머지는 필요한 비트 수만큼 묶어 적는다 |
| `DELTA_BINARY_PACKED` | INT32, INT64 | 이웃 값의 차이를 블록 단위로 비트 패킹한다 |
| `BYTE_STREAM_SPLIT` | FLOAT, DOUBLE 등 | 값의 바이트를 자리별 스트림으로 나눠 뒤의 압축이 잘 먹게 한다. 크기 자체는 그대로다 |

사전이 너무 커지면 plain 인코딩으로 물러난다. parquet-java는 사전 page가 `parquet.dictionary.page.size`(기본 1MB)를 넘으면 그 뒤 값을 plain으로 쓴다. `PLAIN_DICTIONARY`와 `BIT_PACKED`는 스펙에서 deprecated다. 2.14.0에는 부동소수점용 `ALP` 인코딩도 추가됐다. 압축 코덱은 column chunk 메타데이터에 적히고, 실제 압축은 page마다 따로 한다. 스펙의 코덱은 UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, ZSTD, LZ4_RAW이고 옛 LZ4는 deprecated다. 압축이 page 안에 갇혀 있으니 어떤 코덱을 써도 row group 단위로 나눠 읽힌다.

page index는 page 단위 건너뛰기를 위한 두 구조다. `ColumnIndex`는 page마다 min, max, 전부 null인지를 담고, `boundary_order`로 값이 정렬돼 있는지 알려 준다. 정렬돼 있으면 리더가 이진 탐색을 할 수 있다. `OffsetIndex`는 page마다 파일 위치, 압축 크기, 첫 행 번호를 담는다. 둘 다 row group 데이터 뒤, footer 앞에 모여 있어서, 필요한 page만 골라 읽을 때 page 헤더를 하나씩 훑지 않아도 된다.

bloom filter는 min/max가 쓸모없는 고카디널리티 컬럼을 위한 것이다. 사용자 ID처럼 고유값이 많으면 사전은 포기되고 row group의 min/max는 거의 전 범위를 덮는다. Parquet의 bloom filter는 column chunk마다 하나씩 붙는 split block bloom filter로, 값의 plain 인코딩을 xxHash 64비트로 해시한다. 고유값 하나당 약 10.5비트를 주면 거짓 양성이 1%다. 해시가 순서를 지우므로 `=`와 `IN`에만 쓰이고 범위 조건에는 쓰이지 않는다. parquet-java 기본값은 꺼져 있어서 컬럼을 골라 켠다.

```python
# Spark 4.3.0, Parquet 쓰기 옵션으로 컬럼별 bloom filter를 켠다
(df.write.format("parquet")
   .option("parquet.bloom.filter.enabled#user_id", "true")
   .option("parquet.bloom.filter.expected.ndv#user_id", "1000000")
   .save("/tmp/events_parquet"))
```

## ORC는 stripe 안에 인덱스를 데이터 앞에 둔다

ORC 파일은 stripe 여러 개와 끝의 Footer, Postscript로 이뤄진다. stripe 하나는 index stream, data stream, stripe footer 순서다. ORC Java 기본값은 stripe 64MB, row index 간격 10,000행이다. row index는 원시 타입 컬럼마다 10,000행 묶음 하나에 항목 하나를 둔다. 항목에는 그 묶음의 통계(min, max, null 여부 등)와, 각 stream에서 그 묶음이 시작하는 위치가 들어 있다. 위치가 있으니 리더는 조건에 맞는 묶음으로 바로 seek한다.

bloom filter는 `orc.bloom.filter.columns`에 적은 컬럼에만 만들고, 기본 거짓 양성 확률은 0.01이다. bloom filter stream은 row index와 번갈아 놓여서 한 번에 읽히고, min/max 검사를 통과한 묶음에만 확인한다. 문자열은 Murmur3 64비트, 정수와 실수는 Thomas Wang 해시를 쓴다.

문자열 컬럼은 고유값 수가 null 아닌 행 수의 0.8배(`orc.dictionary.key.threshold`) 이하일 때 사전 인코딩을 쓴다. 정수는 RLE v2가 Short Repeat, Direct, Patched Base, Delta 네 하위 방식 중에서 고른다. 압축은 기본 256KB chunk 단위이고, chunk마다 3바이트 헤더에 압축 길이와 "압축 안 함" 표시를 둔다. 압축해서 커지면 원본을 그대로 둔다. ORC Java 2.3.1의 기본 코덱은 ZSTD다.

ORC는 2013년 1월 Hive를 빠르게 하려는 작업에서 나왔고, Hive의 struct, list, map, union을 모두 지원한다. Hive의 full ACID 테이블이 ORC만 받는 것도 이 계보다. ACID 디렉터리 구조는 [Hive 테이블 글](/posts/hive-tables-serde-and-acid/)에 적었다.

## 코덱은 같아도 어디에 걸리느냐가 다르다

| 코덱 | 특징 | 텍스트 파일에 걸면 분할 | Spark 4.3.0 쓰기 기본 |
|---|---|---|---|
| Snappy | 압축률보다 속도를 노린다 | 안 됨 | Parquet, Avro |
| ZSTD | 레벨로 압축률과 속도를 고른다 | 안 됨 | ORC |
| GZIP(deflate) | 가장 널리 읽히는 스트림 압축 | 안 됨 | 없음 |
| bzip2 | 블록 단위 압축이라 경계를 찾을 수 있다 | 됨 | 없음 |

분할 문제는 텍스트 파일 전체를 코덱 하나로 감쌀 때만 생긴다. Avro는 블록, Parquet은 page, ORC는 chunk 안에서 압축하므로 GZIP이나 ZSTD를 써도 파일이 나눠 읽힌다. 기본값도 엔진마다 다르다. Spark는 Parquet에 snappy, ORC에 zstd를 쓰고, Iceberg 1.12.0은 Parquet zstd, ORC zlib, Avro gzip이 기본이다. parquet-java 자체 기본값은 압축 없음이다. 같은 테이블이라도 어느 엔진이 썼는지에 따라 코덱이 섞일 수 있다.

## 중첩 데이터와 스키마 변경은 저장 방식이 갈린다

Avro는 레코드 안에 중첩 레코드, 배열, 맵을 그대로 이어 쓴다. 컬럼 포맷은 중첩을 컬럼으로 펴야 한다.

- Parquet은 Dremel 방식이다. 잎 필드마다 컬럼 하나를 두고, 값마다 definition level(경로의 optional 필드 중 몇 개가 값이 있는지)과 repetition level(어느 반복 필드에서 반복됐는지)을 RLE로 적는다.
- ORC는 타입 트리를 전위 순회로 번호 매겨 자식 컬럼을 따로 둔다. struct는 PRESENT stream과 필드별 자식 컬럼, list와 map은 PRESENT와 원소 개수를 담은 LENGTH stream에 자식 컬럼을 둔다.

어느 쪽이든 `address.city`만 읽는 쿼리는 그 잎 컬럼만 읽는다. 스키마 변경은 맞추는 기준이 갈린다.

| 포맷 | 스키마 위치 | 읽을 때 맞추는 기준 |
|---|---|---|
| CSV | 없음(헤더 줄 정도) | 위치. 컬럼 순서가 바뀌면 값이 다른 컬럼에 들어간다 |
| Avro | 파일 헤더 | 이름. 리더에만 있는 필드는 default로 채운다 |
| Parquet | footer | Spark는 이름. `field_id`가 스펙에 있지만 Spark의 `spark.sql.parquet.fieldId.read.enabled`는 기본 false |
| ORC | footer | ORC Java는 이름. `orc.force.positional.evolution=true`면 위치 |

Parquet과 ORC는 파일마다 자기 스키마를 들고 있어서, 디렉터리 안 파일들의 스키마가 다르면 엔진이 합쳐야 한다. Spark의 `mergeSchema`는 Parquet과 ORC 모두 기본 false라서, 켜지 않으면 파일 하나의 스키마를 기준으로 읽는다. 이름으로 맞추는 한 컬럼 이름 변경은 삭제와 추가로 보인다. 테이블 포맷이 컬럼 ID를 따로 관리하는 이유가 여기 있다.

## 쓰는 단위와 읽는 단위로 고른다

| 자리 | 주로 쓰는 포맷 | 이유 |
|---|---|---|
| Kafka 메시지 | Avro, Protobuf, JSON Schema | 메시지 하나가 레코드 하나라 컬럼 포맷의 이점이 없다. Confluent 스키마 레지스트리가 셋을 지원한다 |
| 원본 착륙 구역 | JSON Lines, CSV | 쓰는 쪽이 단순하고 사람이 열어 볼 수 있다 |
| 행 단위 교환·적재 파일 | Avro | 스키마를 싣고 압축해도 나눠 읽힌다 |
| 레이크 테이블 | Parquet | 컬럼 단위 읽기와 통계. Iceberg 1.12.0의 기본 데이터 파일 포맷이다 |
| Hive 웨어하우스 | ORC | Hive에서 나왔고 ACID 테이블의 필수 포맷이다 |

컬럼 포맷은 row group이나 stripe 하나를 메모리에 모았다가 쓰므로, 레코드가 하나씩 오는 자리에는 맞지 않는다. 행 포맷은 레코드를 바로 붙일 수 있는 대신, 컬럼 하나를 읽으려 해도 모든 필드를 디코딩한다. JSON 텍스트를 받고 ORC로 바꾸는 두 단계 적재는 이 둘을 각자 잘하는 자리에 둔 구성이다.

## 참고 자료

- [Apache Avro 1.12 Specification (Object Container Files, Codecs)](https://avro.apache.org/docs/1.12.0/specification/), [Avro releases](https://avro.apache.org/blog/releases/)
- [Apache Parquet: parquet.thrift (format 2.14.0)](https://github.com/apache/parquet-format/blob/apache-parquet-format-2.14.0/src/main/thrift/parquet.thrift), [Encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/), [Bloom Filter](https://parquet.apache.org/docs/file-format/bloomfilter/), [Nested Encoding](https://parquet.apache.org/docs/file-format/nestedencoding/), [parquet-hadoop README](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md)
- [Apache ORC: Specification v1](https://orc.apache.org/specification/ORCv1/), [Java configuration](https://orc.apache.org/docs/core-java-config.html), [Background](https://orc.apache.org/docs/), [Releases](https://orc.apache.org/releases/)
- Spark 4.3.0: [Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html), [ORC Files](https://spark.apache.org/docs/latest/sql-data-sources-orc.html), [Avro Files](https://spark.apache.org/docs/latest/sql-data-sources-avro.html), [CSV Files](https://spark.apache.org/docs/latest/sql-data-sources-csv.html), [Generic Load/Save (Parquet options)](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html), [FileFormat.scala (isSplitable)](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormat.scala), [SQLConf.scala](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [Hadoop BZip2Codec (SplittableCompressionCodec)](https://hadoop.apache.org/docs/r3.5.0/api/org/apache/hadoop/io/compress/BZip2Codec.html)
- [Apache Iceberg 1.12.0: Configuration](https://iceberg.apache.org/docs/latest/configuration/)
- [Confluent Platform: Schema Registry formats and wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)

## English summary

Row formats like CSV and JSON Lines split on line breaks but carry no schema, and a gzip or snappy codec wrapped around the whole text file makes it unsplittable, so one task reads it all. Avro container files keep the writer schema and codec in the header and end every block with a 16-byte sync marker, so they stay splittable with any codec. Parquet and ORC compress inside pages and chunks, add dictionary and RLE encodings, page or row index statistics, and optional bloom filters for equality lookups on high-cardinality columns. They also flatten nested data into leaf columns and match columns by name when the schema changes. In my Spark Streaming log pipeline, logs landed as JSON text and a separate job converted them to ORC, which keeps cheap appends on the write side and column pruning on the read side at the cost of a second write.
