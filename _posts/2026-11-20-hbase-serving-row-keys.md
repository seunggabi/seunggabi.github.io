---
title: "HBase 서빙 테이블은 행 키를 어떻게 읽어야 빠를까"
date: 2026-11-20 04:52:00 +0900
categories: [Lakehouse & Storage, HBase]
tags: [hbase, row-key, hotspotting, spark, bulk-load, key-value, concept]
---

사용자가 화면을 열 때마다 1년 치 로그를 집계하면 응답이 너무 늦다. 그래서 배치가 결과를 미리 계산해 두고, API는 키 하나로 결과를 꺼내기만 하는 구조를 쓴다. 이때 저장소가 빠른지 느린지는 클러스터 크기보다 행 키를 어떻게 만들었고 어떻게 읽느냐에 달려 있다. [연말 결산 Spark 잡 글](/posts/year-end-recap-spark/)이 이 구조였고, 이 글은 그 설계를 예로 HBase 데이터 모델, 행 키, 읽기와 쓰기 방식을 정리한다. 버전은 HBase 2.5 기준이다.

## 셀 좌표는 행 키, 패밀리, 퀄리파이어, 버전 네 개다

HBase 테이블은 행 키로 정렬된 행의 모음이다. 행 키는 해석하지 않는 바이트 배열이고, 행은 바이트 사전순으로 정렬된다. 셀 하나는 `{행 키, 컬럼 패밀리:퀄리파이어, 버전}`으로 정해진다.

| 요소 | 정하는 시점 | 성질 |
|---|---|---|
| 행 키 | 쓸 때 | 정렬 기준, 바꿀 수 없다 |
| 컬럼 패밀리 | 테이블을 만들 때 | 패밀리마다 파일이 따로 저장된다. 보통 1~3개 |
| 퀄리파이어 | 쓸 때 | 행마다 달라도 된다. 미리 선언하지 않는다 |
| 버전 | 쓸 때(타임스탬프) | 내림차순 저장, 기본 보관 개수는 1 |

셀 값은 저장과 전송 때 늘 자기 좌표와 같이 다닌다. 행 키, 패밀리 이름, 퀄리파이어, 타임스탬프가 셀마다 붙으므로, 값이 작을수록 이름 길이가 차지하는 비중이 커진다. 패밀리 이름은 한 글자에 가깝게 짓는 편이 낫다.

결산 테이블은 이 네 요소를 이렇게 썼다.

| 요소 | 결산 테이블 |
|---|---|
| 행 키 | 사용자 ID |
| 컬럼 패밀리 | 결산용 하나 |
| 퀄리파이어 | `연도_항목_세부` |
| 버전 | 쓰지 않음, 연도는 퀄리파이어에 |

연도를 버전 축에 맡기지 않고 퀄리파이어 앞에 붙인 점이 핵심이다. 패밀리의 기본 보관 버전은 1개라서, 같은 칸에 올해 값을 쓰면 읽을 때는 새 값만 보이고 넘치는 옛 버전은 major compaction 때 지워진다. 연도가 칸 이름에 들어가 있으면 올해와 작년 결산이 서로 다른 칸이 되어 같은 행에 같이 남는다. 대신 해가 지날 때마다 한 행의 칸 수가 늘어난다.

## Get은 키를 알 때, Scan은 범위를 알 때 쓴다

Get은 행 하나를 읽고, Scan은 행 키 범위를 순서대로 훑는다. HBase 안에서 Get은 행 하나짜리 Scan으로 구현되어 있다.

```java
// HBase 2.5 클라이언트 API, 구조만 보여 주는 스케치
Get get = new Get(Bytes.toBytes(userId));
get.addColumn(CF, Bytes.toBytes("2026_album_top"));
get.addColumn(CF, Bytes.toBytes("2026_album_cnt"));
Result row = table.get(get);
```

결산 API가 이 방식이었다. 구독자이면 그해 칸 목록을 지정해 사용자 행을 `Get` 한 번으로 읽는다. 칸 이름을 하나하나 적기 싫다면 `ColumnPrefixFilter`로 `2026_`으로 시작하는 칸만 고를 수도 있다.

Scan은 행 키 앞부분이 같은 행을 모아 읽을 때 쓴다. 결산 글에서 믹스 곡 목록은 별도 테이블에 `사용자_연도_종류` 행 키로 들어갔다. 이런 키는 같은 사용자, 같은 연도의 행이 정렬 순서상 붙어 있어서, 접두사 Scan 한 번으로 그해 믹스 종류를 전부 읽을 수 있다.

```java
// 구조만 보여 주는 스케치
Scan scan = new Scan().setStartStopRowForPrefixScan(Bytes.toBytes(userId + "_2026_"));
try (ResultScanner rs = table.getScanner(scan)) {
    for (Result r : rs) { /* 종류별 곡 목록 */ }
}
```

사용자 행 하나에 칸을 모으는 설계와 `사용자_연도_종류`처럼 행을 나누는 설계는 둘 다 결산 글에 있다. 앞쪽은 Get 한 번에 끝나고, 뒤쪽은 종류가 늘어도 한 행이 커지지 않는다.

## 행 키 앞부분이 쓰기 부하를 나눈다

행이 사전순으로 정렬되므로 연속된 키는 같은 리전에 모인다. 이 성질은 Scan에는 유리하지만, 트래픽이 노드 한두 대에 몰리는 hotspotting의 원인이 된다. 타임스탬프나 1, 2, 3 같은 순번을 행 키 맨 앞에 두면 새 쓰기가 전부 마지막 리전 하나로 간다.

| 기법 | 키 모양 | 쓰기 | 읽기 |
|---|---|---|---|
| 솔팅 | 랜덤 접두사 + 원래 키 | 접두사 개수만큼 리전에 퍼진다 | 키를 다시 만들 수 없어 접두사마다 읽어야 하고, 순서대로 읽기도 어렵다 |
| 해싱 | 원래 키의 해시 접두사 + 원래 키 | 퍼진다 | 클라이언트가 키를 다시 계산해 Get할 수 있다 |
| 키 뒤집기 | 고정 길이·숫자 키를 거꾸로 | 가장 자주 바뀌는 자리가 앞으로 와서 퍼진다 | 원래 순서로 범위 Scan을 못 한다 |
| 접두사 + 시간 | `종류_타임스탬프` | 종류 수만큼 퍼진다 | 종류 안에서는 시간순 Scan이 된다 |
| 역 타임스탬프 | 키 뒤에 `Long.MAX_VALUE - ts` | 앞부분에 따라 다르다 | 최신 행이 먼저 나온다 |

서빙 테이블은 대부분 해싱이나 자연 키 쪽이 맞다. API가 키를 바로 알아야 Get을 쓸 수 있기 때문이다. 결산 테이블은 사용자 ID를 그대로 행 키로 썼으니 API는 요청에 든 사용자 ID로 바로 Get한다.

미리 리전을 나눠 두는 pre-split도 같은 맥락이다. 다만 분할 지점은 실제 키 분포에 맞춰야 한다. 16진수 문자열 키를 바이트 범위로 균등 분할하면 `0`~`9`와 `a`~`f` 사이 바이트가 쓰이지 않아 가운데 리전이 빈다.

공용 목록은 고정된 기본 행 키 하나에 들어 있다. 모든 요청이 이 행을 HBase에서 직접 읽는다면 이 행을 가진 리전 하나에 읽기가 몰린다. 결산 API는 이 행을 연도 접두사를 키로 Spring `@Cacheable`에 캐시하므로 사용자마다 다시 읽지 않고, 그래서 이 리전에 읽기가 몰리지 않는다.

## Spark에서 쓰는 방법은 세 가지다

| 방식 | 경로 | 특징 |
|---|---|---|
| 행마다 `Table.put` | 일반 쓰기 경로(WAL, MemStore) | 단순하다. `Table`은 autoflush를 쓰지 않고, 버퍼 쓰기는 `BufferedMutator` 몫이다 |
| `BufferedMutator` | 일반 쓰기 경로 | 클라이언트에서 모아 보낸다. 버퍼 기본 2MB(`hbase.client.write.buffer`) |
| 벌크 로드 | HFile을 만들어 리전에 바로 옮긴다 | CPU와 네트워크를 덜 쓴다. 리전 경계에 맞춘 정렬과 스테이징 디렉터리가 필요하고, 복제는 기본으로 따라가지 않는다 |

결산 잡은 첫 번째 방식이었다. `mapPartitions` 안에서 파티션마다 테이블 핸들을 한 번 얻고, 행마다 칸 몇 개를 담은 `Put`을 보낸 뒤 `count()`로 실행시켰다. 일반 쓰기 경로라 HFile 스테이징이나 리전 경계 정렬 없이 잡 안에서 쓰기가 끝난다. 일반 쓰기 경로이므로 WAL에 기록되고 복제 대상에도 그대로 포함된다. 빈 문자열로 칸을 덮어 예전 값이 남지 않게 하는 것도 Put 한 번이면 된다. 두 번째 방식인 `BufferedMutator`는 같은 자리에서 Put을 버퍼에 모아 RPC 수를 줄이고, hbase-connectors의 `HBaseContext`는 executor마다 연결 하나를 같이 쓰게 하는 `bulkPut`, `foreachPartition`, `bulkLoad`를 제공한다.

벌크 로드는 쓰기 API를 아예 거치지 않는다. 데이터를 리전 경계에 맞춰 정렬해 HFile로 쓰고, `completebulkload`로 테이블에 옮긴다. MapReduce에서는 `HFileOutputFormat2.configureIncrementalLoad()`가 리전 경계로 파티셔너를 잡아 주고, `HBaseContext`의 `bulkLoad`는 `repartitionAndSortWithinPartitions`로 같은 일을 한다. 복제 클러스터가 있으면 `hbase.replication.bulkload.enabled`(기본 false)를 켜고 `hbase.replication.cluster.id`도 정해야 벌크 로드한 파일도 넘어간다.

## OLAP 쿼리 대신 키-값 저장소를 고르는 조건

| 질문 | 키-값 저장소(HBase) | OLAP 쿼리(Trino, Spark SQL) |
|---|---|---|
| 요청마다 읽는 양 | 키 하나의 행 | 조건에 맞는 파티션과 파일 |
| 집계 시점 | 배치가 미리 | 요청할 때 |
| 새 질문 | 배치와 칸 이름을 바꿔야 한다 | SQL만 바꾸면 된다 |
| 필요 없는 것 | 보조 인덱스, 조인, 타입 있는 컬럼 | 키 설계 |

결산 화면은 왼쪽 조건에 맞았다. 질문은 미리 정해지고, 사용자는 자기 결과만 보며, 모든 집계는 배치가 끝낸다. API에 남는 일은 행 키로 읽고 쉼표 문자열을 쪼개는 것뿐이었다.

HBase는 행이 수억 개 이상일 때 맞는 도구이고, 수천에서 수백만 행이면 데이터가 노드 한두 대에 다 들어가 나머지 클러스터가 논다. RDBMS의 보조 인덱스, 트랜잭션, 풍부한 쿼리 언어도 없다. 행 키로 답할 수 없는 질문이 생기면 배치를 다시 돌리거나 다른 저장소로 가야 한다.

## 참고 자료

- [Apache HBase Reference Guide (book)](https://hbase.apache.org/book.html): Data Model, Rowkey Design, Number of Versions, Client, Bulk Loading, HBase and Spark
- [Apache HBase: Data Model](https://hbase.apache.org/docs/datamodel)
- [Apache HBase: HBase and Schema Design](https://hbase.apache.org/docs/schema-design)
- [Apache HBase: HBase and Spark](https://hbase.apache.org/docs/spark)
- [HBase 2.5 Javadoc: Scan](https://hbase.apache.org/2.5/apidocs/org/apache/hadoop/hbase/client/Scan.html)
- [Apache HBase Downloads](https://hbase.apache.org/downloads.html)
- [apache/hbase-connectors (spark)](https://github.com/apache/hbase-connectors/tree/master/spark)

## English summary

HBase finds a cell by row key, column family, qualifier, and version, and it sorts rows by the bytes of the row key. In my year-end recap jobs the row key was the user ID and the year was a prefix in the qualifier, so one Get with a list of columns returned a user's recap and several years stayed in the same row. Row keys that start with a timestamp or sequence send all writes to one region, and salting, hashing, and key reversal spread them at different costs for reads. From Spark you can write with per-row Puts, a BufferedMutator, or a bulk load of HFiles. My recap jobs wrote with per-row Puts. That path needs no HFile staging or region-boundary sort, and since Puts go through the WAL, replication applies as usual.
