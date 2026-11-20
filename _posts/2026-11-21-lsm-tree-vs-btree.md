---
title: "LSM 트리는 랜덤 쓰기를 어떻게 순차 쓰기로 바꾸나"
date: 2026-11-21 06:00:00 +0900
categories: [Lakehouse & Storage, Storage Engines]
tags: [lsm-tree, b-tree, rocksdb, cassandra, leveldb, compaction, bloom-filter, concept]
---

B-tree에 행 하나를 넣으면 그 키가 들어갈 리프 페이지를 찾아 그 자리에서 고친다. 키가 고르게 흩어져 들어오면 디스크 곳곳의 페이지를 하나씩 다시 쓰게 된다. LSM 트리는 쓰기를 메모리에 모았다가 정렬된 파일로 한꺼번에 내리고, 파일은 다시 고치지 않는다. 그 대가로 읽기는 파일 여러 개를 뒤져야 하고, 쌓인 파일을 합치는 compaction이 뒤에서 계속 돈다.

HBase가 이 구조를 어떻게 구현하는지는 [HBase 쓰기와 읽기 글](/posts/hbase-read-write-path/)에 적었다. 여기서는 엔진에 묶이지 않는 공통 구조와 compaction 전략의 차이를 다룬다.

버전은 RocksDB 11.13, Cassandra 5.0, LevelDB 1.23, MySQL 8.4, PostgreSQL 19 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| B-tree (B+tree) | 고정 크기 페이지의 균형 트리, 페이지를 제자리에서 수정 |
| LSM 트리 | 메모리 버퍼와 불변 정렬 파일 여러 층으로 이뤄진 구조, 1996년 O'Neil 등이 제안 |
| WAL | 메모리 버퍼보다 먼저 쓰는 추가 전용 로그, 장애 뒤 버퍼 복구용 |
| memtable | 최근 쓰기를 키 순서로 담는 메모리 구조, RocksDB 기본 구현은 skip list |
| SSTable (SST) | 키 순서로 정렬된 불변 파일, 데이터 블록과 인덱스, 필터를 담는다 |
| 툼스톤 | 삭제를 나타내는 레코드, 가려진 옛 값이 다 지워질 때까지 남는다 |
| 블룸 필터 | "이 파일에 그 키는 없다"를 확실히 말해 주는 확률적 필터 |
| size-tiered | 비슷한 크기의 파일이 몇 개 모이면 합친다 (Cassandra STCS, RocksDB universal) |
| leveled | 층마다 키 범위가 겹치지 않는 파일 집합 하나, 아래층이 약 10배 크다 |
| 쓰기, 읽기, 공간 증폭 | 실제 쓴 바이트, 실제 읽은 양, 실제 차지한 공간을 논리 데이터 크기로 나눈 값 |
| RUM 추측 | 읽기, 갱신, 공간 오버헤드 중 둘을 묶으면 나머지 하나는 일정 수준 아래로 못 내린다 |

## B-tree는 페이지를 제자리에서 고친다

InnoDB 테이블은 기본 키로 정렬된 B+tree(clustered index)이고 페이지 크기는 기본 16KB다. PostgreSQL은 행을 heap 파일에 두고 B-tree 인덱스를 따로 두며 페이지는 8KB다. 두 엔진 모두 수정할 페이지를 버퍼 풀로 읽어 고치고, 변경을 WAL(InnoDB는 redo log)에 남긴 뒤 나중에 그 페이지를 원래 위치에 덮어쓴다.

PostgreSQL의 UPDATE는 행을 제자리에서 바꾸지 않고 새 튜플 버전을 쓴다. 그래도 새 튜플이 들어가는 heap 페이지와, HOT 업데이트가 아니면 새 항목이 들어가는 인덱스 페이지가 같은 위치에 다시 쓰이므로, 디스크 입장에서는 둘 다 페이지 덮어쓰기 구조다.

이 구조의 비용은 두 가지다. 수십 바이트를 바꿔도 페이지 하나를 통째로 쓴다. 그리고 페이지를 쓰다 중간에 끊기면 반쯤 바뀐 페이지가 남으므로 이를 막는 장치가 따로 든다. InnoDB는 페이지를 제자리에 쓰기 전에 doublewrite 버퍼에 먼저 쓰고, PostgreSQL은 체크포인트 뒤 페이지를 처음 고칠 때 페이지 전체를 WAL에 남긴다(`full_page_writes`).

대신 읽기는 단순하다. 키 하나는 트리에 한 곳에만 있으니, 루트에서 리프까지 페이지 몇 개를 따라가면 끝난다. 범위 조회도 정렬된 리프를 이어서 읽는다.

## LSM은 쓰기를 모아 정렬된 파일로 한 번에 내린다

O'Neil, Cheng, Gawlick, O'Neil의 1996년 논문은 거래 이력 테이블처럼 삽입이 아주 많은 곳에서 B-tree 인덱스를 실시간으로 유지하면 트랜잭션의 I/O 비용이 두 배가 되는 문제에서 출발했다. 메모리의 작은 트리(C0)에 삽입을 모으고, 디스크의 큰 트리(C1)와 순차 I/O로 섞어 내려보내는 rolling merge가 핵심이다. 지금 엔진들은 이 아이디어를 "불변 파일을 쌓고 나중에 합친다"로 단순화했다.

RocksDB의 쓰기 경로는 이렇다.

1. 변경을 WAL 끝에 붙인다. 순차 쓰기 하나다.
2. 같은 변경을 memtable에 넣는다. 기본 구현은 skip list라 키 순서가 유지된다.
3. memtable이 `write_buffer_size`(기본 64MB)를 넘으면 immutable memtable이 되고, 새 memtable이 쓰기를 받는다.
4. 백그라운드 스레드가 immutable memtable을 정렬된 SST 파일로 flush한다. 이 파일은 L0에 들어가고 다시 수정되지 않는다.

디스크에 닿는 쓰기는 WAL 추가와 SST 파일 생성뿐이고 둘 다 순차다. 한 키를 여러 번 고치면 그 버전들은 각각 다른 파일에 남고, 시퀀스 번호로 최신을 가린다.

## 읽기는 최신부터 오래된 순서로 뒤진다

| 단계 | 보는 곳 | 비용을 줄이는 장치 |
|---|---|---|
| 1 | 활성 memtable | 메모리 |
| 2 | immutable memtable들 | 메모리 |
| 3 | L0 파일 전부, 최신부터 | 키 범위가 겹치므로 파일마다 확인, 블룸 필터 |
| 4 | L1 이하 층마다 파일 하나 | 키 범위로 이진 탐색, 블룸 필터 |
| 5 | 고른 SST 안의 블록 | 인덱스 블록으로 데이터 블록 위치, block cache |

Get은 처음 찾은 값에서 멈춘다. 위쪽이 늘 더 최신이므로 처음 찾은 것이 최신 값이고, 그게 툼스톤이면 "없음"이다. 키가 없는 경우가 가장 비싸다. 모든 층을 끝까지 확인해야 하기 때문이다.

블룸 필터가 여기서 일을 한다. 필터가 "없다"고 하면 그 파일은 읽지 않는다. RocksDB는 `filter_policy` 기본값이 비어 있어서 블룸 필터를 직접 켜야 한다. SST 안에서는 인덱스 블록이 데이터 블록(기본 4KB)의 위치를 알려 주고, 읽은 블록은 block cache에 둔다. block cache를 지정하지 않으면 32MB짜리 내부 캐시가 생긴다.

범위 조회는 블룸 필터의 도움을 거의 받지 못한다. memtable과 모든 정렬된 파일 묶음에 반복자를 열고 키 순서로 합쳐야 해서, 파일 묶음 수가 곧 비용이다. prefix 단위 필터(`prefix_extractor`)를 두면 같은 접두사 조회는 줄일 수 있다.

## 삭제도 쓰기 한 번이다

LSM에서 Delete는 툼스톤을 쓰는 일이다. 옛 값은 더 아래층 파일에 그대로 있으니, 툼스톤은 그 값을 가려야 할 동안 남아야 한다. LevelDB는 더 아래층에 그 키 범위와 겹치는 파일이 없을 때에야 compaction에서 툼스톤을 버린다. 그래서 지운 직후에는 공간이 늘고, 툼스톤이 많은 범위를 스캔하면 지워진 레코드를 계속 건너뛰느라 느려진다.

분산 저장소는 조건이 하나 더 붙는다. Cassandra는 툼스톤을 `gc_grace_seconds`(기본 864000초, 10일)가 지나기 전에는 지우지 않는다. 그 사이 삭제를 받지 못한 복제본이 나중에 옛 값을 다시 퍼뜨리는 것을 repair로 막기 위해서다. 노드가 이 기간보다 오래 내려가 있으면 지운 데이터가 되살아날 수 있다.

## compaction 전략이 증폭을 어디에 둘지 정한다

| 전략 | 파일을 합치는 기준 | 쓰기 증폭 | 읽기 | 공간 |
|---|---|---|---|---|
| size-tiered (Cassandra STCS, RocksDB universal) | 비슷한 크기 파일이 몇 개 모이면 | 낮다 | 정렬된 묶음이 많아 느리다 | 큰 compaction 중 일시적으로 두 배까지 |
| leveled (LevelDB, RocksDB 기본, Cassandra LCS) | 층이 목표 크기를 넘으면 다음 층의 겹치는 파일과 | 높다, 흔히 10 이상 | 층마다 파일 하나 | 데이터 대부분이 마지막 층이라 작다 |
| FIFO (RocksDB) | 합치지 않고 총 크기나 TTL을 넘으면 가장 오래된 파일 삭제 | 1 (WAL 제외) | 파일 수만큼 | 상한이 고정 |

STCS는 평균 크기의 0.5배에서 1.5배 사이 파일을 한 버킷으로 묶고, 한 버킷에 4개(`min_threshold`)가 모이면 최대 32개까지 합친다. Cassandra 5.0에서도 기본 전략이다. 한 행의 버전이 여러 크기의 파일에 흩어질 수 있어 읽기가 느리고, 지운 데이터가 언제 빠질지 예측하기 어렵다.

leveled는 L0만 키 범위가 겹친다. L1부터는 한 층 전체가 겹치지 않는 정렬 묶음 하나이고, 아래층 목표 크기는 위층의 10배다. LevelDB는 L0 파일이 4개가 되면 L1으로 합치고 L1 10MB, L2 100MB처럼 키운다. RocksDB는 `max_bytes_for_level_multiplier` 기본 10에, 8.4부터 기본으로 켜진 `level_compaction_dynamic_level_bytes`가 마지막 층의 실제 크기에서 거꾸로 각 층 목표를 계산해 데이터의 약 90%가 마지막 층에 있게 한다. Cassandra LCS는 160MB 파일과 fanout 10을 쓴다. 한 키는 층마다 많아야 파일 하나에 있으니 읽기와 공간이 좋고, 대신 L1 파일 하나를 내릴 때마다 아래층의 겹치는 파일 약 10개를 다시 쓴다.

RocksDB universal은 size-tiered 계열이다. 가장 오래된 묶음을 뺀 나머지 크기가 가장 오래된 묶음 대비 `max_size_amplification_percent`(기본 200)를 넘으면 전부를 하나로 합친다. 200은 살아 있는 데이터의 세 배까지 공간을 허용한다는 뜻이다. FIFO는 이벤트 로그처럼 오래된 데이터를 버려도 되는 곳에 쓰고, 사용자에게 알리지 않고 데이터를 지운다는 점을 감수해야 한다.

Cassandra 5.0에는 둘을 한 설정으로 오가는 UCS(Unified Compaction Strategy)가 들어왔고, 새 워크로드에는 이쪽을 권장한다. `T4`가 기본으로 STCS(임계값 4)에 해당하고, `L10`이 LCS에 해당한다.

```sql
-- Cassandra 5.0 CQL, 쓰기가 많은 테이블과 읽기가 많은 테이블
ALTER TABLE ks.events WITH compaction = {
  'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4' };

ALTER TABLE ks.user_profile WITH compaction = {
  'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10' };
```

## 세 증폭과 RUM 추측

Athanassoulis 등의 2016년 EDBT 논문은 접근 방법(access method)의 비용을 읽기(Read), 갱신(Update), 공간(Memory) 오버헤드로 나누고, 이 중 둘의 상한을 정하면 나머지 하나에 더 낮출 수 없는 하한이 생긴다고 추측했다. B-tree와 LSM, 그리고 LSM 안의 compaction 전략은 이 세 점 사이의 서로 다른 위치다.

| | B-tree | LSM leveled | LSM size-tiered |
|---|---|---|---|
| 쓰기 | 작은 변경도 페이지 단위 랜덤 쓰기 | 순차 쓰기, compaction으로 여러 번 다시 씀 | 순차 쓰기, 다시 쓰는 횟수가 적음 |
| 점 조회 | 트리 높이만큼 페이지 | memtable, L0 파일들, 층마다 파일 하나 | 정렬 묶음마다 확인 |
| 공간 | 페이지 빈 공간과 단편화 | 작음 | 큼 |

LSM이 B-tree보다 쓰기에 유리하다고만 하면 반쯤 맞다. leveled LSM은 같은 바이트를 층마다 다시 써서 쓰기 증폭이 10을 쉽게 넘는다. 차이는 그 쓰기가 큰 순차 I/O이고, 작은 랜덤 페이지 쓰기가 없다는 데 있다. 키가 없는 조회와 범위 조회가 많고 쓰기가 적다면 B-tree가 여전히 유리하다.

## LSM이 쓰이는 곳

HBase와 Cassandra는 서버 단위의 LSM 저장소이고, RocksDB는 다른 시스템 안에 들어가는 임베디드 LSM이다. 데이터 엔지니어링에서는 스트림 처리의 로컬 상태 저장소로 자주 만난다.

Flink의 RocksDB state backend는 증분 체크포인트에서 SST 파일이 불변이라는 점을 그대로 쓴다. 지난 체크포인트 이후 새로 생긴 SST만 원격 저장소에 올리고, 이미 올린 파일은 참조만 늘린다. compaction이 파일을 합쳐 옛 파일을 지우면 새 체크포인트는 그 파일을 참조하지 않고, 참조 수가 0이 되면 원격 사본도 지운다. Flink는 RocksDB의 WAL을 기본으로 끄는데, 장애 복구를 체크포인트가 맡기 때문이다. 체크포인트 쪽 이야기는 [Flink 상태와 체크포인트 글](/posts/flink-state-and-checkpoints/)에 있다.

Kafka Streams의 persistent key-value 저장소도 RocksDB 기반이고, 압축(compaction)된 changelog 토픽에 변경을 백업해 다른 인스턴스에서 상태를 다시 만든다. 로컬 LSM의 compaction과 Kafka 토픽의 compaction이 각자 같은 키의 옛 값을 정리하는 셈이다.

테이블 포맷에도 같은 생각이 보인다. Hudi는 오래된 타임라인 instant를 LSM 구조의 history로 옮기고, merge-on-read는 변경을 따로 쌓았다가 읽을 때 합치는 점에서 memtable과 SST의 관계와 닮았다. 세 포맷의 커밋 구조는 [테이블 포맷 비교 글](/posts/table-formats-compared/)에 정리했다.

## 참고 자료

- O'Neil, Cheng, Gawlick, O'Neil, [The Log-Structured Merge-Tree (LSM-Tree)](https://doi.org/10.1007/s002360050048), Acta Informatica 33(4), 1996
- Athanassoulis et al., [Designing Access Methods: The RUM Conjecture](https://stratos.seas.harvard.edu/publications/designing-access-methods-rum-conjecture), EDBT 2016
- RocksDB wiki: [RocksDB Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview), [Leveled Compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction), [Universal Compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction), [FIFO compaction style](https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style)
- RocksDB v11.13.2 소스: [options.h](https://github.com/facebook/rocksdb/blob/v11.13.2/include/rocksdb/options.h), [advanced_options.h](https://github.com/facebook/rocksdb/blob/v11.13.2/include/rocksdb/advanced_options.h), [table.h](https://github.com/facebook/rocksdb/blob/v11.13.2/include/rocksdb/table.h), [universal_compaction.h](https://github.com/facebook/rocksdb/blob/v11.13.2/include/rocksdb/universal_compaction.h)
- [LevelDB implementation notes (impl.md)](https://github.com/google/leveldb/blob/main/doc/impl.md)
- Cassandra 5.0: [Compaction overview](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/overview.html), [STCS](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/stcs.html), [LCS](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/lcs.html), [UCS](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/ucs.html)
- MySQL 8.4: [InnoDB Doublewrite Buffer](https://dev.mysql.com/doc/refman/8.4/en/innodb-doublewrite-buffer.html), [Clustered and Secondary Indexes](https://dev.mysql.com/doc/refman/8.4/en/innodb-index-types.html)
- PostgreSQL: [WAL configuration, full_page_writes](https://www.postgresql.org/docs/current/runtime-config-wal.html), [Heap-Only Tuples](https://www.postgresql.org/docs/current/storage-hot.html)
- Flink 2.3: [State Backends, Incremental Checkpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/), [Managing Large State in Apache Flink](https://flink.apache.org/2018/01/30/managing-large-state-in-apache-flink-an-intro-to-incremental-checkpointing/), [RocksDBResourceContainer.java](https://github.com/apache/flink/blob/master/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/state/rocksdb/RocksDBResourceContainer.java)
- Kafka Streams 4.4: [Processor API, State Stores](https://kafka.apache.org/44/streams/developer-guide/processor-api/)

## English summary

A B-tree updates fixed-size pages in place, so even a tiny change rewrites a whole page at a random location, while an LSM tree buffers writes in a memtable behind a WAL and flushes them as immutable sorted files. Reads in an LSM check the memtable and then files from newest to oldest, using bloom filters, block indexes and a block cache to skip work, and deletes are tombstones that stay until compaction reaches the oldest data. Size-tiered compaction keeps write amplification low at the cost of reads and space, and leveled compaction does the opposite, which is the RUM conjecture in practice. I built a personalization API on HBase, an LSM store, and the same structure shows up in Flink's RocksDB incremental checkpoints, which upload only new SST files. The choice is where to pay: random page writes now, or sequential rewrites later during compaction.
