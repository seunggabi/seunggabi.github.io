---
title: "범위로 나눌까 해시로 나눌까, 파티셔닝과 샤딩"
date: 2026-11-21 06:04:00 +0900
categories: [Platform & Infra, Distributed Systems]
tags: [partitioning, sharding, consistent-hashing, cassandra, dynamodb, redis, concept]
---

[연말 결산 Spark 잡 글](/posts/year-end-recap-spark/)의 잡들에는 "파티션"이 다섯 가지 뜻으로 나온다. Hive 중간 테이블의 `type`, `dt` 파티션, `RANK() OVER (PARTITION BY user_id ...)`, `DISTRIBUTE BY user_id`, `mapPartitions`, 그리고 사용자 ID를 행 키로 쓰는 HBase 테이블의 리전이다. 나누는 대상도, 나누는 주체도, 수를 바꾸는 비용도 서로 다르다.

공통점은 키를 보고 조각을 고르는 함수가 있다는 것이다. 그 함수가 범위인지 해시인지, 노드가 늘 때 키가 얼마나 옮겨 가는지, 한 키에 몰리는 부하와 보조 인덱스를 어떻게 다루는지에서 시스템들이 갈린다.

버전은 Cassandra 5.0, HBase 2.6, Kafka 4.4, Spark 4.3, Iceberg 1.12 기준이고, DynamoDB와 Redis Cluster는 2026년 11월 공식 문서 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 범위 파티셔닝 | 키를 정렬해 연속 구간마다 나눈다. 범위 조회가 조각 몇 개로 끝난다 |
| 해시 파티셔닝 | 키의 해시 값으로 나눈다. 부하는 고르지만 범위 조회는 모든 조각을 본다 |
| 샤드 | 노드나 DB 인스턴스 하나가 맡는 데이터 조각 |
| 일관된 해싱 | 노드와 키를 같은 해시 링에 놓고, 키 위치에서 시계 방향 첫 노드가 맡는 방식 |
| 가상 노드(vnode) | 물리 노드 하나가 링 위 토큰 여러 개를 갖는 것 |
| 해시 슬롯 | Redis Cluster가 키 공간을 16384개로 고정해 나눈 단위 |
| 핫 키 | 요청이 몰려 파티션 하나를 포화시키는 키 |
| 솔팅 | 키에 접미사를 붙여 한 키의 부하를 여러 파티션에 흩는 기법 |
| 로컬 인덱스 | 파티션마다 자기 데이터만 색인한다. 쓰기는 한 곳, 조회는 전부 |
| 글로벌 인덱스 | 인덱스 키로 따로 파티셔닝한다. 조회는 한 곳, 쓰기는 여러 곳 |

## "파티션" 다섯 가지는 나누는 대상이 다르다

| 쓰는 곳 | 나누는 것 | 정하는 시점 | 수를 바꾸면 |
|---|---|---|---|
| Kafka 토픽 파티션 | 순서가 지켜지는 로그 | 토픽을 만들 때 | 늘리기만 된다. 같은 키의 새 레코드가 다른 파티션으로 갈 수 있다 |
| Spark 파티션 | 태스크 하나가 처리할 행 묶음 | 실행할 때마다 | 셔플이나 `repartition`마다 새로 정해진다 |
| Hive·Iceberg 테이블 파티션 | 읽을 파일을 거르는 단위 | 테이블 설계 때 | Hive는 다시 써야 하고, Iceberg는 새 스펙이 새 데이터부터 적용된다 |
| DB 샤드, HBase 리전 | 노드 하나가 맡는 키 범위 | 운영 중 계속 | 데이터가 노드 사이로 옮겨 간다 |
| SQL `PARTITION BY` | 윈도 함수의 논리 그룹 | 쿼리마다 | 저장에는 영향이 없다 |

부하를 노드에 나누는 단위는 Kafka 파티션과 샤드다. Spark 파티션은 잡 안에서만 산다. 파일을 읽을 때는 `spark.sql.files.maxPartitionBytes`(기본 128MB) 크기로 묶고, 셔플 뒤에는 `spark.sql.shuffle.partitions`(기본 200)를 따르므로 같은 잡에서도 stage마다 수가 다르다. 테이블 파티션은 노드 분산과 관계가 없다. `dt=2026-11-19/` 같은 디렉터리로 읽을 파일을 거르는 장치이고, 메타스토어와의 관계는 [Hive 파티션 글](/posts/hive-partitions-and-metastore/)에 있다. Iceberg의 `bucket[N]` 변환은 Murmur3 해시를 N으로 나눈 나머지라서, 테이블 안의 해시 파티셔닝에 해당한다. 변환 종류는 [Iceberg 글](/posts/iceberg-table-format/)에 정리했다.

## 범위는 범위 조회를, 해시는 고른 쓰기를 얻는다

| 항목 | 범위 | 해시 |
|---|---|---|
| `BETWEEN` 같은 범위 조회 | 해당 구간의 조각 몇 개 | 모든 조각 |
| 시간·순번처럼 늘어나는 키의 쓰기 | 마지막 조각 하나에 몰린다 | 흩어진다 |
| 분할 지점 | 실제 키 분포를 보고 정해야 한다 | 해시 공간을 균등하게 자르면 된다 |
| 예 | HBase 리전, Bigtable tablet | Cassandra 파티션 키, DynamoDB 파티션 키, Kafka 메시지 키 |

많은 저장소가 두 방식을 한 키에 섞는다. Cassandra는 기본 `Murmur3Partitioner`로 파티션 키를 해시해 노드를 고르고, 파티션 안에서는 클러스터링 컬럼 순서로 정렬해 둔다. DynamoDB도 파티션 키는 해시하고 정렬 키는 정렬한다. `(user_id, event_time)`으로 잡으면 사용자들은 흩어지고, 한 사용자의 기간 조회는 파티션 하나에서 끝난다.

HBase는 행 키 전체를 바이트 순서로 정렬하는 범위 방식이다. 결산 테이블은 사용자 ID를 행 키로 써서 API가 요청에 든 ID로 바로 Get했다. 순차 키가 리전 하나에 몰리는 문제와 솔팅·해싱 접두사는 [HBase 행 키 글](/posts/hbase-serving-row-keys/)에 표로 있다.

## `hash % N`은 노드 하나만 늘어도 키 대부분을 옮긴다

노드가 N대에서 N+1대가 되면 `h % N == h % (N+1)`인 키만 자리를 지킨다. 대략 1/(N+1)이다. 4대에서 5대로 가면 키의 80%가 옮겨 간다. 캐시라면 거의 전부가 미스가 된다. Karger 등은 1997년 웹 캐시의 핫스팟 문제를 다루며 일관된 해싱을 제안했다. 노드와 키를 같은 해시 공간(링)에 놓고, 키는 자기 위치에서 시계 방향으로 처음 만나는 노드가 맡는다. 노드가 하나 늘면 그 노드가 들어간 자리의 이웃 구간만 넘겨받는다. Dynamo(2007)는 키를 MD5로 해시한 128비트 값을 링 위 위치로 썼다. 노드마다 점이 하나뿐이면 구간 길이가 들쭉날쭉하다. 그래서 노드 하나에 점(토큰) 여러 개를 주는 가상 노드를 쓴다. 표준 라이브러리만으로 차이를 볼 수 있다.

```python
# Python 3 표준 라이브러리만 사용 (3.14에서 실행)
import bisect, hashlib

def h(s):
    return int.from_bytes(hashlib.md5(s.encode()).digest()[:8], "big")

def ring(nodes, vnodes):
    pts = sorted((h(f"{n}#{i}"), n) for n in nodes for i in range(vnodes))
    return [p for p, _ in pts], [n for _, n in pts]

def owner(r, key):
    tokens, names = r
    return names[bisect.bisect(tokens, h(key)) % len(tokens)]

keys = [f"user:{i}" for i in range(100_000)]
old, new = ["n0", "n1", "n2", "n3"], ["n0", "n1", "n2", "n3", "n4"]
print("mod N", sum(h(k) % 4 != h(k) % 5 for k in keys) / len(keys))
for v in (1, 16, 256):
    r1, r2 = ring(old, v), ring(new, v)
    moved = sum(owner(r1, k) != owner(r2, k) for k in keys) / len(keys)
    load = max(sum(owner(r2, k) == n for k in keys) for n in new) / len(keys)
    print(f"vnodes={v}", round(moved, 3), "largest", round(load, 3))
```

`hash % N`은 키의 79.9%를 옮겼다. 링은 노드당 토큰이 1개일 때 41.5%, 16개일 때 16.8%, 256개일 때 20.5%를 옮겼고, 5대 중 가장 많이 맡은 노드의 몫은 41.5%, 24.1%, 22.8%였다. 이상값은 둘 다 20%다. 토큰이 하나면 새 노드가 큰 구간에 들어가 41.5%를 가져갔다. 정확한 숫자는 해시 함수와 노드 이름에 따라 달라진다.

## 토큰은 많을수록 좋은 것이 아니다

Dynamo 논문 6.2절은 세 전략을 비교했다. 처음에는 노드마다 무작위 토큰 T개를 주고 토큰 사이 구간을 파티션으로 썼다(전략 1). 이후 해시 공간을 같은 크기 Q개로 미리 자르고 노드마다 Q/S개를 맡기는 전략 3으로 옮겼고, 이 방식의 부하 균형이 가장 좋았으며 노드가 들고 있는 멤버십 정보는 천 분의 일 수준으로 줄었다. 핵심은 해시 공간을 자르는 일과 조각을 노드에 놓는 일을 분리한 것이다. 노드가 바뀌어도 조각 경계는 그대로이고 주인만 바뀐다.

Cassandra는 vnode를 쓴다. 3.x까지 기본 `num_tokens`는 256이었고, 5.0의 기본 `cassandra.yaml`은 `num_tokens: 16`과 `allocate_tokens_for_local_replication_factor: 3`이다. 토큰을 줄인 이유는 토큰 하나가 링 위 이웃을 최대 `2 * (RF - 1)`개 늘려서다. 이웃이 많을수록 장애로 일부 범위를 못 쓰게 될 확률이 커지고, repair 작업 수도 는다. 무작위 토큰 16개는 위 실험처럼 치우치므로, 복제 계수를 보고 토큰 위치를 정하는 할당기와 함께 쓴다.

## 재조정은 파티션 수를 어떻게 다루느냐로 갈린다

| 방식 | 파티션 수 | 노드를 늘리면 | 예 |
|---|---|---|---|
| 고정 개수 | 처음에 넉넉히 정하고 유지 | 기존 파티션 일부를 통째로 새 노드에 넘긴다 | Redis Cluster 16384 슬롯, Elasticsearch 샤드, Dynamo 전략 3 |
| 동적 분할 | 데이터 크기를 따라 늘어난다 | 커진 범위를 쪼개고 리전을 옮긴다 | HBase 리전 |
| 노드 비례 | 노드마다 토큰 수가 정해져 있다 | 새 노드가 여러 범위의 일부를 가져간다 | Cassandra vnode |

Redis Cluster는 `HASH_SLOT = CRC16(key) mod 16384`로 키의 슬롯을 정한다. 키와 슬롯의 관계는 바뀌지 않고, 리샤딩은 슬롯의 주인 노드만 바꾼다. 여러 키를 한 슬롯에 두려면 `{user:42}:cart`처럼 중괄호 안 해시 태그만 해시하게 한다. Elasticsearch의 `index.number_of_shards`는 인덱스를 만들 때만 정할 수 있다. 고정 개수 방식은 처음 정한 수가 너무 작으면 조각 하나가 커지고, 너무 크면 조각마다 드는 관리 비용이 쌓인다.

Kafka는 토픽마다 파티션 수가 고정이고, 브로커를 늘려도 파티션은 저절로 옮겨지지 않아 `kafka-reassign-partitions.sh`로 통째로 옮긴다. 수를 늘리면 키와 파티션의 대응이 바뀌고 줄일 수는 없다는 점은 [Kafka 파티션 글](/posts/kafka-partitions-and-lag/)에 있다. HBase는 리전이 정해진 크기를 넘으면 쪼갠다. 새 테이블은 리전 하나로 시작하므로 처음 쓰기는 서버 한 대가 다 받고, 이를 피하려고 분할 지점을 미리 주는 pre-split을 쓴다. 분할 정책과 reference 파일은 [HBase 읽기·쓰기 경로 글](/posts/hbase-read-write-path/)에 있다.

## 핫 키는 어떤 해시로도 흩어지지 않는다

해시는 서로 다른 키를 흩을 뿐이다. 같은 키는 몇 번을 해시해도 같은 파티션으로 간다. 팔로워가 수백만인 계정 하나, 오늘 날짜 하나처럼 키 자체에 요청이 몰리면 링 설계로는 풀리지 않는다. 쓰기가 몰리면 키를 늘린다. DynamoDB의 write sharding은 날짜 파티션 키에 `2014-07-09.1`부터 `.200`까지 접미사를 붙인다. 무작위 접미사면 그날 데이터를 읽을 때 200번 조회해 합쳐야 하고, 항목 하나를 바로 찾을 수도 없다. 주문 ID 같은 값에서 접미사를 계산하면 항목 하나는 바로 찾을 수 있지만, 하루 전체를 읽을 때는 여전히 200번 조회해 합친다. 솔팅은 쓰기 집중을 읽기 팬아웃으로 바꾸는 거래다. 읽기가 몰리면 복제본이나 캐시로 받는다. 결산 API는 모든 사용자가 읽는 공용 행을 애플리케이션 캐시에 두어 그 리전으로 읽기가 몰리지 않게 했다는 점이 행 키 글에 있다.

Spark 집계에서도 같은 일이 생긴다. AQE는 조인 skew는 쪼개지만 `GROUP BY`와 윈도 함수의 skew는 쪼개지 않는다([Spark 셔플 글](/posts/spark-shuffle-and-skew/)). 이때는 `GROUP BY key, salt`로 한 번 부분 집계하고 `GROUP BY key`로 다시 합치는 2단계 집계를 쓴다. 합계나 개수처럼 부분 결과를 다시 합칠 수 있는 집계에서만 된다.

## 보조 인덱스는 쓰기와 읽기 중 한쪽이 흩어진다

데이터를 파티션 키로 나누면 다른 컬럼으로 찾는 조회는 어느 파티션을 볼지 모른다. 보조 인덱스를 어디에 두느냐에 따라 비용이 쓰기 쪽이나 읽기 쪽으로 간다.

| 항목 | 로컬 인덱스 | 글로벌 인덱스 |
|---|---|---|
| 저장 위치 | 데이터와 같은 파티션 | 인덱스 키로 따로 파티셔닝 |
| 쓰기 한 건 | 같은 파티션 하나만 고친다 | 다른 노드의 인덱스 파티션도 고친다 |
| 파티션 키 없는 조회 | 모든 파티션에 묻고 합친다 | 인덱스 파티션 하나에 묻는다 |
| 예 | Cassandra SAI, Phoenix 로컬 인덱스, DynamoDB LSI | DynamoDB GSI, Phoenix 글로벌 인덱스 |

Cassandra 5.0의 SAI는 노드마다 SSTable에 붙여 인덱스를 쓴다. 파티션 키 없이 SAI 컬럼으로 조회하면 코디네이터가 토큰 순서대로 여러 범위에 병렬로 요청을 보내고, limit을 채울 때까지 라운드를 반복한다. HBase 자체에는 보조 인덱스가 없고 Apache Phoenix가 두 방식을 다 준다. Phoenix는 글로벌 인덱스를 읽기가 많은 경우에, 로컬 인덱스를 쓰기가 많은 경우에 권한다. 로컬 인덱스는 4.8부터 같은 테이블의 별도 컬럼 패밀리에 저장되고, 이 인덱스로 읽을 때는 모든 리전을 확인한다.

DynamoDB는 두 방식의 대가를 제약으로 드러낸다. LSI는 테이블의 파티션 키를 그대로 쓰고 정렬 키만 다르며, 테이블을 만들 때만 추가할 수 있다. 파티션 키 값 하나마다 테이블 항목과 인덱스 항목을 합쳐 10GB까지이고, 강한 일관성 읽기를 고를 수 있다. GSI는 아무 속성이나 키로 쓰고, 나중에 추가할 수 있으며, 처리 용량도 따로 갖는다. 대신 GSI 조회는 eventual consistency만 지원하므로, 방금 쓴 항목이 GSI 조회에 바로 나오지 않을 수 있다.

## 참고 자료

- Karger et al., [Consistent Hashing and Random Trees](https://dl.acm.org/doi/10.1145/258533.258660), STOC 1997
- DeCandia et al., [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf), SOSP 2007 (4.1, 4.2, 6.2절)
- [Apache Cassandra 5.0: Dynamo architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html), [cassandra.yaml (cassandra-5.0)](https://github.com/apache/cassandra/blob/cassandra-5.0/conf/cassandra.yaml), [SAI concepts](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/indexing/sai/sai-concepts.html)
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/), [Elasticsearch index settings](https://www.elastic.co/docs/reference/elasticsearch/index-settings/index-modules)
- [Amazon DynamoDB: Secondary indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html), [Write sharding](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-sharding.html)
- [Apache Phoenix: Secondary Indexing](https://phoenix.apache.org/secondary_indexing.html)
- [Spark 4.3 SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), [Apache Iceberg Spec: Partition Transforms](https://iceberg.apache.org/spec/#partition-transforms)
- [Apache HBase Reference Guide](https://hbase.apache.org/book.html): Regions, Pre-splitting

## English summary

The year-end recap jobs I built use the word "partition" in five ways: Hive table partitions, a window PARTITION BY, a Spark shuffle, mapPartitions, and HBase regions keyed by user ID. Range partitioning keeps range scans cheap but sends sequential keys to one node, while hash partitioning spreads writes but makes every range query touch all partitions. Plain hash mod N moves about 80% of keys when four nodes become five, so consistent hashing with virtual nodes moves only the new node's share, and Dynamo later fixed the number of equal-sized partitions to decouple partitioning from placement. No hash spreads a single hot key, so salting trades a write hotspot for read fan-out. Secondary indexes make the same trade: a local index keeps writes in one partition and pays with scatter-gather reads, and a global index answers from one partition but updates other nodes, often asynchronously.
