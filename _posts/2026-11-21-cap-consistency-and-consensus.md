---
title: "네트워크가 끊기면 분산 저장소는 무엇을 내주나"
date: 2026-11-21 06:02:00 +0900
categories: [Platform & Infra, Distributed Systems]
tags: [cap, pacelc, consistency, quorum, raft, zookeeper, cassandra, s3, concept]
---

Cassandra에 `CONSISTENCY ONE`으로 쓰고 곧바로 `ONE`으로 읽으면, 읽기가 다른 복제본에 닿았을 때 옛 값이 나올 수 있다. 기본 설정의 HBase에서는 같은 일이 생기지 않는다. 둘 다 정상 동작이다. 차이는 저장소가 약속한 일관성 모델과, 복제본 몇 개가 응답해야 성공으로 치는지에서 나온다. CAP은 이 질문 중 네트워크가 끊긴 동안의 선택만 답한다.

버전은 Cassandra 5.0.9, ZooKeeper 3.9.7, Kafka 4.4.0, 2026년 11월의 Amazon S3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| CAP | 메시지가 사라질 수 있는 네트워크에서는 원자적 읽기·쓰기와 모든 요청에 대한 응답을 함께 보장할 수 없다는 정리 |
| 분할 내성(P) | 노드 사이 메시지가 사라질 수 있다는 전제. 설계자가 고를 수 없다 |
| PACELC | 분할 때는 가용성과 일관성, 평소에는 지연과 일관성 사이에서 고른다 |
| linearizability | 모든 연산이 호출과 응답 사이 한 순간에 실행된 것처럼 보인다. CAP의 C |
| sequential consistency | 모두가 같은 전체 순서를 보지만, 그 순서가 실제 시각을 따르지 않아도 된다 |
| causal consistency | 인과 관계로 이어진 연산만 모두에게 같은 순서로 보인다 |
| read-your-writes | 한 세션은 자기가 쓴 값 이후를 읽는다 |
| eventual consistency | 새 쓰기가 멈추면 결국 모든 복제본이 같은 값을 돌려준다 |
| quorum, R + W > N | 읽기와 쓰기에 응답하는 복제본 집합이 반드시 겹치도록 정한 수 |
| 합의(consensus) | 노드들이 하나의 로그 순서에 동의하는 일. Raft, ZAB, Paxos |
| 리더 기반, 리더리스 | 쓰기를 리더 한 곳이 받는지, 여러 복제본이 모두 받는지 |
| term, epoch | 리더 임기 번호. 낡은 리더의 요청을 걸러 내는 기준 |

## CAP은 분할이 난 동안에만 고르는 문제다

Gilbert와 Lynch(2002)가 증명한 정리는 범위가 좁다. C는 원자적 읽기·쓰기 레지스터, 지금 말로 linearizability다. A는 장애가 없는 노드가 받은 요청은 모두 결국 응답을 받는다는 뜻이다. P는 노드 사이 메시지가 얼마든지 사라질 수 있다는 가정이다. 이런 네트워크에서 C와 A를 함께 보장하는 알고리즘은 없다. 증명도 짧다. 노드를 {p1}과 나머지로 나누고 그 사이 메시지를 모두 버린다. p1이 쓰기를 받은 뒤 p2가 읽기를 받으면, p2는 그 쓰기가 있었는지 알 수 없다. 바로 답하면 옛 값을 줄 수 있고, 기다리면 답이 영영 없을 수 있다.

그래서 P는 셋 중 하나로 고르는 항목이 아니다. 노드가 둘 이상이고 그 사이가 네트워크라면 메시지는 사라진다. "CA 시스템"은 노드가 하나라는 말이거나, 분할이 나면 정확성을 포기한다는 말이다. Brewer도 2012년 글에서 "셋 중 둘"이라는 표현이 오해를 부른다고 했다. 분할이 없는 동안에는 C와 A를 함께 가질 수 있고, 선택은 분할이 난 동안에만 필요하다. CAP의 A는 운영에서 말하는 가용성보다 엄격하다. 과반 투표를 쓰는 시스템에서 소수 쪽 노드는 요청을 거절하므로 CAP 정의로는 가용하지 않다. 그래도 과반 쪽은 계속 읽기와 쓰기를 받는다. CP로 분류되는 시스템이 장애 중에도 대부분의 요청을 처리하는 이유다.

## PACELC는 평소의 지연까지 묻는다

CAP은 분할이 없을 때에 대해 말하지 않는다. Abadi(2012)는 많은 시스템이 일관성을 낮추는 이유가 분할보다 지연이라고 보고 PACELC로 묶었다. 분할(P)이 나면 가용성(A)과 일관성(C) 중에, 그렇지 않으면(E) 지연(L)과 일관성(C) 중에 고른다. 복제본 여러 곳의 확인을 기다리는 비용은 장애가 없어도 요청마다 낸다. 분류는 설정에 따라 움직여서, 연산마다 consistency level을 바꾸는 Cassandra는 한 클러스터 안에서도 EL과 EC가 섞인다.

| 시스템 (2012년 기본 설정) | 분류 | 근거 |
|---|---|---|
| Dynamo, Cassandra, Riak | PA/EL | 분할 때는 가용성, 평소에는 지연을 위해 일관성을 낮춘다. R + W를 올리면 EC 쪽으로 간다 |
| BigTable, HBase | PC/EC | 키 범위 하나를 서버 하나가 맡는다 |
| VoltDB/H-Store, Megastore | PC/EC | 완전한 ACID를 위해 지연과 가용성을 낸다 |
| PNUTS | PC/EL | 평소에는 지연을 고르고, 분할 때는 가용성을 낸다 |
| MongoDB | PA/EC | 평소에는 일관적이고, 주 노드가 분할되면 복제 안 된 쓰기를 rollback 디렉터리로 뺀다 |

## 일관성 모델은 무엇을 약속하는지로 나눈다

"강한 일관성"은 여러 모델을 뭉뚱그린 말이다. 아래로 갈수록 약속이 약하고, 약한 모델일수록 분할 중에도 응답하기 쉽다.

| 모델 | 약속 | 정의한 곳 |
|---|---|---|
| linearizability | 연산마다 호출과 응답 사이 한 시점이 있고, 결과는 그 시점 순서대로 하나씩 실행한 것과 같다 | Herlihy, Wing (1990) |
| sequential | 모두가 같은 전체 순서를 보고 각 프로세스 안의 순서는 지킨다. 실제 시각과는 어긋나도 된다 | Lamport (1979) |
| causal | 인과로 이어진 쓰기는 모두가 같은 순서로 본다. 동시 쓰기의 순서는 노드마다 달라도 된다 | Ahamad 외 (1995) |
| read-your-writes | 세션 안에서 내 쓰기 이후를 읽는다. 다른 세션의 쓰기는 약속하지 않는다 | Terry 외 (1994) |
| eventual | 새 쓰기가 없으면 결국 모든 복제본이 마지막 값을 돌려준다 | Vogels (2009) |

ZooKeeper는 한 시스템 안에서 두 모델이 갈리는 예다. 쓰기는 리더가 순서를 정하고 쿼럼이 확인해 linearizable이다. 읽기는 클라이언트가 붙은 서버가 바로 답하므로 다른 클라이언트의 최신 쓰기를 아직 못 봤을 수 있다. 한 클라이언트의 업데이트는 보낸 순서대로 적용된다. 다른 클라이언트의 쓰기를 반드시 봐야 하는 읽기라면 그 앞에 `sync()`를 부른다.

## R + W > N은 겹침을 보장할 뿐이다

Dynamo(2007)는 키마다 복제본 N개를 두고 읽기에 R개, 쓰기에 W개의 응답을 요구한다. R + W > N이면 읽기 집합과 쓰기 집합이 적어도 한 노드에서 겹친다. 지연은 R개(W개) 중 가장 느린 노드가 정하므로 R과 W는 보통 N보다 작게 둔다. 원래 Dynamo는 sloppy quorum을 썼다. 장애 중에는 선호 목록에서 건강한 앞쪽 N개 노드에 쓰고 hinted handoff로 나중에 돌려주므로, 그동안은 겹침이 깨질 수 있다.

Cassandra 5.0에서 `QUORUM`은 복제본의 과반(RF/2 + 1, 나눗셈은 버림)이다. 쓰기는 consistency level과 상관없이 모든 복제본으로 가고, level은 코디네이터가 기다릴 응답 수만 정한다. hint는 `ANY`에서만 응답으로 친다. RF가 3일 때 조합은 이렇다.

| 쓰기 | 읽기 | W + R | 겹침 | 복제본 1대가 내려가면 |
|---|---|---|---|---|
| ONE | ONE | 2 | 보장 안 됨 | 둘 다 성공 |
| QUORUM | QUORUM | 4 | 보장 | 둘 다 성공 |
| ALL | ONE | 4 | 보장 | 쓰기 실패 |

겹친다고 linearizable이 되지는 않는다. 쓰기가 모든 복제본으로 나가므로, `QUORUM` 응답을 못 받아 실패한 쓰기도 도착한 복제본에는 남는다. 이후 읽기가 그 복제본을 포함하는지에 따라 보이기도 하고 안 보이기도 한다. 같은 칸에 대한 동시 쓰기는 타임스탬프로 마지막 쓰기가 이기므로 클라이언트나 코디네이터의 시계에 기대고, NTP 같은 시간 동기화가 필요하다. Abadi도 R + W > N으로는 Gilbert와 Lynch의 일관성에 이르지 못한다고 봤다. 비교 후 쓰기가 필요하면 `IF` 조건을 붙인 lightweight transaction을 쓴다. Paxos를 거치므로 일반 쓰기보다 비싸다.

```sql
-- cqlsh, Apache Cassandra 5.0.9, 노드 3대
CREATE KEYSPACE shop
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
CREATE TABLE shop.coupon (code text PRIMARY KEY, owner text);

CONSISTENCY QUORUM;          -- 읽기와 쓰기 모두 2개 응답, 2 + 2 > 3
INSERT INTO shop.coupon (code, owner) VALUES ('A1', 'u1');
SELECT owner FROM shop.coupon WHERE code = 'A1';

SERIAL CONSISTENCY SERIAL;   -- IF 조건은 Paxos로 처리한다
INSERT INTO shop.coupon (code, owner) VALUES ('A2', 'u2') IF NOT EXISTS; -- 결과의 [applied]로 성공 여부를 본다
```

## 리더 기반 복제와 합의는 순서를 한 곳에서 정한다

리더 기반 복제는 리더 하나가 쓰기를 받아 순서를 정하고 팔로워가 그 순서를 따라간다. 리더리스 복제는 어느 복제본이든 코디네이터를 거쳐 쓰기를 받고, 충돌은 Dynamo의 vector clock이나 Cassandra의 타임스탬프로 푼다. 리더 기반에서 어려운 부분은 리더가 죽었을 때 다음 리더에 모두가 동의하는 일이고, 이것이 합의 문제다. Raft(Ongaro, Ousterhout 2014)는 이렇게 동작한다.

- 시간은 term으로 나뉜다. 팔로워가 election timeout 동안 리더 소식을 못 들으면 term을 올리고 후보가 된다. timeout을 무작위로 잡아(논문 예시 150~300ms) 표가 갈리는 일을 줄인다.
- 후보는 전체 서버 과반의 표를 받아야 리더가 된다. 서버는 term마다 한 표만 주고, 자기 로그가 후보보다 최신이면 표를 주지 않는다. 그래서 커밋된 항목은 이후 모든 리더의 로그에 있다.
- 리더가 현재 term에 만든 항목을 과반에 복제하면 커밋된다. 이전 term의 항목은 복제본 수를 세어 커밋하지 않고, 현재 term 항목이 커밋될 때 함께 커밋된다.
- 2f + 1대가 f대 장애를 견딘다. 5대면 2대다.
- 리더에서 읽는다고 바로 linearizable은 아니다. 새 리더는 term 시작에 빈 no-op 항목을 커밋하고, 읽기에 답하기 전에 과반과 하트비트를 주고받아 자기가 아직 리더인지 확인한다. 시계를 믿는 lease로 대신할 수도 있다.

ZooKeeper의 ZAB도 같은 뼈대다. 리더가 제안을 보내고 쿼럼이 확인하면 커밋한다. 트랜잭션 ID(zxid)는 리더가 바뀔 때 올라가는 epoch와 카운터로 이뤄지고, 새 리더는 쿼럼의 팔로워가 자기와 동기화된 뒤에야 활성화된다.

Kafka KRaft는 KIP-595가 "Raft 방언"이라 부르는 프로토콜이다. 리더가 AppendEntries로 밀어 넣는 대신 voter가 Fetch로 끌어오고, Fetch가 리더 생존 확인을 겸해 `controller.quorum.fetch.timeout.ms` 안에 성공 응답이 없으면 선거를 시작한다. term은 epoch, index는 offset이라 부른다. high watermark는 voter 과반에 복제된 가장 큰 오프셋이고, 새 리더는 자기 epoch의 control record를 쓰기 전에는 이를 올리지 않는다. Raft의 no-op과 같은 장치다. 과반 투표는 메타데이터 쿼럼에만 쓰고, 데이터 파티션이 ISR로 f + 1대에서 f대 장애를 견디는 방식은 [Kafka 복제와 ISR 글](/posts/kafka-replication-and-isr/)에 있다.

## 실제 시스템을 이 축에 놓으면

| 시스템 | 복제 방식 | 기본 일관성 | 분할이나 장애 때 |
|---|---|---|---|
| HBase | 리전마다 RegionServer 하나 | 행 단위 강한 일관성 | 죽은 서버의 리전은 복구까지 읽기와 쓰기 불가 |
| Cassandra 5.0 | 리더리스, RF개 복제본 | 연산마다 consistency level로 조절 | level을 채울 복제본이 닿으면 응답 |
| Kafka 4.4 파티션 | 리더와 ISR | 컨슈머는 high watermark까지만 읽는다 | ISR이 `min.insync.replicas`보다 적으면 `acks=all` 쓰기 거절 |
| KRaft, ZooKeeper 3.9 | 과반 합의 | 쓰기 linearizable | 과반이 있는 쪽만 쓰기를 받는다 |
| Amazon S3 | 관리형 | 객체 PUT, DELETE 뒤 GET, LIST가 새 상태를 본다 | 내부 구조는 공개하지 않는다 |

HBase가 행 단위로 강한 이유와 장애 복구 과정은 [HBase 쓰기와 읽기 경로 글](/posts/hbase-read-write-path/)에 있다. [연말 결산 Spark 글](/posts/year-end-recap-spark/)의 잡은 사용자마다 HBase 행 하나에 결과를 썼고, API는 사용자 결과를 그 행에 대한 `Get` 한 번으로 읽었다. 사용자 결과 읽기가 행 하나 안에서 끝나므로 HBase의 행 단위 보장 안에 들어가는 구조다.

S3는 2020년 12월 1일부터 strong read-after-write consistency를 준다. 그 전에는 덮어쓰기와 LIST가 늦게 반영될 수 있어서, EMR은 EMRFS consistent view로, Hadoop은 S3Guard로 일관성 층을 덧댔다. 지금도 남는 한계가 있다. 같은 키에 동시에 PUT하면 타임스탬프가 늦은 쪽이 이기고, S3는 동시 writer를 위한 객체 락을 주지 않는다. 키 여러 개를 원자적으로 바꾸는 방법도 없다. 버킷 설정은 eventual이라, 버전 관리를 처음 켠 뒤에는 15분 기다렸다가 쓰기를 권한다. 테이블 포맷이 커밋의 원자성을 S3가 아닌 카탈로그 포인터 교체에서 얻는 이유이고, 그 구조는 [Iceberg 카탈로그와 커밋 글](/posts/iceberg-catalogs-and-commits/)에 있다.

## 참고 자료

- [Gilbert, Lynch. Brewer's conjecture and the feasibility of consistent, available, partition-tolerant web services. SIGACT News, 2002](https://doi.org/10.1145/564585.564601)
- [Brewer. CAP twelve years later: How the "rules" have changed. IEEE Computer, 2012](https://doi.org/10.1109/MC.2012.37)
- [Abadi. Consistency Tradeoffs in Modern Distributed Database System Design. IEEE Computer, 2012](https://www.cs.umd.edu/~abadi/papers/abadi-pacelc.pdf)
- [Herlihy, Wing. Linearizability: a correctness condition for concurrent objects. ACM TOPLAS, 1990](https://doi.org/10.1145/78969.78972)
- [Lamport. How to Make a Multiprocessor Computer That Correctly Executes Multiprocess Programs. IEEE TC, 1979](https://doi.org/10.1109/TC.1979.1675439)
- [Ahamad et al. Causal memory: definitions, implementation, and programming. Distributed Computing, 1995](https://doi.org/10.1007/BF01784241)
- [Terry et al. Session guarantees for weakly consistent replicated data. PDIS, 1994](https://doi.org/10.1109/PDIS.1994.331722)
- [Vogels. Eventually consistent. CACM, 2009](https://doi.org/10.1145/1435417.1435432)
- [DeCandia et al. Dynamo: Amazon's Highly Available Key-value Store. SOSP, 2007](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Ongaro, Ousterhout. In Search of an Understandable Consensus Algorithm. USENIX ATC, 2014](https://raft.github.io/raft.pdf)
- [Junqueira, Reed, Serafini. Zab: High-performance broadcast for primary-backup systems. DSN, 2011](https://doi.org/10.1109/DSN.2011.5958223)
- [Apache Cassandra 5.0 Dynamo (replication, consistency levels)](https://cassandra.apache.org/doc/5.0/cassandra/architecture/dynamo.html)
- [Apache Cassandra 5.0 CQL Data Manipulation (IF conditions, timestamps)](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/dml.html)
- [ZooKeeper 3.9 Programmer's Guide (Consistency Guarantees)](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
- [ZooKeeper 3.9 Internals (atomic broadcast)](https://zookeeper.apache.org/doc/current/zookeeperInternals.html)
- [KIP-595: A Raft Protocol for the Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum)
- [Apache Kafka 4.4 Design: Replication](https://kafka.apache.org/44/design/design/)
- [Amazon S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel), [Amazon S3 now delivers strong read-after-write consistency (2020-12-01)](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications)

## English summary

The CAP theorem, as proved by Gilbert and Lynch, says that when the network can lose messages, no system can give both linearizable reads and writes and a reply to every request, so partition tolerance is a given and the real choice only appears during a partition. PACELC adds the everyday trade-off between latency and consistency, which every request pays even without failures. Quorums with R + W > N make read and write sets overlap but do not make Cassandra linearizable, because failed writes can linger and concurrent writes are resolved by timestamps; Raft, ZAB, and Kafka's KRaft instead agree on one log order through a majority and terms or epochs. My year-end recap jobs wrote one HBase row per user and the API read each user's results with one Get on that row, which stays inside HBase's per-row guarantee. The trade-off is the same everywhere: one owner per key or a majority vote buys consistency, and the price is waiting during failover or refusing requests on the minority side.
