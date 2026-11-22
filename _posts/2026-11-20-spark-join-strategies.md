---
title: "Spark 조인 전략은 크기 추정과 조인 조건으로 정해진다"
date: 2026-11-20 06:04:00 +0900
categories: [Batch & Query, Spark]
tags: [spark, spark-sql, join, broadcast-join, aqe, bucketing, concept]
description: "같은 조인이 BroadcastHashJoin이 될지 SortMergeJoin이 될지는 크기 추정, 조인 조건, 힌트로 정해진다. Spark의 물리 조인 전략과 선택 순서를 정리한다."
---

같은 조인이 실행 계획에서 `BroadcastHashJoin`으로 잡히면 큰 테이블은 셔플 없이 지나가고, `SortMergeJoin`으로 잡히면 양쪽 다 셔플하고 정렬한다. 어느 쪽이 될지는 Spark가 계획 시점에 추정한 크기, 조인 조건이 등호인지, 힌트, 그리고 실행 중에 AQE가 본 실제 크기로 갈린다. 셔플 자체와 AQE의 skew 처리는 [셔플과 skew 글](/posts/spark-shuffle-and-skew/)에 있다.

버전은 Spark 4.3.0 기준이다.

## 물리 조인은 다섯 가지다

| 전략 | 계획 노드 | 셔플 | 메모리에 올리는 것 | 쓰는 조건 |
|---|---|---|---|---|
| broadcast hash | `BroadcastHashJoin` | 큰 쪽은 없음 | 작은 쪽 전체의 해시 테이블, 모든 executor에 | 등호 조인 |
| shuffle hash | `ShuffledHashJoin` | 양쪽 | 파티션마다 build 쪽 해시 테이블 | 등호 조인 |
| sort-merge | `SortMergeJoin` | 양쪽, 그 뒤 정렬 | 정렬 버퍼 | 등호 조인, 정렬 가능한 키 |
| broadcast nested loop | `BroadcastNestedLoopJoin` | 큰 쪽은 없음 | 작은 쪽 전체 | 조건 제한 없음 |
| cartesian | `CartesianProduct` | 없음, 파티션 쌍마다 계산 | 오른쪽 파티션 행, 넘치면 디스크로 | inner 계열 조인 |

## 힌트가 없으면 위에서부터 차례로 시도한다

등호 조인은 이 순서로 처음 맞는 전략을 고른다.

1. 한쪽이 브로드캐스트할 만큼 작고 조인 종류가 허용하면 broadcast hash join.
2. `spark.sql.join.preferSortMergeJoin`이 false이고, 한쪽이 파티션별 해시 테이블을 만들 만큼 작으며 상대보다 훨씬 작으면 shuffle hash join.
3. 키를 정렬할 수 있으면 sort-merge join.
4. inner 계열이면 cartesian product.
5. 남은 경우는 broadcast nested loop join. OOM이 날 수 있는 마지막 선택이다.

`preferSortMergeJoin`은 내부 설정이고 기본값이 true다. 그래서 힌트나 AQE 없이 크기만으로 shuffle hash join이 잡히는 일은 기본 상태에서 없다. 큰 테이블끼리의 등호 조인이 sort-merge join이 되는 이유다. sort-merge join은 메모리를 덜 쓰고 양쪽이 다 클 때 효율적이다. shuffle hash join은 한쪽이 훨씬 작을 때 full outer join 같은 경우를 빠르게 할 수 있다.

등호가 없는 조인은 한쪽이 작으면 broadcast nested loop join, 아니면 inner 계열일 때 cartesian product, 그것도 아니면 다시 broadcast nested loop join이다. 어느 쪽이든 행 쌍마다 조건을 계산한다. 등호 키와 범위 조건이 함께 있으면 등호 키로 위 순서를 따르고, 범위 조건은 키가 맞은 행 쌍에만 계산한다. [SCD 글](/posts/dimensional-modeling-scd/)에서 사건 시각으로 차원 행을 찾는 조인이 이 모양이다.

| 조인 종류 | broadcast hash에서 브로드캐스트할 수 있는 쪽 |
|---|---|
| inner | 양쪽 다 |
| left outer, left semi, left anti | 오른쪽만 |
| right outer | 왼쪽만 |
| full outer | 없음 |

[연말 결산 글](/posts/year-end-recap-spark/)의 월별 곡과 계절 항목은 그해 사용자 목록에 `RIGHT OUTER JOIN`을 한다. 이 모양에서 브로드캐스트 후보는 왼쪽인 항목 결과뿐이고, 오른쪽 사용자 목록은 크기와 상관없이 브로드캐스트되지 않는다.

## 브로드캐스트 판단은 추정 크기로 한다

기준은 `spark.sql.autoBroadcastJoinThreshold`(기본 10MB)이고 `-1`이면 자동 브로드캐스트를 끈다. 비교하는 값은 논리 계획의 추정 크기 `sizeInBytes`다. 추정은 이렇게 만들어진다.

| 입력 | 추정 크기 |
|---|---|
| 파일 기반 테이블 | 파일 크기 합 x `spark.sql.sources.fileCompressionFactor`(기본 1.0) |
| 카탈로그 통계가 있는 테이블 | `ANALYZE TABLE`로 모은 통계 |
| 크기를 알 수 없는 입력 | `spark.sql.defaultSizeInBytes`(기본 Long.MaxValue), 브로드캐스트 후보에서 빠진다 |

추정이 실제와 어긋나는 지점이 둘 있다. 압축된 Parquet 10MB는 메모리에서 그보다 커지는데, 추정은 압축된 파일 크기를 그대로 쓴다. 그리고 CBO(`spark.sql.cbo.enabled`, 기본 false)가 꺼져 있으면 `WHERE` 필터와 `GROUP BY`는 행 수가 그대로라고 가정하고 컬럼 폭 비율만 반영한다. 파티션 프루닝은 다르다. 파일 기반 테이블에서 읽을 파티션이 정해지면 추정 크기도 남은 파일 크기로 줄어든다.

브로드캐스트 쪽은 driver를 거친다. `BroadcastExchange`가 작은 쪽 행을 driver로 모으고, 거기서 해시 테이블을 만든 뒤 executor로 보낸다. 그래서 크기 추정이 틀리면 driver 메모리가 먼저 부족해진다. 넘을 수 없는 상한도 있다. 크기는 `spark.sql.maxBroadcastTableSize`(기본 8GB, 4.1.0부터), 행 수는 5억 1200만 행이고 키가 long 하나가 아니면 약 3억 4천만 행이다. 전체를 기다리는 시간은 `spark.sql.broadcastTimeout`(기본 300초)이다. client 모드에서 driver 메모리를 코드로 바꿀 수 없다는 점은 [YARN 자원 글](/posts/spark-on-yarn-resources/)에 적었다. 결산 잡은 세션 설정에서 이 임계값을 `-1`로 두고 브로드캐스트 타임아웃을 길게 잡았다. 그 트레이드오프는 셔플 글에 있다. 덧붙이면 `-1`은 자동 판단만 끈다. `BROADCAST` 힌트를 단 쪽은 임계값과 상관없이 브로드캐스트된다.

## 힌트 네 가지와 우선순위

| 힌트 | 별칭 | 고르는 전략 |
|---|---|---|
| `BROADCAST` | `BROADCASTJOIN`, `MAPJOIN` | broadcast hash, 등호가 없으면 broadcast nested loop |
| `MERGE` | `SHUFFLE_MERGE`, `MERGEJOIN` | sort-merge |
| `SHUFFLE_HASH` | 없음 | shuffle hash |
| `SHUFFLE_REPLICATE_NL` | 없음 | cartesian product (inner 계열만) |

양쪽에 다른 힌트가 붙으면 `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL` 순서로 앞의 것이 이긴다. 양쪽 다 `BROADCAST`나 `SHUFFLE_HASH`면 통계상 작은 쪽을 build 쪽으로 쓴다. 테이블 이름은 설명용 예시다.

```sql
-- 적용되지 않는 힌트: left outer join은 왼쪽을 브로드캐스트할 수 없다
SELECT /*+ BROADCAST(l) */ *
FROM user_track_cnt l LEFT JOIN track_meta m ON l.track_id = m.track_id;
```

이 쿼리처럼 조인 종류가 힌트 전략을 지원하지 않으면, Spark는 `HintErrorLogger`로 경고만 남기고 힌트가 없을 때의 순서로 돌아간다. 쿼리는 실패하지 않으므로 계획을 열어 봐야 알 수 있다. DataFrame에서는 `cnt.join(broadcast(meta), "track_id")`나 `cnt.hint("merge")`처럼 단다.

## AQE는 실제 크기를 보고 다시 고른다

| 바꾸는 것 | 기준 | 기본 상태 |
|---|---|---|
| sort-merge를 broadcast hash로 | `spark.sql.adaptive.autoBroadcastJoinThreshold`, 지정하지 않으면 정적 임계값과 같다 | 켜짐 |
| sort-merge를 shuffle hash로 | 모든 파티션이 `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` 이하이고, 그 값이 `advisoryPartitionSizeInBytes`(기본 64MB) 이상 | 임계값 기본 0이라 일어나지 않음 |

AQE(`spark.sql.adaptive.enabled`)는 기본으로 켜져 있고, 첫 줄은 정적 추정이 크게 잡히는 쿼리에서 효과를 낸다. [지표 배치 글](/posts/pyspark-indicator-batch/)처럼 query builder의 SQL을 `spark.sql`로 돌리는 잡에 조인이 들어간다고 가정하면, 필터와 `GROUP BY`를 거친 쪽은 추정이 줄지 않는다. 실제로 임계값보다 작으면 AQE가 그 쪽 셔플이 끝난 시점에 브로드캐스트로 바꾸고, 정렬을 건너뛰며 큰 쪽 셔플 파일은 로컬로 읽는다. 셔플 쓰기는 이미 한 뒤라 처음부터 브로드캐스트로 계획한 것보다는 덜 아낀다.

## 버킷팅은 셔플을 쓰기 시점으로 옮긴다

조인 키로 미리 나눠 쓴 테이블은 읽을 때 셔플이 필요 없다. 버킷 테이블은 `saveAsTable`이나 `CREATE TABLE`로 만드는 영구 테이블에서만 된다.

```sql
CREATE TABLE play_cnt_b USING parquet
CLUSTERED BY (user_id) INTO 64 BUCKETS
AS SELECT user_id, track_id, cnt FROM play_cnt;
```

버킷 테이블을 읽는 scan은 출력 분포를 `HashPartitioning(버킷 컬럼, 버킷 수)`로 알린다. 양쪽이 조인 키와 같은 컬럼, 같은 버킷 수로 버킷팅되어 있으면 sort-merge join 앞의 `Exchange`가 계획에서 빠진다. 버킷 수가 다르면 기본 상태에서는 한쪽을 다시 셔플한다. `spark.sql.bucketing.coalesceBucketsInJoin.enabled`(기본 false)를 켜면 큰 쪽 버킷 수가 작은 쪽의 배수일 때 큰 쪽을 합쳐 맞춘다. 대신 병렬도가 줄어든다. 비용은 쓰는 쪽이 낸다. 버킷 수와 키는 테이블을 만들 때 정해지고, 다른 키로 조인하는 쿼리에는 도움이 되지 않는다.

## 계획에서 조인 노드 읽기

```text
SortMergeJoin [track_id#3], [track_id#10], Inner
:- Sort [track_id#3 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(track_id#3, 200), ENSURE_REQUIREMENTS
+- Sort [track_id#10 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(track_id#10, 200), ENSURE_REQUIREMENTS

BroadcastHashJoin [track_id#3], [track_id#10], Inner, BuildRight, false
:- FileScan parquet ...
+- BroadcastExchange HashedRelationBroadcastMode(...)
```

`BuildRight`는 오른쪽이 해시 테이블 쪽이라는 뜻이다. AQE가 켜져 있으면 실행 전 계획은 `isFinalPlan=false`이고, 실제로 바뀐 조인은 Spark UI SQL 탭의 최종 계획에서 확인한다. 실행 전에는 `SortMergeJoin`이던 노드가 최종 계획에서 `BroadcastHashJoin`으로 바뀌어 있으면 AQE가 전환한 것이다.

## 참고 자료

- [Spark: Performance Tuning (join hints, AQE)](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark: Hints](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)
- [Spark: Bucketing, Sorting and Partitioning](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning)
- [Spark 소스: SparkStrategies.scala (JoinSelection)](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala)
- [Spark 소스: joins.scala (JoinSelectionHelper)](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala)
- [Spark 소스: BroadcastExchangeExec.scala](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala)
- [Spark 소스: SizeInBytesOnlyStatsPlanVisitor.scala](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/statsEstimation/SizeInBytesOnlyStatsPlanVisitor.scala)
- [Spark 소스: PruneFileSourcePartitions.scala](https://github.com/apache/spark/blob/v4.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/PruneFileSourcePartitions.scala)
- [Spark 소스: SQLConf.scala](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)

## English summary

For an equi-join, Spark tries broadcast hash join first, then shuffle hash join only if `preferSortMergeJoin` is off, then sort-merge join, which is why large equi-joins end up as sort-merge joins. The broadcast decision uses an estimated size, and without CBO a filter or `GROUP BY` does not shrink that estimate, while partition pruning does. Hints override this in the order `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`, and a hint the join type cannot use is dropped with only a warning. AQE can switch a sort-merge join to a broadcast join once it sees the real size after the shuffle. My year-end recap jobs turned automatic broadcast off, which also turns off AQE's runtime switch, trading a shuffle on every unhinted join for a driver that never collects a misjudged table.
