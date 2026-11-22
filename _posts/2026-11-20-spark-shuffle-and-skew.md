---
title: "Spark AQE가 셔플 뒤 실제 크기로 다시 정하는 것들"
date: 2026-11-20 04:50:00 +0900
categories: [Batch & Query, Spark]
tags: [spark, spark-sql, shuffle, data-skew, aqe, broadcast-join, window-function, concept]
description: "stage에서 몇몇 태스크만 오래 걸리는 원인인 셔플 파티션 쏠림을 중심으로, Exchange가 생기는 지점과 AQE가 셔플 뒤 실제 크기로 다시 정하는 것들을 정리한다."
---

같은 stage의 태스크 대부분이 금방 끝나는데 몇 개만 오래 붙잡혀 있다면, 원인은 보통 셔플 파티션 하나에 데이터가 몰린 것이다. [연말 결산 글](/posts/year-end-recap-spark/)의 잡들은 1년 치 재생 로그를 `RANK() OVER (PARTITION BY user_id ...)`로 사용자마다 순위를 매겼다. 이런 쿼리는 셔플이 어디서 생기는지, 파티션 하나가 얼마나 커지는지에 따라 실행 시간이 갈린다. 버전은 Spark 4.3 기준이다.

## 셔플은 실행 계획의 Exchange 하나마다 생긴다

연산자마다 입력 데이터가 어떻게 나뉘어 있어야 하는지 요구 조건이 있다. 지금 데이터의 분포가 그 조건을 만족하지 않으면 Spark가 계획 사이에 `Exchange`를 넣는다. 그 지점이 셔플이고 stage 경계다.

| 쿼리 모양 | 필요한 분포 | 계획에 보이는 것 |
|---|---|---|
| `GROUP BY user_id` | 같은 `user_id`가 한 파티션에 | `Exchange hashpartitioning(user_id, 200)` |
| 셔플 조인 (sort-merge) | 양쪽이 조인 키로 같은 방식으로 나뉨 | 양쪽에 `Exchange hashpartitioning` |
| `OVER (PARTITION BY user_id ...)` | 같은 `user_id`가 한 파티션에, 그 안에서 정렬 | `Exchange hashpartitioning` 뒤 `Sort`, `Window` |
| `ORDER BY` 전역 정렬 | 값 범위로 나뉨 | `Exchange rangepartitioning` |
| `DISTRIBUTE BY`, `repartition` | 사용자가 지정한 분포 | `Exchange ... REPARTITION_BY_COL` |
| 브로드캐스트 조인 | 작은 쪽 전체를 모든 executor에 | `BroadcastExchange`, 큰 쪽은 셔플 없음 |

`filter`, `select`, 파티션을 줄이기만 하는 `coalesce(n)`은 셔플을 만들지 않는다. 셔플 파티션 수는 `spark.sql.shuffle.partitions`로 정하고 기본값은 200이다.

결산 잡은 윈도 함수 뒤에 `DISTRIBUTE BY user_id SORT BY user_id, rnk`로 사용자 행을 한 파티션에 모아 `COLLECT_LIST`로 접었다. 윈도와 `DISTRIBUTE BY`가 둘 다 `user_id` 기준이라, 한 사용자의 행은 어느 셔플을 지나든 같은 파티션 하나에 모인다. 사용자 한 명의 행이 아주 많으면 그 파티션만 커진다는 뜻이기도 하다.

## 브로드캐스트 조인은 큰 쪽의 셔플을 없앤다

조인 한쪽이 작으면 그쪽을 통째로 모든 executor에 보내고, 큰 쪽은 셔플 없이 그 자리에서 조인한다. 기준은 `spark.sql.autoBroadcastJoinThreshold`이고 기본값은 10MB, `-1`이면 자동 브로드캐스트를 끈다. 브로드캐스트를 기다리는 시간은 `spark.sql.broadcastTimeout`(기본 300초)이다.

힌트로 직접 정할 수도 있다. `BROADCAST` 힌트는 통계상 크기가 임계값을 넘어도 그 테이블을 브로드캐스트한다. 양쪽에 다른 힌트가 붙으면 `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL` 순서로 우선한다. 조인 종류가 그 전략을 지원하지 않으면 힌트대로 되지 않을 수 있다. 아래 예의 테이블 이름은 설명을 위해 붙인 예시 이름이다.

```sql
SELECT /*+ BROADCAST(m) */ l.user_id, m.album_id, l.cnt
FROM user_track_cnt l JOIN track_meta m ON l.track_id = m.track_id;
```

결산 잡은 세션 설정에서 `autoBroadcastJoinThreshold`를 `-1`로 두고 브로드캐스트 타임아웃을 길게 잡았다. 결산 글에는 이렇게 둔 이유가 나오지 않으므로, 아래는 이 설정이 갖는 일반적인 트레이드오프다. 자동 브로드캐스트는 크기 추정치에 기대는데, 추정이 틀리면 큰 테이블이 드라이버와 executor 메모리로 올라갈 수 있다. 끄면 그 위험이 사라지는 대신, 메타 테이블과의 조인은 양쪽을 다 셔플하는 sort-merge 조인이 된다. 결산 잡은 원천 로그를 먼저 `(user_id, track_id, cnt)`로 줄인 뒤에 메타 테이블과 조인한다. 조인에 들어가는 행이 적을수록 셔플로 옮기는 양도 준다. AQE 쪽 임계값 `spark.sql.adaptive.autoBroadcastJoinThreshold`는 따로 지정하지 않으면 이 값을 그대로 따르므로, `-1`이면 실행 중 브로드캐스트 전환도 꺼진다.

## AQE가 실행 중에 바꾸는 것

AQE(Adaptive Query Execution)는 Spark 3.2.0부터 기본으로 켜져 있다. 셔플이 끝나면 각 파티션의 실제 크기를 알 수 있고, AQE는 그 숫자로 남은 계획을 다시 짠다.

| 하는 일 | 설정과 기본값 | 동작 |
|---|---|---|
| 작은 파티션 합치기 | `coalescePartitions.enabled=true` | 붙어 있는 작은 셔플 파티션을 하나의 태스크로 묶는다 |
| sort-merge를 브로드캐스트로 | `adaptive.autoBroadcastJoinThreshold` | 실제 크기가 임계값보다 작은 쪽이 있으면 브로드캐스트 해시 조인으로 바꾼다 |
| skew 조인 쪼개기 | `skewJoin.enabled=true` | 큰 파티션을 여러 조각으로 나누고 상대편 파티션을 복제해 붙인다 |

설정 이름 앞에는 모두 `spark.sql.adaptive.`가 붙는다.

합치기의 목표 크기는 `advisoryPartitionSizeInBytes`(기본 64MB)다. 그런데 `coalescePartitions.parallelismFirst`가 기본 `true`라서, 기본 상태에서는 64MB를 무시하고 최소 크기 1MB만 지키며 병렬도를 최대한 남긴다. 바쁜 클러스터에서는 이 값을 `false`로 두어야 64MB 기준이 적용되고 작은 태스크가 줄어든다.

[지표 배치 글](/posts/pyspark-indicator-batch/)의 job은 날짜 범위를 하루씩 돌며 `spark.sql`로 지표 하나를 계산한다. 하루 치 입력이 작으면 셔플 파티션 200개 대부분이 작은 파티션이 된다. 합치기는 이런 경우에 태스크 수를 줄인다.

실행 중 브로드캐스트 전환은 처음부터 브로드캐스트로 계획한 것보다는 덜 효율적이다. 양쪽 셔플은 이미 끝났기 때문이다. 대신 양쪽 정렬을 건너뛰고, `localShuffleReader.enabled`가 켜져 있으면 셔플 파일을 로컬에서 읽어 네트워크를 아낀다.

## skew 판정은 두 조건을 같이 본다

파티션 하나가 skew로 판정되려면 두 조건을 다 넘어야 한다.

- 크기가 전체 파티션 크기 중앙값의 `skewJoin.skewedPartitionFactor`배(기본 5.0)보다 크다.
- 크기가 `skewJoin.skewedPartitionThresholdInBytes`(기본 256MB)보다 크다.

중앙값이 1MB인데 한 파티션이 100MB라면 100배지만 256MB를 넘지 못해 그대로 둔다. 쪼갤 때는 `advisoryPartitionSizeInBytes`와 비skew 파티션 평균 중 큰 값을 기준으로 나눈다.

이 처리가 닿는 곳은 sort-merge 조인, shuffled hash 조인, `REBALANCE` 쪽 셔플이다. `GROUP BY`나 윈도 함수의 skew는 AQE가 쪼개지 않는다. 윈도 함수는 같은 `user_id`의 행이 전부 한 태스크에 있어야 순위를 매길 수 있어서, 행이 아주 많은 사용자 한 명은 그대로 긴 태스크 하나가 된다.

윈도 쪽에는 다른 장치가 있다. Spark 3.5.0부터 `RANK`, `DENSE_RANK`, `ROW_NUMBER` 결과를 `rnk <= N`처럼 바깥에서 거르면, 옵티마이저가 `WindowGroupLimit`을 넣는다. 셔플 전에 파티션마다 상위 N개만 남기는 Partial 단계와 셔플 뒤의 Final 단계로 나뉘어, 셔플로 넘어가는 행 수가 줄어든다. 결산 잡의 "사용자별 상위 N개" 쿼리도 이 모양이다. 쿼리 모양이 같다는 뜻이고, 결산 글에는 Spark 버전이 나오지 않으므로 당시 적용됐는지는 별개다. Spark 3.5 이상이라면 이 모양에 적용된다. N이 `spark.sql.optimizer.windowGroupLimitThreshold`(기본 1000)보다 크면 적용되지 않는다. 결산 글의 잡에는 솔팅이나 skew 조인 힌트 같은 skew 전용 코드가 없고, 행 수를 줄인 뒤에 조인하고 순위를 매기는 순서로 셔플 양을 다뤘다.

## explain과 Spark UI에서 보는 순서

먼저 실행 전에 `explain()`으로 `Exchange`가 몇 개인지, 조인이 어떤 전략으로 잡혔는지 본다. AQE가 켜져 있으면 맨 위에 `AdaptiveSparkPlan isFinalPlan=false`가 붙는다. 실행 전 계획이라는 뜻이다.

```text
# 줄인 출력, 모양만 보여 준다
AdaptiveSparkPlan isFinalPlan=false
+- Window [rank(cnt#5L) windowspecdefinition(user_id#1, cnt#5L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rnk#10], [user_id#1], [cnt#5L DESC NULLS LAST]
   +- Sort [user_id#1 ASC NULLS FIRST, cnt#5L DESC NULLS LAST], false, 0
      +- Exchange hashpartitioning(user_id#1, 200), ENSURE_REQUIREMENTS
```

실제로 바뀐 계획은 실행이 끝난 뒤 Spark UI의 SQL 탭에서 본다. 최종 계획은 `isFinalPlan=true`이고, AQE가 손댄 셔플 읽기는 `AQEShuffleRead` 노드에 `coalesced`나 `skewed`로 표시된다. `Exchange` 노드의 `shuffle bytes written`은 그 셔플이 쓴 양이다.

skew는 Stages 탭의 태스크 요약 표에서 드러난다. Duration과 Shuffle Read Size / Records의 Median과 Max가 크게 벌어져 있으면, 그 stage의 파티션 몇 개에 데이터가 몰린 것이다. 그 stage 앞의 `Exchange`가 어떤 키로 나눴는지 SQL 탭에서 찾으면 몰린 키를 좁힐 수 있다.

AQE 설정은 세션을 만들 때 한꺼번에 넘길 수 있다. Spark 3.4.0부터는 [SparkSession config(Map) 글](/posts/spark-session-config-map/)에서 다룬 맵 오버로드를 쓰면 된다.

```python
spark = (SparkSession.builder
         .config(map={
             "spark.sql.adaptive.coalescePartitions.parallelismFirst": False,
             "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
             "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "512m",
         })
         .getOrCreate())
```

PySpark는 `False` 같은 bool 값을 `"false"`로 바꿔 넣는다. `skewedPartitionThresholdInBytes`는 `advisoryPartitionSizeInBytes`보다 크게 둔다. 위 예시에서는 512MB와 128MB다.

## 참고 자료

- [Spark: Performance Tuning (AQE, join hints, coalesce hints)](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark: Web UI (Stages, SQL tab)](https://spark.apache.org/docs/latest/web-ui.html)
- [Spark: SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#spark-sql)
- [Spark 소스: InferWindowGroupLimit.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/InferWindowGroupLimit.scala)
- [Spark 소스: WindowGroupLimitExec.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala)
- [Spark 소스: AQEShuffleReadExec.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AQEShuffleReadExec.scala)
- [Spark 소스: ShuffleExchangeExec.scala (ShuffleOrigin)](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala)
- [SPARK-37099: window group limit for rank-based filter](https://issues.apache.org/jira/browse/SPARK-37099)

## English summary

A shuffle happens at each `Exchange` in the plan, where Spark must regroup rows by a key for an aggregation, a sort-merge join, a window `PARTITION BY`, or a `DISTRIBUTE BY`. AQE, on by default since Spark 3.2.0, reads the real partition sizes after each shuffle and then merges small partitions, switches joins to broadcast, and splits skewed join partitions that are both five times the median and over 256MB. It does not split skew in aggregations or window functions, but since 3.5.0 a filter like `rnk <= N` lets Spark keep only the top N rows per key before the shuffle. In my year-end recap jobs I turned off automatic broadcast joins and reduced log rows before joining metadata. With broadcast off, a wrong size estimate cannot pull a large table into memory, but both sides of every join are shuffled.
