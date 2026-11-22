---
title: "고유 사용자 수를 근사로 세고 스케치로 합치기"
date: 2026-11-21 05:25:00 +0900
categories: [Batch & Query, SQL]
tags: [hyperloglog, datasketches, trino, spark, athena, concept]
description: "일간 고유 사용자 수를 더해도 주간 값이 되지 않는 이유와, HyperLogLog 스케치로 고유 수를 근사해 합치는 방법을 Trino, Spark, Athena 기준으로 설명한다."
---

일간 고유 사용자 수 7개를 더하면 주간 고유 사용자 수가 되지 않는다. 그래서 주간 값은 원천 로그를 7일치 다시 읽어 `COUNT(DISTINCT user_id)`로 세는데, 이 집계는 사용자 ID를 전부 모아 중복을 지워야 끝난다. 월간, 분기, 플랫폼별로 축이 늘 때마다 원천을 다시 읽는다. 오차 1~2%를 받아들이면 하루마다 몇 KB짜리 스케치를 저장해 두고, 주간과 월간은 스케치를 합쳐서 낼 수 있다.

버전은 Trino 484, Spark 4.3, Athena engine version 3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| cardinality | 집합 안의 서로 다른 값 개수 |
| non-additive | 부분 결과를 더해도 전체 값이 되지 않는 지표. 고유 수, 비율, 백분위수 |
| HyperLogLog(HLL) | 해시 값의 앞자리 0 개수로 고유 수를 추정하는 스케치 |
| register (bucket) | HLL이 해시 일부로 고른 칸. 그 칸에 들어온 최대 값만 들고 있다 |
| standard error | 추정값이 참값에서 벗어나는 표준 편차의 비율. HLL은 약 1.04/√m |
| mergeable sketch | 따로 만든 두 스케치를 합치면 합집합의 스케치가 되는 성질 |
| HLL++ | 64비트 해시와 작은 수 보정을 더한 HLL 변형. Spark `approx_count_distinct`가 쓴다 |
| Theta sketch | 기준값 θ 아래의 해시만 표본으로 남겨 교집합과 차집합까지 되는 스케치 |
| T-digest, q-digest | 분포를 요약해 백분위수를 근사하고 합칠 수 있는 구조 |
| KLL sketch | 순위 오차를 보장하는 분위수 스케치. Spark 4.1부터 SQL 함수가 있다 |
| GK summary | Greenwald-Khanna 분위수 요약. Spark `percentile_approx`의 구현 |

## 정확한 고유 수가 비싼 이유

`SUM`은 그룹마다 숫자 하나만 들고 다니면 된다. 파티션마다 부분합을 내고 마지막에 더하면 끝난다. `COUNT(DISTINCT)`는 부분 결과가 숫자가 아니라 값의 집합이다. 같은 사용자가 여러 파티션에 있으니 최종 단계에서 집합끼리 중복을 지워야 하고, 그래서 고유 값 전체가 셔플을 타고 메모리나 디스크에 모인다. 고유 값이 많을수록 비용이 값 수에 비례해 커진다.

한 쿼리에 서로 다른 컬럼의 `COUNT(DISTINCT)`가 여러 개 있으면 Spark는 `RewriteDistinctAggregates` 규칙으로 계획을 바꾼다. `Expand`가 입력 행을 distinct 그룹 수만큼 복제하고, 두 단계 집계로 각 결과를 모은다. 행이 몇 배로 불어난 채 셔플된다.

더 큰 문제는 결과를 재사용할 수 없다는 점이다. 일간 고유 수가 합으로 주간 값을 만들지 못한다는 이야기와 지표 테이블에 분자·분모를 따로 두는 방법은 [지표 테이블 grain 글](/posts/metric-table-grain/)에 적었다. 그 글은 주간 값을 원천에서 7일치 한 번에 세는 방법을 적었다. 정확한 값을 얻는 대신 기간과 축마다 원천을 다시 읽는다. 스케치는 그 반대편 선택지다.

## HyperLogLog는 앞자리 0의 개수를 센다

해시가 고르게 퍼진다면, 앞자리에 0이 k개 연속된 해시는 2^k개 중 하나꼴로 나온다. 지금까지 본 가장 긴 0의 줄이 길수록 서로 다른 값을 많이 봤다는 뜻이다. 값 하나로 추정하면 분산이 너무 크니, HLL은 칸을 여러 개 둔다.

1. 값을 64비트로 해시한다.
2. 앞 p비트로 칸(register) 번호를 고른다. 칸은 m = 2^p개다.
3. 나머지 비트에서 처음 1이 나오는 위치(앞자리 0 개수 + 1)를 구한다.
4. 그 칸에 이 값보다 큰 값이 없으면 바꾼다. 칸마다 최댓값만 남는다.
5. 추정값은 α·m² / Σ 2^(-M[j])다. 칸 값들의 조화 평균을 쓰므로 운 좋게 튄 칸 하나가 결과를 끌어올리지 못한다. 빈 칸이 많을 때는 linear counting으로 보정한다.

Spark의 `HyperLogLogPlusPlusHelper`가 이 순서를 그대로 구현한다. XxHash64로 해시하고, 위쪽 p비트를 칸 번호로, 나머지 비트의 `numberOfLeadingZeros + 1`을 칸 값으로 쓴다.

표준 오차는 약 1.04/√m이고 메모리는 칸 수에 비례할 뿐 고유 값 수와 상관없다. 칸을 4배로 늘려야 오차가 반이 된다.

| 엔진 함수 | 칸 수 | 1.04/√m |
|---|---|---|
| Trino `approx_set` | 4096 | 약 1.6% |
| Trino `approx_distinct` 기본 | 오차 2.3% 목표 (2048칸에 해당) | 약 2.3% |
| Spark `approx_count_distinct` 기본 `relativeSD` 0.05 | p = ⌈2·log2(1.106/0.05)⌉ = 9, 512칸 | 약 4.6% |
| Spark `hll_sketch_agg` 기본 `lgConfigK` 12 | 4096 (`lgConfigK` 4~21 허용) | DataSketches 추정기라 식이 다르다 |

Trino `approx_distinct(x, e)`는 `e`를 0.0040625에서 0.26 사이로 받는다.

## 하루 스케치를 저장하고 주와 달은 합친다

두 HLL을 칸별로 최댓값을 취해 합치면, 두 입력의 합집합으로 처음부터 만든 스케치와 같다. 해시 함수와 칸 수만 같으면 된다. 그래서 숫자는 더할 수 없지만 스케치는 합칠 수 있다. 날짜 하나에 스케치 하나를 저장해 두면 주간, 월간, 임의 기간은 스케치 merge로 끝난다.

Spark 3.5부터 Apache DataSketches 기반의 HLL 함수가 SQL에 있다. 아래는 `spark-sql` 4.3에서 그대로 돌아간다.

```sql
-- Spark 4.3: 날짜별 스케치를 만들고 기간 합집합으로 추정
WITH events(dt, platform, user_id) AS (
  VALUES (DATE'2026-11-16', 'web', 1), (DATE'2026-11-16', 'app', 2),
         (DATE'2026-11-17', 'web', 1), (DATE'2026-11-17', 'web', 2),
         (DATE'2026-11-18', 'app', 2), (DATE'2026-11-18', 'app', 4)
),
daily AS (
  SELECT dt, platform, hll_sketch_agg(user_id, 12) AS sk  -- BINARY로 저장 가능
  FROM events GROUP BY dt, platform
)
SELECT platform,
       hll_sketch_estimate(hll_union_agg(sk)) AS users_3d   -- web 2, app 2
FROM daily GROUP BY platform
UNION ALL
SELECT 'all', hll_sketch_estimate(hll_union_agg(sk)) FROM daily;  -- 3
```

`all` 행은 3이다. 사용자 2가 웹과 앱 양쪽에 있어서 숫자를 더하면 4지만, 스케치를 합치면 한 번만 센다. `hll_union_agg`는 `lgConfigK`가 다른 스케치를 기본으로 거부하고, `allowDifferentLgConfigK`를 true로 주면 합친다.

Trino에서는 `approx_set(user_id)`를 `varbinary`로 바꿔 날짜별 테이블에 넣고, 읽을 때 `cardinality(merge(CAST(hll AS HyperLogLog)))`로 기간을 합친다. `empty_approx_set()`은 빈 스케치다. Athena engine version 3도 함수가 Trino 기반이라 HyperLogLog, quantile digest, T-digest, set digest 함수를 같은 이름으로 쓴다.

엔진 사이에서 HLL 스케치를 섞을 수는 없다. Trino는 airlift의 HyperLogLog(Murmur3 해시, sparse/dense 형식)를, Spark는 DataSketches의 `HllSketch`(`HLL_8`)를 쓰므로 해시와 바이트 형식이 모두 다르다. Trino 484에는 Spark HLL 스케치를 읽는 함수가 없다. 스케치 테이블은 쓰는 엔진을 정하고 만든다.

## 합집합은 되고 교집합은 어렵다

HLL은 칸마다 최댓값만 남기므로 합집합만 자연스럽다. "웹과 앱을 모두 쓴 사용자"는 |A| + |B| - |A∪B|로 구할 수 있지만, 세 추정값의 오차가 모두 결과에 얹힌다. 교집합이 작을수록 오차가 결과보다 커질 수 있다.

Theta sketch는 해시를 [0, 1) 구간의 수로 보고 기준값 θ 아래의 해시만 남긴다. 남은 해시 수를 θ로 나눈 값이 추정값이고, 두 스케치의 실제 해시 표본이 있으므로 교집합과 차집합(AnotB)을 표본끼리 계산한다. 결과의 θ는 두 입력 중 작은 쪽이 되고, 그 입력이 오차를 정한다. 작은 교집합은 표본이 거의 남지 않아 오차가 커지지만, 그래도 inclusion-exclusion보다는 보통 훨씬 정확하다. 단일 스케치의 상대 표준 오차는 1/√(k-1) 이하다.

Spark 4.1부터 `theta_sketch_agg`, `theta_union_agg`, `theta_intersection_agg`, `theta_union`, `theta_intersection`, `theta_difference`, `theta_sketch_estimate`가 있다. `theta_union`의 `lgNomEntries`는 4~26이고 기본 12다. Trino는 Theta 스케치를 만들지 못한다. `datasketches` 커넥터의 `theta_sketch_union`, `theta_sketch_cardinality`로 외부에서 만든 Theta 스케치를 합치고 셀 수만 있다. Trino에서 겹침을 추정할 때는 set digest를 쓴다. HLL과 MinHash를 함께 들고 있어서 `intersection_cardinality`와 `jaccard_index`로 계산한다.

## 백분위수도 더할 수 없다

일간 p95 7개의 평균은 주간 p95가 아니다. 정확한 백분위수는 값을 전부 정렬해야 하고, 부분 결과로는 다시 만들 수 없다. 여기도 합칠 수 있는 요약을 쓴다.

| 엔진 | 함수 | 정확도 조절 |
|---|---|---|
| Trino | `approx_percentile(x, p)`, `approx_percentile(x, w, p)` | 정확도 인자 없음. 내부는 T-digest다 |
| Trino | `qdigest_agg(x, w, accuracy)`, `tdigest_agg(x)` → `merge`, `value_at_quantile` | q-digest는 `accuracy` (0, 1). 둘 다 `varbinary`로 저장 |
| Spark | `percentile_approx(col, p, accuracy)` | 기본 10000, 상대 오차 1/accuracy. GK summary |
| Spark 4.1+ | `kll_sketch_agg_bigint/double/float(expr, k)` → `kll_sketch_get_quantile_*` | `k` 기본 200, 8~65535 |

Spark `percentile_approx`는 결과 값만 돌려주므로 날짜별로 저장해 합칠 수 없다. 기간 합산이 필요하면 KLL 스케치를 저장한다. Trino의 q-digest는 `bigint`, `double`, `real`만 받는다.

## 근사값을 쓰면 안 되는 자리

스케치는 "몇 명인가"에 답하고 "누구인가"에는 답하지 못한다. 고유 수만 남고 ID 목록은 없으므로 특정 사용자를 빼거나, 결과를 다시 사용자 테이블과 조인할 수 없다. HLL 칸은 최댓값이라 한번 들어간 값을 지울 수도 없다.

과금, 정산, 계약 보고처럼 숫자 하나가 돈이나 약속이 되는 자리는 정확한 집계를 쓴다. A/B 실험에서 두 그룹 차이가 1~2%라면 HLL 오차와 크기가 같아 결론이 오차에 묻힌다. 고유 값이 수천 개 수준이면 정확한 집계도 싸다. 그런 지표에는 근사의 이득이 없다.

근사가 맞는 자리는 대시보드의 추세와 탐색이다. 수십 개 지표를 날짜·나라·주기별로 배치로 만드는 구조는 [서비스 지표 배치 글](/posts/pyspark-indicator-batch/)에 있다. 그런 배치에 고유 사용자 지표를 하나 추가한다면 날짜별 스케치 컬럼을 두고 주간·월간은 merge로 낼 수 있다. 원천 재계산을 줄이는 대신 숫자마다 약 2%의 오차 범위를 함께 설명해야 한다.

## 참고 자료

- [Trino 484: Aggregate functions (approx_distinct, approx_percentile)](https://trino.io/docs/current/functions/aggregate.html)
- [Trino 484: HyperLogLog functions](https://trino.io/docs/current/functions/hyperloglog.html)
- [Trino 484: Quantile digest functions](https://trino.io/docs/current/functions/qdigest.html)
- [Trino 484: T-Digest functions](https://trino.io/docs/current/functions/tdigest.html)
- [Trino 484: Set Digest functions](https://trino.io/docs/current/functions/setdigest.html)
- [Trino 484: DataSketches functions](https://trino.io/docs/current/functions/datasketches.html)
- [Trino source: ApproximateDoublePercentileAggregations](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/operator/aggregation/ApproximateDoublePercentileAggregations.java)
- [Spark 4.3: Aggregate functions](https://spark.apache.org/docs/latest/api/sql/agg-functions/)
- [Spark 4.3: Sketch functions](https://spark.apache.org/docs/latest/api/sql/sketch-functions/)
- [Spark source: HyperLogLogPlusPlusHelper](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/HyperLogLogPlusPlusHelper.scala)
- [Spark source: RewriteDistinctAggregates](https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/RewriteDistinctAggregates.scala)
- [Athena engine version 3 functions](https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html)
- [Apache DataSketches: Theta Sketch Accuracy](https://datasketches.apache.org/docs/Theta/ThetaAccuracy.html)
- [Apache DataSketches: Theta Sketch Set Operations Accuracy](https://datasketches.apache.org/docs/Theta/ThetaSketchSetOpsAccuracy.html)
- [Flajolet et al., HyperLogLog (2007)](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
- [Heule et al., HyperLogLog in Practice (2013)](https://research.google/pubs/hyperloglog-in-practice-algorithmic-engineering-of-a-state-of-the-art-cardinality-estimation-algorithm/)

## English summary

COUNT(DISTINCT) has to gather every distinct value, and its results cannot be added across days or segments. HyperLogLog hashes each value, uses some bits to pick a register, keeps the longest run of leading zeros per register, and estimates the count with a harmonic mean, giving a standard error of about 1.04/√m. Because two sketches merge by taking the register-wise maximum, a table of daily sketches can answer weekly and monthly unique counts in Trino (approx_set, merge, cardinality) or Spark 3.5+ (hll_sketch_agg, hll_union_agg, hll_sketch_estimate), while Theta sketches add intersections and differences and T-digest or KLL sketches do the same for percentiles. My metric table post counts weekly users from raw logs for exact numbers; daily sketches would trade that exactness for a fixed 1-2% error and far less rescanning, which fits dashboards but not billing.
