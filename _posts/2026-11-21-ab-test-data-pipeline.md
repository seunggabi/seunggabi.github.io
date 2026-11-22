---
title: "배정부터 SRM 검사까지 A/B 테스트 데이터 파이프라인"
date: 2026-11-21 05:27:00 +0900
categories: [Batch & Query, SQL]
tags: [ab-test, experimentation, spark, sql, statistics, concept]
description: "50:50 실험에서 대조군과 실험군 인원이 어긋나는 SRM 문제를 출발점으로, 해시 버킷팅 배정부터 노출 로깅, SRM 검사까지 A/B 테스트 데이터 파이프라인을 정리한다."
---

50:50으로 나눈 실험의 분석 테이블에 대조군 100,000명, 실험군 98,000명이 들어왔다고 하자. 비율로는 50.5% 대 49.5%라 우연처럼 보이지만 카이제곱 검정의 p-value는 0.00001보다 작다. 이 상태에서 실험군 전환율이 올랐다는 숫자는 믿을 수 없다. 빠진 2,000명이 왜 빠졌는지가 결과와 엮여 있을 수 있기 때문이다. A/B 테스트가 틀리는 곳은 통계 공식보다 배정, 로그, 조인 같은 데이터 쪽인 경우가 많다. 용어와 검사 기준은 Kohavi, Tang, Xu의 *Trustworthy Online Controlled Experiments*(2020)와 Microsoft 실험 플랫폼(ExP) 팀의 논문을 따랐다.

버전은 Spark 4.3 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 해시 버킷팅 | `hash(salt, 단위 ID) mod N`으로 버킷을 정하고 버킷 범위로 variant를 고른다 |
| 실험별 salt | 실험마다 해시 입력을 바꿔 이전 실험의 배정과 독립되게 한다 |
| 무작위 단위, 분석 단위 | 배정을 나누는 단위(보통 사용자)와 지표 분모가 세는 단위(세션, 페이지뷰) |
| 노출(트리거) | 바뀐 화면이나 로직에 단위가 실제로 닿은 시점 |
| counterfactual 로깅 | 대조군도 실험군이었다면 바뀌었을 지점에서 같은 노출 로그를 남긴다 |
| SRM | 관측한 variant 비율이 설정한 비율과 다른 상태 |
| delta method | 비율 지표의 분산을 무작위 단위별 합계로 근사하는 방법 |
| CUPED | 실험 전 기간의 같은 지표로 분산을 줄이는 보정 |
| peeking | 결과를 계속 보다가 유의해지는 순간 멈추는 것. 거짓 양성이 늘어난다 |
| 가드레일 지표 | 목표는 아니지만 나빠지면 안 되는 지표와 신뢰성 검사 |

## 실험마다 salt를 바꾼 해시로 배정한다

배정은 저장된 표를 찾지 않고 함수로 정한다. 같은 입력은 언제나 같은 버킷이 되므로 서버가 몇 대든, 몇 번 다시 계산하든 같은 사용자는 같은 variant를 받는다. 이것이 sticky 배정이다. 배정 결과를 따로 저장하지 않아도 되고, 분석 쪽에서 같은 함수로 다시 계산해 맞는지 확인할 수도 있다. 그러려면 해시 함수는 어느 언어에서나 같은 값이 나오는 것을 고른다. Spark의 `xxhash64`는 seed 42를 쓰고 음수도 돌려주므로(`%` 대신 `pmod`가 필요하다) 앱 쪽 xxHash 라이브러리와 값이 다를 수 있다. md5는 구현과 상관없이 같은 16진 문자열을 돌려준다.

```sql
-- Spark 4.3. Python의 int(md5(...).hexdigest()[:8], 16) % 1000과 같은 값
SELECT user_id,
       CAST(conv(substr(md5(concat('checkout_button_v2', ':', user_id)), 1, 8), 16, 10) AS BIGINT) % 1000 AS bucket
FROM users;
```

salt가 없으면 모든 실험에서 버킷 0~499가 늘 같은 사람이다. 앞 실험이 이 사람들의 행동을 바꿔 놓으면 다음 실험의 실험군에 그 영향이 남는다. Kohavi 등(2012)이 보고한 사례에서는 이 carryover 효과가 실험이 끝난 뒤 3주가량 이어졌고, 나쁜 경험을 준 버그가 있던 실험은 3개월이 지나도 원래 수준으로 돌아오지 않았다. 그래서 실험마다 다른 해시 seed로 다시 섞는다. 위 예에서 실험 ID를 salt로 넣은 이유다. 다시 섞은 배정은 실험 전 기간의 로그에 그대로 적용할 수 있다. 실험을 켜기 전 며칠을 새 배정으로 나눠 비교하면 시간을 들이지 않는 회고형 A/A 테스트가 되고, 핵심 지표가 이미 갈려 있으면 salt를 바꿔 다시 나눈다.

비율을 키울 때도 해시는 그대로 둔다. 실험군을 `bucket < T`, 대조군을 `bucket >= 1000 - T`로 두고 T를 50에서 500으로 늘리면, 새로 들어오는 사람은 원래 실험 밖에 있던 가운데 버킷뿐이고 variant를 바꾸는 사람은 없다. 실험 중에 salt를 바꾸면 모두가 다시 섞여 sticky가 깨진다.

무작위 단위는 오래 유지되는 ID여야 한다. [로그 수집 API 글](/posts/log-collection-api/)의 트랜잭션 ID 쿠키는 30분 동안 요청이 없으면 새 UUID가 된다. 세션을 묶는 데는 맞지만, 사용자 단위 실험의 키로 쓰면 같은 사람이 다음 방문에서 다른 variant를 받을 수 있다. 로그인 ID나 오래 유지되는 기기 ID가 무작위 단위 후보다.

## 배정 로그와 노출 로그는 다른 질문에 답한다

배정은 이 사용자가 어느 variant인가에 답하고, 노출은 이 사용자가 바뀐 부분에 실제로 닿았는가에 답한다. 결제 버튼 실험에서 앱을 연 모든 사용자에게 배정이 이뤄져도 결제 화면까지 오는 사람은 일부다. 결제 화면에 오지 않은 사람은 어느 쪽이든 경험이 같으므로, 이들을 분석에 넣으면 차이를 만드는 사람은 그대로인데 분모와 잡음만 늘어 효과가 희석된다. Kohavi의 책은 이것을 triggering으로 다루고, 노출된 사용자만 분석하라고 한다.

노출 로그는 두 variant에서 같은 코드 위치, 같은 조건으로 남겨야 한다. 대조군은 화면이 바뀌지 않으니 로그를 빼먹기 쉽다. 대조군이 실험군이었다면 바뀌었을 지점에서 남기는 로그가 counterfactual 로깅이다. 이것이 없으면 노출 사용자 수가 실험군 쪽으로 크게 기운 SRM이 생긴다. 노출 로그에는 `experiment_id`, `variant`, `user_id`, `exposure_ts`가 있으면 된다. 노출 로그는 사용자마다 여러 번 찍힌다. 분석에는 실험과 사용자별 첫 노출 시각 하나만 남긴다. 한 사용자가 두 variant에 모두 찍혔다면 ID가 바뀌었거나 캐시가 다른 variant를 보여 준 것이므로 따로 세어 둔다. 지표 이벤트는 첫 노출 이후, 실험 종료 전 것만 붙인다. 노출 전에 일어난 구매는 처리의 영향을 받을 수 없다. 기준 시각과 시간 창을 정하는 문제는 [퍼널 분석 글](/posts/funnel-analysis/)의 전환 시간 창과 같다.

## 사용자 x 실험 한 행짜리 지표 테이블

분석은 원천 이벤트를 다시 읽지 않고, 실험과 사용자마다 한 행인 테이블 하나에서 끝나게 만든다. 행에는 variant, 첫 노출 시각, 그리고 더할 수 있는 사용자별 합계를 넣는다.

```sql
-- Spark 4.3. 테이블과 컬럼 이름은 지어낸 것이다
CREATE OR REPLACE TEMP VIEW user_metrics AS
WITH first_exposure AS (
  SELECT user_id, MIN(exposure_ts) AS first_ts,
         MIN(variant) AS variant, COUNT(DISTINCT variant) AS variant_cnt
  FROM exposures
  WHERE experiment_id = 'checkout_button_v2'
  GROUP BY user_id
)
SELECT f.user_id, f.variant, f.first_ts,
       COUNT(e.event_ts) FILTER (WHERE e.event_name = 'page_view') AS page_views,
       COUNT(e.event_ts) FILTER (WHERE e.event_name = 'click')     AS clicks,
       COUNT(e.event_ts) FILTER (WHERE e.event_name = 'purchase')  AS purchases
FROM first_exposure f
LEFT JOIN events e
  ON e.user_id = f.user_id
 AND e.event_ts >= f.first_ts
 AND e.event_ts < TIMESTAMP '2026-11-15 00:00:00'
WHERE f.variant_cnt = 1
GROUP BY f.user_id, f.variant, f.first_ts;
```

`LEFT JOIN`이어야 노출됐지만 아무 이벤트도 없는 사용자가 0으로 남는다. inner join을 쓰면 이벤트가 없는 사용자가 빠지고, 실험군이 이벤트를 더 만들면 두 variant의 사용자 수가 달라진다. 전환 여부는 `purchases > 0`으로 읽을 때 만든다. 매일 실험 시작일부터 전부 다시 읽기 부담스러우면 `(experiment_id, user_id, dt)` grain으로 하루치 합계를 쌓고, 분석할 때 사용자별로 더한다. 이벤트 수는 날짜로 더해도 되는 additive 값이라 가능하다. 고유 사용자 수나 비율을 날짜 행으로 저장하면 이 합산이 깨진다는 점은 [지표 테이블 grain 글](/posts/metric-table-grain/)과 같다. 늦게 온 이벤트는 최근 며칠을 다시 계산해 덮어쓰면 되고, 날짜 범위를 받아 unique 키로 덮어쓰던 [서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 job 구조가 그대로 맞는다.

## 무작위 단위와 분석 단위가 다르면 분산이 달라진다

사용자 전환율은 무작위 단위와 분석 단위가 모두 사용자다. 사용자 행끼리 독립이라고 보고 두 비율을 비교하면 된다. 클릭률(CTR)을 클릭 수 / 페이지뷰 수로 정의하면 분석 단위는 페이지뷰가 된다. 같은 사람의 페이지뷰끼리는 서로 닮았으므로, 페이지뷰를 독립 표본으로 놓고 분산을 구하면 실제보다 작게 나온다. Deng 등(2018)의 클러스터 시뮬레이션에서 이렇게 구한 표준오차는 0.00522, 실제 표준편차는 0.00895였고, delta method로 구한 값은 0.00908이었다. 유의하지 않은 차이가 유의하게 보이는 쪽으로 틀린다.

delta method는 사용자별 클릭 합 S와 페이지뷰 합 N으로 CTR을 S̄ / N̄ 로 두고, n명일 때 분산을 `(Var(S)/μN² − 2·μS·Cov(S,N)/μN³ + μS²·Var(N)/μN⁴) / n`으로 근사한다. 필요한 값은 사용자별 행에서 집계 한 번으로 나오고, 두 variant의 `ctr_var`를 더해 제곱근을 씌운 것이 차이의 표준오차다.

```sql
-- Spark 4.3. variant별 CTR과 delta method 분산
SELECT variant, COUNT(*) AS users,
       SUM(clicks) / SUM(page_views) AS ctr,
       ( VAR_SAMP(clicks) / POWER(AVG(page_views), 2)
         - 2 * AVG(clicks) * COVAR_SAMP(clicks, page_views) / POWER(AVG(page_views), 3)
         + POWER(AVG(clicks), 2) * VAR_SAMP(page_views) / POWER(AVG(page_views), 4)
       ) / COUNT(*) AS ctr_var
FROM user_metrics
GROUP BY variant;
```

이 계산이 되려면 테이블에 사용자별 비율이 아니라 분자와 분모가 따로 있어야 한다. 사용자별 CTR의 평균은 모든 사용자에게 같은 가중치를 주는 다른 지표다.

## SRM 검사를 효과보다 먼저 돌린다

SRM은 설정한 비율로 나눴을 때 기대하는 사용자 수와 실제 수를 카이제곱 검정으로 비교한다. Microsoft ExP는 거짓 경보를 줄이려고 p < 0.0005를 기준으로 쓰고, 이 검사를 통과해야 결과를 보여 준다. variant가 둘이면 자유도 1에서 이 기준의 임계값은 약 12.12다. 처음의 100,000 대 98,000은 통계량이 20.2라 SRM이다. 같은 비율인 1,000 대 980은 0.20, p는 0.65라 정상이다. 비율이 같아도 표본이 크면 작은 차이가 드러난다.

```sql
-- Spark 4.3. 설정 비율 대비 카이제곱 통계량
WITH counts AS (SELECT variant, COUNT(*) AS n FROM user_metrics GROUP BY variant),
weights AS (SELECT * FROM VALUES ('control', 0.5), ('treatment', 0.5) AS t(variant, weight)),
expected AS (
  SELECT c.n, w.weight * SUM(c.n) OVER () AS e
  FROM counts c JOIN weights w ON c.variant = w.variant
)
SELECT SUM(POWER(n - e, 2) / e) AS chi2 FROM expected;
```

ExP는 SRM의 원인을 단계로 나눈다. 배정 단계의 잘못된 버킷팅, 깨진 사용자 ID, carryover가 있고, 실행 단계에는 한 variant만 리다이렉트를 거치는 경우가 있다. 로그 처리 단계의 잘못된 조인, 분석 단계에서 한쪽에 치우친 조건으로 자르는 경우도 있다. 데이터 쪽에서 자주 보는 것은 뒤의 둘이다. 위의 inner join, 실험군 행동 때문에 봇 필터에 더 걸리는 사용자, 처리 이후에 생긴 속성으로 거른 세그먼트가 그렇다. 노출된 사용자만 보는 분석도 따로 SRM을 통과해야 한다. Kohavi의 책은 SRM을 신뢰성 가드레일로 분류한다. 파이프라인에서는 [데이터 품질 검사 글](/posts/data-quality-checks/)의 적재 단계 건수 검사처럼 분석 앞의 관문 자리에 둔다.

## 일찍 보기, 새로움 효과, 분산 줄이기

매일 갱신되는 대시보드는 peeking을 쉽게 만든다. p-value를 계속 보다가 0.05 아래로 내려온 날 멈추면 거짓 양성 비율이 0.05보다 커진다. 표본 수와 기간을 미리 정하고 그때 판단하거나, 계속 봐도 유효하도록 설계된 순차 검정(always-valid p-value)을 쓴다. 파이프라인 쪽에서는 중간 숫자에 계획한 종료일을 함께 표시하는 정도가 할 수 있는 일이다.

새 기능을 이것저것 눌러 보는 새로움 효과와, 바뀐 화면에 익숙해지는 동안 기존 사용자가 헤매는 primacy 효과는 날짜별 실험군과 대조군 차이를 그려 보면 드러난다. 신규 사용자만 따로 보는 방법도 있다. 다만 Kohavi 등(2012)은 초기 며칠의 추세 대부분이 이런 효과가 아니라 표본이 작아 생긴 통계적 착시였다고 보고했다.

CUPED는 지표 Y를 실험 전 기간의 값 X로 보정한다. `Y − θ(X − E[X])`, `θ = Cov(Y, X) / Var(X)`로 두면 분산이 `1 − ρ²`배가 된다. ρ는 Y와 X의 상관이다. 실험 전 같은 지표가 가장 좋은 공변량이었고, Bing에서는 분산이 약 50% 줄었다. 실험 전 값은 처리의 영향을 받지 않아 편향이 생기지 않는다. 데이터 쪽 할 일은 사용자 x 실험 테이블에 `pre_page_views` 같은 실험 전 컬럼을 하나 더 두는 것이다. 실험 전 기간에 없던 신규 사용자는 값이 비므로, 실험 전 기간에 있었는지를 나타내는 공변량을 함께 둔다.

가드레일은 두 종류다. 페이지 로딩 시간처럼 목표 지표가 올라도 나빠지면 안 되는 조직 가드레일이 있고, SRM처럼 결과를 믿어도 되는지 보는 신뢰성 가드레일이 있다. 둘 다 같은 사용자 x 실험 테이블에서 계산된다. 그래서 이 테이블의 노출 정의가 틀리면 목표 지표와 가드레일이 함께 틀린다.

## 참고 자료

- [Kohavi, Tang, Xu. Trustworthy Online Controlled Experiments (Cambridge University Press, 2020)](https://experimentguide.com/), 목차: 14장 무작위 단위, 18장 분산 추정, 20장 triggering, 21장 SRM
- [Kohavi et al. Trustworthy Online Controlled Experiments: Five Puzzling Outcomes Explained (KDD 2012)](https://exp-platform.com/puzzling-outcomes/)
- [Fabijan et al. Diagnosing Sample Ratio Mismatch in A/B Testing (Microsoft ExP, 2020)](https://www.microsoft.com/en-us/research/articles/diagnosing-sample-ratio-mismatch-in-a-b-testing/)
- [Microsoft ExP. Patterns of Trustworthy Experimentation: Post-Experiment Stage (2021)](https://www.microsoft.com/en-us/research/articles/patterns-of-trustworthy-experimentation-post-experiment-stage/)
- [Deng, Knoblich, Lu. Applying the Delta Method in Metric Analytics (KDD 2018)](https://arxiv.org/abs/1803.06336)
- [Deng, Xu, Kohavi, Walker. Improving the Sensitivity of Online Controlled Experiments by Utilizing Pre-Experiment Data (WSDM 2013)](https://exp-platform.com/cuped/)
- [Johari, Pekelis, Walsh. Always Valid Inference: Bringing Sequential Analysis to A/B Testing](https://arxiv.org/abs/1512.04922)
- Spark 4.3: [Hash functions (md5, xxhash64)](https://spark.apache.org/docs/latest/api/sql/hash-functions/), [Math functions (conv)](https://spark.apache.org/docs/latest/api/sql/math-functions/)

## English summary

An A/B test result can only be trusted if the data pipeline behind it is right. Assignment should be a deterministic hash of a stable unit ID with a per-experiment salt, so it is sticky and independent of earlier experiments; the 30-minute transaction ID cookie from my log API would group sessions but would not be a stable unit for user-level tests. Exposure logs must be written at the same point in both variants, deduplicated to the first exposure, and joined to metric events with a left join so users without events stay in the table. A per-user, per-experiment table with numerators and denominators supports the SRM chi-square check, delta-method variance for ratio metrics, and CUPED. Keeping additive per-user sums instead of ratios costs a little more storage but lets every later test be computed from one table.
