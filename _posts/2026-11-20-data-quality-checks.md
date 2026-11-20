---
title: "수집, 적재, 적재 후로 나눈 데이터 품질 검사"
date: 2026-11-20 04:54:00 +0900
categories: [Data Quality, Testing]
tags: [data-quality, dbt, great-expectations, delta-lake, concept]
---

[로그 수집 API](/posts/log-collection-api/)는 Kafka로 보내기 전에 로그 한 건마다 스키마 버전, 이벤트 시각, 재생 시간 숫자를 확인했다. 그 로그가 Hive에 쌓인 뒤에는 [Spark Streaming으로 Hive에 쌓은 글](/posts/spark-streaming-to-hive/)의 점검 배치가 지난 14일의 행 수를 날짜와 시간별로 셌다. 둘 다 품질 검사지만, 앞은 한 건을 보고 뒤는 쌓인 테이블을 본다. 검사 위치는 셋으로 나눌 수 있다. 들어오는 한 건을 보는 수집 단계, 한 번에 올린 묶음을 보는 적재 단계, 쌓인 테이블을 시간 축으로 보는 적재 후 단계다. 단계마다 묻는 것이 다르고, 실패했을 때 할 수 있는 일도 다르다.

버전은 dbt Core 1.11, GX Core 1.25 기준이다.

| 단계 | 묻는 것 | 실패하면 | 도구 예 |
|---|---|---|---|
| 수집 | 이 한 건이 스키마와 값 범위에 맞는가 | 버리거나, 고치거나, 표시하고 보낸다 | 애플리케이션 코드, Delta `CHECK` |
| 적재 | 이번에 올린 묶음의 건수와 키가 맞는가 | 올리지 않거나 바꿔 끼우기 전에 멈춘다 | `dropDuplicates`, dbt `unique`, `not_null` |
| 적재 후 | 테이블이 제때, 평소만큼, 평소 모양으로 차 있는가 | 경고를 띄우고 사람이 판단한다 | dbt freshness, 과거 대비 건수 비교, GX `mostly` |

## 수집 단계: 한 건마다 버릴지, 고칠지, 표시할지 정한다

수집 단계에서는 레코드 하나만 보고 판단할 수 있는 것만 검사한다. 스키마 버전, 타입, 값의 범위가 여기에 든다. 어려운 건 검사보다 걸렸을 때 무엇을 하느냐다.

| 처리 | 맞는 경우 | 대가 |
|---|---|---|
| 버림 | 뒤 단계가 읽을 수 없는 레코드(스키마 불일치) | 그 건은 지표에서 빠진다 |
| 고침 | 틀린 값이 뒤 단계를 망가뜨리지만, 대체값이 있는 경우(시각) | 지표가 대체값 기준으로 계산된다 |
| 표시 | 값은 의심스럽지만 레코드 자체는 쓸 만한 경우 | 뒤 단계가 표시를 보고 걸러야 한다 |

수집 API는 세 처리를 모두 썼다. 스키마 버전이 토픽 버전과 다르면 Kafka로 보내지 않았다. 이벤트 시각이 7일 전부터 1일 뒤 사이를 벗어나면 서버 시각으로 바꿨다. 재생 시간이나 전체 길이가 음수이거나 재생 시간이 전체 길이를 크게 넘으면 그대로 보내고 에러 로그만 남겼다. 세 경우 모두 원래 로그를 에러 로그에 남겼으니, 나중에 기준이 틀렸다는 걸 알아도 원본으로 되돌릴 수 있다.

테이블 포맷이 검사를 맡을 수도 있다. Delta Lake는 `NOT NULL`과 `CHECK` 제약을 지원한다.

```sql
ALTER TABLE events ADD CONSTRAINT play_ms_non_negative CHECK (play_ms >= 0);
```

제약을 어기는 쓰기는 `InvariantViolationException`으로 실패한다. 제약을 추가할 때도 기존 행이 모두 조건을 만족하는지 먼저 확인한다. 이 방식은 행을 하나씩 골라내지 않고 쓰기를 통째로 거절한다. 그래서 "버림" 쪽에만 맞고, 고치거나 표시하는 일은 그 앞의 코드에서 해야 한다.

## 적재 단계: 건수가 맞는지, 키가 겹치지 않는지

적재 단계에서는 묶음 전체를 본다. 같은 레코드가 두 번 들어왔는지, 읽은 건수와 쓴 건수가 맞는지는 한 건만 봐서는 알 수 없다. 중복은 전달 보장에서 생긴다. Spark Streaming 글의 스트리밍 잡은 오프셋을 컨슈머 자동 커밋에 맡겼다. 재시작하면 메시지가 겹치거나 빠질 수 있고, 복구 모드로 다시 읽어도 겹친다.

매일 그날 폴더를 `dropDuplicates()`로 정리한 뒤 `_TMP` 경로에 쓰고, 원래 폴더와 바꿔 끼웠다. 빠진 메시지는 이 배치로 되살릴 수 없다. 이 구조에서는 스트리밍이 단순하게 남고, 중복 정리는 일 배치가 맡는다. `_TMP`에 먼저 쓰기 때문에 쓰는 도중에 실패해도 원래 데이터는 남는다. 작은 파일을 합치는 잡도 행 수가 상한을 넘는 파티션은 합치지 않고 실패로 표시했다.

dbt에서는 이런 검사를 data test로 적는다. data test는 실패한 행을 돌려주는 `select`이고, 0행이면 통과, 1행 이상이면 실패다. 내장 generic test는 `unique`, `not_null`, `accepted_values`, `relationships` 네 가지다.

```yaml
models:
  - name: events
    columns:
      - name: event_id
        data_tests:
          - unique
          - not_null
      - name: platform
        data_tests:
          - accepted_values:
              arguments:
                values: ['web', 'ios', 'android']
              config:
                severity: warn
```

테스트 입력값을 `arguments:` 아래에 두는 방식은 1.10.5부터 쓸 수 있다. `severity`의 기본값은 `error`이고, `error_if`와 `warn_if`는 기본이 `!=0`이다. `error_if: ">1000"`, `warn_if: ">10"`처럼 실패 건수로 경계를 나눌 수도 있다. `store_failures`를 켜면 실패한 행이 대상 스키마 뒤에 `_dbt_test__audit`가 붙은 스키마의 테이블에 남아, 무엇이 걸렸는지 바로 볼 수 있다.

## 적재 후: 테이블을 시간 축으로 본다

적재가 성공해도 테이블이 맞다는 뜻은 아니다. 클라이언트가 로그를 보내지 않으면 잡은 빈 파티션을 정상적으로 올린다. 이런 문제는 테이블을 시간 축으로 봐야 보인다. 신선도는 dbt source freshness로 본다. 1.9부터 `freshness`는 `config:` 아래에, 1.10부터는 `loaded_at_field`도 `config:` 아래에 둔다. `dbt source freshness`로 실행한다.

```yaml
sources:
  - name: raw
    tables:
      - name: events
        config:
          loaded_at_field: ingested_at
          freshness:
            warn_after: {count: 2, period: hour}
            error_after: {count: 6, period: hour}
```

양은 고정된 범위보다 과거와 비교하는 편이 맞는다. 로그는 요일과 시간대에 따라 양이 다르니, 새벽 3시의 건수와 저녁 9시의 건수를 같은 하한으로 잴 수 없다. GX Core의 내장 volume expectation은 고정 범위(`ExpectTableRowCountToBeBetween`), 고정값(`ExpectTableRowCountToEqual`), 다른 테이블과의 비교(`ExpectTableRowCountToEqualOtherTable`)다. 과거와 비교하려면 쿼리를 직접 쓴다. Spark Streaming 글의 점검 배치가 이 방식이었다. 최근 이틀 중 어떤 시간대가 1~2주 전 같은 시간대 평균보다 크게 낮으면 경고로 표시해 메일로 보내고 메신저로도 알렸다. 같은 생각을 dbt singular test로 쓰면 이렇다.

{% raw %}
```sql
-- tests/assert_hourly_volume_not_dropped.sql (Spark SQL, 기준 0.5는 예시)
{{ config(severity = 'warn') }}
with hourly as (
  select to_date(event_time) as dt, hour(event_time) as hh, count(*) as cnt
  from {{ ref('events') }}
  where event_time >= date_sub(current_date(), 14)
  group by 1, 2
),
baseline as (
  select hh, avg(cnt) as avg_cnt
  from hourly
  where dt between date_sub(current_date(), 14) and date_sub(current_date(), 7)
  group by hh
),
recent_days as (
  select explode(sequence(date_sub(current_date(), 2), date_sub(current_date(), 1))) as dt
)
select d.dt, b.hh, coalesce(h.cnt, 0) as cnt, b.avg_cnt
from recent_days d
cross join baseline b
left join hourly h on h.dt = d.dt and h.hh = b.hh
where coalesce(h.cnt, 0) < b.avg_cnt * 0.5
```
{% endraw %}

`hourly`만 보고 비교하면 로그가 0건인 시간대는 `group by` 결과에 아예 없어서 잡히지 않는다. 가장 심각한 경우를 놓치는 셈이다. 그래서 기준 쪽의 시간대를 날짜와 곱해 놓고 `left join`으로 0을 채웠다. 기준 평균은 여전히 0건인 날을 빼고 계산되므로, 기준 기간에 빈 시간대가 있었다면 평균이 실제보다 높게 잡힌다.

null 비율은 0%를 요구하면 쓸모가 없다. 선택 항목은 원래 가끔 비어 있다. GX Core에서는 `gx.expectations.ExpectColumnValuesToNotBeNull(column="region_code", mostly=0.99)`처럼 `mostly`로 허용 비율을 주고, `batch.validate(expectation)`으로 검사한다.

## 경고는 후보로, 다시 돌리는 길은 열어 두고

적재 후 검사는 오탐이 많다. 연휴, 앱 배포, 마케팅 푸시만으로도 양이 출렁인다. 이 검사를 모두 `error`로 두면 파이프라인이 자주 멈추고, 모두 `warn`으로 두면 아무도 읽지 않는다. 그래서 확실한 규칙(키 중복, 스키마)은 멈추게 하고, 통계적인 규칙(양, null 비율)은 사람에게 알리는 쪽으로 나눈다. 이상 클릭을 찾는 스트림 잡도 같은 식으로 나눌 수 있다. 짧은 시간에 클릭이 몰린 IP나 기기를 바로 차단하지 않고 차단 후보로 쌓아 두면, 정상인데 클릭이 몰리는 경우나 재시작 뒤 이벤트가 몰린 경우를 이 후보 단계가 받아 준다.

경고를 받은 뒤에는 고쳐서 다시 만들 수 있어야 한다. [서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 job은 항상 시작일과 종료일을 받았다. 원천을 고친 뒤에는 날짜 범위만 넓혀 같은 job을 다시 돌린다. 공용 insert가 unique 키로 겹치는 행을 먼저 지우고 넣기 때문에, 같은 날짜를 다시 돌려도 행이 두 번 들어가지 않는다.

## 참고 자료

- [dbt: Add data tests to your DAG](https://docs.getdbt.com/docs/build/data-tests)
- [dbt: severity, error_if, warn_if](https://docs.getdbt.com/reference/resource-configs/severity)
- [dbt: freshness](https://docs.getdbt.com/reference/resource-properties/freshness)
- [GX Core: Volume](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/volume)
- [GX Core: Missingness](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/missingness)
- [GX Core: Connect to dataframe data](https://docs.greatexpectations.io/docs/core/connect_to_data/dataframes/)
- [Delta Lake: Constraints](https://docs.delta.io/latest/delta-constraints.html)

## English summary

Data quality checks ask different questions at ingestion, at load, and after load. At ingestion one record is dropped, fixed, or flagged; in my log collection API a schema version mismatch was dropped, a bad event time was replaced with the server time, and odd playback numbers were sent with an error log, with the original always kept. At load the whole batch is checked; my daily batch removed duplicates with `dropDuplicates()` and swapped the result in through a temporary path, though it could not bring back lost messages. After load the table needs a time axis, and my table check compared hourly row counts over 14 days and sent warnings instead of stopping the pipeline. Warnings like that, or block candidates in a click anomaly job, absorb noisy statistics at the cost of a person reviewing each alert.
