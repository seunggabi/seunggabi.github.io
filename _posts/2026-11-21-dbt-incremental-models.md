---
title: "dbt 증분 모델은 다시 읽는 범위와 키로 정해진다"
date: 2026-11-21 05:23:00 +0900
categories: [Transformation & Modeling, dbt]
tags: [dbt, incremental, spark, trino, athena, scd, ci, concept]
description: "dbt 증분 모델에서 새 행을 어디서부터 읽을지와 기존 테이블에 어떻게 합칠지를 정하는 lookback, incremental_strategy, unique_key 설정을 정리한다."
---

매일 도는 dbt 모델을 `table`로 두면 실행할 때마다 원천 전체를 읽어 테이블을 새로 만든다. `incremental`로 바꾸면 새 행만 읽는 대신 두 가지를 직접 정해야 한다. 새 행을 어디서부터 읽을지, 읽은 행을 기존 테이블에 어떻게 합칠지다. 앞을 잘못 정하면 늦게 온 행이 빠지고, 뒤를 잘못 정하면 다시 돌릴 때 행이 두 벌이 된다. 둘 다 에러 없이 일어난다.

버전은 dbt Core 1.12, dbt-spark 1.11, dbt-athena 1.11, dbt-trino 1.12 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| `ref()`, `source()` | 다른 모델과 원천 테이블을 가리키는 함수. 이 호출로 DAG가 만들어진다 |
| materialization | 모델을 view, table, incremental, ephemeral, materialized_view 중 무엇으로 만들지 |
| `is_incremental()` | 대상 테이블이 있고, incremental이고, `--full-refresh`가 아닐 때만 참. 기존 테이블은 {% raw %}`{{ this }}`{% endraw %}로 가리킨다 |
| lookback | 마지막으로 처리한 지점보다 앞에서부터 겹쳐 읽는 범위 |
| `incremental_strategy` | 새 행을 합치는 방법. append, merge, delete+insert, insert_overwrite, microbatch |
| `unique_key` | merge와 delete+insert가 기존 행을 찾는 키 |
| `on_schema_change` | 모델 컬럼이 바뀌었을 때 기존 테이블을 다루는 규칙. 기본은 `ignore` |
| unit test | 고정 입력으로 모델 SQL의 결과를 확인하는 테스트. 1.8부터 |
| `state:modified`, `--defer` | 바뀐 노드만 고르고, 고르지 않은 상위 모델은 운영 테이블을 읽게 하는 CI 조합 |

## ref와 source가 실행 순서를 정한다

dbt 모델은 `select` 하나다. 다른 모델은 `ref('stg_plays')`로, YAML에 선언한 원천 테이블은 `source('raw', 'plays')`로 가리킨다. 컴파일할 때 이 호출이 대상 환경의 실제 테이블 이름으로 바뀌고, 호출 관계가 DAG가 된다. `dbt build`는 이 DAG 순서로 모델을 만들고 테스트를 돌린다. 테이블 이름을 문자열로 직접 적으면 SQL은 돌지만 DAG에서 빠져 순서가 보장되지 않는다. 모델을 무엇으로 만들지는 materialization이 정한다. 기본인 `view`는 실행마다 `create view as`로, `table`은 `create table as`로 다시 만든다. `incremental`은 처음에만 전체를 만들고 이후엔 새 행을 합친다. `ephemeral`은 테이블을 만들지 않고 아래 모델에 `__dbt__cte__` CTE로 끼워 들어가서 직접 조회할 수 없다. `materialized_view`는 DB의 materialized view를 만들고 관리하는데, 플랫폼마다 지원이 달라 dbt-trino는 지원하고 Snowflake는 대신 dynamic table을 쓴다.

## is_incremental()과 겹쳐 읽는 범위

아래 모델(dbt-spark 1.11)은 날짜·국가별 재생 수를 Iceberg 테이블에 쌓는다. 마지막으로 쌓은 날짜에서 3일 앞부터 다시 읽는다.

{% raw %}
```sql
{{ config(materialized='incremental', file_format='iceberg',
          incremental_strategy='merge', unique_key=['event_date', 'country']) }}

select event_date, country, count(*) as plays
from {{ ref('stg_plays') }}
{% if is_incremental() %}
where event_date >= (select date_sub(max(event_date), 3) from {{ this }})
{% endif %}
group by event_date, country
```
{% endraw %}

처음 실행하면 `is_incremental()`이 거짓이라 필터 없이 전체를 만든다. 그다음부터는 필터가 붙으므로 SQL은 두 경우 모두 맞아야 한다. `max(event_date)`부터만 읽으면 지난 실행 뒤에 늦게 들어온 이틀 전 행이 빠진다. 3일을 겹쳐 읽으면 3일 안에 도착한 행은 그 날짜 집계에 다시 들어간다. 겹쳐 읽은 날짜는 이미 테이블에 있다. 그래서 합치는 방법이 덮어쓰기여야 한다. 위 모델을 `append`로 두면 겹친 3일이 실행마다 한 벌씩 늘어난다. 범위를 넓히면 늦은 행을 더 잡지만 실행마다 읽는 양도 늘어난다. 3일보다 늦은 행은 범위를 지정한 재실행이나 `dbt run --full-refresh --select daily_plays+`로 처리한다. `--full-refresh`는 테이블을 지우고 다시 만들고, `+`로 고른 아래쪽 incremental 모델도 함께 다시 만든다. 다시 만들기 비싼 테이블은 모델에 `full_refresh: false`를 두면 플래그가 있어도 다시 만들지 않는다. [지표 배치 글](/posts/pyspark-indicator-batch/)의 job은 시작일과 종료일을 받아 하루씩 돌았고, 공용 MySQL 코드가 unique 키로 겹치는 행을 지운 뒤 넣었다. dbt 용어로 옮기면 날짜 범위를 받는 재실행과 delete+insert 전략에 해당한다.

## 합치는 방법은 어댑터마다 다르다

| 전략 | 동작 | dbt-spark | dbt-athena | dbt-trino |
|---|---|---|---|---|
| append | 새 행을 그대로 넣는다 | 기본 | 있음 | 기본 |
| merge | `unique_key`가 맞으면 갱신, 없으면 넣는다 | Delta, Iceberg, Hudi만 | Iceberg만 | 커넥터가 MERGE를 지원해야 |
| delete+insert | 새 행과 키가 같은 기존 행을 지우고 넣는다 | 없음 | 없음 | 있음 |
| insert_overwrite | 새 결과에 든 파티션을 통째로 바꾼다 | 있음. Delta 제외 | 기본. Hive 테이블만 | 없음 |
| microbatch | `event_time` 구간마다 그 구간을 바꾼다 | 있음 | 있음 | 있음 |

insert_overwrite는 `unique_key`를 쓰지 않는다. 파티션 단위로 바꾸므로, 결과에 든 파티션의 행을 전부 다시 select해야 한다. dbt-spark에서 `partition_by` 없이 쓰면 테이블 전체가 이번 결과로 바뀐다. dbt-athena는 파티션이 없으면 append로 돌아간다. 같은 설정이 어댑터에 따라 전혀 다르게 동작한다. Athena Iceberg 테이블은 insert_overwrite를 지원하지 않아 merge나 append를 쓴다.

`unique_key`는 컬럼 하나나 목록으로 준다. 키 컬럼에 NULL이 있으면 기존 행과 맞지 않아 중복이 생기니 `coalesce`로 채우거나 대리 키를 만든다. 새 행 안에 같은 키가 두 번 있으면 DB와 전략에 따라 실패한다. dbt-trino는 키가 유일하지 않으면 delete+insert를 쓰라고 안내한다. Airflow에서 날짜 파티션을 덮어쓸지 키로 지우고 넣을지는 [멱등한 Airflow DAG 글](/posts/airflow-idempotent-dags/)에 적었다. dbt에서는 그 선택이 설정 한 줄이 된다.

`on_schema_change`는 모델에 컬럼이 늘거나 줄었을 때를 정한다. `ignore`(기본)는 새 컬럼을 대상에 넣지 않고, 컬럼이 빠지면 실행이 실패한다. `fail`은 스키마가 다르면 멈추고, `append_new_columns`는 새 컬럼만 더하며, `sync_all_columns`는 추가와 삭제, 타입 변경까지 맞춘다. 어느 값도 새 컬럼의 지난 행을 채우지 않고, 최상위 컬럼만 본다. dbt-athena의 Iceberg 증분 모델은 이 설정과 상관없이 모든 컬럼을 맞춘다.

## microbatch는 겹쳐 읽기를 설정으로 옮긴다

1.9부터 있는 microbatch는 `is_incremental()` 필터를 직접 쓰지 않는다. `event_time` 컬럼과 `batch_size`(hour, day, month, year)로 구간을 나누고, 구간마다 쿼리를 한 번씩 돌린다. `lookback`(기본 1)은 마지막 구간보다 앞의 구간 몇 개를 다시 처리한다. 상위 모델에도 `event_time`이 있어야 구간 필터가 자동으로 붙고, 없으면 구간마다 상위 테이블 전체를 읽는다.

{% raw %}
```sql
-- models/plays_mb.sql (dbt-spark 1.11). 상위 stg_plays의 config에도 event_time: event_ts를 둔다
{{ config(materialized='incremental', incremental_strategy='microbatch',
          event_time='event_ts', begin='2026-01-01', batch_size='day', lookback=3,
          partition_by=['event_date'], file_format='parquet') }}

select event_ts, country, user_id, to_date(event_ts) as event_date
from {{ ref('stg_plays') }}
```
{% endraw %}

구간을 바꾸는 방식은 어댑터가 고른다. dbt-spark와 Athena Hive 테이블은 insert_overwrite, Athena Iceberg 테이블은 merge, Snowflake와 Redshift는 delete+insert를 쓴다. 지난 기간은 `dbt run --select plays_mb --event-time-start "2026-11-01" --event-time-end "2026-11-04"`로 다시 돌리고, 두 옵션은 함께 줘야 한다. 실패한 구간만 `dbt retry`로 다시 돌릴 수 있다. dbt는 시각 값을 UTC로 가정한다. 이벤트 시각을 한국 시간으로 저장한 테이블이면 하루 구간의 경계가 9시간 어긋난다.

## data test와 unit test는 보는 것이 다르다

`unique`, `not_null`, `accepted_values`, `relationships` 같은 data test와 `severity`, `store_failures`는 [데이터 품질 검사 글](/posts/data-quality-checks/)에 정리했다.

| | data test | unit test (1.8+) |
|---|---|---|
| 보는 것 | 만들어진 테이블의 데이터 | 고정 입력에 대한 SQL 로직 |
| `dbt build`에서 | 모델을 만든 뒤 | 모델을 만들기 전 |
| 돌리는 곳 | 운영 포함 매 실행 | 개발과 CI. 운영은 `--exclude-resource-type unit_test` |

증분 모델의 필터 쪽은 `is_incremental`을 덮어쓰고 기존 테이블을 `this`로 넣어 확인한다. 아래 테스트는 앞의 `daily_plays`가 3일 겹쳐 읽는지 본다.

```yaml
unit_tests:
  - name: daily_plays_rereads_three_days
    model: daily_plays
    overrides: {macros: {is_incremental: true}}
    given:
      - input: ref('stg_plays')
        rows: [{event_date: 2026-11-10, country: KR}, {event_date: 2026-11-18, country: KR}, {event_date: 2026-11-20, country: KR}]
      - input: this
        rows: [{event_date: 2026-11-20, country: KR, plays: 1}]
    expect:
      rows: [{event_date: 2026-11-18, country: KR, plays: 1}, {event_date: 2026-11-20, country: KR, plays: 1}]
```

`expect`는 최종 테이블이 아니고 이번 실행이 merge하거나 넣을 행이다. merge 자체가 맞게 됐는지는 unit test로 확인할 수 없다. SQL 모델만 대상이고, 바로 위 상위 모델은 DB에 있어야 한다. `dbt run --select ... --empty`로 빈 테이블을 먼저 만들 수 있다.

## snapshot은 dbt가 관리하는 SCD2다

차원 속성이 바뀔 때 행을 늘리는 SCD2의 구조는 [SCD 차원 모델링 글](/posts/dimensional-modeling-scd/)에 적었다. dbt snapshot은 그 테이블을 대신 관리한다. 1.9부터 YAML로 정의한다.

```yaml
snapshots:
  - name: users_snapshot
    relation: source('app', 'users')
    config: {unique_key: user_id, strategy: timestamp, updated_at: updated_at, hard_deletes: invalidate,
             dbt_valid_to_current: "cast('9999-12-31' as timestamp)"}
```

`timestamp` 전략은 `updated_at`이 바뀐 행을, `check` 전략은 `check_cols`에 적은 컬럼 값이 바뀐 행을 새 버전으로 본다. 믿을 만한 `updated_at`이 있으면 `timestamp`가 권장된다. dbt가 `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`을 붙인다. 현재 행의 `dbt_valid_to`는 기본이 NULL이고, `dbt_valid_to_current`로 먼 미래 값을 줄 수 있다. 원천에서 지워진 행은 `hard_deletes`로 정한다. `ignore`(기본)는 그대로 두고, `invalidate`는 `dbt_valid_to`로 닫고, `new_record`는 `dbt_is_deleted` 행을 하나 더 넣는다. snapshot은 실행할 때의 원천 상태를 비교하므로, 두 실행 사이에 여러 번 바뀐 값은 마지막 값만 남는다.

## 바뀐 모델만 CI에서 돌리기

slim CI는 `dbt build --select state:modified+ --defer --state prod-artifacts/`처럼 돌린다. `--state`에는 운영 실행이 남긴 `manifest.json`이 든 경로를 준다. `state:modified`는 그 manifest와 비교해 새로 생겼거나 바뀐 노드를 고르고, `+`가 그 아래 모델까지 더한다. `--defer`는 고르지 않은 상위 모델이 CI 스키마에 없으면 그 `ref`를 운영 테이블로 돌린다. 빈틈도 있다. `var`나 `env_var` 값만 바뀐 경우는 모델까지 lineage가 이어지지 않아 빠질 수 있다. 바뀐 매크로를 쓰는 모델은 모두 modified가 되고, `tags`와 `meta`만 바꾼 것은 modified가 아니다. `--state`와 `--target-path`를 같은 경로로 두면 비교 기준이 덮어써진다. 그리고 CI 스키마에는 증분 모델의 기존 테이블이 없으니 `is_incremental()`이 거짓이 된다. CI는 처음 만드는 쪽만 돌리고, 필터 쪽은 앞의 unit test가 맡는다.

## 참고 자료

- [dbt: Materializations](https://docs.getdbt.com/docs/build/materializations), [Configure incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt: Incremental strategies (adapter support)](https://docs.getdbt.com/docs/build/incremental-strategy), [Microbatch](https://docs.getdbt.com/docs/build/incremental-microbatch)
- [dbt: Spark configurations](https://docs.getdbt.com/reference/resource-configs/spark-configs), [Athena configurations](https://docs.getdbt.com/reference/resource-configs/athena-configs), [Trino configurations](https://docs.getdbt.com/reference/resource-configs/trino-configs)
- [dbt: Unit tests](https://docs.getdbt.com/docs/build/unit-tests), [Snapshots](https://docs.getdbt.com/docs/build/snapshots), [Node selection methods (state)](https://docs.getdbt.com/reference/node-selection/methods), [Defer](https://docs.getdbt.com/reference/node-selection/defer), [State comparison caveats](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats)
- PyPI 최신 버전: [dbt-core](https://pypi.org/project/dbt-core/), [dbt-spark](https://pypi.org/project/dbt-spark/), [dbt-athena](https://pypi.org/project/dbt-athena/), [dbt-trino](https://pypi.org/project/dbt-trino/)

## English summary

A dbt incremental model reads only new rows, so it has to decide how far back to reread and how to merge what it reads. Rereading a few days catches late rows, but then the strategy must overwrite, and the choices differ by adapter: Spark and Athena Hive tables can overwrite partitions, while Trino offers merge and delete+insert but no insert_overwrite. Microbatch (1.9+) turns the lookback into config and reruns fixed time slices. My indicator batch took a date range and deleted rows by unique key before inserting, which maps to delete+insert with an explicit range. A wider lookback catches more late data at the cost of rereading more on every run.
