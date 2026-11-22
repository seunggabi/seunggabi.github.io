---
title: "다시 돌려도 안전한 Airflow DAG의 날짜와 파티션 다루기"
date: 2026-11-20 04:44:00 +0900
categories: [Orchestration, Airflow]
tags: [airflow, idempotency, backfill, concept]
description: "retry, clear, 백필로 태스크가 여러 번 돌아도 같은 결과가 나오도록 Airflow DAG의 날짜와 파티션을 DAG run 기준으로 다루는 방법을 정리한다."
---

Airflow 태스크는 한 번만 돈다는 보장이 없다. 실패하면 retry로 다시 돌고, 사람이 clear를 누르면 또 돌고, 백필을 걸면 지난 날짜로 한꺼번에 돈다. 그때마다 행이 두 배가 되거나 엉뚱한 날짜를 처리하면, 스케줄러가 아무리 잘 돌아도 데이터는 틀린다.

그래서 태스크는 데이터베이스 트랜잭션처럼 다뤄야 하고, 다시 돌려도 같은 결과를 내야 한다. Airflow Best Practices도 이 원칙을 둔다.

버전은 Airflow 3.4 기준이고, 2.x와 다른 곳은 따로 적었다.

## 날짜는 실행 시각이 아니라 DAG run에서 받는다

태스크 안에서 `datetime.now()`로 처리 날짜를 정하면, 같은 run을 내일 다시 돌릴 때 내일 날짜를 처리한다. 백필도 의미가 없어진다. 처리할 날짜는 DAG run이 가진 값에서 가져와야 한다.

| 템플릿 변수 | 의미 |
|---|---|
| {% raw %}`{{ logical_date }}`{% endraw %} | DAG run을 논리적으로 식별하는 시각 |
| {% raw %}`{{ ds }}`{% endraw %} | `logical_date`를 `YYYY-MM-DD`로 쓴 것 |
| {% raw %}`{{ data_interval_start }}`{% endraw %}, {% raw %}`{{ data_interval_end }}`{% endraw %} | 이 run이 맡은 데이터 구간의 시작과 끝 |
| {% raw %}`{{ run_id }}`{% endraw %} | run 식별자. 날짜 값은 DAG 안에서 유일하다고 보면 안 되고, 유일한 키가 필요하면 이걸 쓴다 |

Airflow 3에서는 `execution_date`, `next_ds`, `prev_ds`, `yesterday_ds`, `tomorrow_ds` 같은 컨텍스트 키가 빠졌다. 2.x DAG에서 이 값을 쓰고 있었다면 업그레이드 전에 `logical_date`나 `data_interval_*`로 바꿔야 DAG가 깨지지 않는다.

더 조용히 바뀐 건 `@daily` 같은 cron 스케줄의 data interval이다.

| | Airflow 2.x 기본 | Airflow 3 기본 |
|---|---|---|
| `create_cron_data_intervals` | `True` | `False` |
| `schedule="@daily"`가 쓰는 timetable | `CronDataIntervalTimetable` | `CronTriggerTimetable` |
| 2026-11-20 00:00 UTC에 도는 run의 data interval | 11-19 00:00 ~ 11-20 00:00 | 시작과 끝이 둘 다 11-20 00:00 |
| 그 run의 {% raw %}`{{ ds }}`{% endraw %} | `2026-11-19` | `2026-11-20` |

2.x에서 "어제 하루 치를 처리한다"는 뜻으로 {% raw %}`{{ ds }}`{% endraw %}를 쓰던 DAG를 그대로 3으로 옮기면, 같은 시각에 돌면서 하루 뒤 날짜, 즉 아직 다 쌓이지 않은 오늘을 처리하게 된다. 하루 구간을 다루는 DAG라면 `CronDataIntervalTimetable`을 명시하고 `data_interval_start`를 쓰는 편이 의도가 분명하다. 에셋으로 트리거된 DAG run에는 `logical_date`가 없어서 {% raw %}`{{ ds }}`{% endraw %}도 쓸 수 없다. 그런 run에서도 도는 태스크라면 `ds`에 기대는 곳을 먼저 확인해야 한다. 수동 실행에서는 data interval이 넘겨준 `logical_date`에서 계산된다는 보장이 없다. 사용자가 지정한 날짜가 필요하면 `logical_date`를 직접 쓴다.

## 템플릿은 실행 직전에, 선언된 필드만 렌더링된다

{% raw %}`{{ ds }}`{% endraw %}가 실제 날짜로 바뀌는 시점은 DAG 파일을 파싱할 때가 아니다. 정확히는 오퍼레이터의 `pre_execute`가 불리기 직전이다. 그래서 같은 DAG 코드라도 run마다 자기 날짜로 렌더링되고, 3일 전 run을 clear하면 3일 전 날짜로 다시 렌더링된다. 반대로 DAG 파일 최상위에서 `datetime.now()`를 계산해 인자로 넘기면, 그 값은 파싱할 때 정해진다.

렌더링되는 건 오퍼레이터의 `template_fields`에 이름이 올라간 속성뿐이다. [TableauOperator에 template_fields를 넣은 글](/posts/airflow-tableau-template-fields/)이 이 경우다. {% raw %}`find="{{ params.find }}"`{% endraw %}를 넘겼지만 당시 `TableauOperator`에는 `template_fields`가 없어서 Tableau에 {% raw %}`{{ params.find }}`{% endraw %}라는 문자열이 그대로 갔다. 날짜도 마찬가지다. 템플릿 필드가 아닌 인자에 {% raw %}`{{ ds }}`{% endraw %}를 넣으면 날짜가 아니라 그 문자열이 넘어가고, 다시 돌려도 계속 틀린다.

## 같은 구간을 덮어쓰는 태스크

날짜를 제대로 받아도 쓰는 방식이 append면 retry 한 번에 중복이 생긴다. 멱등하게 쓰는 대표적인 방법은 둘이다.

| 방식 | 하는 일 | 주의할 점 |
|---|---|---|
| 파티션 덮어쓰기 | `INSERT OVERWRITE ... PARTITION (dt = '...')`처럼 그 날짜 파티션만 통째로 바꾼다 | 파티션 키와 run의 날짜가 정확히 맞아야 한다 |
| 키 기준 delete-then-insert | 같은 키(날짜, 날짜와 차원 조합 등)의 행을 지우고 넣는다 | DELETE와 INSERT가 한 트랜잭션으로 커밋되지 않으면 중간 실패 때 그 구간이 빈다 |

INSERT 대신 UPSERT를 쓰고, 특정 파티션을 읽고 쓴다. 읽는 쪽도 마찬가지로, 가장 최신 데이터를 그냥 읽지 말고 run의 구간으로 잘라 읽는다.

{% raw %}
```python
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, CronDataIntervalTimetable

with DAG(
    dag_id="daily_active_users",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
):
    SQLExecuteQueryOperator(
        task_id="load_daily_active_users",
        conn_id="metrics_db",
        sql=[
            "DELETE FROM daily_active_users WHERE dt = '{{ data_interval_start | ds }}'",
            """
            INSERT INTO daily_active_users (dt, users)
            SELECT '{{ data_interval_start | ds }}', COUNT(DISTINCT user_id)
            FROM events
            WHERE event_time >= '{{ data_interval_start }}'
              AND event_time <  '{{ data_interval_end }}'
            """,
        ],
    )
```
{% endraw %}

`sql`은 `SQLExecuteQueryOperator`의 템플릿 필드라서 run마다 그 구간으로 렌더링된다. {% raw %}`'{{ data_interval_start }}'`{% endraw %}는 `pendulum.DateTime`이라 타임존 오프셋(`+00:00`)이 붙은 시각 문자열로 렌더링된다. 컬럼이 날짜만 받는다면 `dt`처럼 {% raw %}`{{ data_interval_start | ds }}`{% endraw %}로 `YYYY-MM-DD`를 만들거나 SQL에서 캐스팅한다. 이 태스크는 retry로 돌든 clear로 돌든 그 날짜의 행을 한 벌만 남긴다.

[서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 배치도 같은 구조였다. job이 항상 시작일과 종료일을 받아 하루씩 돌고, 공용 MySQL 코드의 insert가 테이블 unique 키로 겹치는 행을 먼저 지우고 넣었다. 그래서 같은 날짜를 다시 돌려도 행이 두 번 들어가지 않았고, 과거 지표를 다시 만들 때는 범위만 넓혀 같은 job을 돌렸다. Airflow에서 `data_interval_start`를 받는 태스크가 하는 일과 같다.

## catchup, 백필, retry는 결국 같은 태스크를 다시 부른다

| 상황 | Airflow가 하는 일 |
|---|---|
| retry | 실패한 태스크가 `up_for_retry`가 되고, `retry_delay` 뒤에 같은 run으로 다시 돈다 |
| clear | 태스크 상태를 `None`으로 돌려 같은 run으로 다시 돌게 한다 |
| catchup | `catchup=True`면 지난 run 이후 돌지 않은 스케줄마다 run을 만든다. Airflow 3 기본값은 `False`다 |
| 백필 | 지정한 날짜 범위의 run을 만든다. Airflow 3에서는 `airflow backfill create` 또는 UI로 만든다 |

```bash
airflow backfill create --dag-id daily_active_users \
  --from-date 2026-11-01 --to-date 2026-11-07 \
  --reprocess-behavior failed --max-active-runs 2
```

`--reprocess-behavior`는 이미 run이 있는 날짜를 어떻게 할지 정한다. `none`은 없는 날짜만 만들고, `failed`는 실패한 날짜도 다시 만들고, `completed`는 성공한 날짜까지 다시 만든다. `completed`로 성공한 날짜를 다시 돌려도 괜찮은 건 태스크가 앞 절처럼 덮어쓸 때뿐이다. 백필의 `--max-active-runs`는 DAG의 `max_active_runs`와 별개로 적용된다.

`retries`와 `retry_delay`는 보통 `default_args`에 둔다. `default_args`는 오퍼레이터에만 퍼진다. 나는 `catchup`도 읽게 하자는 PR을 냈고, 메인테이너는 DAG 인자와 오퍼레이터 기본값을 섞지 않는 쪽을 택했다. `catchup`, `schedule`, `max_active_runs`는 DAG 인자로 넘겨야 한다.

## 체크할 것

- 처리 날짜를 `datetime.now()`가 아니라 `data_interval_start`나 `logical_date`에서 받는가
- 그 값을 넣은 인자가 오퍼레이터의 `template_fields`에 있는가
- 같은 run을 두 번 돌려도 결과 행 수가 같은가
- 2.x에서 옮긴 DAG라면 `@daily`의 {% raw %}`{{ ds }}`{% endraw %}가 하루 밀리지 않는가
- `catchup`을 `default_args`가 아니라 DAG 인자로 넘겼는가

마지막 둘은 에러 없이 조용히 틀린다. Airflow 3으로 바꾼 첫날의 {% raw %}`{{ ds }}`{% endraw %}는 정상적으로 렌더링되고, 다만 하루가 다르다.

## 참고 자료

- [Best Practices (Airflow 3.4)](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Templates reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- [Dag Runs: data interval, catchup, backfill, clear](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)
- [Timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html)
- [Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)
- [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
- [Operators: Jinja templating](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)
- [Dags: default_args, DAG discovery](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Tasks: retries](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)

## English summary

Airflow tasks can run more than once because of retries, clears, catchup, and backfills, so each task should give the same result on every run. Take the processing date from the DAG run, such as `data_interval_start`, and pass it only to fields listed in `template_fields`, which render just before the task runs. In Airflow 3, `execution_date` is gone, `catchup` is off by default, and a bare `@daily` schedule uses `CronTriggerTimetable`, so {% raw %}`{{ ds }}`{% endraw %} is one day later than in Airflow 2. I once sent a PR to make `catchup` work from `default_args`, and the maintainers kept DAG arguments and operator defaults separate. Most of all, write idempotently: overwrite the partition, or delete and insert rows by key in one transaction.
