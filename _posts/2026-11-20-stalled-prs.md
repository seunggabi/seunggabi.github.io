---
published: false
title: "닫힌 PR 네 개와 3년 넘게 열린 PR 하나"
date: 2026-11-20 03:40:00 +0900
categories: [Career, Open Source]
tags: [open-source, docker-compose, yaml, aws-cli, mariadb4j]
---

Airflow에 보낸 PR 이야기에 이어, 다른 저장소에서 머지되지 않은 PR들을 모았다. 닫힌 이유가 저마다 다르다.

| PR | 하려던 것 | 결과 |
|---|---|---|
| [docker/compose#9598](https://github.com/docker/compose/pull/9598), [#9596](https://github.com/docker/compose/pull/9596) | Compose 파일의 boolean 값 지원 | Compose v2에서 이미 동작해서 직접 닫음 |
| [MariaDB4j#849](https://github.com/MariaDB4j/MariaDB4j/pull/849) | 임시 디렉터리 이름에 해시코드를 넣어 여러 사용자가 같이 쓸 수 있게 | 빌드 실패, 다른 PR로 대체돼 2025년 머지 |
| [aws/aws-cli#7204](https://github.com/aws/aws-cli/pull/7204) | EMR 클러스터 생성 예제에 Spark SQL 스텝 추가 | 문서 담당자와 논의가 필요하다며 보류, 2년 가까이 지나 닫힘 |
| [airflow-maintenance-dags#135](https://github.com/teamclairvoyant/airflow-maintenance-dags/pull/135) | 유지보수 DAG마다 `doc_md` 추가 | 2023년 6월부터 열려 있음 |

## Compose의 boolean (2022년 6월)

Confluent Kafka 이미지 이슈를 따라가다 Compose 파일의 일부 옵션에 `true`, `false`를 쓰면 검증에 걸리는 걸 봤다. 이슈를 올리고, 스키마에 boolean 타입을 허용하는 PR과 YAML 1.2.2 기준으로 boolean 값을 `true`, `false`로 맞추는 PR을 하나씩 보냈다.

메인테이너 쪽 답은 두 갈래였다. Python으로 된 Compose v1은 몇 달 뒤 지원이 끝나서 보안 수정 말고는 받지 않으니 Go로 된 v2에서 해 보라는 것, 그리고 로깅 드라이버 옵션은 원래 문자열만 받는데 왜 다른 타입이 필요하냐는 것이었다. YAML 1.1이 아니라 1.2.2 명세를 보라고 답했지만, v2에서 다시 해 보니 boolean이 이미 동작했다. 두 PR 모두 이틀 만에 직접 닫았다.

## MariaDB4j의 임시 디렉터리 (2023년 12월)

MariaDB4j는 Java에서 JAR 하나로 테스트용 MariaDB를 띄우는 라이브러리다. 임시 디렉터리 경로가 고정이라 한 서버에서 여러 사용자가 쓰면 부딪힌다는 이슈가 있었다. 디렉터리 이름에 해시코드를 넣어 나누는 PR을 보냈다.

메인테이너는 빌드가 깨져 있으니 로그를 보고 고쳐 달라고 했다. 빌드가 통과하면 리뷰하겠다는 조건이었다. 반년 동안 고치지 않았고, 메인테이너가 같은 내용을 정리한 새 PR을 열면서 내 PR을 닫았다. 그 PR은 다른 기여자가 이어받은 #1145로 대체돼 2025년 4월에 머지됐다.

## aws-cli의 Spark SQL 예제 (2022년 8월)

`aws emr create-cluster`의 `--steps`에 Spark SQL 쿼리를 넣다가 따옴표 파싱 에러를 만났다. 이슈로 남긴 뒤 따옴표를 어떻게 써야 하는지 보여 주는 예제와 테스트를 붙였다.

AWS 쪽에서는 이 변경은 서비스 문서 담당자와 더 논의해야 한다며 이슈에서 이어 가자고 했다. 2년 가까이 지나 다른 담당자가 무엇을 하려던 건지 이슈에 설명해 달라며 PR을 닫았다. 거기에 답하지 않았고, 이슈도 자동으로 닫혔다.

## 3년 넘게 열린 PR

`airflow-maintenance-dags`는 Airflow 메타데이터 DB를 정리하는 DAG 모음이다. DAG마다 `doc_md`를 붙여 Airflow 화면에서 바로 README로 갈 수 있게 하고, 디렉터리 이름을 DAG ID와 맞췄다. 메인테이너를 지목해 리뷰를 부탁했지만 2026년 11월 현재 답이 없다.

## English summary

These are pull requests to other projects that were not merged. Two Docker Compose PRs for boolean values were closed by me after I found that Compose v2 already handled them, while the maintainers had said v1 would only take security fixes. A MariaDB4j PR failed to build and was later replaced by another PR that was merged in 2025. An aws-cli example PR was closed after almost two years when a maintainer asked what I meant and I did not answer, and a PR adding `doc_md` to Airflow maintenance DAGs has been open since June 2023.
