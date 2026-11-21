---
# the default layout is 'page'
icon: fas fa-briefcase
order: 1
title: Portfolio
---

데이터 엔지니어입니다. Kafka, Flink, Iceberg, Athena, Superset으로 이어지는 파이프라인에서 생긴 문제를 끝까지 따라가 원인을 찾고, 원인이 오픈소스에 있으면 그 프로젝트에 직접 고쳐서 올립니다.

- GitHub: [seunggabi](https://github.com/seunggabi)
- LinkedIn: [seunggabi](https://www.linkedin.com/in/seunggabi/)

## 실무에서 한 일

| 분야 | 한 일 | 기술 | 글 |
|---|---|---|---|
| 결제 서비스 데이터 플랫폼 (현재) | MySQL CDC를 Iceberg 레이크하우스로 적재·운영, Kafka·Flink 스트리밍, Airflow·EMR Spark 배치, Superset·DataHub 운영 | Debezium, Kafka Connect, Amazon MSK, Iceberg, Flink, Spark, Airflow, Athena, Superset, DataHub, Terraform | - |
| 모빌리티 서비스 데이터 배치 | 모노 레포와 공용 core 모듈 도입, Airflow DAG 버전 관리 개선, Hive·DynamoDB·S3를 오가는 Kotlin 배치 잡 | Kotlin, Airflow, Hive, DynamoDB, S3, Athena | - |
| 커머스 광고 데이터 | 광고 리포트 배치(Spark 잡과 Spring Batch), 서비스 아홉 개의 빌드·배포 구조 | Kotlin, Spring Boot, Spring Batch, Spark on EMR, MySQL, GitHub Actions, Gradle | [리포트 배치](/posts/ad-report-batch/), [빌드·배포 구조](/posts/ad-platform-ci-cd/) |
| 음악·오디오 서비스 데이터 | 로그 수집 API부터 Hive·Spark 지표 배치, 연말 결산, 대시보드까지 | Spring Boot, Kafka, Logstash, Spark Streaming, Spark SQL, PySpark, Hive, ORC, HBase, MySQL, Django, Vue, Celery, Zeppelin | [수집 API](/posts/log-collection-api/), [스트리밍 적재](/posts/spark-streaming-to-hive/), [Logstash 중계](/posts/logstash-kafka-relay/), [지표 배치](/posts/pyspark-indicator-batch/), [연말 결산](/posts/year-end-recap-spark/), [대시보드 API](/posts/metrics-dashboard-api/) |

## 오픈소스 기여

2018년부터 47개 저장소에 PR 78개를 보냈고 33개가 머지됐습니다. 그중 데이터 도구 쪽 기여만 추렸습니다.

| 프로젝트 | 기여 | 결과 | 글 |
|---|---|---|---|
| Apache Spark | `SparkSession.builder.config(Map)` 추가. Spark 3.4.0에 포함, PySpark도 같은 API를 따라 추가 | 2022-08 커밋 | [글](/posts/spark-session-config-map/) |
| Apache Superset | 데이터셋 시간 포맷에 마이크로초 epoch(`epoch_us`) 지원 추가. 엔진별 SQL, 필터, 검증까지 21개 파일 | 2026-09 머지 | [글](/posts/superset-epoch-us/) |
| Apache Superset | MCP API 키 화면이 발급 가능한 범위만 보여 주도록 수정, 500을 400으로 | 2026-11 머지 | [글](/posts/superset-api-key-scopes/) |
| Apache Superset | master CI를 깨뜨린 import 누락과 번역 템플릿 수정 | 2026-09 머지 | [글](/posts/superset-download-reason-sip/) |
| PyAthena | Athena 복합 타입(MAP, ARRAY, STRUCT) 값이 NULL로 사라지던 문제 수정. 리뷰를 거쳐 "완전히 파싱하거나 원본 그대로" 규칙으로 정리 | 2026-09 머지 | [글](/posts/pyathena-complex-types/) |
| Apache Airflow | `TableauOperator`에 템플릿 필드 추가. Tableau provider 4.1.0에 포함 | 2023-02 머지 | [글](/posts/airflow-tableau-template-fields/) |
| Airflow Helm 차트 | release 이름 길이 제한을 43자에서 40자로 낮춰 StatefulSet 파드 생성 실패를 막음. 차트 8.6.1에 포함 | 2022-06 머지 | [글](/posts/airflow-helm-release-name/) |
| Iceberg Kafka Connect | 대소문자 섞인 이름에서 싱크 태스크가 NPE로 죽던 문제 분석과 수정안 | 저장소 이관으로 닫음 | [글](/posts/iceberg-kafka-connect-mixed-case/) |

진행 중인 제안도 있습니다.

| 프로젝트 | 내용 | 상태 | 글 |
|---|---|---|---|
| Apache Superset | 다운로드 사유 기록 기능 제안(SIP-230), dev@ 논의와 투표 진행 | 투표 중 | [글](/posts/superset-download-reason-sip/) |
| Apache Iceberg | Kafka Connect id-columns 조회에 대소문자 무시 옵션 적용, 에러 메시지 보강 | 리뷰 대기 | [#19042](https://github.com/apache/iceberg/pull/19042) |
| AWS EMR TCO 시뮬레이터 | YARN 작업 기록 수집 스크립트가 활성 EMR 클러스터를 모두 돌도록 | 리뷰 대기 | [글](/posts/emr-yarn-log-collector/) |

## 사이드 프로젝트

| 프로젝트 | 내용 | 기술 |
|---|---|---|
| [claude-dashboard](https://github.com/seunggabi/claude-dashboard) | Claude Code 세션을 k9s처럼 한 화면에서 보고 관리하는 터미널 UI. Homebrew로 배포, v0.15.0 | Go, Bubble Tea, tmux ([글](/posts/claude-dashboard/)) |
| [cron-manager](https://github.com/seunggabi/cron-manager) | crontab을 GUI로 다루는 Electron 앱. 쓰기 전 백업, 실행 잠금 | TypeScript, Electron ([글](/posts/cron-manager/)) |

## 다루는 기술

| 분야 | 기술 |
|---|---|
| 수집·스트리밍 | Kafka, Kafka Connect, Debezium, Amazon MSK, Flink, Spark Streaming, Avro |
| 저장·쿼리 | Spark, Hive, Iceberg, S3 Tables, Parquet, ORC, HDFS, Athena, Trino, MySQL, Redis |
| 오케스트레이션·카탈로그 | Airflow, DataHub |
| 시각화 | Superset, Tableau, Zeppelin |
| 인프라 | Kubernetes, Helm, Terraform, AWS (EMR, MSK, S3, Athena, Lambda, Lake Formation), GitHub Actions, Datadog |
| 언어 | Python, Java, Scala, Kotlin, Go, TypeScript, SQL |

## 기술 개념 정리

실무 글과 오픈소스 기여에서 다룬 개념을 공식 문서 기준으로 다시 정리했습니다.

{% assign concept_posts = site.tags.concept | sort: "date" %}
{% for p in concept_posts %}
- [{{ p.title }}]({{ p.url | relative_url }})
{% endfor %}

## 시스템 디자인

자주 나오는 데이터 시스템 문제를 요구사항, 규모 계산, 구조, 장애 대응 순서로 풀었습니다.

{% assign design_posts = site.tags["system-design"] | sort: "date" %}
{% for p in design_posts %}
- [{{ p.title }}]({{ p.url | relative_url }})
{% endfor %}
