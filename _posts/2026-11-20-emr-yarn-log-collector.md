---
title: "활성 EMR 클러스터를 모두 도는 YARN 기록 수집 PR"
date: 2026-11-20 03:55:00 +0900
categories: [Platform & Infra, AWS]
tags: [open-source, emr, hadoop, yarn, aws-cli]
---

온프레미스 Hadoop을 Amazon EMR로 옮기려면 비용부터 따져야 한다. 지금 클러스터에서 어떤 작업이 얼마나 자원을 쓰는지 알아야 EMR에서 몇 대를 어떤 인스턴스로 띄울지 계산할 수 있다.

AWS가 공개한 [migration-hadoop-to-emr-tco-simulator](https://github.com/awslabs/migration-hadoop-to-emr-tco-simulator)는 이걸 세 단계로 나눈다. YARN 작업 기록을 모으고(`yarn-log-collector`), 분석하고(`yarn-log-analysis`), 최적화한 TCO를 계산한다(`optimized-tco-calculator`). 첫 단계인 수집 스크립트에 기능을 하나 넣는 PR([#8](https://github.com/awslabs/migration-hadoop-to-emr-tco-simulator/pull/8))을 2023년 2월에 보냈다.

## 수집 스크립트가 하는 일

YARN ResourceManager는 REST API로 클러스터에서 실행된 애플리케이션 목록과 자원 사용량을 준다.

```text
GET http://<ResourceManager>:8088/ws/v1/cluster/apps
```

`start-collector.sh`는 이 API를 `curl`로 불러 JSON 파일로 저장한다. 보안 설정에 따라 세 가지 모드가 있다.

| `-m` | 환경 | 호출 |
|---|---|---|
| 1 | HTTPS + Kerberos | `curl -k --negotiate -u: ...` |
| 2 | HTTPS | `curl -k -u: ...` |
| 3 | HTTP | `curl ...` |

README는 이 스크립트를 cron으로 4시간마다 돌리는 예를 든다. 완료된 애플리케이션은 ResourceManager에 무한정 남지 않기 때문에, 주기적으로 받아 쌓아 둬야 분석할 만큼의 기록이 모인다.

## 클러스터가 여러 개라면

원래 스크립트는 `-u`로 ResourceManager 주소 하나를 받았다. EMR 클러스터를 여러 개 띄워 둔 계정이라면 클러스터마다 주소를 찾아 따로 돌려야 한다.

PR에서는 `-u`를 생략하면 `aws emr list-clusters --active`로 활성 클러스터 ID를 받고, `aws emr describe-cluster`로 각 클러스터의 마스터 주소를 `jq`로 뽑아 차례로 수집하게 했다. 이 경로에서는 이미 EMR에 떠 있는 클러스터가 수집 대상이 된다. 주소를 직접 넘기는 기존 방식은 그대로 두고, 인자 형식만 URL 전체 대신 호스트로 바꿨다.

```text
sh start-collector.sh -m 3
### Get emr list-clusters --active.
<ip-1> emr-test
<ip-2> emr-test2
### Start yarn logs collecting from curl.
### HTTP Secure-Mode: 3
...
### Yarn Cluter API target: http://<ip-1>:8088/ws/v1/cluster/apps
```

이 PR은 2026년 11월 현재 리뷰 없이 열려 있다. 저장소의 마지막 push는 2024년 5월이다.

## English summary

AWS's migration-hadoop-to-emr-tco-simulator estimates the cost of moving on-premises Hadoop to Amazon EMR, and its first step collects application history from the YARN ResourceManager REST API with curl, in Kerberos HTTPS, HTTPS, or HTTP mode. My PR made the collector call `aws emr list-clusters --active` when no ResourceManager host is given and collect from every active cluster in turn, using `jq` to read the addresses. The PR has been open without review since February 2023.
