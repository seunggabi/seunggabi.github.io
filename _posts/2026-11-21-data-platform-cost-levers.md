---
title: "AWS 데이터 플랫폼 비용은 어디서 새는가"
date: 2026-11-21 03:26:00 +0900
categories: [Platform & Infra, AWS]
tags: [aws, emr, aws-msk, s3, athena, s3-tables, cost, concept]
---

데이터 플랫폼 청구서는 대개 EMR, MSK, S3, Athena, 데이터 전송 다섯 줄로 갈린다. 줄마다 과금 단위가 다르고, 새는 곳도 다르다. EMR은 일이 끝난 뒤에도 켜져 있는 시간에서, MSK는 한 번 늘리면 줄일 수 없는 디스크에서, S3는 작은 파일과 지워지지 않은 버전에서 샌다. 단가를 깎는 것보다 과금되는 양과 시간을 줄이는 쪽이 대개 효과가 크다.

단가는 리전마다 다르고 자주 바뀌어서 숫자는 넣지 않았다. 아래 표의 과금 단위와 손잡이는 리전이 바뀌어도 그대로다.

버전은 2026년 11월에 확인한 AWS 요금 페이지와 문서 기준이다.

| 서비스 | 과금 단위 | 손잡이 | 흔한 실수 |
|---|---|---|---|
| EMR on EC2 | EC2 요금 + EMR 요금, 초 단위(최소 1분), EBS 별도 | 자동 종료, task 노드 spot, 인스턴스 계열 | 일 끝난 클러스터가 유휴로 남는다 |
| MSK Standard | 브로커 시간, 프로비저닝한 스토리지 GB-월 | 브로커 타입, retention, tiered storage | 버스트형 브로커의 크레딧 잔량을 보지 않는다 |
| S3 일반 버킷 | 저장 GB-월, 요청 1,000건 단위, 전환 요청 | lifecycle, 이전 버전 만료, 파일 크기 | 작은 파일을 Glacier로 옮겨 요청 비용만 낸다 |
| Athena (쿼리당 과금) | 스캔한 바이트 | 파티션, 컬럼 포맷, 압축 | 파티션 조건 없는 쿼리 |
| 데이터 전송 | AZ를 넘는 바이트, 양방향 | 클라이언트와 리더의 AZ 맞추기 | 같은 리전이면 공짜라고 생각한다 |

## EMR은 일을 마친 뒤의 시간이 비싸다

EMR on EC2 노드 하나에는 EC2 요금과 EMR 요금이 같이 붙는다. 둘 다 초 단위로 매기고 최소 1분이다. EBS 볼륨은 따로 과금된다. 할인 수단(spot, Savings Plans)은 EC2 몫에만 적용되고 EMR 몫은 그대로다.

이 구조에서 가장 큰 낭비는 아무것도 하지 않는 클러스터다. 예를 들어 Airflow가 클러스터를 만들고 스텝을 나중에 넣은 뒤 지우는 구조라면 `KeepJobFlowAliveWhenNoSteps`를 켜 둬야 한다. 그러면 종료 태스크가 돌지 못할 때 클러스터는 스텝을 기다리며 계속 과금된다. 클러스터를 만드는 단계에서 idle timeout을 기본으로 넣어 두는 것이 이 경우의 안전장치다.

자동 종료 정책의 idle timeout은 1분에서 7일 사이로 정하고, 지정하지 않으면 60분이다. EMR 5.34.0, 6.4.0 이후는 실행 중인 YARN 애플리케이션과 대기 중인 스텝이 없고 HDFS 사용률이 10% 미만인 등 조건을 모두 만족할 때 유휴로 본다. timeout을 짧게 잡을수록 돈은 덜 새지만, 스텝 사이 간격보다 짧으면 다음 스텝이 들어오기 전에 클러스터가 내려간다. 처음에는 길게 잡았다가 실측한 스텝 간격에 맞춰 줄이는 편이 안전하다. 생성 후 오래된 인스턴스를 알리는 알람은 나이만 보므로, 유휴를 보는 이 정책과 역할이 다르다.

### spot은 task 노드에만 둔다

core는 on-demand로 두고 task만 spot으로 두는 이유는 데이터가 어디 있느냐다. core 노드는 HDFS 블록을 들고 있어서 회수되면 데이터가 사라질 수 있다. task 노드는 HDFS 데이터를 갖지 않으므로 회수돼도 잃는 것은 진행 중이던 태스크뿐이다. AWS도 데이터를 잃으면 안 되는 워크로드에 primary와 core는 on-demand, task는 spot을 권한다.

하나 더 볼 것은 ApplicationMaster 위치다. EMR 6.x부터 YARN 노드 레이블이 기본으로 꺼져 있어서 AM이 task 노드에도 뜰 수 있다. cluster 모드에서는 driver가 AM 안에서 돌기 때문에, AM이 올라간 spot 노드가 회수되면 잡 전체가 죽는다. `yarn.node-labels.enabled`와 `yarn.node-labels.am.default-node-label-expression: 'CORE'`를 주면 AM을 core에만 둘 수 있다. driver가 AM 안에 들어가는 구조는 [Spark on YARN 글](/posts/spark-on-yarn-resources/)에 적었다.

managed scaling에서는 `MaximumOnDemandCapacityUnits`까지 on-demand로 늘리고 나머지를 spot으로 채운다. 이 상한을 core 수와 같게 두면 늘어나는 노드는 모두 spot이 된다. 잡마다 `spark.yarn.maxAppAttempts`로 같은 클러스터에서 AM을 다시 띄우지 않게 하고, 재시도는 클러스터를 새로 만들어 하는 방법도 있다.

### 인스턴스 fleet과 Graviton

instance fleet은 노드 유형마다 여러 인스턴스 타입을 후보로 주고 on-demand와 spot 목표 용량을 따로 정하는 방식이다. 기본 구성은 fleet당 5개이고, 할당 전략을 켜고 CLI나 API로 만들면 30개까지 줄 수 있다. EMR 6.10.0 이후 spot 기본 전략은 `price-capacity-optimized`다. spot을 정해진 시간 안에 못 구하면 `SWITCH_TO_ON_DEMAND`로 넘길 수 있지만, 이 동작은 클러스터를 처음 만들 때만 적용되고 크기를 바꿀 때는 적용되지 않는다. 후보 타입이 많을수록 spot을 구할 확률은 올라가고, executor 크기를 타입마다 맞추는 일은 늘어난다.

Graviton(arm64) 인스턴스는 같은 세대 비슷한 크기의 x86 인스턴스보다 시간당 가격이 낮게 매겨진다. 대가는 아키텍처다. 마스터, core, task를 모두 Graviton 계열로 두면 부트스트랩에서 받는 JDK 같은 바이너리도 aarch64 빌드여야 한다. 네이티브 라이브러리를 쓰는 잡이면 arm64 빌드가 있는지부터 봐야 한다.

### 할당한 만큼 돈을 낸다

YARN은 Spark가 요청한 메모리를 올림해서 컨테이너를 잡고, EC2는 그 컨테이너가 실제로 쓴 메모리와 상관없이 인스턴스 시간으로 과금한다. [EMR YARN 로그 수집 글](/posts/emr-yarn-log-collector/)에서 PR을 보낸 AWS TCO 시뮬레이터의 수집 스크립트가 모으는 `memorySeconds`, `vcoreSeconds`도 할당량이라는 점은 Spark on YARN 글에 적었다. 할당량 기준으로 크기를 잡으면 overhead와 올림까지 들어간 보수적인 숫자가 나온다(추론이다).

## MSK는 브로커 시간과 프로비저닝한 디스크로 낸다

Standard 브로커 요금은 브로커 시간(초 단위)과 프로비저닝한 스토리지 GB-월이 기본이고, 스토리지 처리량을 따로 프로비저닝하면 그 몫이 붙는다. 브로커 사이 복제 트래픽에는 전송 요금이 없다. 클라이언트와 주고받는 데이터에는 일반 AWS 전송 요금이 붙는다.

디스크는 쓴 양이 아니라 잡아 둔 양으로 낸다. 브로커 스토리지는 늘릴 수만 있고 줄일 수 없으며, 늘린 뒤에는 최소 6시간에서 24시간 넘게 다시 늘릴 수 없는 cool-down이 있다. 그래서 디스크가 찰 때 볼륨부터 키우면 그 비용은 계속 남는다. retention을 줄이는 쪽은 되돌릴 수 있다. 디스크가 급히 찰 때는 retention을 잠깐 줄였다가 원래 설정으로 정확히 되돌려 공간을 비우는 방법이 있다. 오래 보관해야 하면 tiered storage가 있고, 저장 GB-월과 읽은 GB로 따로 과금된다. Express 브로커는 과금 축이 달라서, 브로커 시간에 더해 실제로 쓴 스토리지와 써 넣은 데이터 GB로 낸다.

브로커 줄만 보면 싼 T3가 운영 부담을 키울 수 있다. T3 브로커는 CPU 크레딧이 바닥나면 baseline에 묶인다. 이때 CPU 그래프는 낮고 평평한데 커넥터가 메타데이터 타임아웃으로 재시작을 되풀이하고 컨슈머 lag가 줄지 않을 수 있다. CDC 파이프라인이라면 방치했을 때 binlog를 놓쳐 전체 스냅샷을 다시 뜨거나, lag가 retention을 넘어 테이블에 빈 구간이 생긴다. 청구서의 브로커 줄만 보면 T3가 싸지만, 이 복구 작업은 그 줄에 나타나지 않는다. M5로는 `update-broker-type` 한 번에 롤링으로 올라가지만, M5나 M7g에서 T3로 내려오는 변경은 제자리에서 안 된다.

## S3는 객체 수와 버전이 청구서를 키운다

S3 일반 버킷은 저장량, 요청 수, 전송으로 낸다. 요청은 1,000건 단위로 매긴다. 같은 1GB라도 128MB 파일 8개와 1MB 파일 1,000개는 쓰기와 읽기 요청 수가 백 배 넘게 차이 난다. 같은 리전의 S3에서 다른 AWS 서비스로 나가는 전송은 무료라서, EMR과 Athena가 S3를 읽을 때 S3 쪽 비용은 대부분 요청 수에서 나온다.

lifecycle 규칙으로 저장 클래스를 내릴 때도 객체 하나가 전환 요청 하나다. 2024년 9월부터 128KB보다 작은 객체는 기본으로 전환되지 않는다. 작은 객체는 전환 요청 비용이 저장 절감보다 클 수 있기 때문이다. 클래스마다 조건도 있다.

| 저장 클래스 | 최소 저장 기간 | 최소 과금 크기 | 함정 |
|---|---|---|---|
| Standard-IA, One Zone-IA | 30일 | 128KB | 일찍 지우거나 옮기면 남은 기간을 낸다 |
| Glacier Instant Retrieval | 90일 | 128KB | 읽을 때 GB당 조회 요금 |
| Glacier Flexible Retrieval | 90일 | 없음 | 객체마다 메타데이터 40KB가 붙는다 |
| Glacier Deep Archive | 180일 | 없음 | 복원 후에만 읽을 수 있다 |
| Intelligent-Tiering | 없음 | 없음 | 객체당 모니터링 요금, 128KB 미만은 이동 대상이 아니다 |

버전 관리를 켠 버킷은 모든 버전을 온전한 객체로 과금한다. 이 버킷에서 만료 규칙은 삭제 마커만 남기고 이전 버전은 그대로 둔다. 실제로 지우려면 `NoncurrentVersionExpiration`을 따로 걸어야 한다. 버전 관리를 나중에 켠 버킷에서 저장량이 이유 없이 늘면 먼저 볼 곳이 여기다.

## 테이블 정리에 내는 돈과 쿼리에서 아끼는 돈

Athena는 쿼리당 과금 모드에서 스캔한 바이트로 낸다. 읽는 동안 생기는 S3 요청 비용은 S3 쪽에 따로 붙는다. 최소 과금과 스캔을 줄이는 방법은 [Athena 스캔 비용 글](/posts/athena-scan-and-cost/)에 정리했다.

Iceberg 테이블은 여기에 정리 비용이 하나 더 붙는다. 커밋마다 작은 파일과 스냅샷이 생기고, 다시 쓴 파일도 옛 스냅샷이 참조하는 동안은 지워지지 않는다. 일반 버킷에 두면 compaction과 스냅샷 만료를 EMR 같은 컴퓨팅으로 직접 돌리고, S3 Tables에 두면 저장, 요청, 객체당 모니터링 요금에 더해 compaction이 처리한 객체 수와 바이트로 낸다.

compaction에 돈을 쓸 만한지는 그 테이블을 얼마나 자주 읽느냐로 갈린다. 파일 수가 줄면 쿼리마다 읽는 요청 수가 줄고, 쌓인 delete 파일이 반영돼 읽기 계획이 가벼워진다. 자주 읽는 테이블일수록 이 절감이 반복된다. 거의 읽지 않는 테이블이라면 정리 비용이 절감보다 클 수 있다(일반적인 판단이다). 테이블 버킷과 일반 버킷 사이의 선택도 이 계산과 닮았다. 스트리밍으로 작은 파일이 계속 쌓이고 정리 배치 운영을 줄이고 싶으면 테이블 버킷, 파일 크기 관리가 쉽고 비용이 우선이면 일반 버킷에 직접 정리 배치를 두는 식이다. 다만 관리형 정리는 키 기준 중복 제거까지 하지 않으므로, 그런 정리가 필요하면 별도 잡이 남는다.

## 같은 리전 안에서도 AZ를 넘으면 낸다

EC2와 네트워크 인터페이스가 같은 리전의 다른 AZ와 주고받는 데이터는 들어오는 쪽과 나가는 쪽 모두 과금된다. 데이터 플랫폼에서 이 비용이 생기는 자리는 대략 정해져 있다.

| 경로 | AZ 간 전송 요금 |
|---|---|
| EMR 클러스터 안의 셔플 | AZ를 넘지 않는다. fleet에 서브넷을 여러 개 줘도 인스턴스는 한 AZ에만 뜬다 |
| MSK 브로커 사이 복제 | 과금하지 않는다 |
| 프로듀서, 컨슈머와 다른 AZ의 리더 | 있음 |
| 같은 리전 S3에서 EC2로 읽기 | 과금하지 않는다 |

컨슈머 쪽은 Kafka 2.4부터 가까운 팔로워에서 읽을 수 있다. MSK에서는 브로커 설정 `replica.selector.class`를 `org.apache.kafka.common.replica.RackAwareReplicaSelector`로 두고, 컨슈머에 자기 AZ ID(예: `use1-az1`)를 `client.rack`으로 준다. 프로듀서는 여전히 리더에 써야 하므로 쓰기 쪽 AZ 간 전송은 남는다. 세 AZ에 걸친 클러스터에서 리더가 고르게 퍼져 있고 프로듀서가 모든 파티션에 고르게 쓰면, 한 AZ의 프로듀서가 보내는 데이터의 3분의 2 정도는 다른 AZ의 리더로 간다(배치에서 나오는 추론이다).

## 참고 자료

- [Amazon EMR pricing](https://aws.amazon.com/emr/pricing/)
- [Amazon EMR: Using an auto-termination policy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-auto-termination-policy.html)
- [Amazon EMR: Instance types and best practices for Spot instances](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-instances-guidelines.html)
- [Amazon EMR: Planning and configuring instance fleets](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html)
- [Amazon EMR: Managed scaling node allocation strategy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/managed-scaling-allocation-strategy.html)
- [AWS Big Data Blog: Amazon EMR on Graviton2 instances](https://aws.amazon.com/blogs/big-data/amazon-emr-now-provides-up-to-30-lower-cost-and-up-to-15-improved-performance-for-spark-workloads-on-graviton2-based-instances)
- [Amazon MSK pricing](https://aws.amazon.com/msk/pricing/)
- [Amazon MSK: Scale up Standard broker storage](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-storage.html)
- [Amazon MSK: Custom configurations (replica.selector.class)](https://docs.aws.amazon.com/msk/latest/developerguide/msk-configuration-properties.html)
- [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/)
- [Amazon S3: Storage classes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html)
- [Amazon S3: Transitioning objects using S3 Lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html)
- [Amazon S3: Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
- [Amazon Athena pricing](https://aws.amazon.com/athena/pricing/)
- [Amazon Athena FAQs](https://aws.amazon.com/athena/faqs/)
- [Apache Iceberg: Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [AWS Global Network FAQs (data transfer across AZs)](https://aws.amazon.com/about-aws/global-infrastructure/global-network/faqs)

## English summary

A data platform bill on AWS splits into EMR, MSK, S3, Athena, and data transfer, and each line leaks money in a different place. EMR clusters that must stay alive without steps keep billing while idle, so an idle auto-termination policy is the main guard, and only task nodes should run on spot because core nodes hold HDFS data. On MSK, T3 brokers that run out of CPU credits look cheap but can stall connectors, and because broker storage can never shrink, shortening retention for a while is the reversible way to free disk. On S3, small objects drive request and transition costs, and versioned buckets need a noncurrent version rule. Paying for Iceberg compaction is worth it when a table is read often enough that fewer files save more on every query than the maintenance costs.
