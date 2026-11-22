---
title: "YARN 컨테이너는 왜 Spark executor 힙보다 크게 잡힐까"
date: 2026-11-20 05:04:00 +0900
categories: [Batch & Query, Spark]
tags: [spark, yarn, hadoop, emr, dynamic-allocation, concept]
description: "executor-memory 4g로 띄웠는데 YARN 컨테이너는 5GB로 잡히고 가끔 물리 메모리 초과로 죽는 이유를 Spark가 YARN에 컨테이너 크기를 요청하는 방식으로 설명한다."
---

`--executor-memory 4g`로 띄운 잡인데 ResourceManager 화면에서는 executor 컨테이너 하나가 5GB로 잡혀 있다. 같은 잡이 어떤 날은 `Container killed by YARN for exceeding physical memory limits`로 executor를 잃는다. 둘 다 Spark가 YARN에 컨테이너를 어떤 크기로 요청하고, YARN이 그 요청을 어떻게 고치는지에서 나온다.

버전은 Spark 4.3, Hadoop 3 YARN 기준이다. 숫자는 Apache Hadoop 기본값으로 계산했다. EMR은 노드 타입별 값이 달라 결과가 다르다. EMR은 `yarn.scheduler.minimum-allocation-mb`를 1로 두므로, 4g executor 컨테이너가 올림 없이 4505MB 안팎으로 잡힌다.

## 누가 자원을 주고 누가 받는가

| 구성 요소 | 하는 일 |
|---|---|
| ResourceManager (RM) | 클러스터 전체 자원을 보고 큐와 스케줄러 규칙에 따라 컨테이너를 내준다 |
| NodeManager (NM) | 노드마다 하나. 컨테이너 프로세스를 띄우고 메모리 한도를 넘는지 감시한다 |
| ApplicationMaster (AM) | 애플리케이션마다 하나. RM에 executor용 컨테이너를 요청한다 |
| 컨테이너 | 메모리와 vcore가 정해진 실행 단위. executor 하나가 컨테이너 하나다 |

## client 모드와 cluster 모드

차이는 driver가 어디서 도는가다.

| | client | cluster |
|---|---|---|
| driver 위치 | `spark-submit`을 실행한 프로세스 | YARN이 띄운 AM 컨테이너 안 |
| AM의 역할 | 자원 요청만 한다 | 자원 요청과 driver 실행 |
| AM 메모리 | `spark.yarn.am.memory` (기본 512m) | `spark.driver.memory` (기본 1g) |
| 제출한 쪽이 끊기면 | driver가 같이 죽는다 | 앱은 계속 돈다 |

client 모드에서는 driver JVM이 이미 떠 있으므로 `spark.driver.memory`를 애플리케이션 코드의 `SparkConf`로 바꿀 수 없다. `--driver-memory`나 `spark-defaults.conf`로 넣어야 한다.

## 컨테이너 크기는 네 값의 합을 올림한 것이다

Spark가 executor 하나에 요청하는 메모리는 다음 합이다.

```text
spark.executor.memory            (JVM 힙)
+ spark.executor.memoryOverhead   (기본 max(힙 x 0.10, 384MB))
+ spark.memory.offHeap.size       (offHeap.enabled일 때만)
+ spark.executor.pyspark.memory   (지정했을 때만)
```

overhead는 JVM 힙 바깥에서 쓰는 메모리 몫이다. `spark.executor.pyspark.memory`를 따로 두지 않은 PySpark 잡에서는 Python 워커도 이 overhead 안에서 산다. 비율은 `spark.executor.memoryOverheadFactor`(기본 0.10), 최솟값은 `spark.executor.minMemoryOverhead`(기본 384m)이고, `memoryOverhead`를 직접 지정하면 둘 다 무시된다.

RM은 받은 요청을 그대로 쓰지 않는다. CapacityScheduler는 요청 메모리를 `yarn.scheduler.minimum-allocation-mb`(기본 1024)의 배수로 올림하고, `yarn.scheduler.maximum-allocation-mb`(기본 8192)라는 최대치를 넘는 요청은 거부한다. 아래 표는 Apache Hadoop 기본값 기준이다.

| `--executor-memory` | overhead | Spark 요청 | YARN 컨테이너 |
|---|---|---|---|
| 2g | 384MB (204MB가 최솟값보다 작음) | 2432MB | 3072MB |
| 4g | 409MB | 4505MB | 5120MB |
| 8g | 819MB | 9011MB | 제출 실패 |

8g 줄은 기본 최대 할당 8192MB를 넘는다. Spark는 제출 단계에서 RM이 알려 준 최대치와 합을 비교해, 넘으면 `is above the max threshold`로 시작하지도 않는다.

NM은 `yarn.nodemanager.pmem-check-enabled`(기본 true)로 컨테이너의 실제 물리 메모리를 감시한다. 힙은 남았는데 힙 바깥 사용량이 한도를 넘으면 NM이 컨테이너를 죽이고, Spark는 위의 `exceeding physical memory limits` 메시지 끝에 `Consider boosting spark.executor.memoryOverhead`를 붙인다. 이 경우 `--executor-memory`를 늘려도 overhead는 10%만 따라 늘어난다. 직접 올릴 값은 overhead다.

## executor cores와 vcore

`spark.executor.cores`는 YARN에서 기본 1이다. Spark는 이 값을 vcore로 요청하고, executor 안에서 동시에 도는 태스크 수도 이 값으로 정한다.

CapacityScheduler의 기본 `resource-calculator`인 `DefaultResourceCalculator`는 메모리만 본다. 소스상 정규화할 때 메모리만 다시 계산하고 vcore는 1로 만들기 때문에, `--executor-cores 4`로 띄워도 RM 화면에는 컨테이너마다 vcore 1로 보이게 된다. executor 안의 태스크 슬롯은 그대로 4개다. CPU까지 나눠 주려면 `DominantResourceCalculator`로 바꿔야 한다.

## dynamic allocation은 셔플 파일을 어디 둘지가 핵심이다

`spark.dynamicAllocation.enabled`(기본 false)를 켜면 Spark가 executor 수를 부하에 맞춰 늘리고 줄인다. 아래 설정 이름 앞에는 모두 `spark.dynamicAllocation.`가 붙고, `spark.executor.instances`가 `initialExecutors`보다 크면 그 값이 시작 개수가 된다.

| 설정 | 기본값 | 동작 |
|---|---|---|
| `schedulerBacklogTimeout` | 1s | 대기 태스크가 이만큼 밀리면 executor를 요청한다 |
| `sustainedSchedulerBacklogTimeout` | 위와 같음 | 계속 밀리면 이 간격으로 다시 요청. 1, 2, 4, 8개로 늘린다 |
| `executorIdleTimeout` | 60s | 이만큼 놀던 executor를 반납한다 |
| `cachedExecutorIdleTimeout` | infinity | 캐시를 가진 executor는 기본적으로 반납하지 않는다 |

문제는 셔플이다. executor는 map 출력을 자기 디스크에 쓰고 다른 executor의 요청에 직접 응답한다. 그 executor를 반납하면 셔플 파일을 다시 계산해야 한다. 이를 피하는 방법 넷 가운데 YARN에서 손이 덜 가는 두 가지는 다음과 같다. 나머지는 decommission 때 셔플 블록을 다른 executor로 옮기는 방식과 사용자 정의 `ShuffleDataIO` 플러그인이다.

| 방식 | 동작 |
|---|---|
| 외부 셔플 서비스 | NM 안에서 도는 `YarnShuffleService`가 셔플 파일을 대신 내준다. executor가 사라져도 파일은 남는다 |
| 셔플 추적 (`spark.dynamicAllocation.shuffleTracking.enabled`, 기본 true) | 서비스 없이, 활성 잡의 셔플 데이터를 가진 executor를 살려 둔다. `shuffleTracking.timeout` 기본값이 infinity라 셔플이 GC될 때까지 반납이 늦어진다 |

외부 셔플 서비스는 모든 NM에 `spark-<version>-yarn-shuffle.jar`를 클래스패스에 두고 `yarn-site.xml`에 등록한 뒤 NM을 재시작해야 한다. 애플리케이션 쪽에서는 `spark.shuffle.service.enabled=true`를 켠다.

```xml
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle,spark_shuffle</value>
</property>
<property>
  <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
  <value>org.apache.spark.network.yarn.YarnShuffleService</value>
</property>
```

Amazon EMR은 4.4.0부터 `spark.dynamicAllocation.enabled`를 true로 두고 셔플 서비스도 직접 설정해 둔다. EMR의 `spark` 분류에서 `maximizeResourceAllocation`을 true로 주면, core 인스턴스 그룹 한 대에서 executor가 쓸 수 있는 최대치로 `spark.executor.memory`, `spark.executor.cores`, `spark.default.parallelism`(YARN 컨테이너용 CPU 코어의 2배) 등을 `spark-defaults`에 채운다. HBase처럼 다른 분산 애플리케이션이 같이 도는 클러스터에서는 EMR이 따로 넣는 YARN 설정과 부딪혀 Spark 앱이 실패할 수 있다.

## 컨테이너 로그는 끝난 뒤 한곳으로 모인다

실행 중인 컨테이너의 로그는 각 노드의 `yarn.nodemanager.log-dirs` 아래에 있다. `yarn.log-aggregation-enable`(기본 false)을 켜면 앱이 끝난 뒤 로그가 HDFS 같은 파일 시스템의 `yarn.nodemanager.remote-app-log-dir`(기본 `/tmp/logs`)로 옮겨지고 노드의 로컬 파일은 지워진다.

```bash
yarn logs -applicationId application_1700000000000_0042
```

이 명령은 그 앱의 모든 컨테이너 로그를 한 번에 출력한다. 보관 기간 `yarn.log-aggregation.retain-seconds`는 기본 -1, 지우지 않는다는 뜻이다. cluster 모드에서는 driver 로그도 AM 컨테이너 로그로 이 안에 들어간다.

## 할당량 수치와 설정을 넣는 시점

[EMR YARN 로그 수집 글](/posts/emr-yarn-log-collector/)에서 PR을 보낸 AWS의 수집 스크립트는 RM REST API `/ws/v1/cluster/apps`를 불러 애플리케이션 목록과 자원 사용량을 JSON으로 쌓는다. 이 API의 `memorySeconds`, `vcoreSeconds`는 애플리케이션에 할당된 자원을 시간으로 곱한 값이다. 할당량이라면 overhead와 올림이 들어간 컨테이너 크기로 잡힐 것이고, JVM이 실제로 쓴 메모리와는 다를 것이다.

[SparkSession config(Map) 글](/posts/spark-session-config-map/)의 맵 오버로드는 YAML에서 읽은 설정을 빌더에 한꺼번에 넣는다. 그 맵에는 성격이 다른 두 종류가 섞일 수 있다. `spark.driver.memory`는 driver JVM이 뜬 뒤라 코드로 넣어도 반영되지 않고, `spark.executor.instances` 같은 배포 설정도 SparkContext가 이미 떠 있으면 무시된다. 그래서 이런 값은 `spark-submit`이나 설정 파일로 넣는다. [연말 결산 글](/posts/year-end-recap-spark/)의 잡이 세션 설정으로 넣은 `spark.sql.autoBroadcastJoinThreshold`, 그리고 함께 켠 `spark.sql.sources.partitionOverwriteMode`는 실행 중에 바꿀 수 있는 SQL 설정 쪽이다.

## 참고 자료

- [Spark: Running on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html)
- [Spark: Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Spark: Job Scheduling (Dynamic Resource Allocation)](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)
- [Spark 소스: YarnAllocator.scala](https://github.com/apache/spark/blob/master/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala)
- [Spark 소스: Client.scala (verifyClusterResources)](https://github.com/apache/spark/blob/master/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/Client.scala)
- [Spark 소스: ResourceProfile.scala (calculateOverHeadMemory)](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala)
- [Hadoop: yarn-default.xml](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)
- [Hadoop: Capacity Scheduler](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html)
- [Hadoop: ResourceManager REST API](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html)
- [Hadoop 소스: DefaultResourceCalculator.java](https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/resource/DefaultResourceCalculator.java)
- [Amazon EMR: Configure Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html)

## English summary

On YARN, each Spark executor is one container, and the ApplicationMaster asks the ResourceManager for it; in cluster mode the driver runs inside that ApplicationMaster. The request is heap plus overhead, which defaults to the larger of 10% of heap and 384MB, and the CapacityScheduler rounds it up to a multiple of `yarn.scheduler.minimum-allocation-mb`, so with Apache Hadoop defaults a 4g executor becomes a 5120MB container, while EMR's own defaults give a different size. The AWS log collector I contributed to reads allocation numbers from the ResourceManager API, so those numbers should include this overhead and rounding, not what the JVM really used. Dynamic allocation needs shuffle files to outlive executors: an external shuffle service lets executors go early but must run on every NodeManager, while shuffle tracking needs no setup but keeps executors alive until their shuffle data is cleaned up.
