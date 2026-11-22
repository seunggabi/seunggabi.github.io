---
title: "Spark OOM은 누가 프로세스를 죽였는지부터 읽는다"
date: 2026-11-21 05:11:00 +0900
categories: [Batch & Query, Spark]
tags: [spark, memory, oom, yarn, pyspark, concept]
---

Spark 잡이 메모리 때문에 죽었다는 로그는 크게 두 갈래다. 하나는 JVM이 힙을 못 구해 `java.lang.OutOfMemoryError`를 던진 경우이고, 다른 하나는 JVM은 멀쩡한데 YARN이나 Kubernetes가 컨테이너 한도를 넘었다고 프로세스를 죽인 경우다. 두 경우는 고칠 설정이 다르다. 힙을 늘려야 할 때 overhead를 늘리거나, 반대로 하면 비용만 늘고 잡은 또 죽는다.

컨테이너 크기를 계산하는 방법은 [YARN 자원 글](/posts/spark-on-yarn-resources/)에 정리했다. 여기서는 그 컨테이너 안의 힙이 어떻게 나뉘고, 어디가 먼저 터지는지를 본다.

버전은 Spark 4.3.0 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| reserved memory | 힙에서 먼저 떼어 두는 300MB. 통합 메모리 계산에서 빠진다 |
| `spark.memory.fraction` | (힙 - 300MB) 중 execution과 storage가 함께 쓰는 비율, 기본 0.6 |
| `spark.memory.storageFraction` | 통합 영역 중 캐시가 쫓겨나지 않는 몫, 기본 0.5 |
| execution memory | 셔플, 조인, 정렬, 집계가 쓰는 작업 메모리 |
| storage memory | `cache()`, `persist()` 블록과 브로드캐스트 블록이 쓰는 메모리 |
| user memory | 통합 영역 밖 나머지 힙. UDF 객체와 Spark 내부 메타데이터가 쓴다 |
| off-heap | `spark.memory.offHeap.enabled`를 켜면 쓰는 GC 밖 메모리 |
| memoryOverhead | 힙 바깥 네이티브 메모리 몫. 기본은 힙의 10%, 최소 384MB |
| spill | 작업 메모리를 더 못 받을 때 정렬된 중간 결과를 디스크로 내리는 것 |
| `spark.driver.maxResultSize` | action 하나가 driver로 모으는 직렬화 결과의 상한, 기본 1g |
| exit code 52 | executor JVM이 OutOfMemoryError로 스스로 끝날 때 Spark가 쓰는 종료 코드 |
| exit code 137 / 143 | 바깥에서 SIGKILL(128+9) / SIGTERM(128+15)으로 끝난 프로세스 |

## 힙은 세 덩어리로 나뉜다

executor JVM의 힙에서 Spark는 먼저 300MB를 떼어 둔다. 남은 부분의 60%가 execution과 storage가 함께 쓰는 통합 영역이고, 나머지 40%가 user memory다. 힙이 300MB의 1.5배인 450MB보다 작으면 executor는 시작하지 않는다.

`--executor-memory 4g`로 계산하면 다음과 같다. JVM이 보고하는 최대 힙을 기준으로 하므로 Spark UI의 숫자는 조금 다를 수 있다.

| 영역 | 계산 | 크기 |
|---|---|---|
| reserved | 고정 | 300MB |
| 통합 영역 M | (4096 - 300) x 0.6 | 약 2278MB |
| 그중 캐시 보호 구역 R | M x 0.5 | 약 1139MB |
| user memory | (4096 - 300) x 0.4 | 약 1518MB |

통합 영역 안의 경계는 고정돼 있지 않다. 비어 있으면 서로 빌려 쓴다. 다만 규칙이 비대칭이다. execution은 storage가 빌려 간 자리를 캐시 블록을 쫓아내면서 되찾을 수 있지만, 캐시 사용량이 R 아래로 내려가면 더 쫓아내지 않는다. 반대로 storage는 execution이 쓰고 있는 메모리를 뺏지 못한다. 그래서 캐시를 많이 걸어도 셔플은 대개 돌고, 대신 캐시가 밀려나 다시 계산된다.

execution 메모리는 태스크끼리도 나눠 쓴다. executor에서 동시에 도는 태스크가 N개면, 태스크 하나는 execution 풀의 최소 1/2N을 보장받고 최대 1/N까지 받는다. 그보다 더 필요하면 spill한다. `spark.executor.cores`를 4로 두면 위 4g executor에서 태스크 하나가 쓰는 작업 메모리는 많아야 570MB 안팎이다. 코어를 늘리면 처리량은 늘지만 태스크당 메모리는 그만큼 줄어든다.

## spill은 OOM을 막지만 모든 메모리를 지키지는 못한다

정렬, 해시 집계, sort-merge 조인처럼 Spark가 메모리 관리자에게 메모리를 요청하는 연산은 더 받지 못하면 중간 결과를 디스크로 내리고 계속 간다. 이게 spill이다. Spark UI의 스테이지 상세 화면에 `Shuffle spill (memory)`와 `Shuffle spill (disk)`로 보이고, SQL 실행 계획 그래프에는 Sort와 HashAggregate 노드의 `spill size`로 보인다. 앞은 메모리에 있던 역직렬화 크기, 뒤는 디스크에 쓴 직렬화 크기다. spill이 크면 느려지지만 잡은 끝난다.

문제는 메모리 관리자를 거치지 않는 메모리다. Scala나 Java UDF가 만드는 객체, 한 행이 통째로 큰 배열이나 문자열, `collect_list`로 키 하나에 모이는 목록은 user memory 쪽 힙을 그냥 차지한다. 이런 메모리는 spill 대상이 아니어서 넘치면 바로 `OutOfMemoryError`가 난다. 브로드캐스트 해시 테이블도 통째로 메모리에 올라가야 한다.

executor JVM이 OOM으로 죽으면 Spark의 예외 처리기가 종료 코드 52로 프로세스를 끝낸다. YARN에서 이 executor를 잃으면 Spark 로그에 exit status 52와 `OutOfMemoryError` 설명이 함께 남는다.

## 힙 바깥은 overhead가 맡는다

컨테이너 한도는 힙, `memoryOverhead`, off-heap 크기, `spark.executor.pyspark.memory`의 합이다. overhead는 JVM 자체의 네이티브 메모리, 인턴 문자열 같은 VM 오버헤드를 위한 몫이다. `spark.executor.pyspark.memory`를 따로 주지 않은 PySpark 잡에서는 Python 워커도 이 안에서 산다.

Python 워커는 executor JVM 바깥의 별도 프로세스다. Python UDF나 pandas UDF를 쓰면 동시에 도는 태스크마다 워커가 붙어 행을 받아 처리한다. Arrow를 쓰는 pandas UDF는 파티션을 `spark.sql.execution.arrow.maxRecordsPerBatch`(기본 10,000행) 단위 배치로 바꿔 보낸다. `groupBy().applyInPandas()`는 이 배치 한도를 적용하지 않고 그룹 하나를 통째로 메모리에 올린다. 그룹 크기가 치우치면 큰 그룹 하나가 워커 메모리를 넘긴다.

off-heap은 기본으로 꺼져 있다. `spark.memory.offHeap.enabled=true`와 양수 `spark.memory.offHeap.size`를 주면 Tungsten의 execution과 storage가 GC 밖 메모리 풀을 쓰고, 이 풀도 `storageFraction` 비율로 나뉜다. off-heap 크기는 힙에서 빠지지 않는다. 컨테이너 총량을 그대로 두려면 힙을 직접 줄여야 한다.

이 힙 바깥 사용량이 컨테이너 한도를 넘으면 JVM은 OOM을 던질 틈이 없다. YARN NodeManager가 프로세스를 죽이고, Spark는 다음 메시지를 남긴다.

```text
Container killed by YARN for exceeding physical memory limits.
5.1 GB of 5 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
```

이때 YARN 종료 상태는 -104(`KILLED_EXCEEDED_PMEM`)다. NodeManager는 SIGTERM을 먼저 보내고 잠시 뒤 SIGKILL을 보내므로 진단 문자열에 exit code 143이 같이 보이는 경우가 많다. Kubernetes에서는 cgroup 한도를 넘은 컨테이너가 OOMKilled로 137을 받는다. Kubernetes에서 JVM이 아닌 잡(PySpark 등)은 overhead 비율 기본값이 0.40이다.

## driver가 죽는 이유는 따로 있다

driver는 태스크를 돌리지 않으니 executor와 원인이 다르다.

`collect()`, `collectAsList()`, `toPandas()`는 결과 전체를 driver로 모은다. Arrow를 써도 `toPandas()`는 모든 행을 driver에 올린다. `spark.driver.maxResultSize`(기본 1g)는 action 하나의 직렬화 결과가 이 값을 넘으면 잡을 중단시켜 OOM 전에 막아 준다. 이 값을 0이나 큰 값으로 풀면 그 보호가 사라진다.

광고 리포트 배치의 Kotlin 잡은 쿼리 결과를 `collectAsList()`로 driver에 모아 문자열로 만든다. 결과가 작은 메일 첨부나 API 본문용 잡에 맞춘 방식이다. [Spark Streaming 적재 글](/posts/spark-streaming-to-hive/)도 키별로 합친 결과를 `collect`해 HDFS 파일에 붙였고, 그 글에 적은 대로 한 배치의 로그가 많아지면 driver 메모리가 부담이 되는 구조였다. 두 경우 모두 driver 메모리가 결과 크기의 상한이 된다.

브로드캐스트 조인도 driver를 거친다. 작은 쪽 행을 driver로 모아 해시 테이블을 만든 뒤 executor로 보낸다. 크기 추정이 틀려 큰 테이블이 브로드캐스트되면 driver가 먼저 넘친다. 추정 방식과 상한은 [조인 전략 글](/posts/spark-join-strategies/)에 있다.

파티션과 태스크가 지나치게 많아도 driver 메모리가 는다. driver는 셔플마다 map 태스크별로 reduce 파티션의 블록 크기 정보(MapStatus)를 들고 있다. reduce 파티션이 2,000개를 넘으면 Spark는 이 정보를 평균 크기 위주의 압축 형식으로 바꿔 저장한다. map과 reduce 쪽 파티션 수를 동시에 크게 늘리면 이 정보가 곱으로 커진다.

## executor OOM은 대개 태스크 하나가 너무 큰 것이다

같은 executor에서 다른 태스크는 다 끝나고 한두 개만 죽는다면 데이터 모양부터 본다.

| 원인 | 메모리가 커지는 지점 |
|---|---|
| skew된 파티션 | 키 하나가 몰린 파티션이 태스크당 1/N 몫을 넘는다 |
| 너무 적은 파티션 | `coalesce`나 작은 `spark.sql.shuffle.partitions`(기본 200)로 태스크당 입력이 커진다 |
| `explode` | 배열 길이만큼 행이 늘어 같은 파티션이 몇 배가 된다 |
| 넓은 행 | 큰 배열, 맵, 문자열 컬럼은 행 하나가 spill 단위보다 크다 |
| UDF와 Python | 메모리 관리자 밖에서 객체를 만들어 user memory나 overhead를 쓴다 |
| 큰 캐시 | 캐시 배치 크기 `spark.sql.inMemoryColumnarStorage.batchSize`(기본 10,000)를 키우면 캐시할 때 OOM 위험이 커진다 |

skew를 AQE가 어떻게 나누는지, 집계와 윈도우의 skew는 왜 자동으로 안 나뉘는지는 [셔플과 skew 글](/posts/spark-shuffle-and-skew/)에 정리했다. 메모리 쪽에서 덧붙일 것은 하나다. 태스크 하나의 작업 집합이 크면 RDD가 메모리에 다 들어가는 잡에서도 OOM이 난다. 이럴 때 첫 처방은 병렬도를 올려 태스크당 입력을 줄이는 것이다. 태스크가 200ms 정도로 짧아도 Spark는 잘 돈다.

## 증상을 읽고 고치는 순서

| 증상 | 죽인 주체 | 먼저 볼 것 |
|---|---|---|
| `OutOfMemoryError: Java heap space`, executor exit 52 | executor JVM | skew, 파티션 수, UDF, 넓은 행 |
| 긴 GC 시간, `GC overhead limit exceeded`(Parallel GC. JDK 17·21 기본 G1은 이 오류 없이 heap space로 끝난다) | executor JVM | 위와 같음, 캐시 양 |
| `exceeding physical memory limits`, -104, 143 | YARN NodeManager | Python 워커, off-heap, 네이티브 메모리, overhead |
| OOMKilled, 137 | Kubernetes(cgroup) | 위와 같음, 비JVM 잡의 overhead 비율 |
| driver OOM, `maxResultSize` 초과로 잡 중단 | driver | `collect`, `toPandas`, 브로드캐스트, 파티션 수 |

고치는 순서는 비용이 싼 쪽부터다.

1. 죽은 프로세스가 driver인지 executor인지, JVM OOM인지 컨테이너 kill인지 위 표로 가른다.
2. 데이터 모양을 고친다. 파티션 수를 늘리고, skew를 나누고, `collect`를 쓰기로 바꾸고, `explode` 전에 거른다.
3. 태스크당 메모리를 늘린다. `spark.executor.cores`를 줄이거나 힙을 키운다.
4. 컨테이너 kill이면 `spark.executor.memoryOverhead`나 `spark.executor.pyspark.memory`를 따로 올린다. 힙을 늘려도 overhead는 10%만 따라온다.
5. `spark.memory.fraction`과 `storageFraction`은 마지막이다. 기본값을 두라는 게 공식 권고이고, 바꾼다면 old generation 크기에 맞춘다.

1에서 원인을 가르지 않고 3이나 4부터 손대면 숫자만 커진다. 같은 skew 파티션은 executor를 두 배로 키워도 다음 날 데이터가 늘면 또 넘친다.

## 참고 자료

- [Spark: Tuning (Memory Management Overview)](https://spark.apache.org/docs/4.3.0/tuning.html)
- [Spark: Configuration](https://spark.apache.org/docs/4.3.0/configuration.html)
- [Spark: SQL Performance Tuning](https://spark.apache.org/docs/4.3.0/sql-performance-tuning.html)
- [Spark: Web UI](https://spark.apache.org/docs/4.3.0/web-ui.html)
- [PySpark: Apache Arrow in PySpark](https://spark.apache.org/docs/4.3.0/api/python/tutorial/sql/arrow_pandas.html)
- [Spark 소스: UnifiedMemoryManager.scala](https://github.com/apache/spark/blob/v4.3.0/core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala)
- [Spark 소스: ExecutionMemoryPool.scala](https://github.com/apache/spark/blob/v4.3.0/core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala)
- [Spark 소스: MemoryManager.scala](https://github.com/apache/spark/blob/v4.3.0/core/src/main/scala/org/apache/spark/memory/MemoryManager.scala)
- [Spark 소스: SparkExitCode.scala](https://github.com/apache/spark/blob/v4.3.0/core/src/main/scala/org/apache/spark/util/SparkExitCode.scala)
- [Spark 소스: YarnAllocator.scala](https://github.com/apache/spark/blob/v4.3.0/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala)
- [Spark 소스: MapStatus.scala](https://github.com/apache/spark/blob/v4.3.0/core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala)
- [Hadoop: yarn-default.xml](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)

## English summary

Spark sets aside 300MB of the executor heap, gives 60% of the rest to a unified region shared by execution and storage, and leaves 40% for user objects; execution can evict cached blocks down to the protected storage share, but storage can never take execution memory back. Operators that go through the memory manager spill to disk, while UDF objects, huge rows and whole pandas groups do not, so they cause a JVM OutOfMemoryError, which ends the executor with exit code 52. Native memory and Python workers live in memoryOverhead, and when they push the container past its limit YARN kills the process instead, so the fix is overhead, not heap. In my ad report jobs and my Spark Streaming job, results were collected to the driver, which fits small results such as mail attachments but makes driver memory the ceiling on result size.
