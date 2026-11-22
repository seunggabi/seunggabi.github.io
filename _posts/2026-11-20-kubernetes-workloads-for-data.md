---
title: "Pod가 사라졌을 때를 기준으로 쿠버네티스 워크로드 고르기"
date: 2026-11-20 05:13:00 +0900
categories: [Platform & Infra, Kubernetes]
tags: [kubernetes, helm, statefulset, cronjob, jvm, concept]
---

Pod 하나로 띄운 릴레이는 프로세스가 죽으면 다시 뜨지만, 노드가 죽으면 돌아오지 않는다. [Logstash Kafka 릴레이 글](/posts/logstash-kafka-relay/)의 릴레이가 Deployment 없이 Pod 하나로 돌았고, 그 글은 이 점을 매니페스트를 몇 줄로 끝낸 대가로 적었다. 워크로드 객체마다 무엇이 죽었을 때 무엇을 다시 만들어 주는지가 다르고, 데이터 컴포넌트에 맞는 객체도 그 차이로 고른다.

버전은 Kubernetes 1.37, JDK 21 문서 기준이다.

## 컨테이너, Pod, 노드 중 무엇이 죽었나

컨테이너가 죽으면 같은 노드의 kubelet이 같은 Pod 안에서 컨테이너를 다시 띄운다. Pod의 `restartPolicy`는 `Always`, `OnFailure`, `Never` 중 하나이고 기본값은 `Always`다. 재시작 간격은 10초, 20초, 40초로 늘어나 기본값으로 300초에서 멈추고, 10분 동안 문제없이 돌면 처음으로 돌아간다. 이 대기 상태가 `CrashLoopBackOff`다.

Pod 자체는 다른 노드로 옮겨지지 않는다. 노드가 죽으면 그 Pod는 결국 지워지고, 컨트롤러가 UID가 다른 새 Pod를 만든다. 새 Pod가 같은 노드에 뜬다는 보장도 없다. 그래서 워크로드를 고르는 질문은 "Pod가 사라졌을 때 누가 무엇을 같은 모습으로 다시 만드는가"가 된다.

| 객체 | 컨테이너가 죽으면 | Pod가 사라지면 | Pod 이름 | 볼륨 |
|---|---|---|---|---|
| Pod 단독 | `restartPolicy`대로 재시작 | 아무도 다시 만들지 않는다 | 직접 정한 이름 | 직접 붙인 것만 |
| Deployment | 재시작 (`Always`만 허용) | ReplicaSet이 새 Pod를 만든다 | `<이름>-<pod-template-hash>-<무작위>` | 복제본이 같은 템플릿을 쓴다 |
| StatefulSet | 재시작 | 같은 이름의 Pod를 다시 만든다 | `<이름>-<서수>` | Pod마다 PVC, 같은 PVC를 다시 붙인다 |
| Job | `OnFailure`면 같은 Pod에서 재실행 | 새 Pod를 만들고 실패로 센다 | `<이름>-<무작위>` | 템플릿대로 |
| CronJob | Job과 같다 | Job과 같다 | 일정마다 새 Job | 템플릿대로 |

## Pod 하나와 Deployment의 차이는 노드 장애다

그 릴레이는 읽은 위치를 Kafka 컨슈머 그룹의 커밋된 오프셋에 두었으니, 어느 노드에서 다시 떠도 같은 자리에서 이어 읽는다. 이런 프로세스를 `replicas: 1`인 Deployment로 바꿨다면 노드 장애 복구가 생겼을 것이다. Pod 이름과 IP에 기대는 쪽이 없었다고 본다. 대신 매니페스트가 늘어나니, 몇 줄짜리 매니페스트와 노드 장애 복구 중 무엇을 고르느냐의 문제다.

Deployment의 기본 업데이트 방식은 롤링 업데이트다. `maxSurge`(기본 25%, 올림)와 `maxUnavailable`(기본 25%, 내림) 때문에 복제본 1개짜리도 새 Pod를 먼저 띄우고 옛 Pod를 내린다. 잠깐 두 Pod가 같이 돈다. 동시에 둘이 돌면 안 되는 프로세스라면 `strategy.type: Recreate`로 옛 Pod를 모두 내린 뒤 새 Pod를 띄운다.

## StatefulSet은 이름과 볼륨을 서수에 묶는다

StatefulSet의 Pod는 `<StatefulSet 이름>-<서수>`라는 호스트 이름을 갖는다. `serviceName`으로 지정한 headless Service가 있으면 `<Pod 이름>.<Service 이름>.<네임스페이스>.svc.cluster.local`로 찾을 수 있다. headless Service는 직접 만들어야 한다.

`volumeClaimTemplates`의 항목마다 Pod 하나에 PVC 하나가 생기고, 이름은 `<템플릿 이름>-<StatefulSet 이름>-<서수>`다. `web-1`이 다른 노드에서 다시 떠도 `www-web-1`을 다시 붙인다. Pod를 지우거나 StatefulSet을 지워도 PVC는 남는다. `persistentVolumeClaimRetentionPolicy`의 `whenDeleted`, `whenScaled`로 지우게 할 수 있지만 둘 다 기본값은 `Retain`이다.

생성은 0번부터 차례로, 삭제는 큰 번호부터 거꾸로 한다. 기본 `podManagementPolicy`인 `OrderedReady`에서는 앞 번호가 Running이고 Ready여야 다음 Pod를 만든다.

StatefulSet은 같은 정체성을 가진 Pod가 클러스터에 많아야 하나만 돌도록 한다. 노드에 닿을 수 없게 되면 그 노드의 Pod는 `Terminating`이나 `Unknown`으로 남고 자동으로 지워지지 않는다. 그동안 같은 이름의 새 Pod도 생기지 않는다. 이를 강제 삭제로 풀면 쿼럼 기반 시스템에서 같은 멤버가 둘이 되는 split brain이 생길 수 있다. Deployment라면 기본 5분 뒤 축출되면서 새 Pod를 만들었을 상황에서 StatefulSet은 기다린다. 데이터 쪽에서는 이 대기가 안전장치다.

## 이름 길이는 63자보다 짧게 잡아야 한다

StatefulSet 컨트롤러는 Pod에 `<StatefulSet 이름>-<해시>` 값의 `controller-revision-hash` 라벨을 붙이므로, 라벨 값 63자 제한 안에 들려면 StatefulSet 이름은 해시 몫만큼 더 짧아야 한다. [Airflow Helm release 이름 글](/posts/airflow-helm-release-name/)에서 차트 검증은 통과했는데 Redis와 PostgreSQL Pod가 뜨지 않은 것이 이 몫 때문이다. CronJob은 이름 뒤에 11자를 붙여 Job을 만들므로 CronJob 이름은 52자까지다.

## Job과 CronJob은 두 번 돌 수 있다

Job의 Pod 템플릿에는 `restartPolicy`로 `Never`나 `OnFailure`만 쓸 수 있다. `OnFailure`면 실패한 컨테이너를 같은 Pod, 같은 노드에서 다시 돌린다. `Never`면 Pod가 실패로 끝나고 Job 컨트롤러가 새 Pod를 만든다. 재시도는 `backoffLimit`(기본 6)까지이고, 실패한 Pod를 다시 만드는 간격은 10초, 20초, 40초로 늘어난다.

`parallelism: 1`, `completions: 1`, `restartPolicy: Never`로 두어도 같은 프로그램이 두 번 시작될 수 있다. 앞선 실행이 남긴 임시 파일, 잠금, 덜 쓴 출력은 다음 Pod가 처리해야 한다.

CronJob도 일정마다 Job을 대략 한 번 만든다. 경우에 따라 둘이 생기거나 하나도 안 생길 수 있어 Job은 멱등이어야 한다.

| 설정 | 기본값 | 동작 |
|---|---|---|
| `concurrencyPolicy` | `Allow` | 앞 Job이 안 끝나도 새 Job을 만든다. `Forbid`는 건너뛰고 `Replace`는 앞 Job을 갈아 끼운다 |
| `startingDeadlineSeconds` | 없음 | 정한 시각에서 이만큼 늦으면 그 회차를 건너뛴다 |
| `timeZone` | 없음 | 없으면 kube-controller-manager의 로컬 시간대로 일정을 해석한다 |
| `successfulJobsHistoryLimit`, `failedJobsHistoryLimit` | 3, 1 | 끝난 Job을 몇 개 남길지 |

[Cron Manager 글](/posts/cron-manager/)의 앱은 한 대의 crontab을 다루고, "지금 실행"의 중복은 메모리의 실행 중 목록과 잠금 파일로 막았다. 클러스터에서 정기 실행끼리의 중복을 막는 자리는 `concurrencyPolicy: Forbid`다. 같은 Job의 두 번 시작은 이 설정으로도 막히지 않아, 잡 쪽 멱등성이 마지막 방어선이 된다.

## requests는 자리를 잡고 limits는 막는다

| | requests | limits |
|---|---|---|
| 쓰이는 곳 | 스케줄러가 노드를 고를 때 | 커널이 cgroup으로 강제할 때 |
| CPU | 경합할 때 CPU 시간 배분 가중치 | 넘으면 스로틀링. CPU 때문에 죽지는 않는다 |
| 메모리 | 노드 메모리가 모자라면 request를 넘은 Pod가 축출 후보가 된다 | 넘으면 OOM kill. `Reason: OOMKilled`, `Exit Code: 137` |

노드에 여유가 있으면 컨테이너는 request보다 더 쓸 수 있다. limit만 적고 request를 비우면 limit 값이 request로 복사된다. 메모리 limit은 커널이 메모리 압박을 감지했을 때 집행되므로, 넘는 순간 바로 죽지 않을 수도 있다.

## JVM 힙은 메모리 limit에서 계산된다

Linux에서 JVM은 `-XX:+UseContainerSupport`가 기본으로 켜져 있어, 컨테이너에 주어진 메모리와 CPU 수를 읽는다. `-Xmx`가 없으면 최대 힙은 그 메모리의 `MaxRAMPercentage`(기본 25%)다. limit 4GiB 컨테이너의 JVM은 아무 옵션 없이 힙 1GiB로 뜬다. limit이 없으면 기준이 노드 전체 메모리가 된다.

힙이 차면 JVM 안에서 `OutOfMemoryError`가 난다. 메타스페이스, 스레드 스택, 다이렉트 버퍼처럼 힙 밖에서 쓰는 메모리까지 합쳐 limit을 넘으면 커널이 프로세스를 죽이고 `OOMKilled`가 된다. 그래서 limit 전부를 힙으로 주면 안 되고, 비율로 남겨 두는 편이 limit을 바꿀 때도 따라간다.

```yaml
containers:
  - name: app
    image: <image>
    env:
      - name: JAVA_TOOL_OPTIONS
        value: "-XX:MaxRAMPercentage=75"
    resources:
      requests:
        cpu: "500m"
        memory: "2Gi"
      limits:
        memory: "2Gi"
```

이 컨테이너의 최대 힙은 1.5GiB이고, 힙 밖 메모리에 512MiB가 남는다. 메모리 request와 limit을 같게 두면 사용량이 request를 넘는 구간이 없어, 노드 메모리가 모자랄 때 request를 넘은 Pod로 꼽히지 않는다.

광고 플랫폼 빌드 구조의 Spring Boot 공용 Dockerfile은 JDK 11 위에서 컨테이너 메모리의 75%를 힙으로 썼다. 서비스마다 메모리가 달라도 Dockerfile 하나로 맞출 수 있는 대신, 나머지 25%로 힙 밖 메모리를 감당해야 한다.

## 참고 자료

- [Kubernetes: Pod Lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/)
- [Kubernetes: Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
- [Kubernetes: StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
- [Kubernetes: Force Delete StatefulSet Pods](https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/)
- [Kubernetes: Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Kubernetes: CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
- [Kubernetes: Resource Management for Pods and Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Kubernetes: Object Names and IDs](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)
- [Kubernetes: Labels and Selectors](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
- [kubernetes/kubernetes#64023: StatefulSet with long name can not create pods](https://github.com/kubernetes/kubernetes/issues/64023)
- [The java Command (JDK 21)](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html)

## English summary

Choosing a Kubernetes workload for a data component comes down to what gets recreated when a Pod disappears: the kubelet restarts a dead container in place, but only a controller replaces a lost Pod. A Deployment gives a new Pod with a new name, while a StatefulSet brings back the same name and the same PVC and waits rather than run two Pods with one identity. Jobs and CronJobs can start the same work twice, so the work must be idempotent. The Logstash relay I ran as a single Pod kept its read position in Kafka, so a Deployment would have added node failure recovery at the cost of a longer manifest. In the ad platform build I set up, one shared Dockerfile gave the JVM heap 75% of container memory, which fits every service with one file but leaves only 25% for memory outside the heap.
