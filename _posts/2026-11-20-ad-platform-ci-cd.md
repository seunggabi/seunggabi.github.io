---
title: "한 저장소에서 광고 서비스 아홉 개를 배포하는 모듈과 워크플로"
date: 2026-11-20 04:22:00 +0900
categories: [Platform & Infra, GitHub Actions]
tags: [github-actions, gradle, kotlin, docker, helm, spring-cloud-config, ci-cd]
---

이전 회사의 커머스 광고 플랫폼에서 서비스 아홉 개를 한 저장소에서 빌드하고 배포하는 구조를 맡았다. Spring Boot 서비스, Flink 잡, Python 배치, React 관리 화면, 메신저 봇이 한 저장소에 같이 있었고, 환경은 dev, stage, prod 세 개였다. 공통 모듈 하나를 고치면 그걸 쓰는 서비스가 전부 다시 배포돼야 했다. 이 구조는 모듈을 어떻게 묶어 빌드하고, 바뀐 코드에 맞는 서비스만 어떤 경로로 배포할지를 정한다. 회사 내부 이름과 주소는 뺐다.

## 한 저장소 안의 빌드 세 덩어리

빌드는 세 덩어리로 나눴다.

| 덩어리 | 들어 있는 것 | 빌드 도구 |
|---|---|---|
| 메인 Gradle 빌드 | 공통 코어, 설정 서버, 로그 스트림 앱, Flink 잡, 관리 API, 배치 | Gradle (Kotlin 1.7, Spring Boot 2.7) |
| 광고 게이트웨이 | gRPC API 모듈과 도메인 모듈 | 별도 Gradle 빌드 (Kotlin 1.6, Spring Boot 2.5) |
| 그 밖 | 관리 웹, 메신저 봇, 배치용 Python 스크립트 | yarn, pip |

메인 빌드의 루트는 저장소 최상위가 아니라 `package`라는 하위 디렉터리다. 거기 있는 `settings.gradle.kts`가 옆 디렉터리의 모듈들을 `projectDir`로 끌어오고, 버전은 같은 디렉터리의 `gradle.properties` 한 곳에 모았다.

```kotlin
// 구조만 보여 주는 스케치
rootProject.name = "platform"

listOf("core", "config-server", "stream", "flink-job", "admin-api", "batch").forEach {
    include(":$it")
    project(":$it").projectDir = file("../$it")
}
```

모듈 디렉터리는 저장소 최상위에 나란히 두고, 빌드 설정만 한 디렉터리에 모았다. 각 모듈의 배포 설정은 빌드할 때 `../package/gradlew ... --settings-file ../package/settings.gradle.kts`로 이 루트를 가리킨다. 게이트웨이는 자기 `settings.gradle.kts`를 따로 갖고 Kotlin과 Spring Boot 버전도 달랐으며, 공통 코어에 의존하지 않았다.

## 공통 코어 모듈이 퍼뜨리는 것

공통 코어는 실행 jar를 만들지 않는 라이브러리 모듈이다. `bootJar`를 끄고, 다른 모듈이 `implementation(project(":core"))`로 가져다 쓴다. 안의 Kotlin 파일 65개는 Kafka 컨슈머 루프와 레코드 핸들러, WebMvc·WebFlux용 요청·응답 로깅 필터와 공통 응답 모델, 설정 서버 감시, Avro·Jackson·YAML·메신저 전송 유틸로 나뉜다.

코어는 Spring Web과 WebFlux, Spring Cloud Config 클라이언트, Avro와 Confluent serializer, Micrometer Datadog 레지스트리를 `api`로 선언했다. `api`로 선언한 의존성은 코어를 쓰는 모듈의 컴파일 클래스패스에도 그대로 들어간다. 그래서 각 서비스는 이 라이브러리들을 다시 적지 않는다. Flink 잡과 배치는 Spring Boot jar가 아니라 의존성을 전부 푼 fat jar를 만든다. 두 모듈 모두 `isZip64 = true`를 켰다. Gradle에서 이 옵션은 파일이 65535개를 넘는 zip을 만들 때 필요하다.

## 설정 서버: DB에 둔 설정과 변경 감시

설정 서버 모듈은 Spring Cloud Config Server다. 저장소로 Git이 아니라 JDBC 백엔드를 쓰고, 설정 값은 MySQL 테이블에 application, profile, label별로 들어 있다. 값을 읽고 바꾸는 GET/PUT API와 그 API를 부르는 셸 스크립트도 같이 뒀다.

서버는 기본 `JdbcEnvironmentRepository`를 상속해 `findOne`을 덮어썼다. 응답의 `state`와 `version`에 해당 설정 행의 마지막 수정 시각을 넣어서, 설정이 바뀌면 이 값도 바뀐다. 클라이언트 쪽 감시는 공통 코어에 두었다. 정해진 간격으로 상태 값만 다시 받아 보고, 달라졌을 때만 컨텍스트를 갱신해 재배포 없이 값을 바꾼다.

```kotlin
// 구조만 보여 주는 스케치
@Scheduled(fixedDelayString = "\${config.watch.delay}")
fun watch() {
    val old = ConfigClientStateHolder.getState()
    val new = locator.locate(environment).getProperty("config.client.state")?.toString()
    if (new.isNullOrBlank() || old == new) return
    ConfigClientStateHolder.setState(new)
    refresher.refresh()   // @RefreshScope 빈을 다시 만든다
}
```

## 서비스 하나, 환경 하나, 워크플로 하나

배포 워크플로는 서비스와 환경 조합마다 파일 하나로 뒀다. 모두 21개다.

| 배포 단위 | 환경 |
|---|---|
| 설정 서버, 게이트웨이 API, 관리 API, 관리 웹 | dev, stage, prod |
| 로그 스트림 앱, 배치, Flink 잡 두 개 | stage, prod |
| 메신저 봇 | prod |

Flink 잡 두 개는 하나의 Flink 모듈을 서로 다른 배포 설정으로 두 번 배포한 것이다. 트리거는 대부분 같다. `main`에 push되고, 바뀐 파일이 그 서비스 디렉터리나 공통 코어 디렉터리에 있으면 돈다.

```yaml
# 구조만 보여 주는 스케치
on:
  push:
    branches: [ main ]
    paths:
      - admin-api/**
      - core/**
  workflow_dispatch:
```

`paths`에 코어를 같이 적은 것이 Gradle 의존 관계를 워크플로에 옮겨 적은 부분이다. 코어를 쓰지 않는 관리 웹과 봇은 자기 디렉터리만 본다. 별도 도구 없이 파일 경로만으로 배포 대상을 고르니 단순하다. 대신 이 목록은 손으로 맞춘다. 모듈이 다른 모듈을 새로 의존하게 되면 워크플로 파일도 같이 고쳐야 한다.

21개 중 19개가 이 push 트리거로 돌고, 여기에는 prod 워크플로도 들어 있다. 코어 디렉터리를 건드린 PR 하나가 머지되면 코어를 쓰는 서비스들의 모든 환경 워크플로가 동시에 시작된다. 예외는 게이트웨이로, dev와 prod는 `workflow_dispatch`만 두어 손으로 돌리고 stage만 push로 돌렸다.

광고 쪽 다른 저장소는 prod를 다르게 보냈다. 서비스 세 개의 prod 워크플로는 GitHub 릴리스가 발행되고 태그가 서비스 이름 접두사로 시작할 때만 돈다. 접두사를 뗀 값을 버전으로, `{프로필}-{짧은 커밋 해시}`를 이미지 태그로 CD에 넘긴다. `main` 머지로 prod까지 가는 쪽은 손이 덜 가는 대신 코어를 고친 PR 하나가 여러 서비스의 운영 배포를 같이 시작한다. 릴리스로 거는 쪽은 릴리스 하나가 서비스 하나만 운영에 보내고, 그 시점을 사람이 정한다.

## 워크플로 안에서 하는 일: 빌드 Action과 런타임별 Dockerfile

Spring Boot 서비스의 워크플로는 두 단계다. 먼저 사내 빌드·배포 Action이 모듈의 배포 설정 YAML을 읽어, 거기 적힌 Gradle 명령으로 jar를 만들고 공용 Dockerfile을 모듈 디렉터리로 복사해 이미지를 빌드한 뒤 ECR에 push한다. 끝나면 후처리 훅이 복사한 Dockerfile을 지운다. 다음으로 사내 CD 시스템을 부르는 Action에 애플리케이션 이름, 프로필, 이미지 태그를 넘긴다.

Dockerfile은 런타임별로 나눴다. Spring Boot 서비스 공용은 JDK 11 위에서 `JAR`, `APP_PROFILE`을 빌드 인자로 받아 `java -jar`로 띄우고 컨테이너 메모리의 75%를 힙으로 쓴다. Flink 잡은 Flink 1.15 이미지의 Flink 홈 아래에 jar를 두고 S3 파일시스템 플러그인을 켠다. 배치는 Python 3.10에 awscli와 정적 스크립트만 담고, 관리 웹은 Node 16에서 프로필별로 빌드한 정적 파일을 nginx로 옮긴다.

공용 Dockerfile 하나로 여러 서비스를 굽기 위해 jar 이름을 `모듈명.jar`로 고정했다. 각 모듈의 `bootJar`가 `archiveFileName`을 버전 없는 이름으로 덮어쓰고, Dockerfile은 그 이름을 인자로 받는다. 배치는 이미지를 만든 뒤 jar와 Spark 설정 JSON을 S3로 복사하는 단계가 하나 더 있다. 관리 웹은 사내 빌드 Action을 쓰지 않는다. 첫 job에서 `{프로필}-{짧은 커밋 해시}`로 이미지 태그를 만들고, 두 번째 job이 Docker 이미지를 빌드해 ECR에 올리고, 세 번째 job이 CD를 부른다.

## 마일스톤과 담당자는 Action이 붙였다

작성자를 담당자로 지정하는 워크플로와 주간 마일스톤을 미리 만드는 워크플로는 공개 Action을 포크해 기능을 넣은 것이고, 그 과정은 GitHub Action 세 개에 기능을 넣고 버그를 고쳤다에 적었다. 마감이 지난 마일스톤을 닫고 그 제목으로 GitHub 릴리스를 만드는 쪽은 포크가 아니라 내가 만든 작은 Action(`seunggabi/auto-close-milestone`)으로, 매일 0시(UTC)에 돈다.

## Flink 잡은 이미지 태그를 Helm values에 썼다

Flink 잡 워크플로만 모양이 다르다. 이미지를 올린 뒤 CD 시스템을 부르지 않고, 별도의 Helm values 저장소를 체크아웃해서 그 잡과 환경의 values 파일에 있는 `image.tag`를 새 버전으로 바꾼다. 값 수정은 `fjogeleit/yaml-update-action`이 하고, `createPR: false`로 PR 없이 기본 브랜치에 바로 커밋한다.

{% raw %}
```yaml
# 구조만 보여 주는 스케치
helm:
  needs: [ build ]
  steps:
    - uses: fjogeleit/yaml-update-action@main
      with:
        valueFile: flink-deployment/values/<job>/<profile>.values.yaml
        propertyPath: image.tag
        value: ${{ needs.build.outputs.app-version }}
        createPR: false
```
{% endraw %}

버전 규칙은 워크플로 폴더의 README에 적혀 있다. 사내 빌드 Action에 prod 플래그를 끄면 버전이 `0.0.1-rc.{epoch 초}` 형식이 되고 Helm values를 바꾸는 흐름을 탄다. 켜면 `0.0.1` 그대로다. Flink 잡 두 개는 prod 워크플로에서도 이 플래그를 껐다. values 저장소에 커밋한 뒤 클러스터에 반영하는 일은 이 저장소 밖에서 일어난다. 태그가 매번 `0.0.1`이면 values 파일에 쓸 값이 바뀌지 않는다. rc 접미사의 시각 값이 빌드마다 다른 태그를 만들기 때문에, 새 빌드마다 values 저장소에 새 커밋이 생긴다.

## English summary

At a previous company I built the build and deploy structure for nine services of a commerce ad platform in one repository, from Spring Boot services and Flink jobs to a Python batch, a React admin page and a messenger bot. Each service and environment had its own GitHub Actions workflow, 21 in total, and each `paths` filter listed the shared core directory by hand, which is simple but must be updated when module dependencies change. Spring Boot images were built from one shared Dockerfile and sent to the internal CD system, while another ad repository sent production deploys through GitHub releases tagged with a service-name prefix. The Flink jobs wrote a timestamped rc image tag into a Helm values repository, because a fixed `0.0.1` tag would never change the values file and a new build would leave no new commit.
