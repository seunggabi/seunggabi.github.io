---
title: "rename 없는 S3에서 결과를 커밋하는 법"
date: 2026-11-21 06:15:00 +0900
categories: [Platform & Infra, AWS]
tags: [s3, object-storage, s3a, output-committer, multipart-upload, conditional-writes, s3-express, concept]
description: "S3에는 rename이 없어 Spark 잡의 마지막 커밋 단계가 오래 걸리고 중간에 죽으면 일부 파일만 남는다. 이 차이가 커밋 방식, 요청 설계, 동시 쓰기에 미치는 영향을 따라간다."
---

Spark 잡이 S3에 결과 파일 2,000개를 다 쓴 뒤에도 마지막 job commit 단계가 쓰기만큼 오래 걸리기도 한다. 그 사이 잡이 죽으면 출력 경로에는 일부 파일만 옮겨진 상태가 남는다. HDFS에서는 메타데이터만 바꾸던 rename이 S3에서는 객체마다 복사와 삭제로 바뀌기 때문이다.

이 글은 그 차이 하나에서 출발한다. 커밋이 무엇이고 왜 rename에 기대 왔는지, S3가 무엇을 약속하고 무엇을 약속하지 않는지, 그 빈자리를 committer와 conditional write가 어떻게 메우는지를 차례로 따라간다. HDFS와 S3를 항목별로 나란히 놓은 표는 [HDFS와 MapReduce 글](/posts/hdfs-and-mapreduce/)에 있다.

버전은 Hadoop 3.5.0, Spark 4.3.0, Delta Lake 4.0 문서, 2026년 11월 AWS 문서 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| task commit | 태스크 하나가 끝났을 때 성공한 시도 하나의 결과만 확정하는 단계 |
| job commit | 모든 태스크가 확정된 뒤 드라이버가 결과를 최종 경로에 모아 한 번에 보이게 하는 단계 |
| strong read-after-write | PUT·DELETE가 성공 응답을 받은 뒤의 GET, HEAD, LIST는 그 결과를 본다. 2020년 12월부터 |
| 키 단위 원자성 | 키 하나의 갱신은 원자적이고, 여러 키를 묶는 원자적 갱신은 없다 |
| multipart upload | part를 따로 올리고 `CompleteMultipartUpload`로 객체를 만든다. 완료 전에는 보이지 않는다 |
| FileOutputCommitter v1/v2 | 임시 디렉터리에 쓰고 rename으로 커밋하는 Hadoop 기본 committer의 두 알고리즘 |
| S3A committer | `directory`, `partitioned`, `magic`. multipart 완료를 job commit까지 미룬다 |
| EMRFS S3-optimized committer | EMR의 EMRFS 전용 committer. task commit에서 multipart를 완료한다 |
| conditional write | `If-None-Match: *`(없을 때만 쓰기), `If-Match: <ETag>`(그대로일 때만 쓰기) |
| prefix 파티션 | S3가 요청량을 보고 따로 떼어 준 키 범위. 초당 쓰기 3,500건, 읽기 11,000건 이상을 받는다 |
| 503 Slow Down | prefix 파티션이 늘어나는 동안 돌려주는 일시적 거절. 지수 백오프로 재시도한다 |
| directory bucket | 한 AZ에 두는 계층형 버킷. S3 Express One Zone 저장 클래스를 쓰고 `RenameObject`를 지원한다 |

## 커밋은 rename 한 번에 기대 왔다

Spark 잡은 태스크 수백 개가 파일을 나눠 쓴다. 잡이 도는 동안 다운스트림이 출력 경로를 읽으면 반쯤 쓴 결과를 가져간다. 태스크가 실패해 다시 돌거나 투기 실행(speculation)으로 같은 태스크가 두 번 돌면, 같은 데이터를 담은 파일이 두 개 생기기도 한다. 그래서 Hadoop 계열 committer는 오래전부터 이렇게 해 왔다.

1. 각 태스크 시도는 `_temporary` 아래 자기 자리에 쓴다.
2. 태스크가 끝나면 드라이버가 허락한 시도 하나만 자기 결과를 확정한다. 이것이 task commit이다.
3. 모든 태스크가 확정되면 결과를 최종 경로에 모으고 `_SUCCESS`를 남긴다. 이것이 job commit이다.

2번과 3번의 "확정"과 "모으기"가 rename이다. HDFS에서 rename은 NameNode의 메타데이터 한 줄을 바꾸는 일이라, 파일이 몇 개든 즉시 끝나고 성공하거나 실패하거나 둘 중 하나다. 커밋의 안전성이 이 성질 하나에 걸려 있었다.

S3에는 이 성질이 없다. 디렉터리부터 없다. `dt=2026-11-20/part-0001.parquet`는 슬래시가 든 키 하나일 뿐이고, "디렉터리 rename"은 그 접두사를 가진 키를 하나씩 복사하고 지우는 작업이다.

| 파일 2,000개짜리 디렉터리를 옮길 때 | HDFS | S3 |
|---|---|---|
| 실제로 하는 일 | 메타데이터 한 번 수정 | COPY 2,000건 + DELETE 2,000건 |
| 걸리는 시간 | 파일 수와 무관 | 파일 수와 크기에 비례 |
| 중간에 멈추면 | 옮겨지기 전 상태 그대로 | 일부는 새 경로, 나머지는 옛 경로 |

그래서 S3 위의 파이프라인은 "다 쓰고 한 번에 보이게 한다"를 rename 말고 다른 수단으로 만들어야 한다. 그 수단을 고르려면 먼저 S3가 무엇을 약속하는지 알아야 한다.

## S3가 약속하는 것은 키 하나의 원자성까지다

2020년 12월부터 S3는 모든 리전에서 객체 PUT, DELETE에 강한 read-after-write 일관성을 준다. 새 객체를 쓰고 바로 LIST하면 목록에 있고, 지운 뒤 LIST하면 없다. 예전 S3A가 목록 불일치를 덮으려고 DynamoDB에 메타데이터를 따로 두던 S3Guard는 여기서 필요 없어졌다.

| S3가 하는 것 | S3가 하지 않는 것 |
|---|---|
| 쓰기가 성공하면 바로 다음 읽기와 LIST에 보인다 | 여러 키를 한 번에 바꾸는 연산 |
| 키 하나를 덮어쓰는 동안 읽는 쪽은 옛 데이터나 새 데이터 중 하나만 받는다 | 동시 writer를 위한 잠금. 같은 키에 PUT 두 개가 겹치면 타임스탬프가 늦은 쪽이 이긴다 |

버킷 설정 변경은 예외로 최종 일관성이라, 버전 관리를 처음 켰다면 15분쯤 기다린 뒤 객체를 쓴다.

일관성이 강해졌어도 앞의 반쯤 옮겨진 디렉터리 문제는 그대로다. COPY 2,000건과 DELETE 2,000건 각각은 즉시 보이지만, S3는 그 4,000건을 하나로 묶어 주지 않는다. S3에서 원자적인 것은 키 하나를 만들거나 바꾸는 일뿐이다. 뒤에서 볼 committer들도 이 한계를 넘지 못한다. S3A committer는 파일마다 COPY와 DELETE 대신 원자적 요청 하나만 보내 반쯤 보이는 구간을 짧게 만들 뿐이고, EMRFS committer는 파일이 보이는 시점을 task commit으로 앞당긴다. 출력 전체를 키 하나의 갱신에 거는 것은 테이블 포맷이다.

## multipart upload는 완료 전까지 보이지 않는 쓰기다

rename을 대신할 도구가 multipart upload 안에 있다. 원래는 큰 파일을 나눠 올리는 기능이다.

| 항목 | 값 |
|---|---|
| part 수 | 1부터 10,000까지 |
| part 크기 | 5MiB에서 5GiB, 마지막 part는 하한 없음 |

커밋 설계에 쓰이는 성질은 크기가 아니라 가시성이다. 진행 순서는 이렇다.

```text
CreateMultipartUpload    업로드 id를 받는다. 객체는 아직 없다
UploadPart x N           part를 올린다. LIST에도 GET에도 안 보인다
CompleteMultipartUpload  HTTP POST 하나로 객체가 한 번에 생긴다
(또는 AbortMultipartUpload  흔적 없이 치운다)
```

무대 뒤에서 세트를 다 짓고 마지막에 막만 올리는 것과 같다. 데이터 전송은 미리 끝내 두고, 보이게 만드는 일은 키 하나짜리 원자적 요청으로 남겨 둘 수 있다. 이하 `CompleteMultipartUpload`를 "완료 요청"이라고 부른다. 다만 완료도 취소도 못 한 채 남은 업로드의 part는 보이지 않으면서 저장 공간과 요금은 계속 차지하므로, lifecycle 규칙 `AbortIncompleteMultipartUpload`를 걸어 둔다.

## FileOutputCommitter는 rename을 커밋으로 쓴다

Hadoop의 기본 committer는 태스크가 `_temporary` 아래에 쓰고 rename으로 결과를 옮긴다. 두 알고리즘은 옮기는 시점이 다르다.

| 알고리즘 | task commit | job commit | S3에서 |
|---|---|---|---|
| v1 | 태스크 임시 디렉터리를 잡 임시 디렉터리로 rename | 모든 태스크 결과를 출력 경로로 rename하고 `_SUCCESS` | 같은 바이트를 두 번 복사하고, job commit은 단일 스레드라 파일 수만큼 길어진다 |
| v2 | 결과를 바로 출력 경로로 rename | `_SUCCESS`만 쓴다 | 빠르지만 잡이 중간에 죽으면 일부 태스크 결과가 이미 보인다 |

파일 2,000개를 v1으로 S3에 커밋하면 task commit에서 COPY 2,000건과 DELETE 2,000건, job commit에서 다시 COPY 2,000건과 DELETE 2,000건이 나간다. 모두 8,000건이다. 글 첫머리에서 job commit이 쓰기만큼 오래 걸린 것은 그중 job commit의 4,000건을 드라이버 하나가 처리하는 시간이고, 일부 파일만 남은 것은 그 복사가 중간에 멈춘 결과다.

Hadoop 3.5.0의 `mapred-default.xml` 기본값은 2인데, Spark는 `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version`을 1로 덮어쓴다. v2는 task commit이 최종 경로로 바로 옮기므로, 그 도중 실패하면 반쯤 옮겨진 시도의 파일이 재시도한 시도의 파일과 섞일 수 있다(MAPREDUCE-7282).

## S3A와 EMRFS committer는 완료 요청을 미룬다

rename을 피하는 committer는 모두 같은 방법을 쓴다. 최종 경로에 multipart upload로 쓰되 완료 요청만 남겨 두고, 커밋 시점에 보낸다. 데이터는 이미 제자리에 올라가 있으니 옮길 것이 없다.

| committer | 데이터가 먼저 가는 곳 | 출력이 보이는 시점 | 비고 |
|---|---|---|---|
| S3A `directory` | 로컬 디스크, task commit에서 미완료 multipart로 업로드 | job commit | 완료할 업로드 목록은 HDFS 같은 클러스터 파일 시스템에 둔다. 출력 경로에 기존 파일이 있을 때의 처리(`conflict-mode`)를 디렉터리 전체 기준으로 정한다 |
| S3A `partitioned` | 위와 같다 | job commit | 그 처리를 이번에 쓰는 파티션 단위로 좁힌다. Spark 전용 |
| S3A `magic` | 쓰는 즉시 S3로, 미완료 multipart | job commit | 완료할 업로드 목록도 S3에 둔다. Hadoop 3.3.1부터 기본 활성화 |
| EMRFS S3-optimized | 쓰는 즉시 S3로, 미완료 multipart | task commit | EMR 5.20.0부터 기본. 6.4.0부터 Parquet 외 ORC, CSV, JSON도 |

로컬 디스크를 거치는 `directory`와 `partitioned`를 staging committer라고 부른다.

job commit이 어떤 업로드를 완료할지 아는 방법은 `magic`을 보면 분명하다. 태스크는 `out/__magic_job-<jobId>/.../part-0001`처럼 특별한 경로에 파일을 여는 것처럼 보이지만, S3A는 실제로 `out/part-0001`에 multipart upload를 시작한다. 파일을 닫을 때 완료하지 않고, 업로드 id와 part 정보만 담은 작은 `.pending` 파일을 magic 경로에 남긴다. task commit은 그 태스크의 `.pending`들을 `.pendingset` 하나로 모으고, job commit은 `.pendingset`을 읽어 파일마다 완료 요청을 보낸다. 실패하거나 진 태스크 시도의 업로드는 목록에 들어가지 않아 완료되지 않는다. staging committer는 이 목록을 S3 대신 클러스터 파일 시스템에 둘 뿐 원리는 같다.

네 committer를 "언제 무엇이 보이나"로 나란히 놓았다.

| 시점 | FileOutputCommitter v1 | FileOutputCommitter v2 | S3A committer | EMRFS committer |
|---|---|---|---|---|
| 태스크가 쓰는 중 | 안 보임(`_temporary`) | 안 보임(`_temporary`) | 안 보임(미완료 multipart) | 안 보임(미완료 multipart) |
| task commit 뒤 | 안 보임 | 그 태스크 파일이 보임 | 안 보임 | 그 태스크 파일이 보임 |
| job commit 중 | 복사가 끝난 파일부터 보임 | 이미 다 보임 | 완료 요청이 끝난 파일부터 보임 | 이미 다 보임 |
| 잡이 중간에 실패 | job commit 전이면 출력 없음, 중이면 일부 남음 | 커밋된 태스크 파일이 남음 | job commit 전이면 출력 없음, 중이면 일부 남음 | 커밋된 태스크 파일이 남음 |
| 실패 뒤 남는 찌꺼기 | `_temporary` 아래 파일(과금됨) | `_temporary` 아래 파일(과금됨) | 미완료 업로드(안 보이지만 과금됨) | 미완료 업로드 + 이미 보이는 파일 |
| job commit에 드는 일 | 파일마다 COPY + DELETE | `_SUCCESS`만 | 파일마다 완료 요청 하나 | `_SUCCESS`만 |

S3A committer는 v1의 "잡이 끝나기 전에는 아무것도 안 보인다"를 지키면서 job commit의 일을 복사에서 완료 요청으로 줄였다. 다만 완료 요청 2,000건이 나가는 동안에는 일부 파일만 보이는 순간이 여전히 있다. 짧아졌을 뿐 없어지지는 않는다. EMRFS committer는 v2처럼 task commit에서 보이게 해 빠른 대신, 잡이 중간에 실패하면 이미 커밋된 태스크의 파일이 남는다. S3A는 업로드를 job commit까지 미완료로 두므로 드라이버가 죽으면 보이지 않는 part가 남고, EMRFS도 executor가 죽으면 같은 일이 생긴다. 두 committer 모두 `AbortIncompleteMultipartUpload` 규칙이 필요하다.

S3A 쪽 기본값은 조심해야 한다. `fs.s3a.committer.name`의 기본은 여전히 `file`, 즉 rename을 쓰는 옛 committer다. staging committer는 출력 경로에 이미 파일이 있을 때의 동작을 `conflict-mode`로 정한다. 기본 `append`는 기존 파일을 그대로 두고 새 파일을 더한다. 파일 이름에 UUID를 붙이는 것이 기본이라 이름이 겹치지는 않지만, 같은 파티션을 다시 쓰면 옛 결과와 새 결과가 함께 남는다. Spark에서 쓰려면 `spark-hadoop-cloud` 모듈과 두 클래스를 함께 지정한다. 앞의 것은 Spark SQL의 커밋을 Hadoop committer factory로 넘겨 factory가 S3A committer를 고르게 하고, 뒤의 것은 `ParquetOutputCommitter`만 받는 Parquet writer에 그 committer를 끼워 넣는 어댑터다.

```properties
# Spark 4.3.0 + Hadoop 3.5.0, s3a:// 경로에 쓸 때
spark.hadoop.fs.s3a.committer.name magic
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
```

S3A·EMRFS committer가 적용되지 않는 경우도 있다. Spark의 dynamic partition overwrite(이번에 쓴 파티션만 갈아 끼우는 모드)는 job commit에서 파티션 디렉터리를 rename으로 바꿔 넣는다. S3A committer는 이 모드를 지원하지 않아 `PathOutputCommitter does not support dynamicPartitionOverwrite`로 실패한다. EMRFS committer도 이 모드와 사용자 지정 파티션 위치, RDD API, `s3a://` 경로에서는 쓰이지 않는다. EMR은 dynamic partition overwrite만 따로 EMRFS S3-optimized commit protocol(5.30.0, 6.2.0부터 기본)로 rename을 피한다.

## 읽는 쪽은 _SUCCESS를 기다리고 marker 파일을 건너뛴다

committer가 무엇이든 읽는 쪽도 규칙이 필요하다. `_SUCCESS`는 job commit이 끝났다는 표시라, 이 파일이 생긴 뒤에 경로를 읽으면 반쯤 커밋된 결과를 피할 수 있다. v2와 EMRFS committer는 task commit마다 파일이 보이므로, 읽는 쪽이 `_SUCCESS`를 기다리는 것이 유일한 방어선이다. 옛 committer는 0바이트 `_SUCCESS`를 남기고, S3A committer는 `committer` 필드가 든 JSON을 남겨 어떤 committer가 썼는지도 알 수 있다.

반대로 이런 marker 파일을 데이터로 읽으면 안 된다. 이전 회사에서 맡았던 광고 리포트 배치의 reader는 `s3a://` 경로를 나열할 때 이름이 `_`로 시작하는 파일을 건너뛰었다. `_SUCCESS`나 `_temporary` 아래 파일이 Parquet로 읽히다 깨지는 일을 막는 규칙이다.

## 요청 속도는 prefix 파티션 단위로 늘어난다

커밋 방식은 요청이 몇 건, 어디로 가는지도 정한다. v1 job commit의 COPY 2,000건과 DELETE 2,000건은 모두 같은 출력 prefix로 간다.

S3는 prefix 파티션 하나에 초당 PUT·COPY·POST·DELETE 3,500건, GET·HEAD 11,000건 이상을 받는다. 2018년 발표 때 GET 값은 5,500건이었고, 2026년 11월 기준 값은 11,000건이다. 한도는 버킷이 아니라 prefix 파티션에 붙고, prefix 수에는 제한이 없다.

요청이 몰린 prefix는 S3가 점진적으로 쪼개고, 쪼개는 동안에는 503 Slow Down을 돌려준다. 한동안 조용하던 prefix에 갑자기 요청이 몰려도 같다. 한도는 슬래시로 끊은 경로 단계마다 붙지도 않는다. 슬래시는 S3에게 특별한 문자가 아니라서, `dt=2026-11-20/a/b/c/part-0001`처럼 경로를 깊게 해도 키들이 `dt=2026-11-20/`로 시작하는 한 같은 앞부분을 공유한다. 키 맨 앞에 해시 몇 글자를 두면 키의 앞부분이 여러 갈래가 되어, S3가 prefix를 쪼갠 뒤에는 요청이 여러 파티션에 나뉜다. 대신 날짜 경로로 나열하던 Hive 방식 파티션 읽기는 쓸 수 없게 된다. AWS SDK와 S3A는 503을 지수 백오프로 재시도한다.

쓰기 한도가 읽기의 3분의 1 이하라서 파이프라인은 쓰기 쪽에서 먼저 503을 만나기 쉽다. 일 파티션 하나에 태스크 수백 개가 동시에 파일을 만들고, job commit이 요청을 쏟고, 정리 잡이 같은 prefix의 작은 파일을 지운다. 세 가지가 모두 같은 prefix에 몰리고, 파일 수가 요청 수를 정한다. 작은 파일을 줄이는 방법은 [작은 파일 글](/posts/small-files-and-compaction/)에서, 요청 수가 청구서에 붙는 방식은 [비용 글](/posts/data-platform-cost-levers/)에서 다뤘다.

## conditional write는 S3에 put-if-absent를 줬다

committer는 잡 하나의 결과를 보이게 하는 문제였다. 테이블 포맷에는 문제가 하나 더 있다. 두 잡이 같은 테이블에 동시에 커밋하면 둘 중 하나만 이겨야 한다. 잠금이 없는 S3에서는 이것을 만들 수 없었는데, conditional write가 그 빈자리를 채웠다.

2024년 8월 `If-None-Match`, 11월 `If-Match`가 들어왔다. `PutObject`, `CompleteMultipartUpload`, `CopyObject`에 붙일 수 있고, 조건이 틀리면 412 Precondition Failed다. 같은 키에 조건부 쓰기가 겹치면 먼저 끝난 쪽만 성공한다. 2025년 9월부터는 범용 버킷에서 `If-Match`로 조건부 삭제도 된다.

| 헤더 | 뜻 | 데이터베이스로 치면 |
|---|---|---|
| `If-None-Match: *` | 그 키가 아직 없을 때만 쓴다 | put-if-absent, 유일 키 INSERT |
| `If-Match: <ETag>` | 내가 읽은 뒤 아무도 안 바꿨을 때만 쓴다. ETag는 객체 내용이 바뀔 때마다 달라지는 식별값이다 | compare-and-swap, 낙관적 잠금 |

```bash
# AWS CLI v2. 두 writer가 같은 커밋 파일을 만들려고 할 때
aws s3api put-object --bucket amzn-s3-demo-bucket \
  --key tbl/_log/00000000000000000012.json --body commit.json --if-none-match "*"
# 같은 명령을 한 번 더 보내면 PreconditionFailed(412)

# ETag가 그대로일 때만 덮어쓰기
etag=$(aws s3api head-object --bucket amzn-s3-demo-bucket --key tbl/_pointer --query ETag --output text)
aws s3api put-object --bucket amzn-s3-demo-bucket --key tbl/_pointer --body next.json --if-match "$etag"
```

multipart upload와 섞이면 주의할 점이 있다. 진행 중인 multipart upload는 조건 판단에 들어가지 않아서, 업로드하는 사이 다른 writer가 먼저 쓰면 완료 단계에서 412가 난다. 동시 삭제가 끼면 `If-None-Match`는 409 Conflict, `If-Match`는 404를 받을 수 있고, `CompleteMultipartUpload`는 이때 `CreateMultipartUpload`부터 다시 해야 한다.

Iceberg와 Delta Lake는 결과 전체를 거는 원자적 지점을 다른 곳에 둔다.

| 포맷 | 커밋 방식 | S3에 필요한 것 |
|---|---|---|
| Iceberg | 매번 새 이름의 `metadata.json`을 쓰고, 카탈로그의 포인터만 compare-and-swap으로 바꾼다 | 카탈로그를 쓰면 없음. 덮어쓰기가 없고 경쟁은 카탈로그가 판정한다 |
| Delta Lake | `_delta_log` 아래에 `00000000000000000012.json` 같은 다음 버전 파일을 먼저 만든 쪽이 이긴다 | "없을 때만 만들기" |

Iceberg 포인터 교체를 누가 어떻게 하는지는 [Iceberg 카탈로그 글](/posts/iceberg-catalogs-and-commits/)에서 다뤘다. Delta 4.0의 기본 S3 설정에서는 여러 Spark 드라이버가 같은 테이블에 동시에 쓰면 데이터를 잃을 수 있고, 다중 클러스터 쓰기는 실험 기능인 DynamoDB 기반 `S3DynamoDBLogStore`로 푼다. S3가 상호 배제를 주지 않았기 때문이다. conditional write는 그 DynamoDB가 하던 일을 S3 혼자 할 수 있게 만든 원시 연산이다. 다만 Delta 4.0 문서는 conditional write를 언급하지 않고, 다중 클러스터 쓰기에 여전히 DynamoDB를 요구한다. Iceberg, Delta, Hudi가 커밋을 어디에 적는지는 [테이블 포맷 비교 글](/posts/table-formats-compared/)에 정리했다.

## directory bucket의 RenameObject도 디렉터리 커밋은 못 한다

지금까지는 범용 버킷 이야기였다. AWS는 규칙을 바꾼 버킷 두 종류도 내놓았다.

S3 Express One Zone의 directory bucket은 한 AZ에 두는 계층형 버킷이다. 디렉터리가 실제로 생기고 prefix 한도가 없는 대신 버킷당 읽기 200,000 TPS, 쓰기 100,000 TPS가 기본 상한이다. 범용 버킷을 가정한 파이프라인 코드는 아래 차이에서 깨지기 쉽다.

| 항목 | directory bucket |
|---|---|
| LIST | `ListObjectsV2` 결과가 사전순이 아니어서 정렬된 목록을 가정한 코드가 깨진다. prefix는 `/`로 끝나야 하고 구분자는 `/`만 된다 |
| rename | `RenameObject`로 객체 하나의 이름을 바꾼다. `If-None-Match`로 덮어쓰기를 막는다. 객체 하나 단위라 파일 2,000개짜리 출력을 한 번에 보이게 하지는 못한다 |
| append | 기존 객체 뒤에 데이터를 붙일 수 있다(Local Zone이 아닌 AZ의 버킷) |
| multipart | part 번호가 연속이어야 완료된다 |
| 없는 기능 | 버전 관리, Object Lock, 복제, 이벤트 알림. ETag가 MD5가 아니어서 ETag로 MD5를 비교하는 코드도 깨진다 |
| 휴면 | 90일 동안 요청이 없으면 비활성 상태가 되고 깨어나는 동안 503(Local Zone이 아닌 AZ의 버킷) |

S3 Tables의 table bucket은 Iceberg 테이블 전용이다. 데이터 파일은 Iceberg 클라이언트가 테이블 위치에 쓰고, 커밋은 S3 Tables가 메타데이터 위치를 바꾸는 API로 받는다. rename과 동시 쓰기 문제를 버킷 쪽 카탈로그가 가져간 형태이고, compaction과 스냅샷 정리도 관리형 작업으로 돈다. 대신 Iceberg 카탈로그로 S3 Tables에 붙을 수 있는 엔진만 테이블에 쓸 수 있다(S3 Tables나 Glue의 Iceberg REST 엔드포인트, 또는 S3 Tables 카탈로그 클라이언트).

## 묻고 답하기

Q. rename을 안 쓰고 처음부터 최종 경로에 일반 PUT으로 쓰면 안 되나?

쓰는 순간 바로 보이므로 잡이 도는 동안 다운스트림이 반쯤 쓴 결과를 읽는다. 실패한 태스크 시도나 투기 실행으로 두 번 돈 태스크가 남긴 파일도 그대로 남아, 같은 데이터가 두 벌 읽힌다. 완료 요청을 미루는 committer들은 최종 경로에 쓰면서도 이 두 문제를 피한다. 완료하지 않은 업로드는 보이지 않고, task commit에서 이긴 시도의 업로드만 골라 완료하기 때문이다(S3A는 job commit에서, EMRFS는 task commit에서).

Q. Iceberg 테이블에 쓰면 committer 고민이 사라지나?

Iceberg는 데이터 파일을 항상 새 이름으로 쓰고, 그 파일들은 메타데이터 커밋 전까지 어떤 스냅샷에도 속하지 않아 읽히지 않는다. 결과를 보이게 하는 일은 카탈로그 포인터 교체 한 번이 맡으니 rename에 기대지 않는다. 대신 실패한 잡이 남긴, 어느 스냅샷에도 없는 파일은 고아 파일로 남아 따로 정리해야 한다.

## 참고 자료

- AWS S3: [What is Amazon S3 (consistency model)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html), [Strong consistency 발표(2020-12)](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/), [Optimizing performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html), [Performance design patterns](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html), [Multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html), [Document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html)
- AWS S3 conditional writes: [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), [Enforce conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html), [If-None-Match 발표(2024-08)](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/), [If-Match 발표(2024-11)](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/)
- AWS directory bucket: [Working with directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html), [Differences for directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-differences.html), [RenameObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RenameObject.html)
- Amazon EMR: [EMRFS S3-optimized committer](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-committer.html), [Requirements](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-committer-reqs.html), [Multipart uploads](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-committer-multipart.html), [S3-optimized commit protocol](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-commit-protocol.html)
- Hadoop 3.5.0: [S3A committers](https://hadoop.apache.org/docs/r3.5.0/hadoop-aws/tools/hadoop-aws/committers.html), [Committer architecture](https://hadoop.apache.org/docs/r3.5.0/hadoop-aws/tools/hadoop-aws/committer_architecture.html), [mapred-default.xml](https://hadoop.apache.org/docs/r3.5.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
- Spark 4.3.0: [Integration with Cloud Infrastructures](https://spark.apache.org/docs/latest/cloud-integration.html), [Configuration](https://spark.apache.org/docs/latest/configuration.html)
- Delta Lake: [Storage configuration](https://docs.delta.io/latest/delta-storage.html)
- Apache Iceberg: [Reliability](https://iceberg.apache.org/docs/latest/reliability/), [Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)

## English summary

A Spark job commits by making all of its output visible at once, and on HDFS that was a single atomic rename. S3 has no directories, and a rename is a copy plus a delete for every object, so committing 2,000 files with the v1 committer sends 8,000 requests and can stop halfway. S3 gives strong read-after-write consistency since December 2020, but only single-key updates are atomic. A multipart upload stays invisible until it is completed, so the S3A committers and the EMRFS S3-optimized committer write to the final path and delay the completion to job or task commit. That shrinks the commit to one small request per file, but it does not make the whole output atomic. Request rates scale per prefix partition, at least 3,500 writes and 11,000 reads per second today, with 503 Slow Down while S3 splits a hot prefix. Table formats put the whole commit behind one key: Iceberg swaps a catalog pointer, and Delta Lake creates the next log file only if it does not exist. S3 conditional writes now provide that primitive natively, though Delta 4.0 still uses DynamoDB for multi-cluster writes.
