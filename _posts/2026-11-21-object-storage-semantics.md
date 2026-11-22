---
title: "rename 없는 S3에서 결과를 커밋하는 법"
date: 2026-11-21 06:15:00 +0900
categories: [Platform & Infra, AWS]
tags: [s3, object-storage, s3a, output-committer, multipart-upload, conditional-writes, s3-express, concept]
---

Spark 잡이 S3에 결과 파일 2,000개를 다 쓴 뒤에도 마지막 job commit 단계가 쓰기만큼 오래 걸리기도 한다. 그 사이 잡이 죽으면 출력 경로에는 일부 파일만 옮겨진 상태가 남는다. 파일 시스템에서 메타데이터만 바꾸던 rename이 S3에서는 객체마다 복사와 삭제로 바뀌기 때문이다. S3 위의 파이프라인은 "다 쓰고 한 번에 보이게 한다"를 rename 말고 다른 수단으로 만들어야 한다.

HDFS와 S3를 항목별로 나란히 놓은 표는 [HDFS와 MapReduce 글](/posts/hdfs-and-mapreduce/)에 있다. 여기서는 그 차이가 커밋 방식, 요청 설계, 동시 쓰기에 어떻게 번지는지를 따라간다.

버전은 Hadoop 3.5.0, Spark 4.3.0, Delta Lake 4.0 문서, 2026년 11월 AWS 문서 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| strong read-after-write | PUT·DELETE가 성공 응답을 받은 뒤의 GET, HEAD, LIST는 그 결과를 본다. 2020년 12월부터 |
| 키 단위 원자성 | 키 하나의 갱신은 원자적이고, 여러 키를 묶는 원자적 갱신은 없다 |
| partitioned prefix | S3가 요청량을 보고 따로 떼어 준 prefix. 초당 쓰기 3,500건, 읽기 11,000건 이상을 받는다 |
| 503 Slow Down | prefix 파티션이 늘어나는 동안 돌려주는 일시적 거절. 지수 백오프로 재시도한다 |
| multipart upload | part를 따로 올리고 `CompleteMultipartUpload`로 객체를 만든다. 완료 전에는 보이지 않는다 |
| FileOutputCommitter v1/v2 | 임시 디렉터리에 쓰고 rename으로 커밋하는 Hadoop 기본 committer의 두 알고리즘 |
| S3A committer | `directory`, `partitioned`, `magic`. multipart 완료를 job commit까지 미룬다 |
| EMRFS S3-optimized committer | EMR의 EMRFS 전용 committer. task commit에서 multipart를 완료한다 |
| conditional write | `If-None-Match: *`(없을 때만 쓰기), `If-Match: <ETag>`(그대로일 때만 쓰기) |
| directory bucket | 한 AZ에 두는 계층형 버킷. S3 Express One Zone 저장 클래스와 `RenameObject` |

## S3가 약속하는 것은 키 하나의 원자성까지다

2020년 12월부터 S3는 모든 리전에서 객체 PUT, DELETE에 강한 read-after-write 일관성을 준다. 새 객체를 쓰고 바로 LIST하면 목록에 있고, 지운 뒤 LIST하면 없다. 예전 S3A가 목록 불일치를 덮으려고 DynamoDB에 메타데이터를 따로 두던 이유는 여기서 사라졌다.

일관성이 해결하지 못한 것도 분명하다. 키 하나의 갱신은 원자적이라 동시에 읽는 쪽은 옛 데이터나 새 데이터 중 하나만 받는다. 하지만 여러 키를 한 번에 바꾸는 연산은 없다. 같은 키에 PUT 두 개가 겹치면 타임스탬프가 늦은 쪽이 이기고, S3는 동시 writer를 위한 잠금을 주지 않는다. 버킷 설정은 여전히 최종 일관성이라, 버전 관리를 처음 켰다면 15분 정도 기다린 뒤 객체를 쓰는 편이 안전하다.

디렉터리도 없다. `dt=2026-11-20/part-0001.parquet`는 슬래시가 든 키 하나일 뿐이고, "디렉터리 rename"은 그 접두사를 가진 키를 하나씩 복사하고 지우는 작업이다. 파일 1,000개짜리 디렉터리를 옮기면 COPY 1,000건과 DELETE 1,000건이 나가고, 중간에 멈추면 두 경로에 반씩 남는다. 일관성이 강해졌어도 읽는 쪽이 이 반쯤 옮겨진 디렉터리를 보는 것은 막지 못한다.

## 요청 속도는 prefix 파티션 단위로 늘어난다

S3는 분할된 prefix 하나에 초당 PUT·COPY·POST·DELETE 3,500건, GET·HEAD 11,000건 이상을 받는다. 2018년 발표 때 GET 값은 5,500건이었고, 2026년 11월 기준 값은 11,000건이다. 한도는 버킷이 아니라 prefix 파티션에 붙고, prefix 수에는 제한이 없다.

늘어나는 방식이 중요하다. 요청이 몰린 prefix를 S3가 점진적으로 쪼개고, 그동안 503 Slow Down을 돌려준다. 그동안 조용했던 새 prefix에 갑자기 요청이 몰려도 같다. 슬래시는 특별한 문자가 아니라서 경로를 깊게 만든다고 속도가 늘지 않는다. 키 앞쪽에 해시 문자를 두는 것처럼 요청을 안정적인 여러 prefix로 흩는 이름 규칙이 효과가 있다. AWS SDK는 503을 지수 백오프로 재시도하고 기본은 요청당 3회 시도다.

쓰기 한도가 읽기의 3분의 1 이하라서 파이프라인은 쓰기 쪽에서 먼저 503을 만나기 쉽다. 일 파티션 하나에 태스크 수백 개가 동시에 파일을 만들고, job commit이 COPY와 DELETE를 쏟고, 정리 잡이 같은 prefix의 작은 파일을 지운다. 파일 수가 요청 수를 정하니, 작은 파일을 줄이는 방법은 [작은 파일 글](/posts/small-files-and-compaction/)에, 요청 수가 청구서에 붙는 방식은 [비용 글](/posts/data-platform-cost-levers/)에 있다.

## multipart upload는 완료 전까지 보이지 않는 쓰기다

| 항목 | 값 |
|---|---|
| part 수 | 1부터 10,000까지 |
| part 크기 | 5MiB에서 5GiB, 마지막 part는 하한 없음 |
| 객체 최대 크기 | 48.8TiB. 2025년 12월 발표에서 5TB를 50TB로 늘렸다 |
| GET 한 번의 상한 | 5TB. 그보다 큰 객체는 범위를 나눠 동시에 받는다 |

커밋 설계에 쓰이는 성질은 크기가 아니라 가시성이다. part를 다 올려도 `CompleteMultipartUpload`를 보내기 전까지 객체는 없다. 완료 요청 하나가 객체를 한 번에 만들고, `AbortMultipartUpload`는 흔적 없이 치운다. 다만 완료도 취소도 못 한 채 죽은 업로드의 part는 계속 저장 공간을 차지하므로, lifecycle 규칙 `AbortIncompleteMultipartUpload`를 걸어 둔다.

## FileOutputCommitter는 rename을 커밋으로 쓴다

Hadoop의 기본 committer는 태스크가 `_temporary` 아래에 쓰고 rename으로 결과를 옮긴다. 두 알고리즘은 옮기는 시점이 다르다.

| 알고리즘 | task commit | job commit | S3에서 |
|---|---|---|---|
| v1 | 태스크 임시 디렉터리를 잡 임시 디렉터리로 rename | 모든 태스크 결과를 출력 경로로 합치고 `_SUCCESS` | 같은 바이트를 두 번 복사하고, job commit은 단일 스레드라 파일 수만큼 길어진다 |
| v2 | 결과를 바로 출력 경로로 rename | `_SUCCESS`만 쓴다 | 빠르지만 잡이 중간에 죽으면 일부 태스크 결과가 이미 보인다 |

Hadoop 3.5.0의 `mapred-default.xml` 기본값은 2인데, Spark는 `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version`을 1로 덮어쓴다. v2가 MAPREDUCE-7282 같은 정확성 문제를 낼 수 있어서다. HDFS에서는 v1의 rename이 O(1) 원자 연산이라 느림이 job commit 한 지점에 몰릴 뿐이다. S3에서는 그 rename이 중간에 실패할 수 있는 복사로 바뀌어, 이 committer로 S3에 쓰면 결과를 잃거나 망가뜨릴 위험이 생긴다.

## S3A와 EMRFS committer는 multipart 완료를 미룬다

rename 없이 커밋하는 방법은 하나로 모인다. 최종 경로에 multipart upload로 쓰되 완료 POST만 남겨 두고, 커밋 시점에 완료한다.

| committer | 데이터가 먼저 가는 곳 | 출력이 보이는 시점 | 비고 |
|---|---|---|---|
| S3A `directory` | 로컬 디스크, task commit에서 미완료 multipart로 업로드 | job commit | 대기 중인 업로드 정보는 HDFS 같은 클러스터 파일 시스템에 둔다. 대상 디렉터리 전체가 충돌 판단 단위 |
| S3A `partitioned` | 위와 같다 | job commit | 충돌 판단을 쓰는 파티션으로 좁힌다. Spark 전용 |
| S3A `magic` | 쓰는 즉시 S3로, 미완료 multipart | job commit | `__magic_job-<jobId>` 경로 쓰기를 최종 경로 업로드로 바꾼다. Hadoop 3.3.1부터 기본 활성화 |
| EMRFS S3-optimized | 쓰는 즉시 S3로, 미완료 multipart | task commit | EMR 5.20.0부터 기본. 6.4.0부터 Parquet 외 ORC, CSV, JSON도 |

S3A 쪽 기본값은 조심해야 한다. `fs.s3a.committer.name`의 기본은 여전히 `file`, 즉 rename을 쓰는 옛 committer다. staging 계열의 `conflict-mode` 기본은 `append`라서 같은 이름의 파일이 덮일 수 있다. Spark에서 쓰려면 `spark-hadoop-cloud` 모듈과 두 클래스를 함께 지정한다.

```properties
# Spark 4.3.0 + Hadoop 3.5.0, s3a:// 경로에 쓸 때
spark.hadoop.fs.s3a.committer.name magic
spark.sql.sources.commitProtocolClass org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
spark.sql.parquet.output.committer.class org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
```

보이는 시점 차이가 장애 때 남는 상태를 정한다. S3A committer는 job commit 전까지 출력 경로에 아무것도 보이지 않는다. job commit은 완료 POST를 여러 번 보내는 일이라 짧지만, 키 사이의 원자성이 없다는 S3 모델에서 그 짧은 구간까지 사라지지는 않는다. EMRFS committer는 task commit에서 완료하므로 잡이 중간에 실패하면 v2처럼 이미 커밋된 태스크의 파일이 남고, 미완료 업로드가 오래 머무는 만큼 lifecycle 정리가 필요하다.

빠지는 경우도 있다. S3A committer와 S3는 Spark의 dynamic partition overwrite 조건을 만족하지 못해 `PathOutputCommitter does not support dynamicPartitionOverwrite`로 실패한다. EMRFS committer도 이 모드와 사용자 지정 파티션 위치, RDD API, `s3a://` 경로에서는 쓰이지 않는다. EMR은 dynamic partition overwrite만 따로 EMRFS S3-optimized commit protocol(5.30.0, 6.2.0부터 기본)로 rename을 피한다.

`_SUCCESS`는 읽는 쪽의 단서가 된다. 옛 committer는 0바이트 파일을 남기고, S3A committer는 `committer` 필드가 든 JSON을 남긴다. 광고 리포트 배치의 reader는 `s3a://` 경로를 나열할 때 이름이 `_`로 시작하는 파일을 건너뛰었다. 이런 marker 파일을 데이터로 읽지 않는 규칙이다.

## conditional write는 S3에 put-if-absent를 줬다

2024년 8월 `If-None-Match`, 11월 `If-Match`가 들어왔다. `PutObject`, `CompleteMultipartUpload`, `CopyObject`에 붙일 수 있고, 조건이 틀리면 412 Precondition Failed다. 같은 키에 조건부 쓰기가 겹치면 먼저 끝난 쪽만 성공한다. 2025년 9월부터는 범용 버킷에서 `If-Match`로 조건부 삭제도 된다.

```bash
# AWS CLI v2. 두 writer가 같은 커밋 파일을 만들려고 할 때
aws s3api put-object --bucket amzn-s3-demo-bucket \
  --key tbl/_log/00000000000000000012.json --body commit.json --if-none-match "*"
# 같은 명령을 한 번 더 보내면 PreconditionFailed(412)

# ETag가 그대로일 때만 덮어쓰기
etag=$(aws s3api head-object --bucket amzn-s3-demo-bucket --key tbl/_pointer --query ETag --output text)
aws s3api put-object --bucket amzn-s3-demo-bucket --key tbl/_pointer --body next.json --if-match "$etag"
```

모서리가 있다. 진행 중인 multipart upload는 조건 판단에 들어가지 않아서, 업로드하는 사이 다른 writer가 먼저 쓰면 완료 단계에서 412가 난다. 동시 삭제가 끼면 `If-None-Match`는 409 Conflict, `If-Match`는 404를 받을 수 있고, `CompleteMultipartUpload`는 이때 `CreateMultipartUpload`부터 다시 해야 한다. 서명은 SigV4여야 하고, 버킷 정책으로 조건부 쓰기를 강제할 수도 있다.

테이블 포맷은 rename 대신 원자적으로 바꿀 지점 하나를 둔다. Iceberg는 매번 새 이름의 `metadata.json`을 쓰고 카탈로그의 포인터만 compare-and-swap으로 바꾸므로 S3에는 덮어쓰기가 없다. 그 교체를 누가 어떻게 하는지는 [Iceberg 카탈로그 글](/posts/iceberg-catalogs-and-commits/)에 있다. Delta Lake는 `_delta_log` 아래에 버전 번호 파일을 만드는 쪽이라 "없을 때만 만들기"가 필요하다. Delta 4.0의 기본 S3 설정에서는 여러 Spark 드라이버가 같은 테이블에 동시에 쓰면 데이터를 잃을 수 있고, 다중 클러스터 쓰기는 실험 기능인 DynamoDB 기반 `S3DynamoDBLogStore`로 푼다. S3가 상호 배제를 주지 않았기 때문이다. conditional write가 바로 그 원시 연산을 S3 안에 넣었다. 세 포맷이 커밋을 어디에 적는지 비교한 내용은 [테이블 포맷 비교 글](/posts/table-formats-compared/)에 있다.

## directory bucket과 table bucket은 규칙이 다른 버킷이다

S3 Express One Zone의 directory bucket은 한 AZ에 두는 계층형 버킷이다. 디렉터리가 실제로 생기고 prefix 한도가 없는 대신 버킷당 읽기 200,000 TPS, 쓰기 100,000 TPS가 기본 상한이다. 범용 버킷과 다른 점이 파이프라인 코드에 바로 닿는다. 키 순서에 기대어 LIST 결과를 이어 받거나 정렬된 파일 목록을 가정한 코드는 그대로 돌지 않고, ETag가 MD5가 아니라서 체크섬 비교 코드도 깨진다.

| 항목 | directory bucket |
|---|---|
| LIST | `ListObjectsV2` 결과가 사전순이 아니다. prefix는 `/`로 끝나야 하고 구분자는 `/`만 된다 |
| rename | `RenameObject`로 객체 하나의 이름을 바꾼다. `If-None-Match`로 덮어쓰기를 막는다. 디렉터리 단위 연산은 아니다 |
| append | AZ에 있는 버킷은 기존 객체 뒤에 데이터를 붙일 수 있다 |
| multipart | part 번호가 연속이어야 완료된다 |
| 없는 기능 | 버전 관리, Object Lock, 복제, 이벤트 알림, MD5 ETag |
| 휴면 | AZ의 버킷은 90일 동안 요청이 없으면 비활성 상태가 되고 깨어나는 동안 503 |

S3 Tables의 table bucket은 Iceberg 테이블 전용이다. 리전당 기본 100개 버킷, 버킷당 10,000개 테이블까지 만든다. 데이터 파일은 Iceberg 클라이언트가 테이블 위치에 쓰고, 커밋은 S3 Tables가 메타데이터 위치를 바꾸는 API로 받는다. rename과 동시 쓰기 문제를 버킷 쪽 카탈로그가 가져간 형태이고, compaction과 스냅샷 정리도 관리형 작업으로 돈다. 대신 그 카탈로그 입구를 지원하는 엔진만 테이블에 쓸 수 있다.

## 참고 자료

- AWS S3: [What is Amazon S3 (consistency model)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html), [Strong consistency 발표(2020-12)](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/), [Optimizing performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html), [Performance design patterns](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html), [Multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html), [Document history](https://docs.aws.amazon.com/AmazonS3/latest/userguide/WhatsNew.html)
- AWS S3 conditional writes: [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), [Enforce conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html), [If-None-Match 발표(2024-08)](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/), [If-Match 발표(2024-11)](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/)
- AWS directory bucket: [Working with directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html), [Differences for directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-differences.html), [RenameObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RenameObject.html)
- Amazon EMR: [EMRFS S3-optimized committer](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-committer.html), [Requirements](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-committer-reqs.html), [Multipart uploads](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-committer-multipart.html), [S3-optimized commit protocol](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-s3-optimized-commit-protocol.html)
- Hadoop 3.5.0: [S3A committers](https://hadoop.apache.org/docs/r3.5.0/hadoop-aws/tools/hadoop-aws/committers.html), [Committer architecture](https://hadoop.apache.org/docs/r3.5.0/hadoop-aws/tools/hadoop-aws/committer_architecture.html), [mapred-default.xml](https://hadoop.apache.org/docs/r3.5.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)
- Spark 4.3.0: [Integration with Cloud Infrastructures](https://spark.apache.org/docs/latest/cloud-integration.html), [Configuration](https://spark.apache.org/docs/latest/configuration.html)
- Delta Lake: [Storage configuration](https://docs.delta.io/latest/delta-storage.html)

## English summary

S3 gives strong read-after-write consistency since December 2020, but only single-key updates are atomic, there are no real directories, and a rename is a copy plus a delete for every object. Request rates scale per partitioned prefix, at least 3,500 writes and 11,000 reads per second today, with 503 Slow Down while S3 splits a hot prefix. Because a multipart upload stays invisible until it is completed, the S3A committers and the EMRFS S3-optimized committer write to the final path and delay that completion to job or task commit instead of renaming a temporary directory. Conditional writes with If-None-Match and If-Match added put-if-absent and compare-and-swap on a single key, which is the primitive a table format needs when it commits through a log file instead of a catalog pointer. The trade-off is where the one atomic step lives: in the committer's final POSTs, in a catalog, or in a conditional PUT, and each choice limits which engines and bucket types you can use.
