---
title: "데이터 레이크의 권한은 어디서 집행되는가"
date: 2026-11-21 06:08:00 +0900
categories: [Data Quality, Governance]
tags: [access-control, lake-formation, pii, trino, iceberg, aws, concept]
---

Athena에서 분석가에게 주민번호 컬럼을 숨겼는데, 같은 분석가의 IAM 역할에 그 버킷의 `s3:GetObject`가 남아 있다면 숨긴 의미가 없다. Parquet 파일을 받아 열면 컬럼이 다 있다. 고객이 삭제를 요청해 `DELETE`를 돌린 행도 시간 여행 쿼리로는 여전히 보인다. 레이크의 접근 제어와 개인정보 처리는 규칙을 어디에 적느냐보다 누가 그 규칙을 집행하고, 어떤 경로가 그 집행을 건너뛰느냐로 갈린다.

REST 카탈로그와 credential vending의 기본 흐름은 [Iceberg 카탈로그 글](/posts/iceberg-catalogs-and-commits/)에 적었다. 여기서는 그 위에 권한, 개인정보, 감사 기록을 얹는다.

버전은 Trino 484, Iceberg 1.12.0(같은 태그의 REST 카탈로그 스펙), Athena 엔진 버전 3, 2026년 11월의 AWS Lake Formation 문서 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| 인증(authentication) | 요청한 주체가 누구인지 확인하는 것. Trino의 `PASSWORD`, `OAUTH2`, `JWT` 등 |
| 인가(authorization) | 확인된 주체가 무엇을 할 수 있는지 정하는 것 |
| RBAC, ABAC | 역할에 권한을 주는 방식과, 리소스와 주체의 속성(태그)이 맞으면 허용하는 방식. Lake Formation의 ABAC는 LF-Tag |
| data filter | Lake Formation에서 행, 열, 셀 단위로 `SELECT` 결과를 좁히는 그랜트 단위 |
| hybrid access mode | 같은 테이블에 Lake Formation 그랜트와 IAM 정책 두 경로를 함께 두는 등록 방식 |
| credential vending | 카탈로그가 테이블 경로로 범위를 좁힌 단기 자격 증명을 엔진에 내주는 것 |
| 마스킹, 토큰화 | 보여 줄 때만 값을 가리는 것과, 저장할 때 무작위 토큰으로 바꾸고 매핑을 따로 두는 것 |
| salt, pepper | 해시에 섞는 값. salt는 값마다 다르고 해시와 같이 두며, pepper는 비밀로 따로 둔다 |
| SSE-KMS | S3 객체를 KMS 키로 암호화하는 방식. 읽으려면 `kms:Decrypt` 권한이 더 필요하다 |
| CloudTrail 데이터 이벤트 | `GetObject` 같은 객체 단위 호출 기록. 기본으로 꺼져 있다 |

## 인증과 인가는 다른 층에서 실패한다

Trino는 `http-server.authentication.type`으로 인증 방식을 고르고(`PASSWORD`, `OAUTH2`, `JWT`, `KERBEROS`, `CERTIFICATE`, `HEADER`), 어떤 방식이든 TLS나 프록시의 forwarded 처리가 있어야 켜진다. 인증이 끝나면 사용자 이름과 그룹이 정해질 뿐, 무엇을 읽을지는 별도의 access control 설정이 정한다. 아무것도 설정하지 않으면 기본 access control은 사용자 사칭과 graceful shutdown 말고는 모두 허용한다.

RBAC는 역할 수가 테이블 수만큼 늘어나기 쉽다. 속성 기반은 테이블과 컬럼에 `classification=pii` 같은 표시를 달고, 주체에게 표시 조합을 준다. 어느 쪽이든 발급하는 쪽이 자기 권한보다 넓은 권한을 넘겨줄 수 없어야 한다. [Superset API 키 범위 글](/posts/superset-api-key-scopes/)에서 다룬 규칙도 이 원칙이다.

## Lake Formation은 그랜트를 엔진이 집행하게 한다

Lake Formation에 S3 경로를 등록하면 Athena, EMR, Glue, Redshift Spectrum이 테이블을 읽을 때 Data Catalog에 권한을 묻는다. 허용되면 Lake Formation이 그 위치용 단기 자격 증명을 내주고, 엔진은 받은 필터를 적용해 결과를 돌려준다. 이 단계마다 CloudTrail에 `GetDataAccess`가 남는다. data filter는 컬럼 목록과 행 조건을 묶은 것이다. 컬럼만 고르면 열 단위, 행 조건만 주면 행 단위, 둘 다 주면 셀 단위가 된다.

```json
{
  "Name": "kr-no-rrn",
  "DatabaseName": "crm",
  "TableName": "customers",
  "TableCatalogId": "111122223333",
  "RowFilter": {"FilterExpression": "country = 'KR'"},
  "ColumnWildcard": {"ExcludedColumnNames": ["rrn", "phone"]}
}
```

`CreateDataCellsFilter`에 이 객체를 넘기고 `SELECT`를 줄 때 필터를 고른다. 몇 가지 제약이 설계를 바꾼다.

| 제약 | 결과 |
|---|---|
| 필터는 읽기에만 적용되고 `SELECT`로만 줄 수 있다 | 쓰기 권한이 있는 주체에게 컬럼 필터를 걸어도 효과가 없다 |
| 행 조건은 PartiQL `WHERE`의 일부, 함수 불가, 컬럼끼리 비교 불가, 2048자 미만 | 사용자마다 달라지는 조건을 함수로 계산할 수 없어, 조건이 다르면 필터도 따로 만든다 |
| 파티션 키에는 컬럼 필터를 걸 수 없다 | 민감한 값을 파티션 키로 쓰면 컬럼 필터로는 숨길 수 없고, S3 경로에도 값이 드러난다 |
| 사용자와 그 그룹이 각각 행 조건을 받으면 합집합 | 그랜트를 더할수록 보이는 행이 늘어난다 |
| 주체 하나가 테이블 하나에 받을 수 있는 필터는 100개 | 필터를 사용자 수만큼 만드는 설계는 막힌다 |
| 컬럼은 숨길 뿐 값을 바꿔 보여 주지 않는다 | 끝 네 자리만 보이게 하려면 마스킹한 뷰나 엔진 쪽 규칙이 필요하다 |

## LF-Tag는 그랜트 수를 곱에서 합으로 바꾼다

모든 주체에게 모든 리소스를 이름으로 주면 그랜트는 주체 수 곱하기 리소스 수만큼 필요하다. LF-Tag 하나로 주면 주체에게 주는 그랜트와 리소스에 붙이는 태그를 더한 수로 줄어든다. 태그는 데이터베이스에서 테이블로, 테이블에서 컬럼으로 상속되고 아래에서 덮어쓸 수 있다. 그래서 테이블은 `classification=general`을 물려받게 두고, 개인정보 컬럼만 `classification=pii`로 덮어쓴다. 분석가 역할에 `classification=general`의 `SELECT`를 주면 새 테이블이 생겨도 그랜트를 다시 할 필요가 없고, PII 컬럼은 자동으로 빠진다. 한 키에 값을 여러 개 고르면 OR, 키를 여러 개 넣으면 AND로 평가한다.

## hybrid access mode와 IAM 경로는 필터를 건너뛴다

hybrid access mode로 등록한 위치에서는 opt-in한 주체만 Lake Formation 그랜트를 따른다. 이들은 Lake Formation 권한과 IAM 권한을 둘 다 갖춰야 한다. opt-in하지 않은 주체는 `IAMAllowedPrincipals` 그룹 권한을 통해 예전처럼 IAM 정책만으로 읽는다. 기존 잡을 멈추지 않고 사용자 단위로 옮길 수 있는 대신, 옮기는 동안에는 IAM 경로가 data filter를 무시하는 문이 된다. 등록을 마친 뒤에도 같은 문제가 남는다. Lake Formation 그랜트는 Data Catalog를 거치는 읽기에만 걸린다. 그 버킷에 `s3:GetObject`를 가진 IAM 역할은 S3 API로 파일을 통째로 읽는다. 열 필터를 믿으려면 사람 역할에서 레이크 버킷의 직접 S3 권한을 빼야 한다.

## 엔진 규칙과 credential vending은 신뢰하는 엔진에서만 맞물린다

직접 운영하는 Trino에서는 파일 기반 access control로 같은 일을 한다. 규칙은 위에서부터 읽어 처음 맞는 규칙을 쓰고, 규칙이 있는데 아무것도 맞지 않으면 거부한다. `security.refresh-period`를 주면 재시작 없이 다시 읽는다. `group`은 그룹 provider가 설정돼 있어야 맞는다.

```json
{
  "catalogs": [{"group": "analyst", "catalog": "lake", "allow": "read-only"}],
  "tables": [{
    "group": "analyst", "catalog": "lake", "schema": "crm", "table": "customers",
    "privileges": ["SELECT"], "filter": "country = 'KR'",
    "columns": [
      {"name": "phone", "mask": "concat('***-****-', substr(phone, -4))"},
      {"name": "rrn", "allow": false}
    ]
  }]
}
```

Lake Formation과 달리 `mask`로 값을 바꿔 보여 줄 수 있다. 규칙을 Ranger에서 관리하려면 `access-control.name=ranger`로 내장 플러그인을 쓰고, Trino 서비스 정의가 들어 있는 Ranger 2.5.0 이상을 쓴다. 이 플러그인도 행 필터, 컬럼 마스킹, 감사 로그를 지원한다.

Iceberg REST 카탈로그는 클라이언트가 `X-Iceberg-Access-Delegation: vended-credentials`를 보내면 `storage-credentials`에 경로 prefix별 자격 증명을 담아 준다. 1.12.0 스펙에는 자격 증명만 다시 받는 `GET .../tables/{table}/credentials`도 있다. 이 자격 증명은 파일 단위 권한이다. 받은 쪽은 파일 안의 모든 행과 컬럼을 읽을 수 있으니, 행과 컬럼 제한은 그 자격 증명을 쥔 엔진이 지킬 때만 유효하다. 1.12.0 스펙은 `LoadTableResult`에 `read-restrictions`(필수 행 필터, 컬럼별 `show-last-4`, `sha-256-global` 같은 변환)를 넣고, 이를 지원하는 reader가 제한을 다 적용하지 못하면 쿼리를 실패시키게 했다. Lake Formation도 외부 엔진용 자격 증명 API(`GetTemporaryGlueTableCredentials`)를 기본으로 꺼 둔다. 엔진의 IAM 세션 태그를 검증하게 하거나, 테이블 전체 권한이 있는 사용자에 한해 세션 태그 없이 받게 하는 옵션을 켜야 열린다.

## 개인정보 컬럼은 쓰는 시점에 바꿔 둔다

마스킹은 읽는 쪽 규칙이라 원본은 그대로 남는다. 조인 키로 써야 하는 전화번호나 이메일은 적재할 때 바꾼다. 가장 흔한 실수는 `sha256(phone)`이다. 휴대전화 번호 `010`으로 시작하는 11자리는 1억 개뿐이라 전부 해싱해 보면 된다.

```python
# Python 3.14, 표준 라이브러리만 사용
import hashlib, hmac

leaked = hashlib.sha256(b"01012345678").hexdigest()
for n in range(10**8):
    cand = f"010{n:08d}".encode()
    if hashlib.sha256(cand).hexdigest() == leaked:
        print("복원:", cand.decode())  # 한 스레드로 이 번호는 10초 안팎, 1억 개 전체도 1분 남짓
        break

pepper = b"..."  # 데이터와 다른 곳(비밀 값 저장소)에서 읽는 키
token = hmac.new(pepper, b"01012345678", hashlib.sha256).hexdigest()
```

행마다 다른 salt를 섞으면 같은 번호가 다른 해시가 되어 조인이 깨진다. 모든 행에 같은 salt를 쓰고 그 값을 코드나 테이블 옆에 두면 공격자도 salt를 넣고 같은 루프를 돈다. 막는 것은 데이터 밖에 둔 비밀 키, 즉 pepper를 키로 쓰는 HMAC이다. 대가로 키를 바꾸면 이전 토큰과 조인이 끊기고, 키를 가진 사람은 여전히 같은 루프로 되돌릴 수 있다.

토큰화는 원래 값과 무작위 토큰의 매핑을 별도 저장소에 두는 방식이다. 권한이 있으면 되돌릴 수 있고, 매핑을 지우면 토큰만으로는 원래 값을 찾을 수 없다. 해시든 토큰이든 다른 정보와 합쳐 사람을 다시 알아볼 수 있으면 GDPR은 여전히 개인정보로 본다(Recital 26).

SSE-KMS는 저장된 객체를 암호화하면서 두 번째 권한 관문이 된다. 객체를 읽으려면 `s3:GetObject`와 함께 그 키의 `kms:Decrypt`가 있어야 하고, 키 사용은 CloudTrail에 남는다. S3 Bucket Key를 켜면 KMS 요청 비용이 최대 99% 줄어드는 대신 암호화 컨텍스트가 객체 ARN에서 버킷 ARN으로 바뀐다. 권한이 있는 쿼리 앞에서는 암호화가 아무것도 가리지 않는다.

## 지운 행은 마지막 스냅샷이 사라질 때 사라진다

Athena에서 Iceberg 테이블에 `DELETE`를 돌리면 position delete 파일만 쓴다. 원래 행은 데이터 파일에 남고, 이전 스냅샷으로 시간 여행하면 그대로 보인다. 삭제 요청을 끝내려면 세 단계가 필요하다.

1. compaction으로 그 행이 빠진 데이터 파일을 새로 쓴다. delete 파일이 언제 compaction 대상이 되는지는 [행 단위 삭제 글](/posts/iceberg-row-level-deletes/)에 있다.
2. 삭제 이전 스냅샷을 만료시킨다. 보존 기본값과 metadata.json 정리는 [스냅샷과 브랜치 글](/posts/iceberg-snapshots-and-branches/)에 있다. 태그나 브랜치가 옛 스냅샷을 잡고 있으면 만료되지 않는다.
3. 버킷 버전 관리가 켜져 있으면 삭제된 객체가 이전 버전으로 남으니 수명 주기 규칙으로 이전 버전을 지운다.

Athena `VACUUM`은 스냅샷 만료와 고아 파일 삭제를 함께 하고, 기본으로 5일(`vacuum_max_snapshot_age_seconds` 432000)보다 오래된 스냅샷을 지운다. 실행 역할에 `s3:DeleteObject`가 없으면 쿼리는 성공하는데 파일은 지워지지 않는다. 삭제 기한이 있는 데이터라면 이 기본값과 권한이 실제 삭제 시점을 정한다.

## 감사 기록은 켜야 남는다

CloudTrail은 기본으로 관리 이벤트만 남긴다. `GetObject`, `PutObject` 같은 S3 데이터 이벤트는 따로 켜야 하고 비용이 붙는다. Lake Formation을 거친 읽기에서는 `GetDataAccess`와 S3 `GetObject`가 짝을 이룬다. 2026년 2월 전까지 그 `GetObject`에는 등록 역할만 보였다. 지금은 데이터 레이크 설정에 `SET_SOURCE_IDENTITY`를 켜고 등록 역할 신뢰 정책에 `sts:SetSourceIdentity`를 더하면, `sourceIdentity`에 쿼리를 시작한 역할의 고유 ID가 실린다. 이 기록이 말해 주는 것은 누가 언제 어떤 객체를 읽었는지까지다. 왜 읽었는지는 애플리케이션 층에서 남겨야 하고, [Superset 다운로드 사유 글](/posts/superset-download-reason-sip/)에서 내가 제안한 기능이 그 빈칸을 Action Log에 채우는 방식이다.

## 참고 자료

- AWS Lake Formation: [Data filtering and cell-level security](https://docs.aws.amazon.com/lake-formation/latest/dg/data-filtering.html), [Data filtering limitations](https://docs.aws.amazon.com/lake-formation/latest/dg/data-filtering-notes.html), [PartiQL support in row filter expressions](https://docs.aws.amazon.com/lake-formation/latest/dg/partiql-support.html)
- AWS Lake Formation: [Tag-based access control](https://docs.aws.amazon.com/lake-formation/latest/dg/tag-based-access-control.html), [Granting permissions using LF-TBAC](https://docs.aws.amazon.com/lake-formation/latest/dg/granting-catalog-perms-TBAC.html), [Hybrid access mode](https://docs.aws.amazon.com/lake-formation/latest/dg/hybrid-access-mode.html), [How hybrid access mode works](https://docs.aws.amazon.com/lake-formation/latest/dg/hybrid-access-workflow.html), [Underlying data access control](https://docs.aws.amazon.com/lake-formation/latest/dg/access-control-underlying-data.html), [Integrating third-party services](https://docs.aws.amazon.com/lake-formation/latest/dg/Integrating-with-LakeFormation.html), [Using application integration](https://docs.aws.amazon.com/lake-formation/latest/dg/using-cred-vending.html), [Logging with CloudTrail](https://docs.aws.amazon.com/lake-formation/latest/dg/logging-using-cloudtrail.html)
- Trino 484: [File-based access control](https://trino.io/docs/current/security/file-system-access-control.html), [Ranger access control](https://trino.io/docs/current/security/ranger-access-control.html), [Authentication types](https://trino.io/docs/current/security/authentication-types.html), [Binary functions](https://trino.io/docs/current/functions/binary.html), Iceberg 1.12.0 [REST catalog OpenAPI spec](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/open-api/rest-catalog-open-api.yaml)
- Athena: [DELETE on Iceberg tables](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-delete.html), [VACUUM](https://docs.aws.amazon.com/athena/latest/ug/vacuum-statement.html), Amazon S3 [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html), [CloudTrail events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/cloudtrail-logging-s3-info.html), [Deleting object versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html), [OWASP Password Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html), [GDPR Recital 26](https://gdpr-info.eu/recitals/no-26/)

## English summary

Access control in a data lake depends on who enforces the rule and which path skips it. Lake Formation data filters and LF-Tags give row, column, and cell security, but only engines that read through the Data Catalog apply them, so direct S3 permissions and non-opted-in principals in hybrid access mode bypass them. Trino rules and Ranger can mask values, and Iceberg REST credential vending hands file-level credentials, so fine-grained limits hold only inside a trusted engine. Plain hashes of phone numbers can be reversed by trying every number, so a keyed HMAC with a secret kept outside the data is the minimum. CloudTrail data events show who read which object but not why, and my Superset download reason proposal fills that gap by reusing the existing Action Log instead of adding a new table.
