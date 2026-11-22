---
title: "읽기만 해도 원천을 붙잡는 격리 수준과 MVCC"
date: 2026-11-21 06:03:00 +0900
categories: [Batch & Query, SQL]
tags: [isolation-level, mvcc, mysql, innodb, postgresql, debezium, cdc, concept]
description: "읽기만 하는 긴 추출 트랜잭션이 MVCC에서 옛 버전을 붙잡아 원천을 느리게 만드는 원리와, MySQL과 PostgreSQL의 격리 수준별 동작을 정리한다."
---

원천 MySQL에서 큰 테이블을 한 트랜잭션으로 몇 시간 동안 읽는 추출 잡이 있다. 잡은 `SELECT`만 하는데 그동안 원천의 `History list length`가 계속 오르고, 같은 테이블을 읽는 서비스 쿼리가 느려진다. MVCC에서는 읽기만 하는 트랜잭션도 옛 버전을 붙잡고 있기 때문이다. 추출 쿼리가 어느 시점을 보는지, 원천에 무엇을 남기는지가 데이터 엔지니어가 격리 수준을 알아야 하는 이유다.

버전은 MySQL 8.4.13, PostgreSQL 19.0, Debezium 3.7.1 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| ACID | 원자성, 일관성, 격리성, 지속성. 격리 수준은 이 중 I를 얼마나 느슨하게 할지 정한다 |
| MVCC | 행의 여러 버전을 두고, 읽는 쪽이 자기 스냅샷에 맞는 버전을 고르는 방식 |
| dirty read | 커밋되지 않은 다른 트랜잭션의 값을 읽는 것 |
| non-repeatable read | 같은 행을 두 번 읽었는데 그사이 다른 커밋 때문에 값이 바뀐 것 |
| phantom | 같은 조건으로 두 번 읽었는데 그사이 들어온 행 때문에 결과 집합이 달라진 것 |
| lost update | 둘이 같은 값을 읽고 각자 계산해 쓰면서 한쪽 변경이 덮이는 것 |
| write skew | 둘이 같은 조건을 읽고 서로 다른 행을 바꿔, 각자는 맞지만 합치면 조건이 깨지는 것 |
| snapshot isolation | 트랜잭션이 한 시점의 스냅샷을 읽고, 같은 행을 동시에 바꾸면 나중 쪽이 실패하는 격리 |
| consistent read | InnoDB가 read view와 undo log로 옛 버전을 다시 만들어 잠금 없이 읽는 것 |
| next-key lock | InnoDB의 인덱스 레코드 잠금과 그 앞 gap 잠금을 합친 것. 범위에 새 행이 끼는 것을 막는다 |
| history list length | 아직 purge되지 못한 커밋된 트랜잭션의 undo 목록 길이 |
| xmin horizon | PostgreSQL에서 아직 어떤 스냅샷이 볼 수 있어 VACUUM이 지우지 못하는 가장 오래된 트랜잭션 경계 |

## ANSI 네 단계는 현상 세 개로만 정의돼 있다

SQL 표준은 READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE을 dirty read, non-repeatable read, phantom 세 현상을 막느냐로 나눈다. 1995년 SIGMOD의 Berenson, Bernstein, Gray 등은 이 정의가 실제로 쓰이는 격리를 제대로 구분하지 못한다고 비판했다. 커밋 전 값을 덮어쓰는 dirty write, lost update, write skew 같은 현상이 표에 없고, 현상의 문장을 좁게 읽느냐 넓게 읽느냐에 따라 같은 수준이 다른 뜻이 된다. 같은 논문이 정의한 snapshot isolation은 표준의 세 현상을 모두 막지만 write skew를 허용한다. 반대로 잠금으로 구현한 REPEATABLE READ는 phantom을 허용하고 write skew를 막는다. 둘은 어느 쪽이 더 강하다고 줄 세울 수 없다.

그래서 같은 이름의 수준도 DB마다 다르게 동작한다. 이름만 보고 "REPEATABLE READ니까 안전하다"고 판단하면 틀린다. 레이크 쪽 Iceberg도 동시 writer의 충돌 검사 수준에 `serializable`과 `snapshot`이라는 이름을 쓰며, 그 검사 항목은 [Iceberg 행 단위 변경 글](/posts/iceberg-row-level-deletes/)에 있다.

## InnoDB REPEATABLE READ는 읽기와 쓰기가 다른 시점을 본다

InnoDB의 기본 수준은 REPEATABLE READ다. 행마다 마지막으로 바꾼 트랜잭션 ID(`DB_TRX_ID`)와 undo 레코드를 가리키는 roll pointer(`DB_ROLL_PTR`)가 숨어 있고, 일반 `SELECT`는 read view보다 새로 바뀐 행을 undo log로 되돌려 읽는다. read view는 `BEGIN` 때가 아니라 트랜잭션의 첫 consistent read 때 만들어지고, `START TRANSACTION WITH CONSISTENT SNAPSHOT`은 그 자리에서 바로 만든다. READ COMMITTED에서는 문장마다 새 스냅샷을 잡는다.

이 스냅샷은 `SELECT`에만 적용된다. `UPDATE`, `DELETE`, `SELECT ... FOR UPDATE`, `FOR SHARE`는 스냅샷 대신 가장 최근에 커밋된 버전을 읽고 잠근다. 그래서 같은 트랜잭션에서 `SELECT count(*)`가 0을 돌려준 조건으로 `UPDATE`를 하면 다른 트랜잭션이 막 커밋한 10행이 바뀔 수 있다. 여기서 lost update가 나온다. 일반 `SELECT`로 잔액 100을 읽고 애플리케이션이 90을 계산해 `UPDATE ... SET balance = 90`을 보내면, 그사이 다른 트랜잭션이 커밋한 150은 에러 없이 덮인다(DML이 최신 커밋 버전에 적용된다는 동작에서 따라 나오는 추론이다). `SELECT ... FOR UPDATE`로 먼저 잠그거나 `SET balance = balance - 10`처럼 한 문장 안에서 계산해야 한다.

잠그는 범위도 수준에 따라 다르다. REPEATABLE READ에서 unique 인덱스를 등호로 찾으면 그 레코드만 잠그고, 범위나 non-unique 인덱스로 찾으면 훑은 범위에 gap 잠금이나 next-key lock을 건다. gap 잠금끼리는 충돌하지 않고, 그 gap에 행을 넣으려는 insert intention lock만 막는다. READ COMMITTED는 gap 잠금을 외래 키와 중복 키 검사에만 쓰므로 잠금 읽기에서 phantom이 생길 수 있다. SERIALIZABLE은 `autocommit`이 꺼져 있을 때 일반 `SELECT`를 `SELECT ... FOR SHARE`로 바꿔 잠금으로 막는다.

ELT를 MySQL 안에서 돌릴 때 걸리는 지점이 하나 더 있다. `INSERT INTO summary SELECT ... FROM orders`나 `CREATE TABLE ... SELECT`에서 `FOR UPDATE`가 없는 `SELECT` 부분은 기본적으로 더 강한 잠금을 쓰고, 같은 트랜잭션 안에서도 READ COMMITTED처럼 매번 새 스냅샷을 읽는다. 원천 테이블 행에 잠금을 걸지 않으려면 그 세션을 READ COMMITTED로 낮춘다.

## PostgreSQL은 RR에서 스냅샷 격리, SERIALIZABLE에서 SSI다

PostgreSQL의 기본 수준은 READ COMMITTED이고, READ UNCOMMITTED를 요청해도 READ COMMITTED로 동작한다. PostgreSQL은 행을 고칠 때 테이블 안에 새 버전을 쓰고 옛 버전에 `xmax`를 남긴다. REPEATABLE READ의 스냅샷은 트랜잭션 제어 문이 아닌 첫 문장이 시작될 때 잡힌다. 표준이 허용하는 phantom도 이 수준에서 생기지 않는다. 스냅샷 이후 다른 트랜잭션이 바꾼 행을 고치려 하면 `could not serialize access due to concurrent update`로 실패하고, 애플리케이션이 트랜잭션 전체를 다시 돌려야 한다. snapshot isolation의 규칙대로 lost update를 에러로 바꾸는 것이다.

SERIALIZABLE은 그 위에 직렬화 이상 검사를 더한 Serializable Snapshot Isolation이다. 읽은 범위를 `SIReadLock` predicate lock으로 기록하지만 이 잠금은 아무것도 막지 않고 deadlock에도 끼지 않는다. 읽기와 쓰기 사이의 의존이 직렬 순서로 설명되지 않으면 한쪽을 SQLSTATE `40001`로 실패시킨다. `SERIALIZABLE READ ONLY DEFERRABLE`은 이상이 생길 수 없는 스냅샷을 얻을 때까지 시작을 기다리고, 그 뒤에는 실패 없이 읽는다.

| 일반 `SELECT` 뒤 쓰기 기준 | InnoDB RR (기본) | PG RC (기본) | PG RR | PG SERIALIZABLE |
|---|---|---|---|---|
| non-repeatable read, phantom | 안 생긴다 | 생긴다 | 안 생긴다 | 안 생긴다 |
| lost update (읽고 계산해 쓰기) | 조용히 덮인다 | 조용히 덮인다 | 에러, 재시도 | 에러, 재시도 |
| write skew | 생긴다 | 생긴다 | 생긴다 | `40001`, 재시도 |

write skew는 psql 두 개로 바로 볼 수 있다.

```sql
-- PostgreSQL 19.0, psql 세션 두 개. 규칙: 당직은 최소 한 명
CREATE TABLE oncall (doctor text PRIMARY KEY, on_duty boolean);
INSERT INTO oncall VALUES ('a', true), ('b', true);

-- 세션 1, 2 모두
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM oncall WHERE on_duty;              -- 둘 다 2를 본다
-- 세션 1
UPDATE oncall SET on_duty = false WHERE doctor = 'a';
-- 세션 2
UPDATE oncall SET on_duty = false WHERE doctor = 'b';
-- 세션 1, 2 차례로
COMMIT;                                                 -- 둘 다 성공, 당직 0명
-- 두 BEGIN을 ISOLATION LEVEL SERIALIZABLE로 바꾸면 한쪽이 실패한다
-- ERROR:  could not serialize access due to read/write dependencies among transactions
```

두 트랜잭션이 바꾼 행이 달라서 RR의 행 단위 충돌 검사에는 걸리지 않는다.

## CDC 스냅샷은 스냅샷 시점과 로그 위치를 맞추는 일이다

로그 기반 CDC의 초기 스냅샷은 테이블을 읽은 시점과 그 뒤 스트리밍을 시작할 로그 위치가 같아야 한다. 스냅샷과 스트림이 이어지는 자리 전체는 [CDC 글](/posts/change-data-capture/)에 있고, 여기서는 그 자리를 격리 수준이 어떻게 만드는지만 본다.

Debezium MySQL 커넥터의 기본 `snapshot.locking.mode=minimal`은 이 순서로 돈다.

1. `FLUSH TABLES WITH READ LOCK`으로 쓰기를 막는다.
2. `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`와 `START TRANSACTION WITH CONSISTENT SNAPSHOT`으로 read view를 만든다.
3. binlog 위치와 스키마를 읽고 `UNLOCK TABLES`로 쓰기를 풀어 준다.
4. 같은 트랜잭션에서 테이블을 끝까지 읽은 뒤 트랜잭션을 롤백해 닫는다.

전역 잠금은 read view와 binlog 위치 사이에 커밋이 끼지 않게 하는 데만 쓰이고, 스캔의 일관성은 REPEATABLE READ가 맡는다. Amazon RDS와 Aurora처럼 전역 읽기 잠금을 허용하지 않는 곳에서는 `LOCK TABLES` 권한으로 테이블 단위 잠금(소스에서는 `FLUSH TABLES <목록> WITH READ LOCK`)을 걸고, `UNLOCK TABLES`가 스냅샷 트랜잭션을 암묵적으로 커밋해 버리므로 이 잠금은 모든 테이블을 다 읽은 뒤에야 풀리고 스캔 내내 그 테이블에 쓰기가 막힌다. 소스에는 Percona Server의 `LOCK TABLES FOR BACKUP`을 쓰는 `minimal_percona` 모드도 있다. MySQL 8.4 자체의 백업 잠금 `LOCK INSTANCE FOR BACKUP`(`BACKUP_ADMIN` 권한)은 DML은 허용하고, 파일을 만들거나 이름을 바꾸거나 지우는 작업처럼 스냅샷을 어긋나게 할 작업만 막는다. 테이블을 복사해 바꾸는 `ALTER TABLE`이 끼면 consistent read는 `ER_TABLE_DEF_CHANGED`로 실패한다. 잠금을 아예 걸지 않는 `none` 모드는 스냅샷 동안 스키마가 안 바뀔 때만 안전하다. 잠금 없이 청크마다 binlog로 보정하는 방식은 [Flink CDC 증분 스냅샷 글](/posts/flink-cdc-incremental-snapshot/)에 있다.

PostgreSQL 커넥터는 복제 슬롯을 새로 만들 때 슬롯이 내보낸 스냅샷을 `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`와 `SET TRANSACTION SNAPSHOT`으로 가져와 테이블을 읽는다. 슬롯의 시작 LSN과 스캔 시점이 정확히 같아진다. 슬롯이 이미 있으면 `snapshot.isolation.mode` 기본값에 따라 `SERIALIZABLE, READ ONLY, DEFERRABLE` 트랜잭션을 연다. `snapshot.locking.mode` 기본값은 `none`이고, `shared`로 바꾸면 `ACCESS SHARE` 테이블 잠금으로 `ACCESS EXCLUSIVE`가 필요한 DDL만 막는다.

어느 쪽이든 큰 테이블의 스냅샷은 몇 시간짜리 트랜잭션 하나다. 그동안 원천은 그 시점의 옛 버전을 지우지 못한다.

## 하이 워터마크의 상한은 열린 트랜잭션이 정한다

`updated_at` 워터마크가 긴 트랜잭션의 행을 놓치는 과정과 lookback으로 겹쳐 읽는 방법은 [증분 적재 글](/posts/incremental-load-and-merge/)에 있다. 격리 수준으로 보면 원인은 하나다. 추출 쿼리도 스냅샷을 읽으므로, 이미 시각이 찍혔지만 아직 커밋되지 않은 행은 그 쿼리에 보이지 않는다.

lookback 길이를 추측하는 대신 이번 실행의 상한을 열린 트랜잭션에서 구할 수 있다. PostgreSQL `now()`는 트랜잭션 시작 시각이므로, 아직 커밋되지 않은 행의 `updated_at`은 그 행을 쓴 트랜잭션의 `xact_start`보다 이르지 않다. 그러면 `pg_stat_activity`에서 트랜잭션 중인 세션의 가장 이른 `xact_start`와 현재 시각 중 작은 값을 상한으로 두고, 그보다 이른 행은 이미 커밋됐거나 롤백된 것으로 볼 수 있다. MySQL에서는 `information_schema.innodb_trx`의 가장 이른 `trx_started`가 같은 역할을 한다. 이 추론은 `updated_at`을 DB 서버의 시계로 채울 때만 맞는다. 몇 시간 열린 트랜잭션 하나가 있으면 상한이 그 자리에 멈춰 적재가 늦어지는데, 행을 조용히 빠뜨리는 것보다 원인을 찾기 쉽다.

## 오래 읽는 쿼리는 원천의 정리를 멈춘다

InnoDB는 insert undo를 커밋하자마자 버리지만, update undo는 그 옛 버전이 필요할 수 있는 read view가 하나라도 남아 있으면 버리지 못한다. 지운 행도 실제로는 delete 표시만 남았다가 purge가 undo를 버릴 때 사라진다. 그래서 `mysqldump --single-transaction`이나 `autocommit`을 끄고 `COMMIT`을 잊은 `SELECT` 하나가 `SHOW ENGINE INNODB STATUS`의 `History list length`를 키운다. undo 테이블스페이스가 커지고, 자주 바뀌는 행을 읽는 쿼리는 더 긴 undo 체인을 거슬러 옛 버전을 만든다. 보조 인덱스 레코드가 delete 표시됐거나 더 새 트랜잭션이 페이지를 바꿨으면 커버링 인덱스도 못 쓰고 클러스터드 인덱스를 다시 찾는다. 읽기만 하는 트랜잭션도 자주 커밋하라는 이유다.

PostgreSQL에서는 같은 일이 테이블 bloat로 나타난다. VACUUM은 어떤 스냅샷도 볼 수 없게 된 옛 버전만 지운다. `pg_stat_activity`의 `backend_xmin`이 오래된 세션, 오래 커밋하지 않은 prepared transaction, `pg_replication_slots`의 `xmin`이나 `catalog_xmin`이 오래된 슬롯이 그 경계를 잡아 둔다. 멈춘 CDC 커넥터의 논리 복제 슬롯은 WAL과 함께 `catalog_xmin`으로 시스템 카탈로그의 정리도 잡는다. `idle_in_transaction_session_timeout`과 `transaction_timeout`은 기본값이 둘 다 0이라 꺼져 있다.

분석 쿼리를 standby로 보내도 공짜는 아니다. primary의 VACUUM 기록을 재생하다 그 행을 보는 standby 쿼리와 부딪히면 `max_standby_streaming_delay`가 지난 뒤 쿼리가 취소된다. `hot_standby_feedback`을 켜면 취소는 사라지는 대신 primary의 정리가 늦어진다. 그래도 같은 쿼리를 primary에서 돌린 것보다 나쁘지는 않다. 시점 하나로 맞출 필요가 없는 추출이면 PK 범위로 잘라 청크마다 짧은 트랜잭션으로 읽는 편이 원천에 남기는 것이 적다.

## 지우고 넣는 지표 배치에서는

[서비스 지표 배치 글](/posts/pyspark-indicator-batch/)의 공용 MySQL 코드는 unique 키로 겹치는 행을 먼저 지우고 넣어서, 같은 날짜를 다시 돌려도 행이 두 번 들어가지 않게 했다. 그 테이블은 [지표 대시보드 API 글](/posts/metrics-dashboard-api/)의 API가 읽었다. 이런 지우고 넣기에서 격리 수준이 정하는 것은 두 가지다. 두 문장을 한 트랜잭션으로 묶으면 대시보드 쿼리는 READ COMMITTED 이상에서 지우기 전이나 넣은 뒤만 보고, 각각 autocommit이면 그 사이에 빈 날짜를 볼 수 있다. 같은 날짜를 두 job이 동시에 다시 돌리면 REPEATABLE READ에서 아직 행이 없는 범위를 지우는 두 `DELETE`가 같은 gap에 잠금을 나눠 갖고, 이어진 두 `INSERT`가 서로의 gap 잠금을 기다려 deadlock이 될 수 있다(gap 잠금과 insert intention lock의 규칙에서 따라 나오는 추론이다). 날짜 범위 하나를 job 하나가 맡아 차례로 돌면 이 경합은 생기지 않는다.

## 참고 자료

- [MySQL 8.4, Transaction Isolation Levels](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html), [Consistent Nonlocking Reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html), [Locking Reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html), [InnoDB Locking](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking.html)
- [MySQL 8.4, InnoDB Multi-Versioning](https://dev.mysql.com/doc/refman/8.4/en/innodb-multi-versioning.html), [Purge Configuration](https://dev.mysql.com/doc/refman/8.4/en/innodb-purge-configuration.html), [LOCK INSTANCE FOR BACKUP](https://dev.mysql.com/doc/refman/8.4/en/lock-instance-for-backup.html)
- [PostgreSQL 19, Transaction Isolation](https://www.postgresql.org/docs/19/transaction-iso.html), [Routine Vacuuming](https://www.postgresql.org/docs/19/routine-vacuuming.html), [Client Connection Defaults](https://www.postgresql.org/docs/19/runtime-config-client.html), [Hot Standby](https://www.postgresql.org/docs/19/hot-standby.html)
- [Debezium 3.7, MySQL connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html), [PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- Debezium 3.7.1.Final 소스: [BinlogSnapshotChangeEventSource.java](https://github.com/debezium/debezium/blob/v3.7.1.Final/debezium-connector-binlog/src/main/java/io/debezium/connector/binlog/BinlogSnapshotChangeEventSource.java), [PostgresSnapshotChangeEventSource.java](https://github.com/debezium/debezium/blob/v3.7.1.Final/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresSnapshotChangeEventSource.java)
- Berenson et al., [A Critique of ANSI SQL Isolation Levels](https://arxiv.org/abs/cs/0701157), SIGMOD 1995

## English summary

Isolation levels with the same name behave differently: InnoDB REPEATABLE READ reads a snapshot but runs DML and locking reads against the latest committed rows, so a read-then-write silently loses updates, while PostgreSQL REPEATABLE READ is snapshot isolation that fails the second writer and SERIALIZABLE adds SSI checks for write skew. For data pipelines this decides how a CDC snapshot lines up with the log position, why a watermark query misses rows stamped but not yet committed, and why one long read transaction stops InnoDB purge or PostgreSQL VACUUM on the source. In my metric batches at a music service, the shared MySQL writer deleted rows by unique key and inserted them again, a design that keeps reruns simple as long as one job owns a date range at a time.
