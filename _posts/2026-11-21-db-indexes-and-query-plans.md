---
title: "인덱스가 있는데 옵티마이저가 풀 스캔을 고르는 이유"
date: 2026-11-21 06:10:00 +0900
categories: [Batch & Query, SQL]
tags: [index, query-plan, explain, mysql, innodb, postgresql, join, concept]
description: "MySQL orders 테이블에 인덱스가 있는데도 DATE 함수 조건 때문에 풀 스캔이 되는 사례로, InnoDB와 PostgreSQL의 인덱스 구조와 옵티마이저의 실행 계획 선택을 설명한다."
---

원천 MySQL `orders` 테이블의 `updated_at`에 인덱스가 있는데, 하루치를 뽑는 추출 쿼리의 `EXPLAIN`이 `type: ALL`, `key: NULL`을 보여 준다. 조건은 `WHERE DATE(updated_at) = '2026-11-20'`이었다. 같은 테이블에서 30일치를 범위 조건으로 제대로 쓴 backfill 쿼리도 풀 스캔을 골랐다. 앞의 것은 인덱스를 쓸 수 없는 모양으로 쓴 쿼리이고, 뒤의 것은 옵티마이저가 풀 스캔이 더 싸다고 계산한 결과다. 둘을 가르려면 인덱스가 행까지 어떻게 가는지와 실행 계획의 어느 줄을 읽어야 하는지 알아야 한다.

버전은 MySQL 8.4.13(LTS, InnoDB), PostgreSQL 19.0 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| clustered index | InnoDB에서 기본 키 순서로 행 전체를 리프에 담은 B+tree. 테이블 그 자체다 |
| secondary index | 인덱스 컬럼과 기본 키 값만 담는 인덱스. 나머지 컬럼은 clustered index를 다시 찾아 읽는다 |
| heap | PostgreSQL이 행을 순서 없이 쌓는 테이블 파일. 인덱스는 행의 물리 위치를 가리킨다 |
| covering index | 쿼리가 쓰는 컬럼이 모두 인덱스에 있어 테이블 행을 읽지 않아도 되는 인덱스 |
| index-only scan | PostgreSQL에서 인덱스와 visibility map만으로 답하는 스캔 |
| leftmost prefix | 복합 인덱스는 앞 컬럼부터 조건이 걸려야 읽을 범위를 좁힌다는 규칙 |
| skip scan | 앞 컬럼에 조건이 없을 때 그 컬럼의 값마다 인덱스를 따로 찾아 들어가는 방식 |
| selectivity | 조건을 통과하는 행의 비율. 낮을수록 인덱스가 유리하다 |
| cardinality | 컬럼이나 인덱스에 있는 서로 다른 값 개수의 추정치 |
| index nested loop | 바깥 행마다 안쪽 테이블을 인덱스로 찾는 조인 |
| hash join | 한쪽 입력으로 해시 테이블을 만들고 다른 쪽 행으로 찾는 조인 |
| HOT update | PostgreSQL에서 인덱스 컬럼이 그대로이고 같은 페이지에 자리가 있을 때 인덱스를 건드리지 않는 UPDATE |

## InnoDB 보조 인덱스는 기본 키를 거쳐 행에 닿는다

InnoDB 테이블은 기본 키로 정렬된 B+tree 하나이고, 리프 페이지에 행 전체가 있다. 기본 키가 없으면 키 컬럼이 모두 `NOT NULL`인 첫 `UNIQUE` 인덱스를 쓰고, 그것도 없으면 6바이트 row ID로 숨은 `GEN_CLUST_INDEX`를 만든다. 페이지를 제자리에서 고치는 B-tree의 쓰기 구조는 [LSM 트리와 B-tree 글](/posts/lsm-tree-vs-btree/)에 있다.

보조 인덱스 레코드에는 인덱스 컬럼과 그 행의 기본 키 값이 들어 있다. `WHERE email = ?`로 보조 인덱스를 찾으면 기본 키 값을 얻고, 그 값으로 clustered index를 한 번 더 내려가 행을 읽는다. 1건이면 트리 탐색 두 번이지만, 10만 건이면 기본 키 순서와 상관없이 흩어진 탐색이 10만 번 더 붙는다. 기본 키가 길면 모든 보조 인덱스가 같이 커진다.

두 번째 탐색을 없애는 것이 covering index다. 쿼리가 쓰는 컬럼이 보조 인덱스에 다 있으면 clustered index에 가지 않고 `EXPLAIN`의 `Extra`에 `Using index`가 찍힌다. 기본 키는 이미 보조 인덱스에 있으니 커버링 계산에 들어간다. 옵티마이저도 이 점을 쓴다. 기본 키가 `id`이면 `INDEX (updated_at)`을 안에서 `(updated_at, id)`로 다루고(`optimizer_switch`의 `use_index_extensions`, 기본 on), `ref`·`range` 조회와 정렬에 뒤쪽 `id`까지 쓴다.

## PostgreSQL은 인덱스와 heap이 따로 있다

PostgreSQL 테이블은 heap이고, 기본 키도 보조 인덱스와 똑같이 행 위치를 가리키는 인덱스 하나다. 맞는 행이 적으면 `Index Scan`이 인덱스 순서대로 heap을 읽는다. 많으면 `Bitmap Index Scan`이 위치를 모아 비트맵을 만들고, 위의 `Bitmap Heap Scan`이 그 위치를 물리 순서로 정렬해 heap을 읽는다. 흩어진 읽기를 순차 읽기에 가깝게 바꾸는 단계다.

인덱스 항목에는 그 행이 지금 보이는지에 대한 정보가 없다. 그래서 필요한 컬럼이 다 인덱스에 있어도 heap 페이지마다 한 비트인 visibility map을 본다. 그 페이지의 모든 행이 모든 트랜잭션에 보이면 비트가 켜져 있고, 켜진 페이지는 heap을 건너뛰고 꺼진 페이지는 heap을 읽어 확인한다. `EXPLAIN ANALYZE`의 `Index Only Scan` 아래 `Heap Fetches`가 heap까지 간 행 수다. 비트는 VACUUM이 켜므로 자주 바뀌는 테이블에서는 index-only scan이 일반 index scan과 거의 같아진다. 긴 트랜잭션이 VACUUM을 붙잡는 과정은 [격리 수준과 MVCC 글](/posts/isolation-levels-and-mvcc/)에 있다. 검색 키가 아닌 컬럼을 인덱스에 얹을 때는 `CREATE INDEX ON t (x) INCLUDE (y)`를 쓴다.

| | MySQL InnoDB | PostgreSQL |
|---|---|---|
| 행이 있는 곳 | 기본 키 B+tree의 리프 | heap 파일 |
| 보조 인덱스가 가리키는 것 | 기본 키 값 | 행의 물리 위치(TID) |
| 테이블을 건너뛰는 조건 | 인덱스 컬럼과 기본 키로 쿼리가 끝나고, 그 보조 인덱스 페이지를 더 새 트랜잭션이 고치지 않음 | 인덱스 컬럼으로 쿼리가 끝나고, visibility map 비트가 켜져 있음 |
| 계획에 보이는 표시 | `Extra: Using index` | `Index Only Scan`, `Heap Fetches` |

## 복합 인덱스는 왼쪽 컬럼부터 범위를 좁힌다

`INDEX (service, type, date)`가 있으면 앞 컬럼들의 등호 조건, 그리고 등호가 없는 첫 컬럼의 범위 조건까지가 읽을 인덱스 구간을 정한다. 그 뒤 컬럼의 조건은 인덱스 안에서 걸러 테이블 방문은 줄이지만 읽는 인덱스 양은 줄이지 못한다. `WHERE type = 'app' AND date >= ...`처럼 맨 앞 `service`가 빠지면 보통 구간을 만들 수 없다.

예를 들어 지표 테이블을 날짜 범위, 서비스, 플랫폼 구분으로 잘라 읽는다면, 등호로 거는 서비스와 구분을 앞에, 범위로 거는 날짜를 뒤에 두어야 구간이 가장 좁다. 날짜를 맨 앞에 두면 날짜 구간 안의 다른 서비스 행까지 인덱스에서 다 훑는다.

skip scan은 앞 컬럼 조건이 빠진 경우를 일부 살린다. 앞 컬럼의 서로 다른 값마다 `service = 값 AND type = 'app' AND date >= ...` 구간을 따로 만들어 차례로 읽는다. MySQL은 기본으로 켜져 있지만(`optimizer_switch`의 `skip_scan`) 조건이 좁다. 테이블 하나만 참조하고, `GROUP BY`·`DISTINCT`가 없고, 쿼리가 인덱스 컬럼만 쓰고, 건너뛴 컬럼 뒤에 범위 조건이 있어야 한다. 쓰이면 `Extra`에 `Using index for skip scan`이 나온다. PostgreSQL은 18부터 B-tree skip scan을 하며, 앞 컬럼 값의 종류가 적어 인덱스 대부분을 건너뛸 수 있을 때만 고른다. 종류가 많으면 순차 스캔이 낫다고 판단한다.

## 옵티마이저는 읽을 행 수로 고른다

인덱스로 찾는 비용은 맞는 행 수를 따라가고, 풀 스캔 비용은 테이블 크기를 따라간다. 맞는 행이 많아지면 어느 지점부터는 흩어진 탐색 수십만 번보다 테이블을 순서대로 한 번 읽는 쪽이 싸다. PostgreSQL의 1만 행 테이블 `tenk1`에서 조건이 약 70%를 통과하면 `Seq Scan`, 1%면 `Bitmap Heap Scan`, 1행이면 `Index Scan`이 나온다. MySQL은 테이블이 아주 작을 때, `WHERE`와 `ON`에 인덱스로 쓸 조건이 없을 때, 상수 범위가 테이블의 큰 부분을 덮는다고 계산될 때, 카디널리티가 낮은 키라 키 탐색이 너무 많을 때 `type: ALL`을 고른다.

행 수 추정은 통계에서 나온다. MySQL은 등호 구간이 `eq_range_index_dive_limit`보다 적으면 인덱스를 실제로 내려가 세고(index dive), 많으면 인덱스 통계로 추정한다. 인덱스 없는 컬럼의 값이 한쪽에 몰려 있으면 `ANALYZE TABLE orders UPDATE HISTOGRAM ON status WITH 64 BUCKETS`로 히스토그램을 만들 수 있고, 범위 추정이 되는 인덱스 컬럼에는 히스토그램을 쓰지 않는다. PostgreSQL은 `ANALYZE`가 모아 `pg_stats`에 두는 `n_distinct`와 최빈값 목록으로 추정한다.

어제치만 읽던 증분 쿼리를 30일 backfill로 돌리면 같은 SQL이 풀 스캔으로 바뀌는 것도 이 계산이다. 추정이 실제와 맞다면 그 선택이 원천에도 덜 무겁다. 추정이 틀렸다는 근거가 보이면 `ANALYZE TABLE`로 통계부터 새로 하고, `FORCE INDEX`는 그다음이다.

## 컬럼을 함수나 형 변환으로 감싸면 구간을 못 만든다

B-tree가 정렬해 둔 것은 컬럼의 원래 값이다. `DATE(updated_at)`의 결과는 그 순서와 다른 값이라 인덱스 구간으로 바꿀 수 없다. 상수 쪽을 바꿔 원래 컬럼의 범위로 쓴다. Parquet min/max 통계에서 같은 일이 생기는 이유는 [컬럼 함수와 파일 건너뛰기 글](/posts/columnar-files-and-pruning/)에 있다.

```sql
-- MySQL 8.4. orders(id BIGINT PRIMARY KEY, status VARCHAR(20), updated_at DATETIME, INDEX (updated_at))
EXPLAIN SELECT * FROM orders WHERE DATE(updated_at) = '2026-11-20';   -- key가 비어 있다
EXPLAIN SELECT * FROM orders
WHERE updated_at >= '2026-11-20' AND updated_at < '2026-11-21';       -- type: range 후보가 된다
```

암묵적 형 변환은 더 눈에 안 띈다. MySQL에서 문자열 컬럼을 숫자와 비교하면(`WHERE str_col = 1`) 둘을 부동소수로 바꿔 비교한다. `'1'`, `' 1'`, `'1a'`가 모두 1이 되므로 인덱스로 찾을 수 없다. ID를 `VARCHAR`로 둔 원천에 숫자 파라미터를 바인딩하면 이렇게 된다. PostgreSQL에는 `text = integer` 연산자가 없어 같은 쿼리가 조용히 느려지는 대신 에러로 끝난다.

식을 그대로 써야 하면 식에 인덱스를 건다. MySQL은 `CREATE INDEX idx_upd_date ON orders ((DATE(updated_at)))`처럼 괄호로 감싼 functional key part를 쓰고, 이것은 숨은 virtual generated column으로 구현된다. PostgreSQL은 `CREATE INDEX ON orders ((updated_at::date))` 같은 expression index를 쓰는데, 식이 `IMMUTABLE`이어야 해서 `timestamptz`를 `date`로 바꾸는 캐스트는 세션 시간대에 따라 결과가 달라 거부된다. 어느 쪽이든 쿼리가 인덱스와 같은 식을 써야 잡힌다.

## 실행 계획에서 먼저 볼 줄

| 보는 것 | MySQL 8.4 | PostgreSQL 19 |
|---|---|---|
| 접근 방식 | `type`: `const` > `eq_ref` > `ref` > `range` > `index` > `ALL` | `Seq Scan`, `Index Scan`, `Index Only Scan`, `Bitmap Heap Scan` |
| 추정 행 수 | `rows` 곱하기 `filtered`(%)가 다음 단계로 넘어가는 행 | `rows=`는 노드가 내보내는 행 수 |
| 실제 실행 | `EXPLAIN ANALYZE`, 항상 TREE 형식 | `EXPLAIN ANALYZE`, 18부터 `BUFFERS` 기본 포함 |
| 버린 일 | `Extra: Using where`, `Using filesort`, `Using temporary` | `Rows Removed by Filter`, `Heap Fetches` |

```text
-> Filter: (t3.i > 8)  (cost=0.75 rows=1.67) (actual time=0.0168..0.0182 rows=1 loops=1)
```

MySQL `EXPLAIN ANALYZE`의 한 줄은 위 모양이다. 괄호 앞쪽이 추정 비용과 행 수, 뒤쪽이 첫 행까지와 끝까지 걸린 밀리초, 실제 행 수, 반복 횟수이고, 시간은 반복이 여럿이면 한 번의 평균이다. PostgreSQL은 `actual time`과 `rows`가 모두 반복 한 번의 평균이라 `loops`를 곱해야 전체가 되고, 18부터 행 수가 `rows=7000.00`처럼 소수로 나온다. 두 DB 모두 가장 먼저 볼 것은 추정 행 수와 실제 행 수의 차이다. 열 배 이상 벌어진 노드가 있으면 그 위의 조인 방식과 순서도 틀린 추정 위에서 골라졌을 수 있다. `EXPLAIN ANALYZE`는 쿼리를 실제로 실행하므로, 원천의 큰 추출 쿼리에 걸면 그 쿼리를 한 번 더 돌리는 것과 같다.

## 조인 알고리즘은 안쪽 테이블의 인덱스에서 갈린다

| 방식 | 하는 일 | MySQL 8.4 | PostgreSQL 19 |
|---|---|---|---|
| nested loop | 바깥 행마다 안쪽을 다시 읽는다 | 안쪽에 인덱스가 없으면 hash join으로 대신한다 | `Nested Loop` |
| index nested loop | 바깥 행마다 안쪽을 인덱스로 찾는다 | 안쪽 `type`이 `eq_ref`, `ref` | `Nested Loop` 아래 안쪽 `Index Scan`, `loops`가 바깥 행 수 |
| hash join | 한쪽으로 해시 테이블을 만들고 다른 쪽으로 찾는다 | `Using join buffer (hash join)`, TREE에서 `Inner hash join` | `Hash Join` |
| merge join | 양쪽을 조인 키로 정렬해 나란히 읽는다 | 없다 | `Merge Join`, 정렬은 `Sort` 노드나 인덱스 순서 |

MySQL은 8.0.18에서 쓸 인덱스가 없는 등호 조인에 hash join을 넣었고, 8.0.20에서 block nested loop를 없애 그 자리를 모두 hash join으로 바꿨다. 등호 조건이 없는 조인과 outer join, semijoin, antijoin도 여기에 들어간다. 해시 테이블은 `join_buffer_size`까지만 메모리에 두고 넘치면 디스크 파일로 내린다. 바깥이 작고 안쪽 조인 키에 인덱스가 있으면 index nested loop가 여전히 가장 싸고, 양쪽이 크고 인덱스가 없으면 hash join이 된다. 같은 판단을 분산 엔진이 셔플과 브로드캐스트로 하는 방식은 [Spark 조인 전략 글](/posts/spark-join-strategies/)에 있다.

## 인덱스 하나마다 쓰기가 늘어난다

INSERT 한 번은 clustered index와 모든 보조 인덱스에 항목을 하나씩 넣는다. InnoDB에서 인덱스 컬럼을 바꾸는 UPDATE는 옛 보조 인덱스 레코드에 삭제 표시를 하고 새 레코드를 넣으며, 옛 레코드는 purge가 나중에 지운다. MySQL 8.4는 `innodb_change_buffering` 기본값이 `none`이라, 버퍼 풀에 없는 보조 인덱스 페이지는 쓰는 순간 읽어 와서 고친다.

PostgreSQL은 HOT update가 아니면 바뀌지 않은 컬럼의 인덱스까지 포함해 모든 인덱스에 새 항목을 넣는다. HOT이 되려면 인덱스가 걸린 컬럼(BRIN 같은 summarizing 인덱스는 빼고)이 하나도 안 바뀌고, 같은 heap 페이지에 새 버전이 들어갈 자리가 있어야 한다. 그래서 증분 추출을 위해 `updated_at`에 B-tree 인덱스를 걸면, `updated_at`을 갱신하는 모든 UPDATE가 HOT이 될 수 없다. 추출 쿼리의 이득을 원천 쓰기 비용으로 치르는 셈이고, `pg_stat_all_tables`의 `n_tup_upd`와 `n_tup_hot_upd`로 그 비율이 보인다.

안 쓰는 인덱스는 비용만 남긴다. MySQL은 `ALTER TABLE orders ALTER INDEX idx_x INVISIBLE`로 옵티마이저에서만 숨겨 영향을 본 뒤 지울 수 있고, 숨긴 동안에도 쓰기 때 유지는 계속된다. PostgreSQL은 `pg_stat_user_indexes`의 `idx_scan`으로 쓰이지 않는 인덱스를 찾는다.

unique 키 인덱스는 쓰기마다 유지 비용이 들지만, 다시 돌릴 때 지울 행을 키로 바로 찾게 하고 중복도 막는다. [PySpark 지표 배치 글](/posts/pyspark-indicator-batch/)의 공용 MySQL 코드는 테이블 unique 키로 겹치는 행을 먼저 지우고 넣어서, 같은 날짜를 다시 돌려도 행이 두 번 들어가지 않았다.

## 증분 추출 쿼리를 인덱스에 맞추기

`updated_at`과 `id`로 페이지를 넘기는 증분 쿼리는 InnoDB에서 `INDEX (updated_at)` 하나로 `(updated_at, id)` 순서를 얻는다. `ORDER BY updated_at, id`를 인덱스 순서로 대신할 수 있어서 페이지마다 짧은 구간을 `range`로 읽는다. 워터마크와 lookback, 커서 조건은 [증분 적재 글](/posts/incremental-load-and-merge/)에 있다. `SELECT *`라면 행마다 clustered index 탐색이 붙으므로, 한 번에 넓은 구간을 읽을수록 옵티마이저가 풀 스캔으로 넘어갈 가능성이 커진다. 한 실행에 읽는 구간을 좁게 두는 것이 계획을 안정시키는 방법이다.

큰 추출을 replica로 보내면 primary의 버퍼 풀과 디스크를 서비스 쿼리와 나눠 쓰지 않는다. replica는 복제된 DDL로 primary와 같은 인덱스를 갖는다. 대신 MySQL에서 트랜잭션 안에서 읽은 테이블의 metadata lock은 트랜잭션이 끝날 때까지 남고, 다른 세션의 `ALTER TABLE`은 그동안 기다린다. replica의 복제 적용 스레드도 그 세션 중 하나이니, primary에서 같은 테이블에 DDL이 돌면 긴 추출이 끝날 때까지 replica 지연이 쌓인다(metadata lock 규칙에서 따라 나오는 추론이다). 실행의 상한을 배치 서버 시계로 정한다면 replica의 `SHOW REPLICA STATUS` `Seconds_Behind_Source`만큼은 당겨야 아직 적용되지 않은 행을 건너뛰지 않는다(이것도 추론이다). 이 값은 replica가 이미 받은 이벤트만 기준으로 재므로 받기부터 밀리면 0으로 보일 수 있고, 복제 스레드가 멈추면 NULL이다. PostgreSQL standby에서 긴 쿼리가 취소되는 조건은 격리 수준 글에 있다.

## 참고 자료

- [MySQL 8.4, Clustered and Secondary Indexes](https://dev.mysql.com/doc/refman/8.4/en/innodb-index-types.html), [Use of Index Extensions](https://dev.mysql.com/doc/refman/8.4/en/index-extensions.html), [Multiple-Column Indexes](https://dev.mysql.com/doc/refman/8.4/en/multiple-column-indexes.html), [Range Optimization (Skip Scan)](https://dev.mysql.com/doc/refman/8.4/en/range-optimization.html)
- [MySQL 8.4, Avoiding Full Table Scans](https://dev.mysql.com/doc/refman/8.4/en/table-scan-avoidance.html), [Optimizer Statistics](https://dev.mysql.com/doc/refman/8.4/en/optimizer-statistics.html), [Type Conversion in Expression Evaluation](https://dev.mysql.com/doc/refman/8.4/en/type-conversion.html), [CREATE INDEX](https://dev.mysql.com/doc/refman/8.4/en/create-index.html)
- [MySQL 8.4, EXPLAIN Output Format](https://dev.mysql.com/doc/refman/8.4/en/explain-output.html), [EXPLAIN Statement](https://dev.mysql.com/doc/refman/8.4/en/explain.html), [Hash Join Optimization](https://dev.mysql.com/doc/refman/8.4/en/hash-joins.html), [8.0 Hash Join Optimization](https://dev.mysql.com/doc/refman/8.0/en/hash-joins.html)
- [MySQL 8.4, Change Buffer](https://dev.mysql.com/doc/refman/8.4/en/innodb-change-buffer.html), [Metadata Locking](https://dev.mysql.com/doc/refman/8.4/en/metadata-locking.html), [MySQL 8.4 Release Notes](https://dev.mysql.com/doc/relnotes/mysql/8.4/en/)
- [PostgreSQL 19, Index-Only Scans and Covering Indexes](https://www.postgresql.org/docs/19/indexes-index-only-scans.html), [Multicolumn Indexes](https://www.postgresql.org/docs/19/indexes-multicolumn.html), [Using EXPLAIN](https://www.postgresql.org/docs/19/using-explain.html), [Planner/Optimizer](https://www.postgresql.org/docs/19/planner-optimizer.html), [Heap-Only Tuples](https://www.postgresql.org/docs/19/storage-hot.html)
- [PostgreSQL 18.0 Release Notes](https://www.postgresql.org/docs/release/18.0/)

## English summary

An InnoDB table is a B+tree ordered by primary key, so a secondary index lookup finds the primary key and then searches the clustered index again, while PostgreSQL indexes point into a heap and index-only scans still depend on the visibility map. The optimizer compares the estimated rows an index would fetch with the cost of a full scan, so a wide backfill range can rightly turn into a table scan, and a function or implicit cast on the indexed column stops it from building an index range at all. Reading EXPLAIN ANALYZE comes down to access type and the gap between estimated and actual rows; joins pick index nested loop when the inner table has a usable index and hash join (MySQL 8.0.18+) or merge join (PostgreSQL) otherwise. In my metric batches at a music service, the shared MySQL writer deleted overlapping rows by a unique key before inserting, so rerunning a date did not duplicate rows. Every index is that kind of trade-off: faster lookups for the reads and reruns that need them, paid for on every write.
