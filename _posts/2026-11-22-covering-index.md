---
title: "커버링 인덱스가 테이블을 건너뛰는 조건과 대가"
date: 2026-11-22 12:30:00 +0900
categories: [Batch & Query, SQL]
tags: [index, covering-index, mysql, innodb, postgresql, query-plan, pagination, concept]
---

`status`로 거른 주문을 `id` 순서로 1,000행씩 잘라 읽는 backfill 쿼리는 처음 몇 페이지는 금방 끝난다. `OFFSET`이 50만을 넘으면 한 페이지에 몇 초씩 걸린다. `EXPLAIN`은 `status` 인덱스를 쓴다고 나온다.

```sql
-- MySQL 8.4, InnoDB. orders(id BIGINT PRIMARY KEY, status VARCHAR(20), updated_at DATETIME, ... , INDEX (status))
SELECT * FROM orders
WHERE status = 'PAID'
ORDER BY id
LIMIT 1000 OFFSET 500000;
```

느린 이유는 건너뛰는 50만 행이다. MySQL은 `OFFSET`만큼의 행을 읽어서 버리는데, `SELECT *`이라 버릴 행마다 보조 인덱스에서 기본 키를 얻고 clustered index를 한 번 더 내려가 행 전체를 읽는다. 쿼리에 필요한 컬럼이 인덱스에 다 있으면 이 두 번째 탐색이 없어진다. 이것이 커버링 인덱스다. 인덱스가 행까지 가는 길과 실행 계획 읽는 법은 [인덱스와 실행 계획 글](/posts/db-indexes-and-query-plans/)에 적었고, 여기서는 커버링이 되는 조건과 그 대가를 다룬다.

버전은 MySQL 8.4(LTS, InnoDB), PostgreSQL 19 기준이다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| covering index | 쿼리가 참조하는 컬럼이 모두 들어 있어 테이블 행을 읽지 않고 답하는 인덱스 |
| `Using index` | MySQL `EXPLAIN`의 `Extra` 값. 인덱스만 읽고 행은 읽지 않았다는 표시 |
| `Using index condition` | 인덱스 컬럼 조건을 스토리지 엔진에서 먼저 거르는 ICP. 행은 여전히 읽는다 |
| deferred join | 인덱스만으로 기본 키를 먼저 고르고, 고른 키로만 행을 읽는 쿼리 모양 |
| keyset pagination | `OFFSET` 대신 마지막으로 읽은 키 다음부터 읽는 페이지 나누기 |
| `INCLUDE` | PostgreSQL에서 검색 키가 아닌 컬럼을 인덱스 리프에 얹는 문법 |

## 참조하는 컬럼이 모두 있어야 한다

커버링은 `SELECT` 목록만 보고 정해지지 않는다. `WHERE`, `ORDER BY`, `GROUP BY`, 조인 조건까지 쿼리가 그 테이블에서 쓰는 모든 컬럼이 인덱스에 있어야 한다. InnoDB 보조 인덱스에는 기본 키가 붙어 있으니 기본 키 컬럼은 따로 넣지 않아도 된다.

| 쿼리 (인덱스는 `INDEX (status)`, 기본 키 `id`) | 커버링 | 이유 |
|---|---|---|
| `SELECT id FROM orders WHERE status = 'PAID'` | 됨 | `status`와 `id` 모두 인덱스에 있다 |
| `SELECT COUNT(*) FROM orders WHERE status = 'PAID'` | 됨 | `status` 조건을 인덱스에서 확인하고 세면 된다 |
| `SELECT id, updated_at FROM orders WHERE status = 'PAID'` | 안 됨 | `updated_at`이 인덱스에 없다 |
| `SELECT id FROM orders WHERE status = 'PAID' AND updated_at > ?` | 안 됨 | 조건 컬럼도 행에서 읽어야 한다 |

마지막 두 줄은 `INDEX (status, updated_at)`으로 바꾸면 커버링이 된다.

`Extra`의 두 값은 이름이 비슷해서 자주 헷갈린다. `Using index`는 행을 읽지 않았다는 뜻이다. `Using index condition`은 인덱스에 있는 컬럼으로 먼저 걸러 행을 읽는 횟수를 줄였다는 뜻이고, 거른 뒤 남은 행은 읽는다. 커버링이면 행을 읽을 일이 없어 ICP를 쓰지 않으므로 두 값은 함께 나오지 않는다.

## OFFSET 페이지를 인덱스 안에서 고르기

앞의 backfill 쿼리는 행을 고르는 일과 행을 읽는 일을 나누면 빨라진다. 안쪽 서브쿼리는 `status`와 `id`만 쓰니 `INDEX (status)`로 커버링되고, 바깥은 고른 1,000개 키로만 clustered index를 읽는다.

```sql
SELECT o.*
FROM orders o
JOIN (
  SELECT id FROM orders
  WHERE status = 'PAID'
  ORDER BY id
  LIMIT 1000 OFFSET 500000
) page ON page.id = o.id
ORDER BY o.id;
```

안쪽은 여전히 인덱스 항목 50만 개를 지나간다. 다만 그 50만 번은 인덱스 리프를 순서대로 읽는 일이라, 흩어진 clustered index 탐색 50만 번보다 훨씬 싸다. 페이지가 뒤로 갈수록 느려지는 성질 자체를 없애려면 마지막으로 읽은 키에서 이어 읽는다.

```sql
SELECT * FROM orders
WHERE status = 'PAID' AND id > :last_id
ORDER BY id
LIMIT 1000;
```

InnoDB에서 `INDEX (status)`는 사실상 `(status, id)`라 `status = 'PAID' AND id > :last_id`가 그대로 인덱스 구간이 된다. 이 keyset 방식은 페이지마다 1,000개만 읽는다. 대신 특정 페이지 번호로 바로 건너뛸 수 없고, 정렬 키가 유일해야 한다. backfill처럼 처음부터 끝까지 순서대로 훑는 작업에는 이 제약이 문제가 되지 않는다.

증분 추출에도 같은 분리가 통한다. `SELECT id FROM orders WHERE updated_at >= ? AND updated_at < ?`는 `INDEX (updated_at)`만으로 끝나 원천 부담이 작다. 행 전체는 모은 키를 묶음 단위로 끊어 읽으니, 넓은 시간 구간을 한 번에 읽다가 옵티마이저가 풀 스캔으로 넘어가는 일도 피한다. 키를 하나씩 따로 조회하면 [N+1 쿼리 글](/posts/n-plus-one-queries/)의 문제가 된다.

## PostgreSQL은 INCLUDE로 컬럼을 얹는다

PostgreSQL에서는 검색에 쓰지 않는 컬럼을 `INCLUDE`로 리프에만 얹는다.

```sql
-- PostgreSQL 19
CREATE UNIQUE INDEX orders_order_no_idx ON orders (order_no) INCLUDE (status, amount);
```

`INCLUDE` 컬럼은 검색 조건으로 범위를 좁히는 데 쓰이지 않고, 유일성도 `order_no`에만 걸린다. 키에 `(order_no, status, amount)`를 다 넣으면 유일성 의미가 바뀐다. 유일 제약은 `order_no`에 그대로 두고 `status`, `amount`까지 인덱스에서 읽고 싶을 때 이 문법이 필요하다. B-tree, GiST, SP-GiST가 지원하고, 키와 `INCLUDE`를 합쳐 32개 컬럼까지다.

`INCLUDE`는 표현식 인덱스에서도 쓸모가 있다. `SELECT lower(email) FROM users WHERE lower(email) = ?`는 인덱스에 `lower(email)`이 있어도 플래너가 원본 `email`을 요구해 index-only scan을 고르지 않을 수 있다. `CREATE INDEX ON users (lower(email)) INCLUDE (email)`로 원본도 얹으면 된다.

## 컬럼이 다 있어도 테이블에 다녀오는 때

인덱스에 컬럼이 다 있어도 행이 지금 보이는지는 인덱스만으로 모를 때가 있다. PostgreSQL은 visibility map 비트가 꺼진 heap 페이지를 heap에서 다시 확인하고, 그 횟수가 `EXPLAIN ANALYZE`의 `Heap Fetches`로 보인다. 읽는 heap 페이지 대부분의 비트가 켜져 있어야 index-only scan이 이득이고, 쓰기가 몰리는 테이블에서는 `Index Only Scan`이 찍혀도 `Heap Fetches`가 반환 행 수와 비슷하게 나오기도 한다.

InnoDB는 보조 인덱스 페이지마다 그 페이지를 고친 트랜잭션 중 가장 큰 id(소스 코드의 `PAGE_MAX_TRX_ID`)를 둔다. 이 값이 지금 read view가 확실히 볼 수 있는 범위를 넘거나 레코드가 삭제 표시돼 있으면, 커버링을 포기하고 clustered index에서 행을 찾는다. 그 행의 `DB_TRX_ID`로 읽어야 할 버전인지 보고, 아니면 undo 로그에서 옛 버전을 만든다. 오래 열린 트랜잭션이 이 과정을 무겁게 만드는 이유는 [격리 수준과 MVCC 글](/posts/isolation-levels-and-mvcc/)에 있다.

PostgreSQL에서는 `Heap Fetches`로 드러나지만, MySQL은 `Using index`가 찍힌 채로 clustered index를 다녀와도 계획에 흔적이 없어 자주 바뀌는 테이블에서는 실행 시간으로만 짐작할 수 있다.

## 컬럼을 얹을수록 인덱스는 테이블을 닮아 간다

커버링 인덱스는 테이블의 일부를 정렬된 사본으로 하나 더 들고 있는 것이다. 사본이 넓어질수록 대가가 커진다.

| 대가 | 내용 |
|---|---|
| 쓰기 | 얹은 컬럼이 바뀌면 그 인덱스도 고친다. PostgreSQL에서는 인덱스 컬럼이 바뀌는 UPDATE가 HOT update가 되지 못한다 |
| 크기 | 리프가 넓어지면 같은 메모리에 들어가는 항목 수가 줄고, 검색도 느려질 수 있다 |
| 기능 | PostgreSQL B-tree는 `INCLUDE`가 있는 인덱스에서 중복 제거(deduplication)를 쓰지 않는다 |
| 한계 | InnoDB는 한 인덱스에 16개 컬럼, `DYNAMIC` 행 형식에서 키 3,072바이트까지다 |

PostgreSQL 문서도 키가 아닌 컬럼, 특히 넓은 컬럼은 아껴 얹으라고 권한다. 그래서 커버링 인덱스는 자주 돌고 느린 쿼리 몇 개에만 맞추고, 만든 뒤 `Using index`나 낮은 `Heap Fetches`가 실제로 나오는지 확인한다.

## 묻고 답하기

Q. 기본 키로 조회하면 커버링이 필요 없나?

InnoDB에서는 그렇다. clustered index의 리프가 곧 행이라, 기본 키 조회는 처음부터 행을 읽는다. 커버링이 의미 있는 것은 보조 인덱스로 찾는 쿼리다. PostgreSQL은 기본 키도 heap을 가리키는 인덱스라, 키 컬럼만 읽거나 `PRIMARY KEY (id) INCLUDE (...)`로 컬럼을 얹었을 때 index-only scan이 될 수 있다.

Q. 조건 없는 `COUNT(*)`에도 인덱스가 도움이 되나?

조건 없는 `COUNT(*)`는 InnoDB가 가장 작은 보조 인덱스를 훑어 센다. clustered index를 다 읽는 것보다 싸지만 항목을 전부 읽는 것은 같다. MVCC에서는 트랜잭션마다 보이는 행이 달라서 InnoDB는 정확한 행 수를 따로 저장하지 않는다.

## 참고 자료

- [MySQL 8.4, Clustered and Secondary Indexes](https://dev.mysql.com/doc/refman/8.4/en/innodb-index-types.html), [Use of Index Extensions](https://dev.mysql.com/doc/refman/8.4/en/index-extensions.html), [Index Condition Pushdown Optimization](https://dev.mysql.com/doc/refman/8.4/en/index-condition-pushdown-optimization.html), [EXPLAIN Output Format](https://dev.mysql.com/doc/refman/8.4/en/explain-output.html)
- [MySQL 8.4, InnoDB Multi-Versioning](https://dev.mysql.com/doc/refman/8.4/en/innodb-multi-versioning.html), [InnoDB Limits](https://dev.mysql.com/doc/refman/8.4/en/innodb-limits.html), [LIMIT Query Optimization](https://dev.mysql.com/doc/refman/8.4/en/limit-optimization.html)
- [PostgreSQL 19, Index-Only Scans and Covering Indexes](https://www.postgresql.org/docs/19/indexes-index-only-scans.html), [CREATE INDEX](https://www.postgresql.org/docs/19/sql-createindex.html), [B-Tree Deduplication](https://www.postgresql.org/docs/19/btree.html), [Heap-Only Tuples](https://www.postgresql.org/docs/19/storage-hot.html)

## English summary

A covering index answers a query from the index alone, so InnoDB skips the second search into the clustered index. Every column the query touches has to be in the index, including WHERE, ORDER BY and join columns, and InnoDB adds the primary key for free. `Using index` in EXPLAIN means no row reads, while `Using index condition` only filters earlier and still reads rows. For deep OFFSET pages, pick the keys in a covered subquery and join back, or better, continue from the last key. PostgreSQL adds payload columns with INCLUDE, but an index-only scan still checks the visibility map, and InnoDB goes back to the clustered index when a recent transaction changed the index page. Covering indexes are best kept narrow, because every extra column is paid for on writes and in memory.
