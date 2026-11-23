---
title: "커버링 인덱스가 테이블을 건너뛰는 조건과 대가"
date: 2026-11-22 12:30:00 +0900
categories: [Batch & Query, SQL]
tags: [index, covering-index, mysql, innodb, query-plan, pagination, mvcc, concept]
description: "OFFSET이 커질수록 느려지는 backfill 쿼리를 예로, 커버링 인덱스가 테이블 행 조회를 건너뛰는 조건과 인덱스를 키우는 대가를 MySQL InnoDB 기준으로 실측과 함께 다룬다."
---

`status`로 거른 주문을 `id` 순서로 1,000행씩 잘라 읽는 backfill 쿼리는 처음 몇 페이지는 금방 끝난다. `OFFSET`이 40만쯤 되면 한 페이지에 0.5초를 넘기고, 깊어질수록 계속 늘어난다. `EXPLAIN`은 `status` 인덱스를 쓴다고 나온다.

```sql
-- MySQL 8.4, InnoDB. orders(id BIGINT PRIMARY KEY, status VARCHAR(20), updated_at DATETIME, ... , INDEX (status))
SELECT * FROM orders
WHERE status = 'PAID'
ORDER BY id
LIMIT 1000 OFFSET 500000;
```

느린 이유는 건너뛰는 50만 행이다. MySQL은 `OFFSET`만큼 행을 읽어서 버리는데, `SELECT *`라 버릴 행까지 전부 테이블에서 읽는다. 쿼리에 필요한 컬럼이 인덱스에 다 있으면 테이블을 읽지 않아도 되는데, 그런 인덱스를 커버링 인덱스라고 한다. 인덱스가 행까지 가는 길은 [인덱스와 실행 계획 글](/posts/db-indexes-and-query-plans/)에 적었고, 여기서는 커버링이 되는 조건과 그 대가를 MySQL 8.4(LTS, InnoDB) 기준으로 다룬다.

## 핵심 키워드

| 키워드 | 한 줄 뜻 |
|---|---|
| clustered index | InnoDB 테이블 자체. 기본 키 순서로 정렬된 B-tree이고 리프에 행 전체가 있다 |
| 보조 인덱스(secondary index) | 기본 키가 아닌 컬럼으로 만든 인덱스. 리프에 인덱스 컬럼 값과 기본 키만 있다 |
| covering index | 쿼리가 참조하는 컬럼이 모두 들어 있어 테이블 행을 읽지 않고 답하는 인덱스 |
| `Using index` | MySQL `EXPLAIN`의 `Extra` 값. 인덱스만 읽고 행은 읽지 않았다는 표시 |
| ICP(Index Condition Pushdown) | 인덱스에 있는 컬럼 조건을 행을 읽기 전에 먼저 거르는 최적화. `EXPLAIN`에 `Using index condition`으로 나온다 |
| deferred join | 인덱스만으로 기본 키를 먼저 고르고, 고른 키로만 행을 읽는 쿼리 모양 |
| keyset pagination | `OFFSET` 대신 마지막으로 읽은 키 다음부터 읽는 페이지 나누기 |
| MVCC | 행을 고쳐도 옛 버전을 남겨 두고, 트랜잭션마다 볼 수 있는 버전을 골라 읽는 방식 |

## 보조 인덱스로 행을 읽는 두 단계

앞의 표처럼 InnoDB 테이블은 clustered index 자체다. `INDEX (status)` 같은 보조 인덱스의 리프에는 행 위치 대신 그 행의 기본 키 값이 들어 있다.

```text
-- INDEX (status) 리프: (status, id)
('PAID', 4) ('PAID', 8) ('PAID', 12) ...

-- clustered index 리프: (id, status, updated_at, ...)
(4, 'PAID', '2026-01-01 00:06', ...)
(8, 'PAID', '2026-01-01 00:12', ...)
```

그래서 `WHERE status = 'PAID'`로 행을 읽으면 두 단계를 거친다. 보조 인덱스에서 `'PAID'` 항목을 찾아 기본 키 4, 8, 12를 얻고, 키마다 clustered index를 루트부터 내려가 행을 읽는다. 두 번째 단계는 키마다 따로 하는 탐색이라 행 수만큼 비용이 든다. 커버링 인덱스는 이 두 번째 단계를 없앤다.

행 위치 대신 기본 키를 두는 것은 페이지가 나뉘어 행이 옮겨 가도 보조 인덱스를 고치지 않기 위해서다. 그 결과 기본 키가 길면 모든 보조 인덱스가 함께 커진다. 또 `INDEX (status)`는 같은 `status` 안에서 기본 키 순서로 정렬돼 있어 사실상 `(status, id)` 인덱스다. `WHERE status = 'PAID' ORDER BY id`도 정렬 없이 인덱스 순서대로 읽힌다.

## 참조하는 컬럼이 모두 있어야 한다

커버링은 `SELECT` 목록만 보고 정해지지 않는다. `WHERE`, `ORDER BY`, `GROUP BY`, 조인 조건까지 쿼리가 그 테이블에서 쓰는 모든 컬럼이 인덱스에 있어야 한다. 위 예시처럼 보조 인덱스 리프에는 기본 키가 붙어 있으니 기본 키 컬럼은 따로 넣지 않아도 된다.

| 쿼리 (인덱스는 `INDEX (status)`, 기본 키 `id`) | 커버링 | 이유 |
|---|---|---|
| `SELECT id FROM orders WHERE status = 'PAID'` | 됨 | `status`와 `id` 모두 인덱스 리프에 있다 |
| `SELECT COUNT(*) FROM orders WHERE status = 'PAID'` | 됨 | `status` 조건을 인덱스에서 확인하고 세면 된다 |
| `SELECT status, COUNT(*) FROM orders GROUP BY status` | 됨 | 인덱스 전체를 훑지만(`type=index`) 행은 읽지 않는다 |
| `SELECT id, updated_at FROM orders WHERE status = 'PAID'` | 안 됨 | `updated_at`이 인덱스에 없다 |
| `SELECT id FROM orders WHERE status = 'PAID' AND updated_at > ?` | 안 됨 | 조건 컬럼도 행에서 읽어야 한다 |

마지막 두 줄은 `INDEX (status, updated_at)`을 쓰면 커버링이 된다.

## 테스트 테이블

이 글의 `EXPLAIN` 결과와 시간은 로컬 Docker에 띄운 MySQL 8.4.13에서 잰 값이다. `orders`에 300만 행을 넣었고 `status`는 네 값이 25%씩이라 `PAID`는 75만 행이다. 데이터는 476MB이고 버퍼 풀은 기본값 128MB라, 행 읽기가 버퍼 풀 안에서 다 끝나지 않는다. 시간은 같은 쿼리를 세 번 돌린 중앙값이다.

## Using index와 Using index condition

`Extra`의 두 값은 이름이 비슷해서 자주 헷갈린다. 같은 조건에서 `SELECT` 목록만 바꾸면 차이가 보인다.

```sql
-- 인덱스: INDEX (status, updated_at)
EXPLAIN SELECT id, updated_at FROM orders
WHERE status = 'PAID' AND updated_at >= '2026-03-01' AND updated_at < '2026-03-08';
-- Extra: Using where; Using index

EXPLAIN SELECT * FROM orders
WHERE status = 'PAID' AND updated_at >= '2026-03-01' AND updated_at < '2026-03-08';
-- Extra: Using index condition; Using MRR

-- 인덱스: INDEX (status)
EXPLAIN SELECT * FROM orders WHERE status = 'PAID' LIMIT 10;
-- Extra: NULL
```

| `Extra` | 두 번째 단계(행 읽기) | 뜻 |
|---|---|---|
| `Using index` | 없음 | 필요한 컬럼이 다 인덱스에 있다 |
| `Using index condition` | 걸러지고 남은 행만 | 인덱스에 있는 컬럼으로 조건을 먼저 거른 뒤, 남은 행만 읽는다 |
| `NULL` | 찾은 행 전부 | 인덱스로 찾은 행마다 테이블을 읽는다 |

`condition`이 붙으면 행을 여전히 읽는다고 기억하면 된다. 함께 붙는 `Using where`는 스토리지 엔진이 넘긴 값을 서버가 한 번 더 거른다는 뜻이라 행을 읽었는지와는 상관이 없다. `Using MRR`은 읽을 기본 키를 묶음으로 모아 정렬한 뒤 순서대로 읽는다는 표시다. ICP는 행을 읽어야 하는 쿼리에서만 표시되므로 `EXPLAIN`에 `Using index`와 함께 나오지 않는다.

ICP는 인덱스에서 읽을 범위(어디서 시작해 어디서 멈출지)를 정하는 데 못 쓰이는 조건에서 효과를 본다. 위 쿼리는 `updated_at` 조건이 이미 범위를 좁혀서 ICP가 걸러낼 것이 거의 없다. 컬럼에 함수를 씌우면 범위를 정하는 데 못 쓰는데, 이때 차이가 난다.

```sql
-- 인덱스: INDEX (status, updated_at). 결과 60,123행
SELECT * FROM orders WHERE status = 'PAID' AND MONTH(updated_at) = 6;
```

| 설정 | `Extra` | 시간 |
|---|---|---|
| ICP 켬(기본) | `Using index condition` | 0.12초 |
| `SET optimizer_switch = 'index_condition_pushdown=off'` | `Using where` | 1.18초 |

두 경우 모두 인덱스에서 `PAID` 항목 75만 개를 지나간다. ICP를 켜면 인덱스에 있는 `updated_at`으로 6월이 아닌 항목을 먼저 버리고 6만 행만 테이블에서 읽는다. 끄면 75만 행을 모두 읽은 뒤 서버에서 거른다.

## 복합 인덱스는 등호 조건 컬럼을 앞에 둔다

커버링이 된다고 다 같은 인덱스는 아니다. 일주일치 `PAID` 주문의 `id`를 고르는 쿼리를 컬럼 순서만 다른 두 인덱스로 돌려 봤다. 둘 다 `Using where; Using index`라 행은 읽지 않는다. 인덱스 항목을 몇 개 읽었는지는 `FLUSH STATUS` 뒤에 쿼리를 돌리고 `SHOW SESSION STATUS LIKE 'Handler_read_next'`로 봤다.

```sql
SELECT id FROM orders
WHERE status = 'PAID' AND updated_at >= '2026-03-01' AND updated_at < '2026-03-08';
-- 결과 15,588행
```

| 인덱스 | 읽은 인덱스 항목 수(`Handler_read_next`) |
|---|---|
| `INDEX (status, updated_at)` | 15,588 |
| `INDEX (updated_at, status)` | 62,352 |

`(status, updated_at)`은 `PAID` 안에서 날짜 순으로 정렬돼 있어, 일주일치가 인덱스에서 한 덩어리다. `(updated_at, status)`는 날짜가 먼저라 그 일주일의 네 가지 상태를 모두 지나가며 `PAID`만 고른다. 정확히 네 배인 것은 상태가 네 가지라서다. 범위 조건 뒤에 오는 컬럼은 읽을 범위를 좁히지 못하고 거르는 데만 쓰이므로, `=` 조건 컬럼을 앞에, 범위 조건 컬럼을 뒤에 둔다.

## 넓은 범위에서 옵티마이저가 인덱스를 버리는 이유

증분 추출처럼 시간 범위로 읽을 때는 커버링 여부가 계획을 통째로 바꾼다. 같은 `INDEX (status, updated_at)`에서 범위를 6월 이후로 넓혔다. 결과는 42만 8,883행으로 테이블의 약 14%다.

```sql
EXPLAIN SELECT id, updated_at FROM orders WHERE status = 'PAID' AND updated_at >= '2026-06-01';
-- type: range, key: (status, updated_at), Extra: Using where; Using index

EXPLAIN SELECT * FROM orders WHERE status = 'PAID' AND updated_at >= '2026-06-01';
-- type: ALL, key: NULL (풀 스캔)
```

`SELECT *`는 찾은 행마다 clustered index를 다시 내려가야 한다. 옵티마이저는 이 범위를 82만 행쯤으로 추정했고(실제는 약 43만), 그만큼 행마다 탐색하는 것이 테이블을 처음부터 끝까지 읽는 것보다 비싸다고 보고 풀 스캔을 골랐다. 커버링 쿼리는 행을 읽지 않으니 같은 범위에서도 인덱스를 쓴다.

원천 DB에서 증분 추출을 할 때 이 차이를 이용한다. `INDEX (updated_at)`이 있다면 `SELECT id FROM orders WHERE updated_at >= ? AND updated_at < ?`는 인덱스만으로 키를 모은다. 행 전체는 모은 키를 1,000개씩 묶어 `WHERE id IN (...)`으로 읽는다. 인덱스로 키만 먼저 고르고 행은 나중에 읽는 이 모양이 deferred join이다. 원천은 풀 스캔을 하지 않고, 한 번에 읽는 양도 일정하다. 키를 하나씩 따로 조회하면 [N+1 쿼리 글](/posts/n-plus-one-queries/)의 문제가 된다.

## 깊은 OFFSET: deferred join과 keyset

처음 backfill 쿼리도 deferred join으로 고친다. 안쪽 서브쿼리는 `status`와 `id`만 쓰니 `INDEX (status)`로 커버링되고, 앞에서 본 대로 이 인덱스는 `(status, id)` 순서라 정렬도 필요 없다. 바깥은 고른 1,000개 키로만 clustered index를 읽는다.

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

안쪽은 여전히 인덱스 항목 50만 개를 지나간다. 다만 인덱스 리프를 순서대로 읽기만 하니, 키마다 행 전체를 찾아 읽는 50만 번보다 훨씬 싸다. 페이지가 뒤로 갈수록 느려지는 성질 자체를 없애려면 keyset pagination을 쓴다. 마지막으로 읽은 키 다음부터 읽는 방식이다.

```sql
-- 첫 페이지는 id 조건 없이 읽고, 그 페이지의 마지막 id를 다음 :last_id로 넘긴다
SELECT * FROM orders
WHERE status = 'PAID' AND id > :last_id
ORDER BY id
LIMIT 1000;
```

테스트 테이블에서 `PAID` 75만 행을 세 깊이에서 1,000행씩 읽었다.

| 건너뛰는 행 | `OFFSET` 그대로 | deferred join | keyset |
|---|---|---|---|
| 10만 | 96ms | 9.5ms | 1.2ms |
| 40만 | 504ms | 31.9ms | 2.5ms |
| 70만 | 813ms | 54.9ms | 2.1ms |

| 방식 | 사용한 인덱스 | 건너뛰는 것 | 테이블 행 읽기 | 페이지 번호로 이동 |
|---|---|---|---|---|
| `OFFSET` 그대로 | `(status)` | 행(테이블까지 읽음) | 건너뛴 행 + 1,000 | 됨 |
| deferred join | `(status)` | 인덱스 항목만 | 1,000 | 됨 |
| keyset | `PRIMARY` | 없음 | 약 4,000 읽고 1,000 반환 | 안 됨 |

`OFFSET` 그대로는 깊이에 비례해 늘고, deferred join은 그보다 10~16배 빠르지만 역시 깊이를 따라 늘어난다. keyset만 깊이와 상관없이 같다.

keyset에서 옵티마이저는 `INDEX (status)` 대신 기본 키를 `id` 순서로 읽으며 `status`를 거르는 계획을 골랐다. `PAID`가 4행에 1행꼴이라 기본 키 4,000개쯤만 지나가면 1,000행이 차기 때문이다. `PAID`가 드문 값이었다면 기본 키를 따라가며 지나갈 행이 훨씬 많아지므로, `(status, id)` 순서인 `INDEX (status)`로 바로 찾아가는 쪽이 낫다.

keyset은 특정 페이지 번호로 바로 건너뛸 수 없고, 정렬 키가 유일해야 한다. `updated_at`처럼 값이 겹치는 키로 자르면 경계에 걸친 같은 값이 `>`에서는 빠지고 `>=`에서는 두 번 읽힌다. 그래서 `id`를 붙여 `(updated_at, id)` 순서로 자른다. 이때 `WHERE (updated_at, id) > (:t, :id)`로 쓰면 `INDEX (updated_at)`이 있어도 테스트에서는 인덱스를 처음부터 훑는 계획(`type=index`)이 나왔다. 풀어 쓴 `WHERE updated_at > :t OR (updated_at = :t AND id > :id)`는 `type=range`로 바로 찾아간다.

## 컬럼이 다 있어도 테이블에 다녀오는 경우: MVCC

인덱스에 컬럼이 다 있어도 그 행이 지금 내 트랜잭션에 보이는지는 인덱스만으로 모를 때가 있다. MVCC 때문이다. A 트랜잭션이 행을 고치는 동안 B가 같은 행을 읽으면, 데이터베이스는 B를 기다리게 하지 않고 고치기 전 버전을 보여 준다. 그러려면 행마다 누가 고쳤는지 알아야 한다.

InnoDB에서 그 정보는 clustered index 행에만 있다. 행마다 마지막으로 고친 트랜잭션 id(`DB_TRX_ID`)가 붙고, 고치기 전 버전은 undo 로그에 남는다. 트랜잭션은 처음 읽을 때(READ COMMITTED에서는 문장마다) 어느 트랜잭션까지 끝났는지 스냅샷(read view)을 찍고, `DB_TRX_ID`를 이 스냅샷과 비교해 볼 버전을 고른다.

보조 인덱스 항목에는 `DB_TRX_ID`가 없다. 그리고 보조 인덱스는 값을 제자리에서 고치지 않는다. `status`가 바뀌면 옛 항목에 삭제 표시를 하고 새 항목을 넣는다. 그래서 인덱스 항목만 보고는 이 값이 내 스냅샷에서 유효한지 알 수 없다. InnoDB는 이를 페이지 단위로 판단한다. 보조 인덱스 페이지마다 그 페이지를 고친 트랜잭션 중 가장 큰 id(소스 코드의 `PAGE_MAX_TRX_ID`)를 둔다.

| 보조 인덱스 상태 | InnoDB가 하는 일 |
|---|---|
| 페이지의 `PAGE_MAX_TRX_ID`가 내 read view를 만들 때 진행 중이던 트랜잭션 중 가장 오래된 것의 id보다 작고, 항목에 삭제 표시도 없음 | 인덱스 값만 읽는다. 커버링 그대로 |
| `PAGE_MAX_TRX_ID`가 그 id 이상임(이미 끝난 트랜잭션이 고쳤어도 해당), 또는 항목에 삭제 표시가 있음 | clustered index에서 행을 찾아 `DB_TRX_ID`를 보고, 볼 버전이 아니면 undo 로그로 옛 버전을 만든다 |

판단 단위가 페이지라서, 페이지 안의 한 행만 최근에 바뀌어도 그 페이지에서 읽는 항목은 전부 clustered index에 다녀온다. 쓰기가 몰리는 최근 범위를 커버링 인덱스로 읽으면 이 일이 자주 생긴다. 오래 열린 트랜잭션이 있으면 그 트랜잭션이 볼 수도 있는 옛 버전과 삭제 표시 항목을 지우지 못하고, 다른 쿼리들은 옛 버전을 만들려고 더 긴 undo 체인을 거슬러 올라간다.

MVCC와 별개로, `SELECT ... FOR UPDATE`처럼 배타 잠금을 거는 읽기는 보조 인덱스로 찾아도 clustered index 레코드까지 잠가야 해서 커버링이어도 테이블에 다녀온다.

이 확인들은 실행 중에 일어나서 `EXPLAIN`에는 드러나지 않는다. 계획은 여전히 `Using index`이고, 자주 바뀌는 테이블에서는 주로 실행 시간으로 짐작한다. 비교하자면 PostgreSQL은 행을 인덱스와 별개인 heap에 두고, heap 페이지마다 모든 행이 모두에게 보이는지 나타내는 비트를 visibility map에 둔다. 이 비트는 VACUUM이 켜고, 꺼진 페이지는 index-only scan 중에도 heap을 확인하러 간다. 그 횟수가 `EXPLAIN ANALYZE`의 `Heap Fetches`로 보인다. 격리 수준별 read view 동작은 [격리 수준과 MVCC 글](/posts/isolation-levels-and-mvcc/)에 있다.

## 테이블 전체를 청크로 옮기기

조건 없이 테이블 전체를 backfill할 때는 보조 인덱스가 필요 없다. 기본 키 keyset이 clustered index를 처음부터 순서대로 읽는다.

```sql
-- 첫 청크는 :last_id = 0. 1,000행보다 적게 오면 끝
SELECT * FROM orders
WHERE id > :last_id
ORDER BY id
LIMIT 1000;
-- EXPLAIN: type=range, key=PRIMARY
```

청크마다 autocommit으로 끝내고, 대상에 쓴 뒤 마지막 `id`를 저장해 두면 중간에 끊겨도 거기서 이어 읽는다. 루프 전체를 트랜잭션 하나로 묶으면 몇 시간짜리 트랜잭션이 된다. 앞에서 본 대로 그동안 원천은 옛 버전을 지우지 못하고, 원천의 다른 쿼리들까지 긴 undo 체인을 따라 읽게 된다. backfill이 도는 동안 이미 지나간 `id`가 바뀌거나 새 주문이 들어오면 이 루프는 잡지 못하니, 시작 시각 이후 변경은 증분 추출이나 CDC로 따로 받는다.

## 컬럼을 얹을수록 인덱스는 테이블을 닮아 간다

커버링 인덱스는 테이블의 일부를 정렬된 사본으로 하나 더 들고 있는 것이다. 사본이 넓어질수록 대가가 커지니, 자주 돌고 느린 쿼리 몇 개에만 맞춘다.

| 대가 | 내용 |
|---|---|
| 쓰기 | 얹은 컬럼이 바뀔 때마다 그 인덱스에도 삭제 표시와 새 항목 삽입이 생긴다 |
| 크기 | 리프가 넓어지면 같은 버퍼 풀에 들어가는 항목 수가 줄어, 다른 쿼리가 쓰던 페이지까지 밀어낸다 |
| 한계 | InnoDB는 한 인덱스에 16개 컬럼, `DYNAMIC` 행 형식과 기본 16KB 페이지에서 키 3,072바이트까지다 |

앞부분이 겹치는 인덱스를 정리할 때도 기본 키가 붙는다는 점을 같이 본다. `INDEX (status, updated_at)`이 있으면 `WHERE status = ?`는 이 인덱스로도 찾을 수 있다. 하지만 `INDEX (status)`는 `(status, id)` 순서라 `WHERE status = ? ORDER BY id`를 정렬 없이 처리하고, `(status, updated_at)`은 그 순서를 주지 못한다. 위의 deferred join이 그런 쿼리라, 이런 쿼리가 있으면 `INDEX (status)`는 남겨 둔다.

## 묻고 답하기

Q. `SELECT *` 쿼리도 커버링 인덱스로 만들 수 있나?

모든 컬럼을 인덱스에 넣으면 되지만, 그러면 테이블을 통째로 한 벌 더 들고 있는 셈이라 쓰기와 크기 대가가 거의 두 배가 된다. 컬럼이 많은 테이블은 16개 컬럼 한도에도 걸린다. `SELECT *`가 필요한 쿼리는 커버링 인덱스로 키만 고르고 행은 고른 키로만 읽는 deferred join이 현실적이다.

Q. 조건 없는 `COUNT(*)`에도 인덱스가 도움이 되나?

조건 없는 `COUNT(*)`는 InnoDB가 가장 작은 보조 인덱스를 훑어 센다. clustered index를 다 읽는 것보다 싸지만 항목을 전부 읽는 것은 같다. MVCC에서는 트랜잭션마다 보이는 행이 달라서, InnoDB는 정확한 행 수를 따로 저장해 두지 않는다.

## 참고 자료

- [MySQL 8.4, Clustered and Secondary Indexes](https://dev.mysql.com/doc/refman/8.4/en/innodb-index-types.html), [Use of Index Extensions](https://dev.mysql.com/doc/refman/8.4/en/index-extensions.html), [Index Condition Pushdown Optimization](https://dev.mysql.com/doc/refman/8.4/en/index-condition-pushdown-optimization.html), [Multi-Range Read Optimization](https://dev.mysql.com/doc/refman/8.4/en/mrr-optimization.html), [EXPLAIN Output Format](https://dev.mysql.com/doc/refman/8.4/en/explain-output.html), [Row Constructor Expression Optimization](https://dev.mysql.com/doc/refman/8.4/en/row-constructor-optimization.html)
- [MySQL 8.4, InnoDB Multi-Versioning](https://dev.mysql.com/doc/refman/8.4/en/innodb-multi-versioning.html), [Consistent Nonlocking Reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html), [Locks Set by Different SQL Statements](https://dev.mysql.com/doc/refman/8.4/en/innodb-locks-set.html), [InnoDB Limits](https://dev.mysql.com/doc/refman/8.4/en/innodb-limits.html), [LIMIT Query Optimization](https://dev.mysql.com/doc/refman/8.4/en/limit-optimization.html)
- [PostgreSQL 19, Index-Only Scans and Covering Indexes](https://www.postgresql.org/docs/19/indexes-index-only-scans.html), [Routine Vacuuming](https://www.postgresql.org/docs/19/routine-vacuuming.html)

## English summary

A secondary index lookup in InnoDB has two steps: find the primary key in the index, then search the clustered index for the row. A covering index removes the second step, so every column the query touches has to be in the index, and InnoDB adds the primary key for free. In EXPLAIN, `Using index` means no row reads, while `Using index condition` filters on index columns first and still reads the rows that pass. On a 3-million-row test table, turning ICP off made one query about ten times slower. Putting the equality column before the range column cut index reads from 62,352 to 15,588. Reading a page 700,000 rows deep took 813ms with OFFSET, 55ms with a deferred join and 2ms with keyset pagination. A covered read can still visit the clustered index, because only the clustered index row holds the transaction id that MVCC needs. I keep covering indexes narrow and few, because every extra column is paid for on writes and in the buffer pool.
