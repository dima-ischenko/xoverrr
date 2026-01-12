-- ClickHouse initialization script for integration tests

DROP TABLE IF EXISTS default.orders;

CREATE TABLE default.orders (
    id          UInt32,
    created_at  Date,
    amount      Float64
)
ENGINE = MergeTree()
ORDER BY id;


INSERT INTO default.orders(id, created_at, amount)
select 1, toDate('2024-01-01'), 100 union all
select 2, toDate('2024-01-02'), 150 union all
select 3, toDate('2024-01-03'), 200 union all
select 4, toDate('2024-01-04'), 250 union all
select 5, toDate('2024-01-05'), 300;


DROP TABLE IF EXISTS default.empty_table;

CREATE TABLE default.empty_table (
    id UInt32,
    created_at Date
)
ENGINE = MergeTree()
ORDER BY id;
