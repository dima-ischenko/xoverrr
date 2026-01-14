
CREATE SCHEMA IF NOT EXISTS test;

SET search_path TO test;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    created_at  DATE NOT NULL,
    amount      NUMERIC(10,2) NOT NULL
);

INSERT INTO orders (id, created_at, amount) VALUES
(1, '2024-01-01', 100.00),
(2, '2024-01-02', 150.00),
(3, '2024-01-03', 200.00),
(4, '2024-01-04', 250.00),
(5, '2024-01-05', 300.00);

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  DATE NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

INSERT INTO customers (id, name, created_at, updated_at) VALUES
(1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
(2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
(3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00');

DROP TABLE IF EXISTS empty_table;
CREATE TABLE empty_table (
    id         INTEGER,
    created_at DATE
);
