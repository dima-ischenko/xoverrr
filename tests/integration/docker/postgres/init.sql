
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


DROP TABLE IF EXISTS empty_table;
CREATE TABLE empty_table (
    id         INTEGER,
    created_at DATE
);
