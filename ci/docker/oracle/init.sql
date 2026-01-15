--

ALTER SESSION SET CONTAINER = test_db;

CREATE USER test IDENTIFIED BY test_pass;
GRANT CONNECT, RESOURCE TO test;
ALTER USER test QUOTA UNLIMITED ON system;
ALTER SESSION set current_schema = test;



CREATE TABLE orders (
    id          NUMBER PRIMARY KEY,
    created_at  DATE NOT NULL,
    amount      NUMBER(10,2) NOT NULL
);

INSERT INTO orders (id, created_at, amount) VALUES (1, DATE '2024-01-01', 100);
INSERT INTO orders (id, created_at, amount) VALUES (2, DATE '2024-01-02', 150);
INSERT INTO orders (id, created_at, amount) VALUES (3, DATE '2024-01-03', 200);
INSERT INTO orders (id, created_at, amount) VALUES (4, DATE '2024-01-04', 250);
INSERT INTO orders (id, created_at, amount) VALUES (5, DATE '2024-01-05', 300);


CREATE TABLE customers (
    id          NUMBER PRIMARY KEY,
    name        VARCHAR2(100),
    created_at  DATE,
    updated_at  DATE
);

INSERT INTO customers VALUES (1, 'Alice',   DATE '2024-01-01', DATE '2024-01-01');
INSERT INTO customers VALUES (2, 'Bob',     DATE '2024-01-02', DATE '2024-01-02');
INSERT INTO customers VALUES (3, 'Charlie', DATE '2024-01-03', DATE '2024-01-03');


CREATE TABLE empty_table (
    id          NUMBER PRIMARY KEY,
    created_at  DATE
);