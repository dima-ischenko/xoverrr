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


CREATE TABLE empty_table (
    id          NUMBER PRIMARY KEY,
    created_at  DATE
);