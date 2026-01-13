-- Oracle XE initialization script for integration tests
-- Uses a dedicated schema to mimic real-life source systems
/*
ALTER PLUGGABLE DATABASE XEPDB1 OPEN;
ALTER PLUGGABLE DATABASE XEPDB1 SAVE STATE;docker exec -it oracle_xe sqlplus sys/test@XEPDB1 as sysdba

ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD';

CREATE USER SRC IDENTIFIED BY test;
GRANT CONNECT, RESOURCE TO SRC;

ALTER USER SRC QUOTA UNLIMITED ON USERS;

-- Orders table
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE SRC.orders';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE SRC.orders (
    id          NUMBER PRIMARY KEY,
    created_at  DATE NOT NULL,
    amount      NUMBER(10,2) NOT NULL
);

INSERT INTO SRC.orders (id, created_at, amount) VALUES (1, DATE '2024-01-01', 100);
INSERT INTO SRC.orders (id, created_at, amount) VALUES (2, DATE '2024-01-02', 150);
INSERT INTO SRC.orders (id, created_at, amount) VALUES (3, DATE '2024-01-03', 200);
INSERT INTO SRC.orders (id, created_at, amount) VALUES (4, DATE '2024-01-04', 250);
INSERT INTO SRC.orders (id, created_at, amount) VALUES (5, DATE '2024-01-05', 300);

COMMIT;


-- Customers table
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE SRC.customers';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/
*/

ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE TABLE customers (
    id          NUMBER PRIMARY KEY,
    name        VARCHAR2(100),
    created_at  DATE,
    updated_at  DATE
);

-- One intentional mismatch vs Postgres (name for id=2)
INSERT INTO customers VALUES (1, 'Alice',   DATE '2024-01-01', DATE '2024-01-01');
INSERT INTO customers VALUES (2, 'Bob',     DATE '2024-01-02', DATE '2024-01-02');
INSERT INTO customers VALUES (3, 'Charlie', DATE '2024-01-03', DATE '2024-01-03');


COMMIT;

/*
-- Empty table
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE SRC.empty_table';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE SRC.empty_table (
    id          NUMBER,
    created_at  DATE
);
*/