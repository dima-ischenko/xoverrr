CREATE SCHEMA IF NOT EXISTS public;

DROP TABLE IF EXISTS public.orders;
CREATE TABLE public.orders (
    id          INTEGER PRIMARY KEY,
    created_at  DATE NOT NULL,
    amount      NUMERIC(10,2) NOT NULL
);

INSERT INTO public.orders (id, created_at, amount) VALUES
(1, '2024-01-01', 100.00),
(2, '2024-01-02', 150.00),
(3, '2024-01-03', 200.00),
(4, '2024-01-04', 250.00),
(5, '2024-01-05', 300.00);


DROP TABLE IF EXISTS public.customers;
CREATE TABLE public.customers (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  DATE NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

-- One intentional mismatch vs Oracle (name for id=2)
INSERT INTO public.customers (id, name, created_at, updated_at) VALUES
(1, 'Alice',   '2024-01-01', '2024-01-01 10:00:00'),
(2, 'Robert',  '2024-01-02', '2024-01-02 11:00:00'),
(3, 'Charlie', '2024-01-03', '2024-01-03 12:00:00');


DROP TABLE IF EXISTS public.empty_table;
CREATE TABLE public.empty_table (
    id         INTEGER,
    created_at DATE
);
