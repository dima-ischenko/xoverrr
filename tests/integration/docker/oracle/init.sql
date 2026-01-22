--

ALTER SESSION SET CONTAINER = test_db;

CREATE USER test IDENTIFIED BY test_pass;
GRANT CONNECT, RESOURCE TO test;
ALTER USER test QUOTA UNLIMITED ON system;
ALTER SESSION set current_schema = test;



CREATE TABLE imalive (
    id          NUMBER
);

insert into imalive(id)
select 1 from dual;

commit;