
CREATE SCHEMA IF NOT EXISTS test;

SET search_path TO test;

ALTER USER test_user SET SEARCH_PATH TO test, public;


CREATE TABLE imalive (
    id         INTEGER
);


INSERT INTO imalive (id) 
select 1;

commit;