USE test;

CREATE TABLE imalive (
    id UInt32
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO imalive(id)
select 1;