CREATE TEMPORARY TABLE test_alter(a INT);

INSERT INTO test_alter VALUES(1), (2), (3);

SELECT * FROM test_alter;

ALTER TABLE test_alter 
    ADD COLUMN b INT,
    ADD COLUMN c INT,
    ADD COLUMN d INT;

SELECT * FROM test_alter;

ALTER TABLE test_alter 
    DROP COLUMN a,
    RENAME COLUMN b to b2,
    ADD COLUMN e INT;

SELECT * FROM test_alter;


ALTER TABLE IF EXISTS test_alter 
    ADD COLUMN one_more INT;

SELECT * FROM test_alter;

