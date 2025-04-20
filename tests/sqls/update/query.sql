CREATE TEMPORARY TABLE test_update CLONE tests.data.sales;

SELECT * FROM test_update;

UPDATE test_update SET price = price * 3;

SELECT * FROM test_update;

UPDATE test_update SET price = price - 100 WHERE price > 200;

SELECT * FROM test_update;

