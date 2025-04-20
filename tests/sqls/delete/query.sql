CREATE TEMPORARY TABLE test_delete CLONE tests.data.sales;

SELECT * FROM test_delete;

DELETE FROM test_delete WHERE price > 100;

SELECT * FROM test_delete;

DELETE FROM test_delete;

SELECT * FROM test_delete;

