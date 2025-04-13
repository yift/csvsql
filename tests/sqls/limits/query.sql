SELECT * FROM tests.data.numbers limit 0;
SELECT * FROM tests.data.numbers limit 4;
SELECT * FROM tests.data.numbers limit 20;
SELECT * FROM tests.data.numbers OFFSET 1 limit 2;
SELECT * FROM tests.data.numbers OFFSET 5 limit 2;
SELECT * FROM tests.data.numbers OFFSET 0 limit 2;
SELECT * FROM tests.data.numbers OFFSET 20 limit 2;
SELECT * FROM tests.data.numbers OFFSET 20 limit 20;