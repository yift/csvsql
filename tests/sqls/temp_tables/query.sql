CREATE TEMPORARY TABLE my.table(a INT, b TEXT, c BIGINT);

INSERT INTO my.table VALUES(1, 'text 1', 100);
INSERT INTO my.table VALUES(2, 'text 2', 200);
INSERT INTO my.table VALUES(3, 'text 3', 300);
INSERT INTO my.table VALUES(4, 'text 4', 400);
INSERT INTO my.table VALUES(5, 'text 5', 500);
INSERT INTO my.table VALUES(6, 'text 6', 600);
INSERT INTO my.table VALUES(7, 'text 7', 700);

SELECT a + c, b FROM my.table;

DROP TABLE my.table;
