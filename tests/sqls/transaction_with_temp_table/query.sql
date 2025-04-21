START TRANSACTION;
    CREATE TEMPORARY TABLE table_two(col TEXT);
    INSERT INTO table_two VALUES('CREATED ONE');
    SELECT * FROM table_two;
COMMIT;

SELECT * FROM table_two;


CREATE TEMPORARY TABLE table_three(col TEXT);
INSERT INTO table_three VALUES('CREATED three');
SELECT * FROM table_three;

START TRANSACTION;
    UPDATE table_two SET col = 'CHANGED one';
    UPDATE table_three SET col = 'CHANGED two';
    SELECT * FROM table_three;
COMMIT;

SELECT * FROM table_two;
SELECT * FROM table_three;

START TRANSACTION;
    UPDATE table_two SET col = 'CHANGED three';
    SELECT * FROM table_two;
    CREATE TEMPORARY TABLE table_four(col TEXT);
    INSERT INTO table_four VALUES('CREATED four');
ROLLBACK;

SELECT * FROM table_two;
