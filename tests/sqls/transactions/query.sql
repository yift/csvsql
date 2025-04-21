DROP TABLE IF EXISTS target.tests.integration.transactions;

CREATE TABLE target.tests.integration.transactions(col TEXT);

INSERT INTO target.tests.integration.transactions VALUES('Before');

SELECT * FROM target.tests.integration.transactions;

START TRANSACTION;

UPDATE target.tests.integration.transactions SET col = 'INSIDE ONE';

SELECT * FROM target.tests.integration.transactions;

UPDATE target.tests.integration.transactions SET col = 'INSIDE TWO';

SELECT * FROM target.tests.integration.transactions;

COMMIT;

SELECT * FROM target.tests.integration.transactions;

UPDATE target.tests.integration.transactions SET col = 'BEFORE SECOND TRANSACTION';
SELECT * FROM target.tests.integration.transactions;

START TRANSACTION;

UPDATE target.tests.integration.transactions SET col = 'IN SECOND TRANSACTION';
SELECT * FROM target.tests.integration.transactions;

ROLLBACK;

SELECT * FROM target.tests.integration.transactions;

DROP TABLE IF EXISTS target.tests.integration.transactions;
