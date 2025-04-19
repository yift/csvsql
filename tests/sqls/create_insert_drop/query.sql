DROP TABLE IF EXISTS target.tests.integration.create_insert_drop;
CREATE TABLE target.tests.integration.create_insert_drop (col_one INT, col_two TEXT);

INSERT INTO target.tests.integration.create_insert_drop VALUES(1, 'a'), (2, 'b');

INSERT INTO target.tests.integration.create_insert_drop(col_one, col_two) SELECT id, name from tests.data.customers;

SELECT col_one, col_two FROM target.tests.integration.create_insert_drop;

DROP TABLE IF EXISTS target.tests.integration.create_insert_drop;
