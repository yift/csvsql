ALTER TABLE no.such.table
    ADD COLUMN b INT;

ALTER TABLE tests.data.dir
    ADD COLUMN b INT;

ALTER TABLE tests.data.dates
    ADD COLUMN b JSONB;

ALTER TABLE tests.data.dates
    ADD PRIMARY KEY (date_1);

ALTER TABLE tests.data.dates
    ADD COLUMN aa INT;

ALTER TABLE tests.data.dates
    ADD COLUMN ts INT;


ALTER TABLE tests.data.dates
    DROP COLUMN sgfds;

ALTER TABLE tests.data.dates
    DROP COLUMN ts RESTRICT;