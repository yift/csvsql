CREATE TABLE test_one(col TEXT);
---


CREATE GLOBAL TABLE tab(col TEXT) ;
---


CREATE OR REPLACE TABLE tab(col TEXT) ;
---


CREATE TRANSIENT TABLE tab(col TEXT) ;
---



CREATE TABLE test_one(
    col TEXT,
    CONSTRAINT uq UNIQUE (col)
);
---



CREATE TABLE test_one(col TEXT) ON COMMIT DROP;
---


CREATE TABLE  test_one ON CLUSTER cluster (col TEXT);
---



CREATE TABLE test_one(
    col TEXT,
    PRIMARY KEY (col)
);
---

CREATE TEMPORARY TABLE test_one(col TEXT);
CREATE TEMPORARY TABLE test_one(col TEXT);
---

CREATE TEMPORARY TABLE tests.data.artists(col TEXT);
---
