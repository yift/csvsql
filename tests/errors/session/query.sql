ROLLBACK TO SAVEPOINT my_savepoint;
---

CREATE TEMP TABLE a(a0 INT);
CREATE TEMP TABLE a(a0 INT);
---
COMMIT;
---
ROLLBACK;
---
START TRANSACTION READ ONLY;
---
BEGIN Deferred TRANSACTION;
---

BEGIN;
BEGIN;
---
