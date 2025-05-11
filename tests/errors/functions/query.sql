SELECT COUNT(1, 2) FROM tests.data.sales;
---
SELECT MID('test', 2, 3, 4) FROM tests.data.sales;
---
SELECT LEFT('test', 2, 3, 4) FROM tests.data.sales;
---
SELECT LTRIM() FROM tests.data.sales;
---
SELECT PI(44) FROM tests.data.sales;
---
SELECT COUNT(*) WITHIN GROUP (ORDER BY Name ASC) AS Departments
FROM tests.data.sales
GROUP BY id;
---
SELECT id,
       MIN(price) OVER (),
       MAX(price) OVER ()
FROM tests.data.sales;
---
SELECT COUNT()
FROM tests.data.sales;
---
SELECT COUNT(DISTINCT *)
FROM tests.data.sales;
---
SELECT LTRIM(DISTINCT 'test') FROM tests.data.sales;
---
