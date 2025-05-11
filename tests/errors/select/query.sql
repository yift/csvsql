SELECT * FROM tests.data.customers FETCH FIRST 20 ROWS ONLY;
---

SELECT * FROM tests.data.customers FOR UPDATE;
---
SELECT * FROM tests.data.customers FOR JSON AUTO;
---
WITH employee AS (SELECT * FROM Employees)
SELECT * FROM employee WHERE ID < 20
UNION ALL
SELECT * FROM employee WHERE Sex = 'M'
---
SELECT column_name(s) FROM table1
UNION
SELECT column_name(s) FROM table2;
---

SELECT DISTINCT Country FROM Customers;
---
SELECT TOP 20 Country FROM Customers;
---
SELECT * INTO CustomersBackup2017
FROM Customers;
---
SELECT * FROM person
    LATERAL VIEW EXPLODE(ARRAY(30, 60)) tableName AS c_age
---
SELECT age, name FROM person CLUSTER BY age;
---
SELECT age, name FROM person DISTRIBUTE BY age;
---
SELECT * FROM table_name SORT BY column_name;
---
SELECT SalesOrderID AS OrderNumber,
    ProductID,
    OrderQty AS Qty,
    SUM(OrderQty) OVER win AS Total,
    AVG(OrderQty) OVER (win PARTITION BY SalesOrderID) AS Avg,
    COUNT(OrderQty) OVER (
        win ROWS BETWEEN UNBOUNDED PRECEDING
            AND 1 FOLLOWING
        ) AS Count
FROM Sales.SalesOrderDetail
WHERE SalesOrderID IN (43659, 43664)
    AND ProductID LIKE '71%'
WINDOW win AS
    (
        ORDER BY SalesOrderID, ProductID
    );
---
SELECT c2, SUM(c3) OVER (PARTITION BY c2) as r
  FROM t1
  WHERE c3 < 4
  GROUP BY c2, c3
  HAVING SUM(c1) > 3
  QUALIFY r IN (
    SELECT MIN(c1)
      FROM test
      GROUP BY c2
      HAVING MIN(c1) > 3);
---
SELECT COUNT(*)
FROM tests.data.artists
GROUP BY ALL;
---
SELECT *
FROM foo (ARGUMENT);
---
SELECT emp_id, first_name, last_name FROM employees WITH (FORCESEEK) WHERE emp_id = 123456;
---
SELECT 
    c.category_name,
    p.title,
    p.views
FROM 
    tests.data.dates AS c
LEFT JOIN LATERAL (
    SELECT 
        title,
        views
    FROM 
        tests.data.sales
    WHERE 
        category_id = c.id
    ORDER BY 
        views DESC
    LIMIT 2
) AS p ON true
ORDER BY 
    c.category_name ASC, p.views DESC;
---
SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t (num,letter);
---
SELECT * FROM TABLE(a0);
---
