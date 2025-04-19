CREATE TEMPORARY TABLE sales AS SELECT id, "customer id" FROM tests.data.sales WHERE "delivered at" IS NOT NULL;
SELECT * FROM sales;

CREATE TEMPORARY TABLE customers CLONE tests.data.customers;
SELECT * FROM customers;

CREATE TEMPORARY TABLE artist LIKE tests.data.artists;
SELECT * FROM artist;

SELECT company, COUNT(*) AS count FROM customers, sales WHERE customers.id = sales."customer id" GROUP BY  company ORDER BY company;
