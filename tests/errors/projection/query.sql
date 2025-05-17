SELECT sales.* FROM tests.data.sales;
---
SELECT id ILIKE 'hello' FROM tests.data.sales;
---
SELECT id FROM tests.data.sales, tests.data.customers;
---
SELECT EXTRACT(quarter FROM dt) AS should_be_empty_one, EXTRACT(quarter FROM ts) AS should_be_empty_two FROM tests.data.dates ORDER BY amount;
---
