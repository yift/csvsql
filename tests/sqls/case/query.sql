SELECT "delivery cost",
       CASE
         WHEN "delivery cost" < 0.5 THEN 1
         WHEN "delivery cost" < 1 THEN 2
         WHEN "delivery cost" < 10 THEN 3
         ELSE 4
       END AS "one",
       CASE
         WHEN "delivery cost" < 0.5 THEN "delivery cost"
         WHEN "delivery cost" < 1 THEN "delivery cost" / 2
         WHEN "delivery cost" < 10 THEN "delivery cost" / 10
       END AS "two",
FROM   tests.data.sales;