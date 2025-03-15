SELECT "customer id", COUNT(*), SUM(price) - 1, AVG("delivery cost" * 3), MIN("tax percentage"), MAX("tax percentage") FROM tests.data.sales group by "customer id" ORDER BY "customer id";
