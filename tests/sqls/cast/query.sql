SELECT CONCAT('|', TRY_CAST(amount AS TEXT), '|') FROM tests.data.dates;

SELECT TRY_CAST('true' AS BOOLEAN) FROM tests.data.dates;

SELECT TRY_CAST('1002' AS INT) FROM tests.data.dates;

SELECT TRY_CAST('2025-01-21' AS DATE) FROM tests.data.dates;

SELECT TRY_CAST('2025-01-21 11:20:01' AS TIMESTAMP) FROM tests.data.dates;