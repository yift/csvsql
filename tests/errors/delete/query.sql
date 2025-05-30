DELETE FROM tests.data.artists;
---


DELETE FROM tests.data.artists, tests.data.sales;
---


DELETE FROM BOOK
USING AUTHOR
WHERE (
  BOOK.AUTHOR_ID = AUTHOR.ID
  AND AUTHOR.LAST_NAME = 'Poe'
);
---



DELETE FROM BOOK
WHERE (
  BOOK.AUTHOR_ID = AUTHOR.ID
  AND AUTHOR.LAST_NAME = 'Poe'
)
LIMIT 20;
---


DELETE FROM external_data RETURNING id;
---


DELETE FROM BOOK
WHERE (
  BOOK.AUTHOR_ID = AUTHOR.ID
  AND AUTHOR.LAST_NAME = 'Poe'
)
ORDER BY AUTHOR.LAST_NAME;
---



DELETE table1 
FROM table1 JOIN table2 
ON table1.attribute_name = table2.attribute_name
;
---


