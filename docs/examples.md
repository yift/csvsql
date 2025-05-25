# Examples of usage of csvsql

Here are a few simple example of SQL one can use in csvsql:
(See example data in [here](./examples/data))

1. To get all the data from a file named `file.csv`:
``` sql
SELECT * FROM file;
```

2. To get only the name and the price columns from a file named `products.csv` in a directory: `availability/customers/details/` where price is set from the cheaper to the most expensive:
``` sql
SELECT
  name, price
FROM 
  availability.customers.details.products
WHERE
  price IS NOT NULL
ORDER BY price;
```

3. To count and get the age range of dogs saved in a file called `pets.csv`:
``` sql
SELECT
    MIN(age), MAX(age), COUNT(*)
from pets
WHERE type = 'dog';
```

3. To count and get the age range of all the pets saved in a file called `pets.csv` by type of pet where the number of pets is larger than 10:
``` sql
SELECT
    type, MIN(age), MAX(age), COUNT(*)
from pets
group by type
HAVING COUNT(*) > 20
;
```

3. To get the addresses of the owners of the 10 oldest pets:
``` sql
SELECT
    owners.name, owners.address
from pets, owners
WHERE pets.owner_id = owners.id
ORDER BY pets.age
LIMIT 10
;
```

4. Working on a temporary table:
First we can create a temporary table that includes only the cats that are older then 3:
``` sql
CREATE TEMPORARY TABLE older_cats AS
SELECT
    *
from pets
WHERE pets.type = 'cat' AND pets.age > 3
;
```
Now we can find all the owners of those cats:
``` sql
SELECT older_cats.id as id, owners.name AS owner_name, older_cats.name AS cat_name, phone FROM older_cats JOIN owners ON owners.id = older_cats.owner_id ORDER BY owners.name;
```

Then we can delete a few rows from the temporary table:
``` sql
DELETE FROM older_cats WHERE id IN (1656517935, 9848604329, 7999194771);
```

And, when we finish working on the table, we can drop it:
``` sql
DROP TABLE older_cats;
```