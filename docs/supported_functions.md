# List of supported functions
Bellow is a list of all the supported functions by csvsql
## Binary operators

| Operator| Description    | Examples |
|---------|----------------|----------|
| `+` | Adds two numbers  | `4 + 5` will give us `9` |
|  `*` | Multiply two numbers  | `4 * 5` will give us `20` |
|  `/` | Divide two numbers | `4 / 5` will give us `0.8`, `20 / 2` will give us `2` |
|  `-` | Subtract two numbers | `4 - 5` will give us `-1`    |
|  `%` | Find the Modulo of two numbers | `7 % 5` will give us `2`, `45 % 11` will give us `1` |
|  `\|\|` | Concat two strings | `7 \|\| 5` will give us `75`, `'one ' \|\| 'two'` will give us `one two` |
|  `<` | Return true only if the left argument is less than the right argument  | `7 < 5` will give us `false`, `5 < 7` will give use `true` |
|  `>` | Return true only if the left argument is more than the right argument   | `7 > 5` will give us `true`  |
|  `=` | Return true if the left argument if the same as right | `7 = 5` will give us `false`, `'one' = 'one'` will give us `true`  |
|  `<=`| Return true only if the left argument is less or equals to the right argument | `7 <= 5` will give us `false` |
|  `>=`| Return true only if the left argument is more or equals to the right argument | `7 >= 5` will give us `true` |
|  `!=`| Return true if the left argument is not the same as the right argument (same as `<>`) | `7 != 5` will give us `true` |
|  `<>`| Return true if the left argument is not the same as the right argument (same as `!=`) | `7 <> 5` will give us `true` |
|  `AND`| Return `true` if both arguments are `true`, `null` if either arguments is not a Boolean, `false` in any other case | `true AND true` will give us `true`, `true AND false` will give us `false` |
|  `OR`| Return `false` if both arguments are `false`, `null` if either arguments is not a Boolean, `true` in any other case | `true OR true` will give us `true`, `false OR false` will give us `false` |
|  `XOR`| Return `true` if on argument is `true` and the other one is `false`, `null` if either arguments is not a Boolean, `false` in any other case | `true XOR true` will give us `false`, `false XOR false` will give us `false` |
| `IN` | Check is an expression value contains with in a list or a subquery | `3 IN (4, 3, 1)` or `5 IN (SELECT age FROM pets)`
| `NOT IN` | Negate the `IN` operator | `3 NOT IN (4, 3, 1)` or `5 NOT IN (SELECT age FROM pets)`
| `BETWEEN` | Check if an expression is between two numeric values (not the `AND` operator) | `7 BETWEEN 5 AND 12`
| `NOT BETWEEN` | Negate the between operator | `7 NOT BETWEEN 5 AND 12`
| `RLIKE` | Check if the expression matches a regular expression (Regular expression rules are defined in [here](https://docs.rs/regex/latest/regex/)) | `'200' RLIKE '[0-9]+`
| `NOT RLIKE` | Negate the RLIKE operator| `'200' NOT RLIKE '[0-9]+`
| `SIMILAR TO` | Same as `RLIKE| `'200' SIMILAR TO '[0-9]+`
| `NOT SIMILAR TO` | Same as `NOT RLIKE| `'200' NOT SIMILAR TO '[0-9]+`
| `REGEXP` | Same as `RLIKE| `'200' REGEXP '[0-9]+`
| `NOT REGEXP` | Same as `NOT RLIKE| `'200' NOT REGEXP '[0-9]+`



## Unary operators
Please note, some operator has prefix format and some postfix format.

| Operator| Description    | Examples |
|---------|----------------|----------|
| `IS FALSE` | Check if the expression is `false` (will return `true` if it's `false` or `false` for any other value) | `1 + 1 IS FALSE`
| `IS NOT FALSE` | Check if the expression is not `false` (will return `false` if it's `false` or `true` for any other value) | `1 + 1 IS NOT FALSE`
| `IS TRUE` | Check if the expression is `true` (will return `true` if it's `true` or `false` for any other value) | `1 + 1 IS TRUE`
| `IS NOT TRUE` | Check if the expression is not `true` (will return `false` if it's `true` or `true` for any other value) | `1 + 1 IS NOT TRUE`
| `IS NULL` | Check if the expression is empty  | `1 + 1 IS NULL`
| `IS NOT NULL` | Check if the expression is not empty | `1 + 1 IS NOT NULL`
| `NOT` | return `true` if the expression is `false` or `false` if the expression is `true`, `null` for non Boolean expression. Note, this is a prefix operator | `NOT 1 > 3`
| `-` | If the expression is numeric, return the negative value of that expression. Note, this is a prefix operator | `- (3 + 1)`
| `+` | If the expression is numeric, return the value of that expression. Note, this is a prefix operator | `+ (3 + 1)`


## Aggregation Functions

| Function| Description    | Examples |
|---------|----------------|----------|
| `COUNT` | Counts the number of items. One can use `COUNT(*)` or `COUNT(DISTINCT age)` | `COUNT(id)` |
| `MAX` | Returns the maximal value | `MAX(age)` |
| `MIN` | Returns the minimal value | `MIN(age)` |
| `AVG` | Returns the average value | `AVG(price)` |
| `SUM` | Returns the sum of all the values | `SUM(price)` |
| `ANY_VALUE` | Return any value from the group | `ANY_VALUE(date)` |


## Functions

| Function| Description    | Examples |
|---------|----------------|----------|
| `TRY_CAST` | Will cast an expression to another datatype, if failed, will return `null` | `TRY_CAST('1002' AS INT)` |
| `CAST` | same as `TRY_CAST` | `CAST('1002' AS INT)` |
| `EXTRACT` | extract value from a date or a timestamp field | `EXTRACT(day FROM '2025-03-10')` or `EXTRACT(hour FROM '2025-03-10 20:00:10')` |
| `CEIL` | Return the ceiling of a number | `CEIL(10.32)` |
| `FLOOR` | Return the floor of a number | `FLOOR(10.32)` |
| `POSITION` | Return the one based index of a substring within a string (will return null if either argument is not a string) | `POSITION('old' IN 'gold')` |
| `SUBSTRING` | Create a substring from a string. Can have two (the string and the start index - one based) or three (the maximal length of the results) arguments  | `SUBSTRING('Gold' FROM 2)` or `SUBSTRING('gold' FROM 2 FOR 1)`
| `ABS` | Return the absolute value of a number | `ABS(22)` |
| `ASCII` | Returns the ascii value of the first character of a string argument | `ASCII('a')` |
| `CHR` | Return the character of an ascii value of a numeric argument | `CHR(97)` |
| `LENGTH` | Return the number of character in a string argument | `LENGTH('Test')` |
| `CHAR_LENGTH` | Same as length | `CHAR_LENGTH('Test')` |
| `CHARACTER_LENGTH` | Same as length | `CHARACTER_LENGTH('Test')` |
| `COALESCE` | Same the first non empty argument | `CHARACTER_LENGTH(NULL, NULL, 4)` |
| `CONCAT` | Concatenate all the arguments into a string | `CONCAT('h', 'e', 'll', 'o', ' ', 'world')` |
| `CONCAT_WS` | Concatenate all the arguments from the second one to a string with the first argument as a separator | `CONCAT_WS(' ', 'hello', 'world')` |
| `CURRENT_DATE` | Return the current date (in UTC)| `CURRENT_DATE()` |
| `CURDATE` | Same as `CURRENT_DATE` | `CURDATE()` |
| `NOW` | Return the current timestamp in UTC | `NOW()` |
| `CURRENT_TIME` | Same as `NOW` | `CURRENT_TIME()` |
| `CURRENT_TIMESTAMP` | Same as `NOW` | `CURRENT_TIMESTAMP()` |
| `CURTIME` | Same as `NOW` | `CURTIME()` |
| `LOCALTIME` | Same as `NOW` (notice, this is not local) | `LOCALTIME()` |
| `CURRENT_TIME` | Same as `NOW` (notice, this is not local) | `CURRENT_TIME()` |
| `USER` | Return the os username | `USER()` |
| `CURRENT_USER` | Same as `USER` | `CURRENT_USER()` |
| `FORMAT` | Format date or timestamp to a string. See available formats in [chron docs](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) | `FORMAT(NOW(), '%c')` |
| `DATE_FORMAT` | Same as `FORMAT`| `DATE_FORMAT(NOW(), '%c')` |
| `TIME_FORMAT` | Same as `FORMAT`| `TIME_FORMAT(NOW(), '%c')` |
| `TO_CHAR` | Same as `FORMAT`| `TO_CHAR(NOW(), '%c')` |
| `TO_TIMESTAMP` | Create a timestamp from the number of seconds since Unix epoch | `TO_TIMESTAMP(1400234500)` |
| `FROM_UNIXTIME` | Same as `TO_TIMESTAMP` | `FROM_UNIXTIME(1400234500)` |
| `GREATEST` | Return the greatest of all the arguments | `GREATEST(100, 20, 102, 80)` |
| `LEAST` | Return the lower of all the arguments | `LEAST(100, 20, 102, 80)` |
| `IF` | If the first argument is `true` return the second argument, if it is `false` returns the second argument | `IF(5 > 10, 'Yes', 'No')` |
| `NULLIF` | If the first argument is the same as the second argument, return empty value, if they are not the same, return the first argument | `NULLIF(1, 10)` |
| `LOWER` | Convert a string to lower case | `LOWER('HELLO')` |
| `LCASE` | Same as `LOWER` | `LCASE('HELLO')` |
| `UPPER` | Convert a string to upper case | `UPPER('hello')` |
| `UCASE` | Same as `UPPER` | `UCASE('hello')` |
| `LEFT` | Take the first n character of a string | `LEFT('hello world', 5)` |
| `RIGHT` | Take the last n character of a string | `RIGHT('hello world', 5)` |
| `LPAD` | pad text from the beginning so it will be in a given length | `LPAD(' ', 'test', 8)` |
| `RPAD` | pad text from the end so it will be in a given length | `RPAD(' ', 'test', 8)` |
| `LTRIM` | remove any leading white space characters | `LTRIM('   hello')` |
| `RTRIM` | remove any trailing white space characters | `RTRIM('hello   ')` |
| `PI` | Return PI (up to 10 digits) | `PI()` |
| `RANDOM` | If it has no argument, return a random number between 0 and 1. If it has a positive numeric argument, return a random integer number between the 0 and the number. Note, this will not use a secure random generator. | `RANDOM()` or `RANDOM(10)` |
| `RAND` | Same as `RANDOM`. | `RAND()` or `RAND(10)` |
| `POSITION` | Returns the position of a substring within a string (1 based index). If it has a third numeric argument, will start the lookup from that index | `POSITION('str', 'full string')` or `POSITION('str', 'full string', 2)` |
| `LOCATE` | Same as `POSITION` | `LOCATE('str', 'full string')` or `LOCATE('str', 'full string', 2)` |
| `REPEAT` | Repeat a string argument a few times | `REPEAT('Test', 2)` |
| `REPLACE` | Replace all the occurrences of a string within a string with another string | `REPLACE('text', 't', '-')` |
| `REGEX_LIKE` | With two argument behave like the `RLIKE` operator. Adding a third argument will add flags to the regular expression. See available flags in the [regex doc](https://docs.rs/regex/latest/regex/) | `REGEX_LIKE('200', '[0-9]+')` or `REGEX_LIKE('Hello', '[a-z]+', 'i')` |
| `REGEX_REPLACE` | Replace all the occurrences of a regular expression with a given string | `REGEX_REPLACE('10 + 10 = 20', '[0-9]+', '<number>')` |
| `REGEXP_SUBSTR` | Finds a substring that match a regular expression. The third optional argument can be the position to start looking from (1 based index), The fourth optional index can be thee occurrence number to find (default to the first occurrence, 1 based index), the fifth optional argument can be the regular expression flags | `REGEXP_SUBSTR('this 100 is a number', '[0-9]+')` or `REGEXP_SUBSTR('this 100 is a number', '[a-z]+', 5)` or `REGEXP_SUBSTR('this 100 is a number', '[a-z]+', 5, 2)` or or `REGEXP_SUBSTR('this 100 is a number', '[a-z]+', 5, 2, 'i')` |
| `REVERSE` | Reverse a string argument | `REVERSE('some')` |
| `LN` | Finds the natural logarithm of a number | `LN(100)` |
| `EXP` | Finds the natural exponent of a number | `EXP(100)` |
| `LOG` | With a single numeric argument, finds the 10 based logarithm of the number. With two arguments, find the first argument logarithm of the second argument | `LOG(100)` or `LOG(3, 9)` |
| `LOG2` | Finds the 2 based logarithm of the number | `LOG2(16)` |
| `LOG10` | Finds the 10 based logarithm of the number | `LOG10(1000)` |
| `POW` | Find the first argument to the power of the second argument | `POW(2, 4)` |
| `POWER` | Same as `POW` | `POWER(2, 4)` |
| `ROUND` | With a single argument, round the value of the argument to the nearest integer. With two arguments, round the value of the first argument to the second argument digits after the decimal point. | `ROUND(1.35)` or `ROUND(1.411, 2)` |
| `SQRT` | Finds the square root of a number. | `SQRT(64)` |


## Case function
The case function has a few conditions, and the return value will be the first condition that is true. If no condition is true, we will use the `ELSE` value, if there is no else value, we will default to null.
For example:

```sql
       CASE
         WHEN "delivery cost" < 0.5 THEN 1
         WHEN "delivery cost" < 1 THEN 2
         WHEN "delivery cost" < 10 THEN 3
         ELSE 4
       END AS "one",
```
or
```sql
       CASE
         WHEN "delivery cost" < 0.5 THEN "delivery cost"
         WHEN "delivery cost" < 1 THEN "delivery cost" / 2
         WHEN "delivery cost" < 10 THEN "delivery cost" / 10
       END AS "two",
```
 
