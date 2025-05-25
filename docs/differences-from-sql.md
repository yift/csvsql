# Differences between csvsql and SQL database
Those are some of the major differences between csvsql and SQL database.

## Everything is in memory
When working with a real database, the database server use the file system to persist all the data. In csvsql the data is loaded to the memory, including sort, uniqueness and joins. So, if you have a huge size of data, this is clearly not the right tool for the job.

## No indexes
Unlike read SQL databases, csvsql has no indexes or constraints. So, sorting and filtering performance can not be improve.

## Dynamic data types
Since the data is store as CSV file, one can never be certain of the type of value in each column. For example, the `CREATE TABLE table(name TEXT)` and `CREATE TABLE table(name INT)` will have the same effect.

## No nulls
`NULL` in SQL is not a value, so the value of something like `SELECT NULL = NULL` will not be `TRUE` but `NULL` (and one should use `SELECT NULL IS NULL` instead). In csvsql there is no real null, instead we have an empty value (which will give true for both `= NULL` and `IS NULL`).

## All the numbers are big decimal
Unlike a real database that has different kind of numeric types (float, double, int, decimal...), csvsql uses oonly big decimal as the numeric type. It means that heavy numeric calculation will be slower and take more memory than any real database.

## No timezone support
All the date and timestamps in csvsql are kept and used without any time zone support. This allows us to export the results to an excel file.

## Case sensitive
While SQL is by definition not case sensitive, the names (of tables/files and of columns) in csvsql are case sensitive.

## No real transactions
Because csvsql is a command line utility that reads file from the file system it has no transactions, and one can change the file manually while the process is running.

