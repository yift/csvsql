# How to use csvsql

## Basic concepts
### File names
When one run csvsql one has a  home directory (by default it's the current directory, one can change it using the `-m` command line argument).
csvsql assumes that every CSV file (that is, every file that has a `.csv` extension) is a table and that every directory is a database. So, to get all the items in a file name `data.csv` within a directory name `dir` one should use:
```sql
SELECT * FROM dir.data;
```
Note that the name is case sensitive.
Note that the `.csv` extension is omitted (adding it will look for a file name `dir/data/csv.csv`).
Note that the dot (`.`) character is used to identify the directory (one can have more than one level, that is `scheme.db.table` will look for `scheme/db/table.csv`).

If one chose to use the predefine commands (see bellow) then using the table name `$` will try to read the `stdin` as a CSV.

When changing directory (using the `USE` command), one can use the dollar `$` to go to the parent directory.

### "Transactions"
While csvsql has no real transaction, it has a transaction like interface. If one start a transaction the engine will create a temporary directory, and will save all the changes to that directory. A `ROLLBACK` will simply delete that directory. A `COMMIT` (available only in write mode, see bellow) will copy all the files from the temporary directory to the correct location. While there is no locking mechanism, the engine will remember the hash of the content of every file it read and if the file had changes since it was read the commit will fail.

### Temporary tables
Temporary tables are just temporary files. The engine will delete all of them once the process is killed (if it is killed gracefully).

## Using the command
### Using the terminal
By default csvsql reads the SQL commands from the console. One can run `csvsql` and it will start with the current directory as the home directory. To enter a multiline query, use the backslash (`\`) character at the end of the line (like bash). Use semicolon (`;`) to put two SQLs in the same line.
If you are using a terminal the history will be saved into `~/.config/csvsql/.history` file (or the OS alternative to the configuration folder).
If you are not using a terminal (for example, the command run as a pipe of another command or the stdin is a file) the history will not be saved. Running with the `-n` flag will force csvsql to use this mode.

### Using predefine commands
Using the `-c` argument. One can have multiple `-c` and separate the commands using semicolon (`;`). For example:
```bash
csvsql \
   -c 'SELECT * FROM tests.data.sales; SELECT * FROM tests.data.customers' \
   -c 'SELECT * FROM tests.data.artists'
```
### Output to files
By default the output of csvsql create a TUI table on the terminal (one can turn off the table TUI using the `-d` flag). This is nice for interactive process but if one want to save the data to the file system to use it in the future one should use the `-o` argument. By default this will create a directory and put all the outputs as CSV files in that directory. One can change the format using the `-p` argument (supported formats besides the default CSV are HTML, JSON, TXT and XLS - the latter will produce a single file with a sheet for every query).

### Write mode
By default csvsql runs in read only mode, that is, it will not change any file in the local file system beside temporary files. To move to a write mode, use the `-w` command. Do note, this can change the files in your file system.

### Header line
By default csvsql will assume that the first line of every CSV file it reads is the headers, i.e. the name of the column. One can use the `-f` flag to turn this off, without it the column names will follow the Excel column name standard with dollar (`$`) postfix (i.e. the first column will be named `A$` and the second one will be named `B$`)
In case we one of the rows have more columns than the header row, the engine will default the name of the column to the Excel column name standard (see above)