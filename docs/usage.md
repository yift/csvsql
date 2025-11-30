# How to use csvsql

## Basic concepts

### File names
When you run csvsql, you have a home directory (by default it's the current directory; you can change it using the `-m` command line argument).

csvsql assumes that every CSV file (that is, every file that has a `.csv` extension) is a table and that every directory is a database. So, to get all the items in a file named `data.csv` within a directory named `dir`, you should use:
```sql
SELECT * FROM dir.data;
```
Note that the name is case sensitive.

Note that the `.csv` extension is omitted (adding it will look for a file named `dir/data/csv.csv`).

Note that the dot (`.`) character is used to identify the directory (you can have more than one level; that is, `scheme.db.table` will look for `scheme/db/table.csv`).

If you choose to use the predefined commands (see below), then using the table name `$` will try to read the `stdin` as a CSV.

When changing directories (using the `USE` command), you can use the dollar sign `$` to go to the parent directory.

### "Transactions"
While csvsql has no real transactions, it has a transaction-like interface. If you start a transaction, the engine will create a temporary directory and will save all the changes to that directory. A `ROLLBACK` will simply delete that directory. A `COMMIT` (available only in write mode; see below) will copy all the files from the temporary directory to the correct location. While there is no locking mechanism, the engine will remember the hash of the content of every file it reads, and if the file has changed since it was read, the commit will fail.

### Temporary tables
Temporary tables are just temporary files. The engine will delete all of them once the process is killed (if it is killed gracefully).

## Using the command

### Using the terminal
By default, csvsql reads the SQL commands from the console. You can run `csvsql` and it will start with the current directory as the home directory. To enter a multiline query, use the backslash (`\`) character at the end of the line (like bash). Use a semicolon (`;`) to put two SQLs on the same line.

If you are using a terminal, the history will be saved into the `~/.config/csvsql/.history` file (or the OS alternative to the configuration folder).

If you are not using a terminal (for example, the command runs as a pipe of another command or the stdin is a file), the history will not be saved. Running with the `-n` flag will force csvsql to use this mode.

### Using predefined commands
You can use the `-c` argument. You can have multiple `-c` flags and separate the commands using a semicolon (`;`). For example:
```bash
csvsql \
   -c 'SELECT * FROM tests.data.sales; SELECT * FROM tests.data.customers' \
   -c 'SELECT * FROM tests.data.artists'
```

### Output to files
By default, the output of csvsql creates a TUI table on the terminal (you can turn off the table TUI using the `-d` flag). This is nice for interactive processes, but if you want to save the data to the file system to use it in the future, you should use the `-o` argument. By default, this will create a directory and put all the outputs as CSV files in that directory. You can change the format using the `-p` argument (supported formats besides the default CSV are HTML, JSON, TXT, and XLS - the latter will produce a single file with a sheet for every query).

### Write mode
By default, csvsql runs in read-only mode; that is, it will not change any file in the local file system besides temporary files. To move to write mode, use the `-w` command. Do note, this can change the files in your file system.

### Header line
By default, csvsql will assume that the first line of every CSV file it reads is the headers, i.e., the names of the columns. You can use the `-f` flag to turn this off; without it, the column names will follow the Excel column name standard with a dollar sign (`$`) postfix (i.e., the first column will be named `A$` and the second one will be named `B$`).

In case one of the rows has more columns than the header row, the engine will default the name of the column to the Excel column name standard (see above).