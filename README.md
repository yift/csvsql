# csvsql
CSV SQL is a SQL-like engine that works on CSV files.

## Why
The aim of this utility is to read and manipulate CSV files from the command line without the need to use point-and-click spreadsheet applications.

## What can you use it for
The csvsql was built to be a command line interface to read and manipulate CSV files. It's a fast and simple command line alternative to Microsoft Excel or LibreOffice Calc. You can use it to read and manipulate CSV files while separating the source data from the visualization (the formats, charts, hidden columns, filters...) and the actions (the SQL queries).

## What can't you use it for
It is not a real SQL engine; not only will it not support a lot of the features that all SQL databases have, it also diverges from some of the core SQL features and has no concept of indexes.
See more details [here](docs/differences-from-sql.md)


## Getting started
See installation instructions [here](docs/install.md).

See more details on how to use csvsql [here](docs/usage.md).

See some examples [here](docs/examples.md).

## List of supported functions
See [list of supported functions](docs/supported_functions.md).
