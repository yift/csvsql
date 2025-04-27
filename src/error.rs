use csv::Error as CsvError;
use sqlparser::parser::ParserError;
use std::io::Error as IoError;
use thiserror::Error;

use crate::{engine, results::ColumnIndexError, writer::WriterError};

#[derive(Error, Debug)]
pub enum CvsSqlError {
    #[error("Parse error: `{0}`")]
    ParserError(#[from] ParserError),
    #[error("Engine error: `{0}`")]
    EngineError(#[from] engine::EngineError),
    #[error("Write error: `{0}`")]
    WriterError(#[from] WriterError),
    #[error("IO Error: `{0}`")]
    IoError(#[from] IoError),
    #[error("CSV Error: `{0}`")]
    CsvError(#[from] CsvError),
    #[error("Unsupported: `{0}`")]
    Unsupported(String),
    #[error("TODO: `{0}`")]
    ToDo(String),
    #[error("Nothing to select")]
    NoSelect,
    #[error("`{0}`")]
    ColumnIndexError(#[from] ColumnIndexError),
    #[error("Can't aggregate without a group")]
    NoGroupBy,
    #[error("Ofset must be a positive number")]
    NoNumericOffset,
    #[error("Limit must be a positive number")]
    NoNumericLimit,
    #[error("Table `{0}` already exists.")]
    TableAlreadyExists(String),
    #[error("Cannot write to permenent file in read only mode.")]
    ReadOnlyMode,
    #[error("Non temporary table `{0}` already exists.")]
    NonTemporaryTableyExists(String),
    #[error("Temporary table `{0}` already exists.")]
    TemporaryTableyExists(String),
    #[error("Missing table name.")]
    MissingTableName,
    #[error("Nothing to insert.")]
    NoInsertSource,
    #[error("Number of column to insert should match the number of columns in the source.")]
    InsertMismatch,
    #[error("Table `{0}` not exists.")]
    TableNotExists(String),
    #[error("Table `{0}` not temporary.")]
    TableNotTemporary(String),
    #[error("Table `{0}` missing structure.")]
    NoTableStructuye(String),
    #[error("Update with multiple assignment for the same columns.")]
    MultiplieAssignment,
    #[error("Can not delete from more than one table.")]
    MultiplieTableDelete,
    #[error("Nothing to delete.")]
    NothingToDelete,
    #[error("Column `{0}` already exists.")]
    ColumnAlreadyExists(String),
    #[error("There is already a transaction in progress.")]
    TransactionInProgress,
    #[error("There is no transaction in progress.")]
    NoTransactionInProgress,
    #[error("File `{0}` created after transaction started.")]
    FileCreatedUnexpectedly(String),
    #[error("File `{0}` changed after transaction started.")]
    FileChangedUnexpectedly(String),
    #[error("File `{0}` deleted after transaction started.")]
    FileRemovedUnexpectedly(String),
    #[error("Could not create output: `{0}`.")]
    OutputCreationError(String),
    #[error("Can not use stdin as a table in interactive mode.")]
    StdinUnusable,
}
