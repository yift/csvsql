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
    #[error("Unsupported: `{0}`")]
    Unsupported(String),
    #[error("TODO: `{0}`")]
    ToDo(String),
    #[error("Nothing to select")]
    NoSelect,
    #[error("`{0}`")]
    ColumnIndexError(#[from] ColumnIndexError),
}
