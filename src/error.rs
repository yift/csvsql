use sqlparser::parser::ParserError;
use std::io::Error as IoError;
use thiserror::Error;

use crate::{engine, writer::WriterError};

#[derive(Error, Debug)]
pub enum CdvSqlError {
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
    #[error("Cannot find columns: `{0}`")]
    NoSuchColumn(String),
}
