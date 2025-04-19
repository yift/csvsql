use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::results::Name;
use crate::{args::Args, dialect::FilesDialect, results::ResultSet};
use sqlparser::ast::ObjectName;
use sqlparser::parser::Parser;
use std::{env::current_dir, path::PathBuf};
use thiserror::Error;

pub struct Engine {
    pub(crate) first_line_as_name: bool,
    pub(crate) home: PathBuf,
}
impl TryFrom<&Args> for Engine {
    type Error = EngineError;
    fn try_from(args: &Args) -> Result<Self, Self::Error> {
        let home = args
            .home
            .clone()
            .or_else(|| current_dir().ok())
            .ok_or(EngineError::NoHomeDir)?;
        Ok(Self {
            home: home.clone(),
            first_line_as_name: args.first_line_as_name,
        })
    }
}

trait AppendName {
    fn append(&self, name: &str) -> Self;
}
impl AppendName for Option<Name> {
    fn append(&self, name: &str) -> Self {
        match self {
            None => Some(name.into()),
            Some(parent) => Some(parent.append(name)),
        }
    }
}

impl Engine {
    pub fn execute_commands(&self, sql: &str) -> Result<Vec<ResultSet>, CvsSqlError> {
        let dialect = FilesDialect {};
        let mut results = Vec::new();
        for statement in Parser::parse_sql(&dialect, sql)? {
            results.push(statement.extract(self)?);
        }
        Ok(results)
    }

    pub fn prompt(&self) -> String {
        let name = self
            .home
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap_or_default();
        format!("{}> ", name)
    }

    pub(crate) fn file_name(&self, name: &ObjectName) -> (PathBuf, Option<Name>) {
        let file_name = &name.0;
        let mut file_names = file_name.iter().peekable();
        let mut path = self.home.to_path_buf();
        let mut result_name = None;
        while let Some(name) = file_names.next() {
            let name = name.to_string();
            result_name = result_name.append(&name);
            if file_names.peek().is_none() {
                path = path.join(format!("{}.csv", name));
            } else {
                path = path.join(name);
            }
        }
        (path, result_name)
    }
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Cannot find home directory")]
    NoHomeDir,
}
