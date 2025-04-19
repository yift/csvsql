use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::results::Name;
use crate::session::Session;
use crate::{args::Args, dialect::FilesDialect, results::ResultSet};
use sqlparser::ast::ObjectName;
use sqlparser::parser::Parser;
use std::cell::RefCell;
use std::{env::current_dir, path::PathBuf};
use thiserror::Error;

pub struct Engine {
    pub(crate) first_line_as_name: bool,
    pub(crate) home: PathBuf,
    session: RefCell<Session>,
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
            session: RefCell::new(Session::default()),
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

pub(crate) struct FoundFile {
    pub(crate) is_temp: bool,
    pub(crate) path: PathBuf,
    pub(crate) result_name: Name,
    pub(crate) exists: bool,
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

    pub(crate) fn file_name(&self, name: &ObjectName) -> Result<FoundFile, CvsSqlError> {
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
        let Some(result_name) = result_name else {
            return Err(CvsSqlError::MissingTableName);
        };
        let mut exists = path.exists();
        let mut is_temp = false;
        if let Some(temp_path) = self.session.borrow().get_temporary_table(&result_name) {
            path = temp_path;
            is_temp = true;
            exists = true;
        };
        Ok(FoundFile {
            is_temp,
            path,
            result_name,
            exists,
        })
    }

    pub(crate) fn drop_temporary_table(&self, file: &FoundFile) -> Result<(), CvsSqlError> {
        self.session
            .borrow_mut()
            .drop_temporary_table(&file.result_name)
    }

    pub(crate) fn create_temp_file(&self, name: &ObjectName) -> Result<FoundFile, CvsSqlError> {
        let non_temp = self.file_name(name)?;
        if non_temp.is_temp {
            return Ok(non_temp);
        }

        if non_temp.path.exists() {
            return Err(CvsSqlError::NonTemporaryTableyExists(
                non_temp.result_name.full_name(),
            ));
        }

        let path = self
            .session
            .borrow_mut()
            .create_temporary_table(&non_temp.result_name)?;

        Ok(FoundFile {
            is_temp: true,
            path,
            result_name: non_temp.result_name,
            exists: false,
        })
    }
    pub(crate) fn get_file_name(&self, file: &FoundFile) -> String {
        if file.is_temp {
            "TEMPRARY_FILE".to_string()
        } else {
            file.path
                .strip_prefix(&self.home)
                .ok()
                .and_then(|f| f.to_str())
                .unwrap_or_default()
                .to_string()
        }
    }
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Cannot find home directory")]
    NoHomeDir,
}
