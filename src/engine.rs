use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::results::Name;
use crate::session::Session;
use crate::stdin_as_table::{StdinReader, create_stdin_reader};
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
    read_only: bool,
    stdin: RefCell<Box<dyn StdinReader>>,
}
impl TryFrom<&Args> for Engine {
    type Error = EngineError;
    fn try_from(args: &Args) -> Result<Self, Self::Error> {
        let home = args
            .home
            .clone()
            .or_else(|| current_dir().ok())
            .ok_or(EngineError::NoHomeDir)?;
        let stdin = RefCell::new(create_stdin_reader(args.command.is_some()));
        Ok(Self {
            home: home.clone(),
            first_line_as_name: !args.first_line_as_data,
            session: RefCell::new(Session::default()),
            read_only: !args.writer_mode,
            stdin,
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
    pub(crate) original_path: Option<PathBuf>,
    pub(crate) read_only: bool,
}
impl FoundFile {
    fn get_display_path(&self) -> Option<&PathBuf> {
        if self.is_temp {
            None
        } else if let Some(path) = &self.original_path {
            Some(path)
        } else {
            Some(&self.path)
        }
    }
}
pub struct CommandExecution {
    pub sql: String,
    pub results: ResultSet,
}
impl Engine {
    pub fn execute_commands(&self, sql: &str) -> Result<Vec<CommandExecution>, CvsSqlError> {
        let dialect = FilesDialect {};
        let mut all_results = Vec::new();
        for statement in Parser::parse_sql(&dialect, sql)? {
            let sql = statement.to_string();
            let results = statement.extract(self)?;
            all_results.push(CommandExecution { sql, results });
        }
        Ok(all_results)
    }

    pub fn prompt(&self) -> String {
        let name = self
            .home
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap_or_default();
        let active_transaction = if self.session.borrow().transaction.is_some() {
            "* "
        } else {
            ""
        };
        format!("{} {}", name, active_transaction)
    }

    pub(crate) fn file_name(&self, name: &ObjectName) -> Result<FoundFile, CvsSqlError> {
        if name.0.len() == 1 {
            if let Some(name) = name.0.first() {
                if name.to_string() == "$" {
                    let path = self.stdin.borrow_mut().path()?;
                    return Ok(FoundFile {
                        is_temp: false,
                        path,
                        result_name: "$".into(),
                        exists: true,
                        original_path: None,
                        read_only: true,
                    });
                }
            }
        }
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
        let original_path = if let Some(ref mut transaction) = self.session.borrow_mut().transaction
        {
            let original_path = path;
            path = transaction.access_file(&original_path)?;
            Some(original_path)
        } else {
            None
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
            original_path,
            read_only: self.session.borrow().transaction.is_none() && !is_temp && self.read_only,
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
            original_path: None,
            read_only: false,
        })
    }
    pub(crate) fn get_file_name(&self, file: &FoundFile) -> String {
        file.get_display_path()
            .and_then(|p| p.strip_prefix(&self.home).ok())
            .and_then(|p| p.to_str())
            .unwrap_or("TEMPORARY_FILE")
            .to_string()
    }

    pub(crate) fn start_transaction(&self) -> Result<(), CvsSqlError> {
        self.session.borrow_mut().start_transaction()
    }
    pub(crate) fn commit_transaction(&self) -> Result<(), CvsSqlError> {
        if self.read_only {
            return Err(CvsSqlError::ReadOnlyMode);
        }
        self.session.borrow_mut().commit_transaction()
    }
    pub(crate) fn rollback_transaction(&self) -> Result<(), CvsSqlError> {
        self.session.borrow_mut().rollback_transaction()
    }
}

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Cannot find home directory")]
    NoHomeDir,
}
