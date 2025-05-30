use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::results::Name;
use crate::results_builder::build_simple_results;
use crate::session::Session;
use crate::stdin_as_table::{StdinReader, create_stdin_reader};
use crate::value::Value;
use crate::{args::Args, dialect::FilesDialect, results::ResultSet};
use sqlparser::ast::ObjectName;
use sqlparser::parser::Parser;
use std::cell::RefCell;
use std::{env::current_dir, path::PathBuf};
use thiserror::Error;

pub struct Engine {
    pub(crate) first_line_as_name: bool,
    home: RefCell<PathBuf>,
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
        let home = RefCell::new(home.clone());
        Ok(Self {
            home,
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
        let home = self.home.borrow();
        let name = home
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
        let mut path = self.home.borrow().to_path_buf();
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

    pub(crate) fn home(&self) -> PathBuf {
        self.home.borrow().to_path_buf()
    }

    pub(crate) fn change_home(&self, name: &ObjectName) -> Result<ResultSet, CvsSqlError> {
        if name.0.is_empty() {
            return Err(CvsSqlError::Unsupported("USE without database name".into()));
        }
        let mut path = self.home.borrow().clone();
        let mut relative = String::new();
        for name in &name.0 {
            if name.to_string() == "$" {
                let Some(parent) = path.parent() else {
                    return Err(CvsSqlError::CannotAccessParentDir(path));
                };
                path = parent.to_path_buf();
                relative = format!("{}/..", relative);
            } else {
                path = path.join(name.to_string());
                relative = format!("{}/{}", relative, name);
                if !path.is_dir() {
                    return Err(CvsSqlError::NotADir(relative));
                };
            }
        }
        self.home.replace_with(|_| path);
        build_simple_results(vec![
            ("action", Value::Str("USE".to_string())),
            ("path", Value::Str(relative)),
        ])
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
            .and_then(|p| p.strip_prefix(self.home.borrow().as_path()).ok())
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

#[cfg(test)]
mod tests {

    use sqlparser::ast::Ident;

    use super::*;

    struct FakeStdIn {}
    impl StdinReader for FakeStdIn {
        fn path(&mut self) -> Result<PathBuf, CvsSqlError> {
            Ok(PathBuf::from("/stdin"))
        }
    }

    #[test]
    fn read_stdin_as_dollar() -> Result<(), CvsSqlError> {
        let args = Args::default();
        let mut engine = Engine::try_from(&args)?;
        engine.stdin = RefCell::new(Box::new(FakeStdIn {}));
        let name = ObjectName::from(vec![Ident::from("$")]);

        let file = engine.file_name(&name)?;

        assert!(!file.is_temp);
        assert_eq!(file.path.to_str().unwrap_or_default(), "/stdin");
        assert_eq!(file.result_name.full_name(), "$".to_string());
        assert!(file.exists);
        assert_eq!(file.original_path, None);
        assert!(file.read_only);

        Ok(())
    }

    #[test]
    fn missing_file_name() -> Result<(), CvsSqlError> {
        let args = Args::default();
        let engine = Engine::try_from(&args)?;
        let name = ObjectName::from(vec![]);

        let err = engine.file_name(&name).err().unwrap();

        assert!(matches!(err, CvsSqlError::MissingTableName));

        Ok(())
    }
}
