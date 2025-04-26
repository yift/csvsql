use std::{
    collections::HashMap,
    fs::{self},
    path::PathBuf,
    rc::Rc,
};

use sha256::try_digest;
use sqlparser::ast::{
    BeginTransactionKind, Ident, Statement, TransactionMode, TransactionModifier,
};
use tempfile::NamedTempFile;

use crate::{
    engine::Engine,
    error::CvsSqlError,
    result_set_metadata::SimpleResultSetMetadata,
    results::ResultSet,
    results_data::{DataRow, ResultsData},
    session::TemporaryFiles,
    value::Value,
};

struct TransactionFile {
    temporary_file: NamedTempFile,
    original_hash: Option<String>,
}
#[derive(Default)]
pub(crate) struct Transaction {
    pub(crate) temporary_tables: TemporaryFiles,
    transaction_tables: HashMap<PathBuf, TransactionFile>,
}

impl Transaction {
    pub(crate) fn access_file(&mut self, file: &PathBuf) -> Result<PathBuf, CvsSqlError> {
        if let Some(path) = self.transaction_tables.get(file) {
            Ok(path.temporary_file.path().to_path_buf())
        } else {
            let temporary_file = NamedTempFile::with_suffix(".csv")?;
            let ret_path = temporary_file.path().to_path_buf();
            let original_hash = if file.exists() {
                fs::copy(file, &ret_path)?;
                Some(try_digest(file)?)
            } else {
                fs::remove_file(&ret_path)?;
                None
            };
            self.transaction_tables.insert(
                file.to_path_buf(),
                TransactionFile {
                    temporary_file,
                    original_hash,
                },
            );
            Ok(ret_path)
        }
    }
    pub(crate) fn commit(&mut self) -> Result<(), CvsSqlError> {
        // Verify before the commit
        for (original_file, file_to_replace) in &self.transaction_tables {
            if original_file.exists() {
                if let Some(hash) = &file_to_replace.original_hash {
                    let new_hash = try_digest(original_file)?;
                    if new_hash.as_str() != hash {
                        return Err(CvsSqlError::FileChangedUnexpectedly(
                            original_file.to_str().unwrap_or_default().to_string(),
                        ));
                    }
                } else {
                    return Err(CvsSqlError::FileCreatedUnexpectedly(
                        original_file.to_str().unwrap_or_default().to_string(),
                    ));
                }
            } else if file_to_replace.original_hash.is_some() {
                return Err(CvsSqlError::FileRemovedUnexpectedly(
                    original_file.to_str().unwrap_or_default().to_string(),
                ));
            }
        }

        for (original_file, file_to_replace) in self.transaction_tables.drain() {
            let path = file_to_replace.temporary_file.path().to_path_buf();
            if !path.exists() {
                if original_file.exists() {
                    fs::remove_file(original_file)?;
                }
            } else {
                fs::copy(path, original_file)?;
            }
        }

        Ok(())
    }
    pub(crate) fn rollback(&mut self) -> Result<(), CvsSqlError> {
        Ok(())
    }
}

pub(crate) fn start_transaction(
    engine: &Engine,
    modes: &[TransactionMode],
    transaction: &Option<BeginTransactionKind>,
    modifier: &Option<TransactionModifier>,
    statements: &[Statement],
    exception_statements: &Option<Vec<Statement>>,
) -> Result<ResultSet, CvsSqlError> {
    if !modes.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "Transactions with mode".to_string(),
        ));
    }
    if modifier.is_some() {
        return Err(CvsSqlError::Unsupported(
            "Transactions with modifier".to_string(),
        ));
    }
    if let Some(kind) = transaction {
        if kind != &BeginTransactionKind::Transaction {
            return Err(CvsSqlError::Unsupported(format!(
                "Transactions with kind {}",
                kind
            )));
        }
    }
    if !statements.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "Transactions with statement".to_string(),
        ));
    }
    if exception_statements.is_some() {
        return Err(CvsSqlError::Unsupported(
            "Transactions with exception statements".to_string(),
        ));
    }
    engine.start_transaction()?;

    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("action");
    let metadata = metadata.build();

    let row = vec![Value::Str("START TRANSACTION".to_string())];
    let row = DataRow::new(row);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };

    Ok(results)
}

pub(crate) fn commit_transaction(
    engine: &Engine,
    modifier: &Option<TransactionModifier>,
) -> Result<ResultSet, CvsSqlError> {
    if modifier.is_some() {
        return Err(CvsSqlError::Unsupported(
            "Transactions with modifier".to_string(),
        ));
    }
    engine.commit_transaction()?;

    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("action");
    let metadata = metadata.build();

    let row = vec![Value::Str("COMMIT".to_string())];
    let row = DataRow::new(row);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };

    Ok(results)
}

pub(crate) fn rollback_transaction(
    engine: &Engine,
    savepoint: &Option<Ident>,
) -> Result<ResultSet, CvsSqlError> {
    if savepoint.is_some() {
        return Err(CvsSqlError::Unsupported(
            "Transactions with savepoint".to_string(),
        ));
    }
    engine.rollback_transaction()?;

    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("action");
    let metadata = metadata.build();

    let row = vec![Value::Str("ROLLBACK".to_string())];
    let row = DataRow::new(row);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };

    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use crate::{args::Args, engine::Engine, error::CvsSqlError};

    #[test]
    fn test_file_change_during_transaction() -> Result<(), CvsSqlError> {
        let working_dir = tempdir()?;
        fs::create_dir_all(&working_dir)?;
        let table = working_dir.path().join("tab.csv");
        fs::write(table, "col\n1\n2\n")?;

        let args = Args {
            writer_mode: true,
            home: Some(working_dir.path().to_path_buf()),
            ..Args::default()
        };
        let engine_with_transaction = Engine::try_from(&args)?;
        engine_with_transaction.execute_commands("START TRANSACTION;")?;
        engine_with_transaction.execute_commands("INSERT INTO tab VALUES(4);")?;

        let engine_without_transaction = Engine::try_from(&args)?;
        engine_without_transaction.execute_commands("INSERT INTO tab VALUES(5);")?;

        let err = engine_with_transaction.execute_commands("COMMIT;").err();

        assert!(matches!(
            err.unwrap(),
            CvsSqlError::FileChangedUnexpectedly(_)
        ));

        Ok(())
    }

    #[test]
    fn test_file_deleted_during_transaction() -> Result<(), CvsSqlError> {
        let working_dir = tempdir()?;
        fs::create_dir_all(&working_dir)?;
        let table = working_dir.path().join("tab.csv");
        fs::write(table, "col\n1\n2\n")?;

        let args = Args {
            home: Some(working_dir.path().to_path_buf()),
            writer_mode: true,
            ..Args::default()
        };
        let engine_with_transaction = Engine::try_from(&args)?;
        engine_with_transaction.execute_commands("START TRANSACTION;")?;
        engine_with_transaction.execute_commands("INSERT INTO tab VALUES(4);")?;

        let engine_without_transaction = Engine::try_from(&args)?;
        engine_without_transaction.execute_commands("DROP TABLE tab")?;

        let err = engine_with_transaction.execute_commands("COMMIT;").err();

        assert!(matches!(
            err.unwrap(),
            CvsSqlError::FileRemovedUnexpectedly(_)
        ));

        Ok(())
    }

    #[test]
    fn test_file_created_during_transaction() -> Result<(), CvsSqlError> {
        let working_dir = tempdir()?;
        fs::create_dir_all(&working_dir)?;

        let args = Args {
            home: Some(working_dir.path().to_path_buf()),
            writer_mode: true,
            ..Args::default()
        };
        let engine_with_transaction = Engine::try_from(&args)?;
        engine_with_transaction.execute_commands("START TRANSACTION;")?;
        engine_with_transaction.execute_commands("CREATE TABLE tab(a0 INT)")?;

        let engine_without_transaction = Engine::try_from(&args)?;
        engine_without_transaction.execute_commands("CREATE TABLE tab(a0 INT)")?;

        let err = engine_with_transaction.execute_commands("COMMIT;").err();

        assert!(matches!(
            err.unwrap(),
            CvsSqlError::FileCreatedUnexpectedly(_)
        ));

        Ok(())
    }

    #[test]
    fn test_err_in_read_only_mode() -> Result<(), CvsSqlError> {
        let working_dir = tempdir()?;
        fs::create_dir_all(&working_dir)?;

        let args = Args {
            home: Some(working_dir.path().to_path_buf()),
            writer_mode: false,
            ..Args::default()
        };
        let engine_with_transaction = Engine::try_from(&args)?;
        engine_with_transaction.execute_commands("START TRANSACTION;")?;
        engine_with_transaction.execute_commands("CREATE TABLE tab(a0 INT)")?;

        let err = engine_with_transaction.execute_commands("COMMIT;").err();

        assert!(matches!(err.unwrap(), CvsSqlError::ReadOnlyMode));

        Ok(())
    }
}
