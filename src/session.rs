use std::{collections::HashMap, path::PathBuf};

use tempfile::NamedTempFile;

use crate::{error::CvsSqlError, results::Name, transaction::Transaction};

#[derive(Default)]
pub(crate) struct Session {
    temporary_tables: TemporaryFiles,
    pub(crate) transaction: Option<Transaction>,
}

#[derive(Default)]
pub(crate) struct TemporaryFiles {
    temporary_tables: HashMap<String, NamedTempFile>,
}

impl TemporaryFiles {
    pub(crate) fn create_temporary_table(&mut self, name: &Name) -> Result<PathBuf, CvsSqlError> {
        let name = name.full_name();
        if self.temporary_tables.contains_key(&name) {
            return Err(CvsSqlError::TableAlreadyExists(name));
        }
        let temporary_file = NamedTempFile::with_suffix(".csv")?;
        let path = temporary_file.path().to_path_buf();

        self.temporary_tables.insert(name, temporary_file);

        Ok(path)
    }

    pub(crate) fn drop_temporary_table(&mut self, name: &Name) -> Result<(), CvsSqlError> {
        let name = name.full_name();
        if self.temporary_tables.remove(&name).is_none() {
            Err(CvsSqlError::TableNotExists(name))
        } else {
            Ok(())
        }
    }

    pub(crate) fn get_temporary_table(&self, name: &Name) -> Option<PathBuf> {
        let name = name.full_name();
        let file = self.temporary_tables.get(&name)?;
        Some(file.path().to_path_buf())
    }

    pub(crate) fn commit(&mut self, session: &mut TemporaryFiles) {
        for (name, file) in self.temporary_tables.drain() {
            session.temporary_tables.insert(name, file);
        }
    }
}

impl Session {
    pub(crate) fn create_temporary_table(&mut self, name: &Name) -> Result<PathBuf, CvsSqlError> {
        if let Some(ref mut transaction) = self.transaction {
            transaction.temporary_tables.create_temporary_table(name)
        } else {
            self.temporary_tables.create_temporary_table(name)
        }
    }

    pub(crate) fn drop_temporary_table(&mut self, name: &Name) -> Result<(), CvsSqlError> {
        if let Some(ref mut transaction) = self.transaction {
            transaction.temporary_tables.drop_temporary_table(name)
        } else {
            self.temporary_tables.drop_temporary_table(name)
        }
    }

    pub(crate) fn get_temporary_table(&self, name: &Name) -> Option<PathBuf> {
        if let Some(transaction) = &self.transaction
            && let Some(path) = transaction.temporary_tables.get_temporary_table(name)
        {
            return Some(path);
        }
        self.temporary_tables.get_temporary_table(name)
    }

    pub(crate) fn start_transaction(&mut self) -> Result<(), CvsSqlError> {
        if self.transaction.is_some() {
            return Err(CvsSqlError::TransactionInProgress);
        }
        self.transaction = Some(Transaction::default());
        Ok(())
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<(), CvsSqlError> {
        let Some(ref mut transaction) = self.transaction.take() else {
            return Err(CvsSqlError::NoTransactionInProgress);
        };
        transaction
            .temporary_tables
            .commit(&mut self.temporary_tables);

        transaction.commit()?;

        Ok(())
    }

    pub(crate) fn rollback_transaction(&mut self) -> Result<(), CvsSqlError> {
        let Some(ref mut transaction) = self.transaction.take() else {
            return Err(CvsSqlError::NoTransactionInProgress);
        };
        transaction.rollback()?;

        Ok(())
    }
}
