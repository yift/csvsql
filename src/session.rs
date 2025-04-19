use std::{collections::HashMap, path::PathBuf};

use tempfile::NamedTempFile;

use crate::{error::CvsSqlError, results::Name};

#[derive(Default)]
pub(crate) struct Session {
    temporary_tables: HashMap<String, NamedTempFile>,
}

impl Session {
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
}
