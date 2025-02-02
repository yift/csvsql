use std::collections::{hash_map::Entry, HashMap};

use crate::{
    results::{Column, ColumnIndexError, Name, ResultSetMetadata},
    util::SmartReference,
};

enum ColumnInResult {
    Column(Column),
    Ambiguous,
}

pub(crate) struct SimpleResultSetMetadata {
    columns: Vec<Name>,
    name: Option<Name>,
    column_names: HashMap<Name, ColumnInResult>,
}
impl ResultSetMetadata for SimpleResultSetMetadata {
    fn number_of_columns(&self) -> usize {
        self.columns.len()
    }
    fn column_name(&self, column: &Column) -> Option<&Name> {
        self.columns.get(column.get_index())
    }

    fn result_name(&self) -> Option<&Name> {
        self.name.as_ref()
    }
    fn column_index(&self, name: &Name) -> Result<SmartReference<Column>, ColumnIndexError> {
        match self.column_names.get(name) {
            None => Err(ColumnIndexError::NoSuchColumn(name.full_name())),
            Some(ColumnInResult::Ambiguous) => {
                Err(ColumnIndexError::AmbiguousColumnName(name.full_name()))
            }
            Some(ColumnInResult::Column(c)) => Ok(SmartReference::Borrowed(c)),
        }
    }
}
impl SimpleResultSetMetadata {
    pub(crate) fn new(name: Option<Name>) -> Self {
        Self {
            columns: vec![],
            name,
            column_names: HashMap::new(),
        }
    }
    pub(crate) fn add_column(&mut self, name: &str) {
        let index = Column::from_index(self.columns.len());
        let full_name = match &self.name {
            Some(parent_name) => parent_name.append(name),
            None => name.into(),
        };
        self.set_name_to_index(&index, &full_name);
        self.columns.push(full_name);
    }
    fn set_name_to_index(&mut self, index: &Column, name: &Name) {
        for name in name.available_names() {
            match self.column_names.entry(name) {
                Entry::Vacant(entry) => {
                    entry.insert(ColumnInResult::Column(index.clone()));
                }
                Entry::Occupied(mut entry) => {
                    entry.insert(ColumnInResult::Ambiguous);
                }
            };
        }
    }
}
