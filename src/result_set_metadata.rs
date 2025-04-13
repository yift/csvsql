use std::{
    collections::{HashMap, hash_map::Entry},
    rc::Rc,
};

use crate::{
    results::{Column, ColumnIndexError, Name},
    util::SmartReference,
};

pub enum Metadata {
    Simple(SimpleResultSetMetadata),
    Product(ProductResultSetMetadata),
    Grouped {
        parent: Rc<Metadata>,
        this: Box<Metadata>,
    },
}
impl Metadata {
    pub fn column_index(&self, name: &Name) -> Result<SmartReference<Column>, ColumnIndexError> {
        match self {
            Metadata::Simple(data) => data.column_index(name),
            Metadata::Product(data) => data.column_index(name),
            Metadata::Grouped { parent: _, this } => this.column_index(name),
        }
    }
    pub fn column_name(&self, column: &Column) -> Option<&Name> {
        match self {
            Metadata::Simple(data) => data.column_name(column),
            Metadata::Product(data) => data.column_name(column),
            Metadata::Grouped { parent: _, this } => this.column_name(column),
        }
    }
    pub fn number_of_columns(&self) -> usize {
        match self {
            Metadata::Simple(data) => data.columns.len(),
            Metadata::Product(data) => {
                data.left.number_of_columns() + data.right.number_of_columns()
            }
            Metadata::Grouped { parent: _, this } => this.number_of_columns(),
        }
    }
    pub fn result_name(&self) -> Option<&Name> {
        match self {
            Metadata::Simple(data) => data.name.as_ref(),
            Metadata::Product(_) => None,
            Metadata::Grouped { parent: _, this } => this.result_name(),
        }
    }
    pub(crate) fn product(left: &Rc<Self>, right: &Rc<Self>) -> Self {
        let left = left.clone();
        let right = right.clone();
        Metadata::Product(ProductResultSetMetadata { left, right })
    }

    pub fn columns(&self) -> Box<dyn Iterator<Item = Column>> {
        Box::new((0..self.number_of_columns()).map(Column::from_index))
    }
}
pub struct ProductResultSetMetadata {
    left: Rc<Metadata>,
    right: Rc<Metadata>,
}
impl ProductResultSetMetadata {
    fn column_index(
        &self,
        name: &crate::results::Name,
    ) -> Result<SmartReference<Column>, ColumnIndexError> {
        let left_result = self.left.column_index(name);
        let right_result = self.right.column_index(name);
        match (&left_result, &right_result) {
            (Err(ColumnIndexError::AmbiguousColumnName(_)), _) => left_result,
            (_, Err(ColumnIndexError::AmbiguousColumnName(_))) => right_result,
            (Ok(_), Ok(_)) => Err(ColumnIndexError::AmbiguousColumnName(name.full_name())),
            (Ok(_), Err(ColumnIndexError::NoSuchColumn(_))) => left_result,
            (Err(ColumnIndexError::NoSuchColumn(_)), Ok(right_result)) => {
                let col =
                    Column::from_index(right_result.get_index() + self.left.number_of_columns());
                Ok(SmartReference::Owned(col))
            }
            (Err(ColumnIndexError::NoSuchColumn(_)), Err(ColumnIndexError::NoSuchColumn(_))) => {
                right_result
            }
        }
    }
    fn column_name(&self, column: &Column) -> Option<&Name> {
        self.left.column_name(column).or_else(|| {
            self.right.column_name(&Column::from_index(
                column.get_index() - self.left.number_of_columns(),
            ))
        })
    }
}

enum ColumnInResult {
    Column(Column),
    Ambiguous,
}

pub struct SimpleResultSetMetadata {
    columns: Vec<Name>,
    name: Option<Name>,
    column_names: HashMap<Name, ColumnInResult>,
}
impl SimpleResultSetMetadata {
    fn column_name(&self, column: &Column) -> Option<&Name> {
        self.columns.get(column.get_index())
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
    pub(crate) fn build(self) -> Metadata {
        Metadata::Simple(self)
    }
    pub(crate) fn len(&self) -> usize {
        self.columns.len()
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
