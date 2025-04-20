use std::{ops::Deref, rc::Rc};

use sqlparser::ast::{Ident, ObjectName};
use thiserror::Error;

use crate::{
    result_set_metadata::Metadata,
    results_data::{DataRow, ResultsData},
    value::Value,
};

#[derive(Clone, Debug)]
pub struct Column {
    column: usize,
}
impl Column {
    pub fn get_index(&self) -> usize {
        self.column
    }
    pub fn from_index(column: usize) -> Self {
        Self { column }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Name {
    elements: Vec<String>,
}

impl Name {
    pub fn append(&self, name: &str) -> Self {
        let mut elements = self.elements.clone();
        let name = name.to_string();
        elements.push(name);
        Self { elements }
    }
    pub fn parent(&self) -> Option<Self> {
        if self.elements.len() <= 1 {
            None
        } else {
            let mut elements = self.elements.clone();
            elements.pop();
            Some(Self { elements })
        }
    }
    pub fn full_name(&self) -> String {
        self.elements.join(".")
    }
    pub fn short_name(&self) -> &str {
        let name = match self.elements.last() {
            None => "",
            Some(name) => name,
        };
        name
    }
    pub fn available_names(&self) -> Vec<Self> {
        let short_name = self.short_name();
        let mut parent_list = match self.parent() {
            Some(parent) => parent
                .available_names()
                .iter()
                .map(|f| f.append(short_name))
                .collect(),
            None => vec![],
        };
        parent_list.push(short_name.into());
        parent_list
    }
}

impl From<&str> for Name {
    fn from(value: &str) -> Self {
        Self {
            elements: vec![value.to_string()],
        }
    }
}
impl From<String> for Name {
    fn from(value: String) -> Self {
        Self {
            elements: vec![value],
        }
    }
}
impl From<Vec<String>> for Name {
    fn from(value: Vec<String>) -> Self {
        Self { elements: value }
    }
}
impl From<&ObjectName> for Name {
    fn from(value: &ObjectName) -> Self {
        Self {
            elements: value.0.iter().map(|i| i.to_string()).collect(),
        }
    }
}
impl From<&Ident> for Name {
    fn from(value: &Ident) -> Self {
        Self {
            elements: vec![value.value.to_string()],
        }
    }
}

#[derive(Error, Debug)]
pub enum ColumnIndexError {
    #[error("Cannot find columns: `{0}`")]
    NoSuchColumn(String),
    #[error("Ambiguous column name: `{0}`")]
    AmbiguousColumnName(String),
}

pub struct ResultSet {
    pub metadata: Rc<Metadata>,
    pub data: ResultsData,
}
impl ResultSet {
    pub fn columns(&self) -> Box<dyn Iterator<Item = Column>> {
        self.metadata.columns()
    }
    pub fn value<'a>(&self, name: &Name, row: &'a DataRow) -> &'a Value {
        match self.metadata.column_index(name) {
            Ok(column) => row.get(column.deref()),
            Err(_) => &Value::Empty,
        }
    }
}
