use std::{fmt::Display, rc::Rc};

use crate::value::Value;

#[derive(Clone)]
pub struct Row {
    row: usize,
}

impl Row {
    pub fn get_index(&self) -> usize {
        self.row
    }
    pub fn from_index(row: usize) -> Self {
        Self { row }
    }
}

#[derive(Clone)]
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

pub struct ResultName {
    elements: Vec<String>,
}

#[derive(Clone)]
pub struct ColumnName {
    parent: Rc<ResultName>,
    name: String,
}

impl ResultName {
    pub fn root() -> Self {
        Self {
            elements: Vec::new(),
        }
    }
    pub fn append(&self, name: &str) -> Self {
        let mut elements = self.elements.clone();
        let name = name.to_string();
        elements.push(name);
        Self { elements }
    }
    pub fn parent(&self) -> Self {
        let mut elements = self.elements.clone();
        elements.pop();
        Self { elements }
    }
    pub fn matches(&self, other: &Self) -> bool {
        // self can be longer then other
        if let Some(other_name) = other.elements.last() {
            if let Some(my_name) = self.elements.last() {
                if my_name == other_name {
                    self.parent().matches(&other.parent())
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            true
        }
    }
    pub fn names(&self) -> &[String] {
        &self.elements
    }
}
impl ColumnName {
    pub fn new(parent: &Rc<ResultName>, name: &str) -> Self {
        Self {
            parent: parent.clone(),
            name: name.to_string(),
        }
    }
    pub fn simple(name: &str) -> Self {
        Self {
            parent: Rc::new(ResultName::root()),
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn parent(&self) -> &Rc<ResultName> {
        &self.parent
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

pub trait ResultSet {
    fn number_of_rows(&self) -> usize;
    fn number_of_columns(&self) -> usize;
    fn column_name(&self, column: &Column) -> Option<ColumnName>;
    fn column_index(&self, name: &ColumnName) -> Option<&Column>;
    fn result_name(&self) -> Option<&Rc<ResultName>>;
    fn get(&self, row: &Row, column: &Column) -> &Value;

    fn value(&self, row: &Row, name: &ColumnName) -> &Value {
        match self.column_index(name) {
            Some(column) => self.get(row, column),
            None => &Value::Empty,
        }
    }
    fn rows(&self) -> Box<dyn Iterator<Item = Row>> {
        Box::new((0..self.number_of_rows()).map(|row| Row { row }))
    }
    fn columns(&self) -> Box<dyn Iterator<Item = Column>> {
        Box::new((0..self.number_of_columns()).map(|column| Column { column }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn result_name_matches_two_empty_results() {
        let first = ResultName::root();
        let second = ResultName::root();

        assert!(first.matches(&second));
    }

    #[test]
    fn result_name_matches_empty_results_with_full() {
        let first = ResultName::root().append("one").append("two");
        let second = ResultName::root();

        assert!(first.matches(&second));
    }

    #[test]
    fn result_name_matches_full_results_with_full() {
        let first = ResultName::root().append("one").append("two");
        let second = ResultName::root().append("two");

        assert!(first.matches(&second));
    }

    #[test]
    fn result_name_matches_full_results_with_full_other() {
        let first = ResultName::root().append("one").append("two");
        let second = ResultName::root().append("one").append("Two");

        assert!(!first.matches(&second));
    }

    #[test]
    fn result_name_matches_empty_results_with_full_too_short() {
        let first = ResultName::root().append("two");
        let second = ResultName::root().append("one").append("two");

        assert!(!first.matches(&second));
    }

    /*
        pub fn matches(&self, other: &Self) -> bool {
        // self can be longer then other
        if let Some(other_name) = other.elements.last() {
            if let Some(my_name) = self.elements.last() {
                if my_name == other_name {
                    self.parent().matches(&other.parent())
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            true
        }
    }

     */
}
