use std::cmp::Ordering;

use crate::{results::Column, value::Value};

pub struct DataRow {
    row: Vec<Value>,
}
impl DataRow {
    pub fn get<'a>(&'a self, column: &Column) -> &'a Value {
        self.row.get(column.get_index()).unwrap_or(&Value::Empty)
    }
    pub(crate) fn new(row: Vec<Value>) -> Self {
        Self { row }
    }
}
pub struct ResultsData {
    rows: Vec<DataRow>,
}
impl ResultsData {
    pub fn iter(&self) -> impl Iterator<Item = &DataRow> {
        self.rows.iter()
    }
    pub fn into_iter(self) -> impl Iterator<Item = DataRow> {
        self.rows.into_iter()
    }
    pub(crate) fn new(rows: Vec<DataRow>) -> Self {
        Self { rows }
    }
    pub(crate) fn sort_by(&mut self, by: impl FnMut(&DataRow, &DataRow) -> Ordering) {
        self.rows.sort_by(by);
    }
}
