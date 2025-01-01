use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::results::Column;
use crate::results::ResultName;
use crate::results::Row;
use crate::results::{ColumnName, ResultSet};
use crate::util::SmartReference;
use crate::value::Value;

struct NamedResultSet {
    name: Rc<ResultName>,
    results: Box<dyn ResultSet>,
}

impl ResultSet for NamedResultSet {
    fn number_of_rows(&self) -> usize {
        self.results.number_of_rows()
    }
    fn number_of_columns(&self) -> usize {
        self.results.number_of_columns()
    }
    fn column_name(&self, column: &Column) -> Option<ColumnName> {
        self.results
            .column_name(column)
            .map(|name| ColumnName::new(&self.name, name.name()))
    }
    fn column_index(&self, name: &ColumnName) -> Option<Column> {
        if self.name.matches(name.parent()) {
            let name = match self.results.result_name() {
                Some(parent) => ColumnName::new(parent, name.name()),
                None => ColumnName::simple(name.name()),
            };
            self.results.column_index(&name)
        } else {
            None
        }
    }

    fn get(&self, row: &Row, column: &Column) -> SmartReference<Value> {
        self.results.get(row, column)
    }
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        Some(&self.name)
    }
}
pub fn alias_results(alias: &Ident, results: Box<dyn ResultSet>) -> Box<dyn ResultSet> {
    let name = Rc::new(ResultName::root().append(&alias.value));
    Box::new(NamedResultSet { name, results })
}
