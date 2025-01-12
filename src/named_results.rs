use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::results::Column;
use crate::results::ResultName;
use crate::results::{ColumnName, ResultSet};
use crate::util::SmartReference;
use crate::value::Value;

struct NamedResultSet {
    name: Rc<ResultName>,
    results: Box<dyn ResultSet>,
}

impl ResultSet for NamedResultSet {
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
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        Some(&self.name)
    }
    fn next_if_possible(&mut self) -> bool {
        self.results.next_if_possible()
    }
    fn revert(&mut self) {
        self.results.revert()
    }
    fn get<'a>(&'a self, column: &Column) -> SmartReference<'a, Value> {
        self.results.get(column)
    }
}
pub fn alias_results(alias: &Ident, results: Box<dyn ResultSet>) -> Box<dyn ResultSet> {
    let name = Rc::new(ResultName::root().append(&alias.value));
    Box::new(NamedResultSet { name, results })
}
