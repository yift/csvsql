use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::results::Column;
use crate::results::ResultSet;
use crate::results::ResultSetMetadata;
use crate::simple_result_set_metadata::SimpleResultSetMetadata;
use crate::util::SmartReference;
use crate::value::Value;

struct NamedResultSet {
    metadata: Rc<dyn ResultSetMetadata>,
    results: Box<dyn ResultSet>,
}

impl ResultSet for NamedResultSet {
    fn next_if_possible(&mut self) -> bool {
        self.results.next_if_possible()
    }
    fn revert(&mut self) {
        self.results.revert()
    }
    fn get<'a>(&'a self, column: &Column) -> SmartReference<'a, Value> {
        self.results.get(column)
    }
    fn metadate(&self) -> &Rc<dyn ResultSetMetadata> {
        &self.metadata
    }
}
pub fn alias_results(alias: &Ident, results: Box<dyn ResultSet>) -> Box<dyn ResultSet> {
    let name = alias.value.as_str();
    let mut metadata = SimpleResultSetMetadata::new(Some(name.into()));
    for column in 0..results.metadate().number_of_columns() {
        if let Some(name) = results.metadate().column_name(&Column::from_index(column)) {
            metadata.add_column(name.short_name());
        }
    }

    let metadata = Rc::new(metadata);
    Box::new(NamedResultSet { metadata, results })
}
