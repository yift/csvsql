use std::rc::Rc;

use sqlparser::ast::Ident;

use crate::result_set_metadata::SimpleResultSetMetadata;
use crate::results::Column;
use crate::results::ResultSet;

pub fn alias_results(alias: &Ident, results: ResultSet) -> ResultSet {
    let name = alias.value.as_str();
    let mut metadata = SimpleResultSetMetadata::new(Some(name.into()));
    for column in 0..results.metadata.number_of_columns() {
        if let Some(name) = results.metadata.column_name(&Column::from_index(column)) {
            metadata.add_column(name.short_name());
        }
    }

    let metadata = Rc::new(metadata.build());
    ResultSet {
        metadata,
        data: results.data,
    }
}
