use std::{collections::HashMap, ops::Deref, rc::Rc};

use sqlparser::ast::Expr;

use crate::{
    engine::Engine,
    error::CdvSqlError,
    projections::{Projection, SingleConvert},
    results::{Column, ResultSet},
    value::Value,
};
struct FilteredResults {
    results: Box<dyn ResultSet>,
    condition: Box<dyn Projection>,
}
impl ResultSet for FilteredResults {
    fn number_of_columns(&self) -> usize {
        self.results.number_of_columns()
    }
    fn column_name(&self, column: &Column) -> Option<crate::results::ColumnName> {
        self.results.column_name(column)
    }
    fn column_index(&self, name: &crate::results::ColumnName) -> Option<Column> {
        self.results.column_index(name)
    }
    fn result_name(&self) -> Option<&Rc<crate::results::ResultName>> {
        self.results.result_name()
    }
    fn revert(&mut self) {
        self.results.revert();
    }
    fn get<'a>(&'a self, column: &Column) -> crate::util::SmartReference<'a, crate::value::Value> {
        self.results.get(column)
    }
    fn next_if_possible(&mut self) -> bool {
        while self.results.next_if_possible() {
            if self.condition.get(self.results.deref()).deref() == &Value::Bool(true) {
                return true;
            }
        }
        false
    }
}

pub fn make_filter(
    engine: &Engine,
    filter: &Option<Expr>,
    results: Box<dyn ResultSet>,
) -> Result<Box<dyn ResultSet>, CdvSqlError> {
    let Some(condition) = filter else {
        return Ok(results);
    };
    let condition = condition.convert_single(results.deref(), engine)?;

    Ok(Box::new(FilteredResults { results, condition }))
}
