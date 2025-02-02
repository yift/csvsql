use std::{ops::Deref, rc::Rc};

use crate::error::CvsSqlError;
use crate::results::ResultSetMetadata;
use crate::{
    engine::Engine,
    projections::{Projection, SingleConvert},
    results::{Column, ResultSet},
    value::Value,
};
use sqlparser::ast::Expr;
struct FilteredResults {
    results: Box<dyn ResultSet>,
    condition: Box<dyn Projection>,
}
impl ResultSet for FilteredResults {
    fn metadate(&self) -> &Rc<dyn ResultSetMetadata> {
        self.results.metadate()
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
) -> Result<Box<dyn ResultSet>, CvsSqlError> {
    let Some(condition) = filter else {
        return Ok(results);
    };
    let condition = condition.convert_single(results.deref(), engine)?;

    Ok(Box::new(FilteredResults { results, condition }))
}
