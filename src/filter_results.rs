use std::ops::Deref;

use crate::error::CvsSqlError;
use crate::results_data::ResultsData;
use crate::{engine::Engine, projections::SingleConvert, results::ResultSet, value::Value};
use sqlparser::ast::Expr;
pub fn make_filter(
    engine: &Engine,
    filter: &Option<Expr>,
    results: ResultSet,
) -> Result<ResultSet, CvsSqlError> {
    let Some(condition) = filter else {
        return Ok(results);
    };
    let condition = condition.convert_single(&results.metadata, engine)?;
    let data = results
        .data
        .into_iter()
        .filter(|row| condition.get(row).deref() == &Value::Bool(true))
        .collect();
    let data = ResultsData::new(data);
    Ok(ResultSet {
        metadata: results.metadata,
        data,
    })
}
