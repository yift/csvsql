use std::ops::Deref;

use crate::error::CvsSqlError;
use crate::group_by::{GroupRow, GroupedResultSet};
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
        .map(|d| GroupRow {
            group_rows: vec![],
            data: d,
        })
        .filter(|row| condition.get(row).deref() == &Value::Bool(true))
        .map(|r| r.data)
        .collect();
    let data = ResultsData::new(data);
    Ok(ResultSet {
        metadata: results.metadata,
        data,
    })
}
pub fn apply_having(
    engine: &Engine,
    filter: &Option<Expr>,
    results: &mut GroupedResultSet,
) -> Result<(), CvsSqlError> {
    let Some(condition) = filter else {
        return Ok(());
    };
    let condition = condition.convert_single(&results.metadata, engine)?;
    results
        .rows
        .retain(|row| condition.get(row).deref() == &Value::Bool(true));
    Ok(())
}
