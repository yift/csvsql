use std::ops::Deref;

use bigdecimal::ToPrimitive;
use sqlparser::ast::{Expr, Offset};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    group_by::{GroupRow, GroupedResultSet},
    projections::SingleConvert,
    result_set_metadata::{Metadata, SimpleResultSetMetadata},
    results_data::DataRow,
    value::Value,
};

pub(crate) fn trim(
    limit: &Option<Expr>,
    offset: &Option<Offset>,
    engine: &Engine,
    results: &mut GroupedResultSet,
) -> Result<(), CvsSqlError> {
    if let Some(offset) = offset {
        let metadata = Metadata::Simple(SimpleResultSetMetadata::new(None));
        let offset = offset.value.convert_single(&metadata, engine)?;
        let data_row = DataRow::new(vec![]);
        let temp_row = GroupRow {
            data: data_row,
            group_rows: vec![],
        };
        let offset = offset.get(&temp_row);

        let Value::Number(offset) = offset.deref() else {
            return Err(CvsSqlError::NoNumericOffset);
        };
        let Some(offset) = offset.to_usize() else {
            return Err(CvsSqlError::NoNumericOffset);
        };
        if offset >= results.rows.len() {
            results.rows.clear();
            return Ok(());
        }

        results.rows.drain(0..offset);
    }
    if let Some(limit) = limit {
        let metadata = Metadata::Simple(SimpleResultSetMetadata::new(None));
        let limit = limit.convert_single(&metadata, engine)?;
        let data_row = DataRow::new(vec![]);
        let temp_row = GroupRow {
            data: data_row,
            group_rows: vec![],
        };
        let limit = limit.get(&temp_row);

        let Value::Number(limit) = limit.deref() else {
            return Err(CvsSqlError::NoNumericLimit);
        };
        let Some(limit) = limit.to_usize() else {
            return Err(CvsSqlError::NoNumericLimit);
        };
        results.rows.truncate(limit);
    }

    Ok(())
}
