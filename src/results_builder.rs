use std::rc::Rc;

use crate::{
    error::CvsSqlError,
    result_set_metadata::{Metadata, SimpleResultSetMetadata},
    results::ResultSet,
    results_data::{DataRow, ResultsData},
    value::Value,
};

pub(crate) fn build_empty_results(cols: &[&str]) -> Result<ResultSet, CvsSqlError> {
    let mut metadata = SimpleResultSetMetadata::new(None);
    for col in cols {
        metadata.add_column(col);
    }
    let metadata = Metadata::Simple(metadata);

    let row = DataRow::new(vec![]);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };
    Ok(results)
}

pub(crate) fn build_simple_results(data: Vec<(&str, Value)>) -> Result<ResultSet, CvsSqlError> {
    let mut metadata = SimpleResultSetMetadata::new(None);
    for col in &data {
        metadata.add_column(col.0);
    }
    let metadata = Metadata::Simple(metadata);

    let mut row = vec![];
    for col in data {
        row.push(col.1);
    }
    let row = DataRow::new(row);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };
    Ok(results)
}
