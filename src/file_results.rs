use std::rc::Rc;

use csv::ReaderBuilder;
use sqlparser::ast::ObjectName;

use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::result_set_metadata::SimpleResultSetMetadata;
use crate::results_data::{DataRow, ResultsData};
use crate::{results::ResultSet, value::Value};

fn get_default_header(index: usize) -> String {
    let mut index = index;
    let mut title = String::from("$");
    let first = 'A' as usize;
    let size = 'Z' as usize - first + 1;
    loop {
        let chr = index % (size);
        index -= chr;
        title.insert(0, char::from((chr + first) as u8));
        if index == 0 {
            break;
        }
        index = index / size - 1;
    }
    title
}

pub fn read_file(engine: &Engine, name: &ObjectName) -> Result<ResultSet, CvsSqlError> {
    let (path, result_name) = engine.file_name(name);
    if !path.exists() {
        return Err(CvsSqlError::TableNotExists(
            path.to_str().unwrap_or_default().to_string(),
        ));
    }

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(engine.first_line_as_name)
        .from_path(path)?;

    let mut metadata = SimpleResultSetMetadata::new(result_name);

    if engine.first_line_as_name {
        let header = reader.headers()?;
        for h in header {
            metadata.add_column(h);
        }
    }
    let mut data = Vec::new();
    for records in reader.records() {
        let mut values = Vec::new();
        let records = records?;
        for (index, record) in records.iter().enumerate() {
            let value = Value::from(record);
            values.push(value);
            if index >= metadata.len() {
                metadata.add_column(&get_default_header(index));
            }
        }
        let values = DataRow::new(values);
        data.push(values);
    }
    let metadata = Rc::new(metadata.build());
    let data = ResultsData::new(data);
    let results = ResultSet { metadata, data };

    Ok(results)
}
