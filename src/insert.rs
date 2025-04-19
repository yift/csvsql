use std::{fs::OpenOptions, rc::Rc};

use crate::{result_set_metadata::Metadata, value::Value, writer::Writer};
use bigdecimal::BigDecimal;
use bigdecimal::FromPrimitive;
use sqlparser::ast::{Insert, TableObject};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    extractor::Extractor,
    file_results::read_file,
    result_set_metadata::SimpleResultSetMetadata,
    results::{Name, ResultSet},
    results_data::{DataRow, ResultsData},
    writer::new_csv_writer,
};

impl Extractor for Insert {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.or.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with or".into()));
        }
        if self.ignore {
            return Err(CvsSqlError::Unsupported("INSERT IGNORE".into()));
        }
        if self.table_alias.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with alias".into()));
        }
        if !self.assignments.is_empty() {
            return Err(CvsSqlError::Unsupported("INSERT with assignments".into()));
        }
        if self.overwrite {
            return Err(CvsSqlError::Unsupported("INSERT with overwrite".into()));
        }
        if self.partitioned.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with partitioned".into()));
        }
        if !self.after_columns.is_empty() {
            return Err(CvsSqlError::Unsupported("INSERT with after_columns".into()));
        }
        if self.on.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with on".into()));
        }
        if self.returning.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with returning".into()));
        }
        if self.replace_into {
            return Err(CvsSqlError::Unsupported("INSERT with replace_into".into()));
        }
        if self.priority.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with priority".into()));
        }
        if self.insert_alias.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with insert_alias".into()));
        }
        if self.settings.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with settings".into()));
        }
        if self.format_clause.is_some() {
            return Err(CvsSqlError::Unsupported("INSERT with format_clause".into()));
        }

        let TableObject::TableName(name) = &self.table else {
            return Err(CvsSqlError::Unsupported("INSERT with a table name".into()));
        };

        let current_data = read_file(engine, name)?;
        let file = engine.file_name(name)?;

        let mut columns = vec![];
        if self.columns.is_empty() {
            for col in current_data.columns() {
                columns.push(col);
            }
        } else {
            for name in &self.columns {
                let name = name.to_string();
                let name: Name = name.into();
                let col = current_data.metadata.column_index(&name)?;
                columns.push(col.clone());
            }
        };
        let mut metadata = SimpleResultSetMetadata::new(None);
        for col in &columns {
            metadata.add_column(format!("{}", col.get_index()).as_str());
        }

        let Some(source) = &self.source else {
            return Err(CvsSqlError::NoInsertSource);
        };
        let data_to_insert = source.extract(engine)?;
        if data_to_insert.metadata.number_of_columns() != columns.len() {
            dbg!(data_to_insert.metadata.number_of_columns());
            dbg!(columns.len());
            return Err(CvsSqlError::InsertMismatch);
        }
        let mut rows = vec![];
        for row in data_to_insert.data.iter() {
            let mut values = vec![];
            for col in &columns {
                let data = row.get(col);
                values.push(data.clone());
            }
            let values = DataRow::new(values);
            rows.push(values);
        }
        let len = rows.len();

        let metadata = Rc::new(metadata.build());
        let data = ResultsData::new(rows);
        let results = ResultSet { metadata, data };
        let file = OpenOptions::new().append(true).open(file.path)?;
        let mut writer = new_csv_writer(file);
        writer.append(&results)?;

        let mut metadata = SimpleResultSetMetadata::new(None);
        metadata.add_column("action");
        metadata.add_column("number_of_rows");
        let metadata = Metadata::Simple(metadata);

        let row = vec![
            Value::Str("INSERT".to_string()),
            Value::Number(BigDecimal::from_usize(len).unwrap()),
        ];
        let row = DataRow::new(row);
        let data = vec![row];
        let data = ResultsData::new(data);
        let metadata = Rc::new(metadata);
        let results = ResultSet { metadata, data };

        Ok(results)
    }
}
