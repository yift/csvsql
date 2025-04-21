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
    let file = engine.file_name(name)?;
    if !file.exists {
        return Err(CvsSqlError::TableNotExists(file.result_name.full_name()));
    }

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(engine.first_line_as_name)
        .from_path(file.path)?;

    let mut metadata = SimpleResultSetMetadata::new(Some(file.result_name));

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
#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use crate::{args::Args, engine::Engine, error::CvsSqlError, results::Column};

    #[test]
    fn read_file_with_missing_headers() -> Result<(), CvsSqlError> {
        let working_dir = tempdir()?;
        fs::create_dir_all(&working_dir)?;
        let table = working_dir.path().join("tab.csv");
        fs::write(table, "col1\n1,2\n2\n")?;

        let args = Args {
            command: None,
            first_line_as_name: true,
            home: Some(working_dir.path().to_path_buf()),
        };
        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands("SELECT * FROM tab")?;
        assert_eq!(results.len(), 1);
        let results = results.first().unwrap();
        assert_eq!(results.metadata.number_of_columns(), 2);

        assert_eq!(
            results
                .metadata
                .column_name(&Column::from_index(0))
                .unwrap()
                .short_name(),
            "col1"
        );
        assert_eq!(
            results
                .metadata
                .column_name(&Column::from_index(1))
                .unwrap()
                .short_name(),
            "B$"
        );
        assert_eq!(results.data.iter().count(), 2);

        let results = engine.execute_commands("SELECT col1, B$ FROM tab")?;
        assert_eq!(results.len(), 1);
        let results = results.first().unwrap();
        assert_eq!(results.data.iter().count(), 2);

        Ok(())
    }

    #[test]
    fn read_file_no_headers() -> Result<(), CvsSqlError> {
        let working_dir = tempdir()?;
        fs::create_dir_all(&working_dir)?;
        let table = working_dir.path().join("tab.csv");
        fs::write(table, "col1\n1,2\n2\n")?;

        let args = Args {
            command: None,
            first_line_as_name: false,
            home: Some(working_dir.path().to_path_buf()),
        };
        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands("SELECT * FROM tab")?;
        assert_eq!(results.len(), 1);
        let results = results.first().unwrap();
        assert_eq!(results.metadata.number_of_columns(), 2);

        assert_eq!(
            results
                .metadata
                .column_name(&Column::from_index(0))
                .unwrap()
                .short_name(),
            "A$"
        );
        assert_eq!(
            results
                .metadata
                .column_name(&Column::from_index(1))
                .unwrap()
                .short_name(),
            "B$"
        );
        assert_eq!(results.data.iter().count(), 3);

        let results = engine.execute_commands("SELECT A$, B$ FROM tab")?;
        assert_eq!(results.len(), 1);
        let results = results.first().unwrap();
        assert_eq!(results.data.iter().count(), 3);

        Ok(())
    }
}
