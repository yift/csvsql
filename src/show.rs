use std::path::{self, Path};
use std::rc::Rc;
use std::time::SystemTime;
use std::{fs, path::PathBuf};

use chrono::{DateTime, Utc};
use sqlparser::dialect::Dialect;

use crate::dialect::FilesDialect;
use crate::result_set_metadata::{Metadata, SimpleResultSetMetadata};
use crate::results_data::ResultsData;
use crate::{
    engine::Engine, error::CvsSqlError, results::ResultSet, results_data::DataRow, value::Value,
};

pub(crate) fn show_tables(engine: &Engine, full: &bool) -> Result<ResultSet, CvsSqlError> {
    let home = engine.home();
    let mut rows = vec![];
    dir(&home, &mut rows, full, "")?;

    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("table");
    metadata.add_column("file_size");
    metadata.add_column("created_at");
    metadata.add_column("modified_at");
    metadata.add_column("path");
    let metadata = Metadata::Simple(metadata);

    let data = ResultsData::new(rows);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };
    Ok(results)
}

fn get_table_name(file: &Path) -> Option<String> {
    let dialect = FilesDialect {};
    if file
        .extension()
        .and_then(|f| f.to_str())
        .unwrap_or_default()
        != "csv"
    {
        return None;
    }
    let stem = file.with_extension("");
    let name = stem
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or_default();
    match name.chars().next() {
        None => return None,
        Some(ch) => {
            if !dialect.is_identifier_start(ch) {
                return None;
            }
        }
    }
    if name.chars().any(|c| !dialect.is_identifier_part(c)) {
        return None;
    }

    Some(name.to_string())
}

fn dir(
    path: &PathBuf,
    results: &mut Vec<DataRow>,
    full: &bool,
    root: &str,
) -> Result<(), CvsSqlError> {
    let paths = fs::read_dir(path)?;
    for path in paths {
        let path = path?;
        let metadata = path.metadata()?;
        let file_name = path.file_name();
        let path = path.path();
        if metadata.is_dir() && *full {
            let name = file_name.to_str().unwrap_or_default();
            let name = format!("{}{}.", root, name);
            dir(&path, results, full, &name)?;
        } else if metadata.is_file() {
            let Some(name) = get_table_name(&path) else {
                continue;
            };
            let name = format!("{}{}", root, name);
            let len = metadata.len().into();
            let absolute = path::absolute(path)?;
            let absolute = absolute.to_str().unwrap_or_default().to_string();
            let data = vec![
                Value::Str(name),
                Value::Number(len),
                metadata.created().ok().into(),
                metadata.modified().ok().into(),
                Value::Str(absolute),
            ];
            let row = DataRow::new(data);
            results.push(row);
        }
    }

    Ok(())
}

pub(crate) fn show_databases(engine: &Engine) -> Result<ResultSet, CvsSqlError> {
    let home = engine.home();
    let mut rows = vec![];
    dir_dbs(&home, &mut rows, "")?;

    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("database");
    metadata.add_column("number_of_tables");
    metadata.add_column("created_at");
    metadata.add_column("path");
    let metadata = Metadata::Simple(metadata);

    let data = ResultsData::new(rows);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };
    Ok(results)
}

fn dir_dbs(path: &PathBuf, results: &mut Vec<DataRow>, root: &str) -> Result<u64, CvsSqlError> {
    let paths = fs::read_dir(path)?;
    let mut count = 0;
    for path in paths {
        let path = path?;
        let metadata = path.metadata()?;
        let file_name = path.file_name();
        let path = path.path();
        if metadata.is_dir() {
            let name = file_name.to_str().unwrap_or_default();
            let name = if root.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", root, name)
            };
            let tables = dir_dbs(&path, results, &name)?;
            if tables > 0 {
                let absolute = path::absolute(path)?;
                let absolute = absolute.to_str().unwrap_or_default().to_string();
                let data = vec![
                    Value::Str(name),
                    Value::Number(tables.into()),
                    metadata.created().ok().into(),
                    Value::Str(absolute),
                ];
                let row = DataRow::new(data);
                results.push(row);
            }
        } else if metadata.is_file() && get_table_name(&path).is_some() {
            count += 1;
        }
    }

    Ok(count)
}

impl From<Option<SystemTime>> for Value {
    fn from(value: Option<SystemTime>) -> Self {
        match value {
            None => Value::Empty,
            Some(ts) => {
                let utc: DateTime<Utc> = ts.into();
                let naive = utc.naive_utc();
                Value::Timestamp(naive)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{
        collections::HashMap,
        fs::{File, create_dir_all},
    };

    use crate::{args::Args, results::Column};
    use bigdecimal::{BigDecimal, FromPrimitive};
    use chrono::{Duration, NaiveDateTime};
    use tempfile::{TempDir, tempdir};

    use super::*;
    use std::io::Write;

    fn create_table(root: &Path, name: &str, content: &str) -> Result<(), CvsSqlError> {
        let mut table = File::create(root.join(name))?;
        writeln!(&mut table, "{}", content)?;
        Ok(())
    }
    fn create_db(root: &Path, name: &str) -> Result<PathBuf, CvsSqlError> {
        let path = root.join(name);
        create_dir_all(&path)?;
        Ok(path)
    }

    fn prepare_system() -> Result<TempDir, CvsSqlError> {
        let home = tempdir()?;
        let home_path = home.path().to_path_buf();
        let db1 = create_db(&home_path, "db1")?;
        let db2 = create_db(&home_path, "db2")?;
        let db3 = create_db(&db2, "db3")?;
        let empty = create_db(&home_path, "empty")?;

        create_table(&home_path, "table_one.csv", "test")?;
        create_table(&home_path, "table_two.csv", "another,test")?;
        create_table(&home_path, "table_three.csv", "one,more,another,test")?;
        create_table(&home_path, "not_csv", "one")?;
        create_table(&home_path, "0.start_with_digit.csv", "one")?;
        create_table(&home_path, "has-dash.csv", "one")?;
        create_table(&db1, "another_table.csv", "1")?;
        create_table(&db1, "yet_another_table.csv", "10")?;
        create_table(&db1, "and_one_more.csv", "12345")?;
        create_table(&db2, "more.csv", "abc")?;
        create_table(&db2, "even_more.csv", "123456")?;
        create_table(&db3, "in_3.csv", "12345")?;
        create_table(&db3, "another_in_3.csv", "123")?;
        create_table(&empty, "not_csv", "one")?;

        Ok(home)
    }

    #[test]
    fn test_show_tables() -> Result<(), CvsSqlError> {
        let started = Utc::now().naive_utc() - Duration::seconds(20);

        let home = prepare_system()?;
        let ready_at = Utc::now().naive_utc() + Duration::seconds(20);
        let args = Args {
            home: Some(home.path().to_path_buf()),
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands("SHOW TABLES;")?;
        assert_eq!(results.len(), 1);

        let results = &results.first().unwrap().results;
        assert_eq!(results.metadata.number_of_columns(), 5);

        let mut tables = HashMap::new();

        let table_col = Column::from_index(0);
        for (index, row) in results.data.iter().enumerate() {
            let name = row.get(&table_col).to_string();
            tables.insert(name, index);
        }

        assert_eq!(tables.len(), 3);

        let table_one = results.data.get(*tables.get("table_one").unwrap()).unwrap();
        verify_row_length(table_one, 5)?;
        verify_row_times(table_one, &started, &ready_at);

        let table_two = results.data.get(*tables.get("table_two").unwrap()).unwrap();
        verify_row_length(table_two, 13)?;
        verify_row_times(table_two, &started, &ready_at);

        let table_three = results
            .data
            .get(*tables.get("table_three").unwrap())
            .unwrap();
        verify_row_length(table_three, 22)?;
        verify_row_times(table_three, &started, &ready_at);

        Ok(())
    }

    #[test]
    fn test_show_full_tables() -> Result<(), CvsSqlError> {
        let started = Utc::now().naive_utc() - Duration::seconds(20);

        let home = prepare_system()?;
        let ready_at = Utc::now().naive_utc() + Duration::seconds(20);
        let args = Args {
            home: Some(home.path().to_path_buf()),
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands("SHOW FULL TABLES;")?;
        assert_eq!(results.len(), 1);

        let results = &results.first().unwrap().results;
        assert_eq!(results.metadata.number_of_columns(), 5);

        let mut tables = HashMap::new();

        let table_col = Column::from_index(0);
        for (index, row) in results.data.iter().enumerate() {
            let name = row.get(&table_col).to_string();
            tables.insert(name, index);
        }

        assert_eq!(tables.len(), 10);

        let table_one = results.data.get(*tables.get("table_one").unwrap()).unwrap();
        verify_row_length(table_one, 5)?;
        verify_row_times(table_one, &started, &ready_at);

        let table_two = results.data.get(*tables.get("table_two").unwrap()).unwrap();
        verify_row_length(table_two, 13)?;
        verify_row_times(table_two, &started, &ready_at);

        let table_three = results
            .data
            .get(*tables.get("table_three").unwrap())
            .unwrap();
        verify_row_length(table_three, 22)?;
        verify_row_times(table_three, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db1.another_table").unwrap())
            .unwrap();
        verify_row_length(table, 2)?;
        verify_row_times(table, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db1.yet_another_table").unwrap())
            .unwrap();
        verify_row_length(table, 3)?;
        verify_row_times(table, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db1.and_one_more").unwrap())
            .unwrap();
        verify_row_length(table, 6)?;
        verify_row_times(table, &started, &ready_at);

        let table = results.data.get(*tables.get("db2.more").unwrap()).unwrap();
        verify_row_length(table, 4)?;
        verify_row_times(table, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db2.even_more").unwrap())
            .unwrap();
        verify_row_length(table, 7)?;
        verify_row_times(table, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db2.even_more").unwrap())
            .unwrap();
        verify_row_length(table, 7)?;
        verify_row_times(table, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db2.db3.in_3").unwrap())
            .unwrap();
        verify_row_length(table, 6)?;
        verify_row_times(table, &started, &ready_at);

        let table = results
            .data
            .get(*tables.get("db2.db3.another_in_3").unwrap())
            .unwrap();
        verify_row_length(table, 4)?;
        verify_row_times(table, &started, &ready_at);

        Ok(())
    }

    fn verify_row_times(
        row: &DataRow,
        data_creation_started: &NaiveDateTime,
        data_creation_ended: &NaiveDateTime,
    ) {
        let created_at = Column::from_index(2);
        let Value::Timestamp(created_at) = row.get(&created_at) else {
            panic!("Can not get creation time")
        };
        assert!(created_at >= data_creation_started && created_at <= data_creation_ended);
        let modified_at = Column::from_index(3);
        let Value::Timestamp(modified_at) = row.get(&modified_at) else {
            panic!("Can not get creation time")
        };
        assert!(modified_at >= data_creation_started && modified_at <= data_creation_ended);
    }

    fn verify_row_length(row: &DataRow, expected_length: u64) -> Result<(), CvsSqlError> {
        let file_size = Column::from_index(1);
        assert_eq!(
            row.get(&file_size),
            &Value::Number(BigDecimal::from_u64(expected_length).unwrap())
        );
        let path = Column::from_index(4);
        let Value::Str(path) = row.get(&path) else {
            panic!("No path")
        };
        let path = Path::new(path);
        assert_eq!(path.metadata()?.len(), expected_length);
        Ok(())
    }

    #[test]
    fn test_show_databases() -> Result<(), CvsSqlError> {
        let started = Utc::now().naive_utc() - Duration::seconds(20);

        let home = prepare_system()?;
        let ready_at = Utc::now().naive_utc() + Duration::seconds(20);
        let args = Args {
            home: Some(home.path().to_path_buf()),
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands("SHOW DATABASES;")?;
        assert_eq!(results.len(), 1);

        let results = &results.first().unwrap().results;
        assert_eq!(results.metadata.number_of_columns(), 4);

        let mut dbs = HashMap::new();

        let db_col = Column::from_index(0);
        for (index, row) in results.data.iter().enumerate() {
            let name = row.get(&db_col).to_string();
            dbs.insert(name, index);
        }

        assert_eq!(dbs.len(), 3);

        let db1 = results.data.get(*dbs.get("db1").unwrap()).unwrap();
        verify_db_row(db1, &started, &ready_at, 3)?;

        let db2 = results.data.get(*dbs.get("db2").unwrap()).unwrap();
        verify_db_row(db2, &started, &ready_at, 2)?;

        let db3 = results.data.get(*dbs.get("db2.db3").unwrap()).unwrap();
        verify_db_row(db3, &started, &ready_at, 2)?;

        Ok(())
    }
    fn verify_db_row(
        row: &DataRow,
        data_creation_started: &NaiveDateTime,
        data_creation_ended: &NaiveDateTime,
        number_of_elements: u64,
    ) -> Result<(), CvsSqlError> {
        let created_at = Column::from_index(2);
        let Value::Timestamp(created_at) = row.get(&created_at) else {
            panic!("Can not get creation time")
        };
        assert!(created_at >= data_creation_started && created_at <= data_creation_ended);

        let number_of_tables = Column::from_index(1);
        let Value::Number(number_of_tables) = row.get(&number_of_tables) else {
            panic!("Can not get number of tables")
        };
        assert_eq!(
            number_of_tables,
            &BigDecimal::from_u64(number_of_elements).unwrap()
        );

        let path = Column::from_index(3);
        let Value::Str(path) = row.get(&path) else {
            panic!("No path")
        };
        let path = Path::new(path);
        assert!(path.metadata()?.is_dir());

        Ok(())
    }
}
