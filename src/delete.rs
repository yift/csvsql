use std::{fs::OpenOptions, ops::Deref, rc::Rc};

use bigdecimal::{BigDecimal, FromPrimitive};
use sqlparser::ast::{Delete, FromTable, TableFactor};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    extractor::Extractor,
    group_by::GroupRow,
    projections::SingleConvert,
    result_set_metadata::SimpleResultSetMetadata,
    results::ResultSet,
    results_data::{DataRow, ResultsData},
    value::Value,
    writer::{Writer, new_csv_writer},
};

impl Extractor for Delete {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.using.is_some() {
            return Err(CvsSqlError::Unsupported("DELETE... USING".to_string()));
        }
        if self.limit.is_some() {
            return Err(CvsSqlError::Unsupported("DELETE... LIMIT".to_string()));
        }
        if self.returning.is_some() {
            return Err(CvsSqlError::Unsupported("DELETE... RETURNING ".to_string()));
        }
        if !self.order_by.is_empty() {
            return Err(CvsSqlError::Unsupported("DELETE... ORDER BY ".to_string()));
        }
        if !self.tables.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "DELETE... FROM TABLE ".to_string(),
            ));
        }

        let table = match &self.from {
            FromTable::WithFromKeyword(table) => table,
            FromTable::WithoutKeyword(table) => table,
        };
        if table.len() > 1 {
            return Err(CvsSqlError::MultiplyTableDelete);
        }
        let Some(table) = table.first() else {
            return Err(CvsSqlError::NothingToDelete);
        };

        let table_file = match &table.relation {
            TableFactor::Table {
                name,
                alias: _,
                args: _,
                with_hints: _,
                version: _,
                with_ordinality: _,
                partitions: _,
                json_path: _,
                sample: _,
                index_hints: _,
            } => engine.file_name(name)?,
            _ => {
                return Err(CvsSqlError::Unsupported(
                    "Delete not for a table".to_string(),
                ));
            }
        };
        if table_file.read_only {
            return Err(CvsSqlError::ReadOnlyMode);
        }

        let current_data = table.relation.extract(engine)?;

        let filter = match &self.selection {
            Some(expr) => Some(expr.convert_single(&current_data.metadata, engine)?),
            None => None,
        };

        let mut new_data = vec![];
        let mut count = 0;
        for row in current_data.data.into_iter() {
            let row = GroupRow {
                data: row,
                group_rows: vec![],
            };

            let delete_row = if let Some(filter) = &filter {
                filter.get(&row).deref() == &Value::Bool(true)
            } else {
                true
            };
            if !delete_row {
                new_data.push(row.data);
            } else {
                count += 1;
            }
        }

        let metadata = current_data.metadata.clone();
        let data = ResultsData::new(new_data);
        let results = ResultSet { metadata, data };

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(table_file.path)?;
        let mut writer = new_csv_writer(file, engine.first_line_as_name);
        writer.write(&results)?;

        let mut metadata = SimpleResultSetMetadata::new(None);
        metadata.add_column("action");
        metadata.add_column("number_of_rows");
        let metadata = metadata.build();

        let row = vec![
            Value::Str("DELETED".to_string()),
            Value::Number(BigDecimal::from_usize(count).unwrap()),
        ];
        let row = DataRow::new(row);
        let data = vec![row];
        let data = ResultsData::new(data);
        let metadata = Rc::new(metadata);
        let results = ResultSet { metadata, data };

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{Statement, TableWithJoins},
        parser::Parser,
    };

    use crate::{args::Args, dialect::FilesDialect};

    use super::*;

    #[test]
    fn test_delete_without_tables() -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "DELETE FROM test_one";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::Delete(mut delete)) = statement.into_iter().next() else {
            panic!("Not a delete statement");
        };

        delete.from = FromTable::WithoutKeyword(vec![]);

        let Err(err) = delete.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::NothingToDelete));

        Ok(())
    }

    #[test]
    fn test_delete_not_a_table() -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "DELETE FROM test_one WHERE expr";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::Delete(mut delete)) = statement.into_iter().next() else {
            panic!("Not a delete statement");
        };
        let expr = delete.selection.take().unwrap();

        delete.from = FromTable::WithoutKeyword(vec![TableWithJoins {
            joins: vec![],
            relation: TableFactor::TableFunction { expr, alias: None },
        }]);

        let Err(err) = delete.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }
}
