use std::{fs::OpenOptions, rc::Rc};

use crate::results_builder::build_simple_results;
use crate::{value::Value, writer::Writer};
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
            return Err(CvsSqlError::Unsupported(
                "INSERT without assignments".into(),
            ));
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
        let mut writer = new_csv_writer(file, engine.first_line_as_name);
        writer.append(&results)?;

        build_simple_results(vec![
            ("action", Value::Str("INSERT".to_string())),
            (
                "number_of_rows",
                Value::Number(BigDecimal::from_usize(len).unwrap()),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{
            Ident, InputFormatClause, InsertAliases, MysqlInsertPriority, OnInsert,
            SqliteOnConflict, Statement,
        },
        parser::Parser,
    };

    use crate::{args::Args, dialect::FilesDialect};

    use super::*;

    fn test_unsupported(change: fn(&mut Insert)) -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "INSERT INTO test_one(col) VALUES (1)";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::Insert(mut insert)) = statement.into_iter().next() else {
            panic!("Not an insert statement");
        };
        change(&mut insert);

        let Err(err) = insert.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }

    #[test]
    fn insert_with_or() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.or = Some(SqliteOnConflict::Replace))
    }

    #[test]
    fn insert_ignore() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.ignore = true)
    }

    #[test]
    fn insert_with_alias() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.table_alias = Some(Ident::from("alias")))
    }

    #[test]
    fn insert_with_overwrite() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.overwrite = true)
    }

    #[test]
    fn insert_with_partitioned() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.partitioned = Some(vec![]))
    }

    #[test]
    fn insert_with_after_columns() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.after_columns = vec![Ident::from("col")])
    }

    #[test]
    fn insert_with_on() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.on = Some(OnInsert::DuplicateKeyUpdate(vec![])))
    }

    #[test]
    fn insert_with_returning() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.returning = Some(vec![]))
    }

    #[test]
    fn insert_with_replace_into() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.replace_into = true)
    }

    #[test]
    fn insert_with_priority() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.priority = Some(MysqlInsertPriority::Delayed))
    }

    #[test]
    fn insert_with_insert_alias() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| {
            let alias = InsertAliases {
                col_aliases: None,
                row_alias: vec![Ident::from("row")].into(),
            };
            insert.insert_alias = Some(alias)
        })
    }

    #[test]
    fn insert_with_settings() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| insert.settings = Some(vec![]))
    }

    #[test]
    fn insert_with_format() -> Result<(), CvsSqlError> {
        test_unsupported(|insert| {
            let clause = InputFormatClause {
                ident: Ident::from("test"),
                values: vec![],
            };
            insert.format_clause = Some(clause)
        })
    }
}
