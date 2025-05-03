use std::{fs::OpenOptions, rc::Rc};

use sqlparser::ast::{
    AlterTableOperation, ColumnDef, DropBehavior, HiveSetLocation, Ident, MySQLColumnPosition,
    ObjectName,
};

use crate::{
    cast::AvailableDataTypes,
    engine::Engine,
    error::CvsSqlError,
    file_results::read_file,
    result_set_metadata::{Metadata, SimpleResultSetMetadata},
    results::{Column, ColumnIndexError, ResultSet},
    results_data::{DataRow, ResultsData},
    value::Value,
    writer::{Writer, new_csv_writer},
};

pub(crate) fn alter(
    engine: &Engine,
    name: &ObjectName,
    if_exists: bool,
    operations: &[AlterTableOperation],
    location: &Option<HiveSetLocation>,
    on_cluster: &Option<Ident>,
) -> Result<ResultSet, CvsSqlError> {
    if location.is_some() {
        return Err(CvsSqlError::Unsupported("ALTER TABLE with location".into()));
    }
    if on_cluster.is_some() {
        return Err(CvsSqlError::Unsupported("ALTER TABLE with location".into()));
    }
    let table_file = engine.file_name(name)?;
    let file_name = engine.get_file_name(&table_file);
    let current_data = read_file(engine, name);
    let mut current_data = match current_data {
        Ok(data) => data,
        Err(CvsSqlError::TableNotExists(_)) => {
            if if_exists {
                return build_empty_results();
            } else {
                return current_data;
            }
        }
        _ => {
            return current_data;
        }
    };

    for oper in operations {
        match oper {
            AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists,
                column_def,
                column_position,
            } => {
                current_data =
                    add_column(current_data, *if_not_exists, column_def, column_position)?;
            }
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                drop_behavior,
            } => {
                current_data = drop_column(current_data, column_name, if_exists, drop_behavior)?;
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                current_data = rename_column(current_data, old_column_name, new_column_name)?;
            }
            _ => {
                return Err(CvsSqlError::Unsupported(format!(
                    "ALTER TABLE with operation: {}",
                    oper
                )));
            }
        }
    }

    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(table_file.path)?;
    let mut writer = new_csv_writer(file);
    if engine.first_line_as_name {
        writer.write(&current_data)?
    } else {
        writer.append(&current_data)?
    };

    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("action");
    metadata.add_column("table");
    metadata.add_column("file");
    let metadata = Metadata::Simple(metadata);

    let row = vec![
        Value::Str("ALTERED".to_string()),
        Value::Str(table_file.result_name.full_name()),
        Value::Str(file_name),
    ];
    let row = DataRow::new(row);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };

    Ok(results)
}

fn build_empty_results() -> Result<ResultSet, CvsSqlError> {
    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("action");
    metadata.add_column("table");
    metadata.add_column("file");
    let metadata = Metadata::Simple(metadata);

    let row = DataRow::new(vec![]);
    let data = vec![row];
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };
    Ok(results)
}

fn add_column(
    result_to_change: ResultSet,
    if_not_exists: bool,
    column_def: &ColumnDef,
    column_position: &Option<MySQLColumnPosition>,
) -> Result<ResultSet, CvsSqlError> {
    let name = (&column_def.name).into();
    let exists = match result_to_change.metadata.column_index(&name) {
        Ok(_) => true,
        Err(ColumnIndexError::AmbiguousColumnName(_)) => true,
        Err(ColumnIndexError::NoSuchColumn(_)) => false,
    };
    if exists {
        if if_not_exists {
            return Ok(result_to_change);
        } else {
            return Err(CvsSqlError::ColumnAlreadyExists(
                column_def.name.value.to_string(),
            ));
        };
    };

    AvailableDataTypes::try_from(&column_def.data_type)?;
    if !column_def.options.is_empty() {
        return Err(CvsSqlError::Unsupported(format!(
            "ALTER TABLE with options - {}",
            column_def
        )));
    }
    let position = match &column_position {
        None => result_to_change.metadata.number_of_columns(),
        Some(p) => {
            return Err(CvsSqlError::Unsupported(format!(
                "ALTER TABLE ADD COLUMN with position - {}",
                p,
            )));
        }
    };

    let mut metadata =
        SimpleResultSetMetadata::new(result_to_change.metadata.result_name().cloned());
    for (i, col) in result_to_change.columns().enumerate() {
        if i == position {
            metadata.add_column(name.short_name());
        }
        let current_name = result_to_change
            .metadata
            .column_name(&col)
            .map(|c| c.short_name())
            .unwrap_or_default();
        metadata.add_column(current_name);
    }
    if position == result_to_change.metadata.number_of_columns() {
        metadata.add_column(name.short_name());
    }
    let position = Column::from_index(position);
    let mut rows = vec![];
    for mut row in result_to_change.data.into_iter() {
        let value = Value::Empty;
        row.insert_at(&position, value);
        rows.push(row);
    }

    let metadata = Rc::new(metadata.build());
    let data = ResultsData::new(rows);
    let results = ResultSet { metadata, data };

    Ok(results)
}

fn drop_column(
    result_to_change: ResultSet,
    column_name: &Ident,
    if_exists: &bool,
    drop_behavior: &Option<DropBehavior>,
) -> Result<ResultSet, CvsSqlError> {
    if let Some(drop) = drop_behavior {
        return Err(CvsSqlError::Unsupported(format!(
            "ALTER TABLE DROP COLUMN ... {}",
            drop,
        )));
    }
    let name = column_name.into();
    let index = match result_to_change.metadata.column_index(&name) {
        Ok(index) => index,
        Err(err) => {
            if *if_exists {
                return Ok(result_to_change);
            } else {
                return Err(err.into());
            }
        }
    };
    let mut metadata =
        SimpleResultSetMetadata::new(result_to_change.metadata.result_name().cloned());

    for col in result_to_change.columns() {
        if col.get_index() != index.get_index() {
            let current_name = result_to_change
                .metadata
                .column_name(&col)
                .map(|c| c.short_name())
                .unwrap_or_default();
            metadata.add_column(current_name);
        }
    }
    let mut rows = vec![];
    for mut row in result_to_change.data.into_iter() {
        row.delete_at(&index);
        rows.push(row);
    }

    let metadata = Rc::new(metadata.build());
    let data = ResultsData::new(rows);
    let results = ResultSet { metadata, data };

    Ok(results)
}

fn rename_column(
    result_to_change: ResultSet,
    from: &Ident,
    to: &Ident,
) -> Result<ResultSet, CvsSqlError> {
    let from = from.into();
    let index = result_to_change.metadata.column_index(&from)?;
    let mut metadata =
        SimpleResultSetMetadata::new(result_to_change.metadata.result_name().cloned());

    for col in result_to_change.columns() {
        if col.get_index() != index.get_index() {
            let current_name = result_to_change
                .metadata
                .column_name(&col)
                .map(|c| c.short_name())
                .unwrap_or_default();
            metadata.add_column(current_name);
        } else {
            metadata.add_column(&to.value);
        }
    }

    let metadata = Rc::new(metadata.build());
    let results = ResultSet {
        metadata,
        data: result_to_change.data,
    };

    Ok(results)
}
