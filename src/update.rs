use std::{collections::HashMap, fs::OpenOptions, ops::Deref};

use bigdecimal::{BigDecimal, FromPrimitive};
use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, SelectItem, SqliteOnConflict, TableFactor, TableWithJoins,
};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    extractor::Extractor,
    group_by::GroupRow,
    projections::SingleConvert,
    results::{Column, ResultSet},
    results_builder::build_simple_results,
    results_data::ResultsData,
    value::Value,
    writer::{Writer, new_csv_writer},
};

pub(crate) fn update_table(
    engine: &Engine,
    table: &TableWithJoins,
    assignments: &[Assignment],
    selection: &Option<Expr>,
    returning: &Option<Vec<SelectItem>>,
    or: &Option<SqliteOnConflict>,
) -> Result<ResultSet, CvsSqlError> {
    if !table.joins.is_empty() {
        return Err(CvsSqlError::Unsupported("Update with join".to_string()));
    }
    if assignments.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "Update with nothing to do".to_string(),
        ));
    }
    if returning.is_some() {
        return Err(CvsSqlError::Unsupported(
            "Update with returning".to_string(),
        ));
    }
    if or.is_some() {
        return Err(CvsSqlError::Unsupported("Update with or".to_string()));
    }

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
                "Update not for a table".to_string(),
            ));
        }
    };
    if table_file.read_only {
        return Err(CvsSqlError::ReadOnlyMode);
    }

    let current_data = table.relation.extract(engine)?;

    let filter = match selection {
        Some(expr) => Some(expr.convert_single(&current_data.metadata, engine)?),
        None => None,
    };

    let mut to_set = HashMap::new();
    for a in assignments {
        let value = a.value.convert_single(&current_data.metadata, engine)?;
        let field = match &a.target {
            AssignmentTarget::ColumnName(col) => current_data.metadata.column_index(&col.into())?,
            AssignmentTarget::Tuple(_) => {
                return Err(CvsSqlError::Unsupported(
                    "Update with tuple assignment".to_string(),
                ));
            }
        };
        if to_set.insert(field.get_index(), value).is_some() {
            return Err(CvsSqlError::MultiplyAssignment);
        }
    }

    let mut new_data = vec![];
    let mut count = 0;
    for row in current_data.data.into_iter() {
        let mut row = GroupRow {
            data: row,
            group_rows: vec![],
        };

        let use_row = if let Some(filter) = &filter {
            filter.get(&row).deref() == &Value::Bool(true)
        } else {
            true
        };
        if use_row {
            for (col, value) in &to_set {
                let new_value = value.get(&row);
                let col = Column::from_index(*col);
                row.data.set(&col, new_value.clone());
            }
            count += 1;
        }
        new_data.push(row.data);
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

    build_simple_results(vec![
        ("action", Value::Str("UPDATE".to_string())),
        (
            "number_of_rows",
            Value::Number(BigDecimal::from_usize(count).unwrap()),
        ),
    ])
}
