use std::collections::HashMap;

use sqlparser::ast::Expr;

use crate::{
    engine::Engine,
    error::CvsSqlError,
    projections::SingleConvert,
    result_set_metadata::{Metadata, SimpleResultSetMetadata},
    results::ResultSet,
    results_data::DataRow,
    value::Value,
};

pub struct GroupRow {
    pub data: DataRow,
    pub group_rows: Vec<GroupRow>,
}

pub struct GroupedResultSet {
    pub metadata: Metadata,
    pub rows: Vec<GroupRow>,
}

impl From<ResultSet> for GroupedResultSet {
    fn from(value: ResultSet) -> Self {
        let rows = value.data.into_iter().map(|data| GroupRow {
            data,
            group_rows: vec![],
        });
        let metadata = value.metadata;
        Self {
            metadata,
            rows: rows.collect(),
        }
    }
}
pub fn group_by(
    engine: &Engine,
    group_by: &[Expr],
    results: ResultSet,
) -> Result<GroupedResultSet, CvsSqlError> {
    if group_by.is_empty() {
        return Ok(results.into());
    }
    let mut projections = Vec::new();

    for expr in group_by {
        let item = expr.convert_single(&results.metadata, engine)?;
        projections.push(item);
    }
    let mut metadata = SimpleResultSetMetadata::new(results.metadata.result_name().cloned());
    for n in &projections {
        metadata.add_column(n.name());
    }
    let metadata = Metadata::Simple(metadata);

    let mut groups: HashMap<Vec<Value>, Vec<GroupRow>> = HashMap::new();
    for row in results.data.into_iter() {
        let row = GroupRow {
            data: row,
            group_rows: vec![],
        };
        let mut key = Vec::new();
        for item in &projections {
            let item = item.get(&row).clone();
            key.push(item);
        }
        groups.entry(key).or_default().push(GroupRow {
            data: row.data,
            group_rows: vec![],
        });
    }

    let rows: Vec<GroupRow> = groups
        .into_iter()
        .map(|(k, group_rows)| {
            let data = DataRow::new(k);
            GroupRow { data, group_rows }
        })
        .collect();
    let metadata = Metadata::Grouped {
        parent: Box::new(results.metadata),
        this: Box::new(metadata),
    };

    Ok(GroupedResultSet { rows, metadata })
}

pub fn force_group_by(results: ResultSet) -> GroupedResultSet {
    let mut group_rows = vec![];
    for row in results.data.into_iter() {
        group_rows.push(GroupRow {
            data: row,
            group_rows: vec![],
        });
    }
    let rows = vec![GroupRow {
        data: DataRow::new(vec![]),
        group_rows,
    }];
    let metadata = SimpleResultSetMetadata::new(results.metadata.result_name().cloned());
    let metadata = Metadata::Simple(metadata);
    let metadata = Metadata::Grouped {
        parent: Box::new(results.metadata),
        this: Box::new(metadata),
    };

    GroupedResultSet { rows, metadata }
}
