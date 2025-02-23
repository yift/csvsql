use std::{collections::HashMap};

use sqlparser::ast::Expr;

use crate::{
    engine::Engine, error::CvsSqlError, projections::SingleConvert, result_set_metadata::Metadata, results::ResultSet, results_data::{DataRow, ResultsData}, value::Value
};

pub enum IntrimResultSet {
    UnGrouped(ResultSet),
    Grouped(GroupedResultSet),
}


pub struct GroupedResultSet {
    parent: ResultSet,
    groups: Vec<Vec<usize>>,
}

pub fn group_by(
    engine: &Engine,
    group_by: &[Expr],
    results: ResultSet,
) -> Result<IntrimResultSet, CvsSqlError> {
    if group_by.is_empty() {
        return Ok(IntrimResultSet::UnGrouped(results));
    }
    let mut projections = Vec::new();

    for expr in group_by {
        let item = expr.convert_single(&parent, engine)?;
        projections.push(item);
    }
    let mut groups: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
    for (index, row) in results.data.iter().enumerate() {
        let mut key = Vec::new();
        for item in &projections {
            let item = item.get(row).clone();
            key.push(item);
        }
        groups.entry(key).or_default().push(index);
    }
    let groups: Vec<Vec<usize>> = groups.into_values().collect();

    Ok(IntrimResultSet::Grouped(GroupedResultSet {
        parent: results,
        groups,
    }))
}

impl IntrimResultSet {
    pub fn metadata(&self) -> &Metadata {
        match self {
            IntrimResultSet::Grouped(grp) => &grp.parent.metadata,
            IntrimResultSet::UnGrouped(res) => &res.metadata,
        }
    }
}