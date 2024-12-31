use sqlparser::ast::{SelectItem, WildcardAdditionalOptions};

use crate::error::CdvSqlError;
use crate::results::ResultName;
use crate::{
    results::{Column, ColumnName, ResultSet, Row},
    value::Value,
};
use std::collections::HashMap;
use std::rc::Rc;

trait Projection {
    fn get<'a>(&self, results: &'a dyn ResultSet, row: &Row) -> &'a Value;
    fn name(&self) -> &ColumnName;
}
struct ColumnProjection {
    column: Column,
    column_name: ColumnName,
}
impl Projection for ColumnProjection {
    fn get<'a>(&self, results: &'a dyn ResultSet, row: &Row) -> &'a Value {
        results.get(row, &self.column)
    }
    fn name(&self) -> &ColumnName {
        &self.column_name
    }
}

struct ResultsWithProjections {
    projections: Vec<Box<dyn Projection>>,
    names: HashMap<String, Vec<Column>>,
    results: Box<dyn ResultSet>,
}

impl ResultSet for ResultsWithProjections {
    fn number_of_rows(&self) -> usize {
        self.results.number_of_rows()
    }
    fn number_of_columns(&self) -> usize {
        self.projections.len()
    }
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        self.results.result_name()
    }
    fn column_name(&self, column: &Column) -> Option<ColumnName> {
        self.projections
            .get(column.get_index())
            .map(|projection| projection.name().clone())
    }
    fn column_index(&self, name: &ColumnName) -> Option<&Column> {
        if let Some(projections) = self.names.get(name.name()) {
            for idx in projections {
                if self.projections[idx.get_index()]
                    .name()
                    .parent()
                    .matches(name.parent())
                {
                    return Some(idx);
                }
            }
        }
        None
    }

    fn get(&self, row: &Row, column: &Column) -> &Value {
        self.projections
            .get(column.get_index())
            .map(|p| p.get(&*self.results, row))
            .unwrap_or(&Value::Empty)
    }
}

pub fn make_projection(
    parent: Box<dyn ResultSet>,
    items: &[SelectItem],
) -> Result<Box<dyn ResultSet>, CdvSqlError> {
    let mut projections = Vec::new();
    for item in items {
        let mut items = item.convert(&*parent)?;
        projections.append(&mut items);
    }
    let mut names: HashMap<String, Vec<Column>> = HashMap::new();
    for (idx, p) in projections.iter().enumerate() {
        names
            .entry(p.name().name().to_string())
            .and_modify(|lst| lst.push(Column::from_index(idx)))
            .or_insert(vec![Column::from_index(idx)]);
    }
    Ok(Box::new(ResultsWithProjections {
        projections,
        names,
        results: parent,
    }))
}
trait Convert {
    fn convert(&self, parent: &dyn ResultSet) -> Result<Vec<Box<dyn Projection>>, CdvSqlError>;
}
impl Convert for SelectItem {
    fn convert(&self, parent: &dyn ResultSet) -> Result<Vec<Box<dyn Projection>>, CdvSqlError> {
        match self {
            SelectItem::Wildcard(options) => options.convert(parent),
            _ => Err(CdvSqlError::ToDo(format!("Select {}", self))),
        }
    }
}
impl Convert for WildcardAdditionalOptions {
    fn convert(&self, parent: &dyn ResultSet) -> Result<Vec<Box<dyn Projection>>, CdvSqlError> {
        if self.opt_ilike.is_some() {
            return Err(CdvSqlError::Unsupported("Select * ILIKE".into()));
        }
        if self.opt_exclude.is_some() {
            return Err(CdvSqlError::Unsupported("Select * EXCLUDE".into()));
        }
        if self.opt_except.is_some() {
            return Err(CdvSqlError::Unsupported("Select * EXCEPT".into()));
        }
        if self.opt_replace.is_some() {
            return Err(CdvSqlError::Unsupported("Select * REPLACE".into()));
        }
        if self.opt_rename.is_some() {
            return Err(CdvSqlError::Unsupported("Select * RENAME".into()));
        }
        let mut projections: Vec<Box<dyn Projection>> = Vec::new();
        for column in parent.columns() {
            let Some(column_name) = parent.column_name(&column) else {
                return Err(CdvSqlError::Unsupported(
                    "Select * with unnamed column".into(),
                ));
            };
            projections.push(Box::new(ColumnProjection {
                column,
                column_name,
            }));
        }

        Ok(projections)
    }
}
