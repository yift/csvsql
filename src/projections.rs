use sqlparser::ast::{BinaryOperator, Expr, SelectItem, WildcardAdditionalOptions};

use crate::error::CdvSqlError;
use crate::results::ResultName;
use crate::util::SmartReference;
use crate::{
    results::{Column, ColumnName, ResultSet, Row},
    value::Value,
};
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

trait Projection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet, row: &Row) -> SmartReference<'a, Value>;
    fn name(&self) -> SmartReference<'_, ColumnName>;
}
struct ColumnProjection {
    column: Column,
    column_name: ColumnName,
}
impl Projection for ColumnProjection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet, row: &Row) -> SmartReference<'a, Value> {
        results.get(row, &self.column)
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        SmartReference::Borrowed(&self.column_name)
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
    fn column_index(&self, name: &ColumnName) -> Option<Column> {
        if let Some(projections) = self.names.get(name.name()) {
            for idx in projections {
                if self.projections[idx.get_index()]
                    .name()
                    .parent()
                    .matches(name.parent())
                {
                    return Some(idx.clone());
                }
            }
        }
        None
    }

    fn get(&self, row: &Row, column: &Column) -> SmartReference<Value> {
        self.projections
            .get(column.get_index())
            .map(|p| p.get(&*self.results, row))
            .unwrap_or(Value::Empty.into())
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
            SelectItem::UnnamedExpr(exp) => exp.convert(parent),
            SelectItem::ExprWithAlias { expr, alias } => {
                let data = expr.convert_single(parent)?;
                let alias = ColumnName::simple(&alias.value);
                Ok(vec![Box::new(AliasProjection { data, alias })])
            }
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
trait SingleConvert {
    fn convert_single(&self, parent: &dyn ResultSet) -> Result<Box<dyn Projection>, CdvSqlError>;
}

trait BinaryFunction {
    fn calculate(
        &self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<Value>;
    fn name(&self) -> &str;
    fn is_operator(&self) -> bool;
}

struct Plus {}
impl BinaryFunction for Plus {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        (left.deref() + right.deref()).into()
    }
    fn name(&self) -> &str {
        "+"
    }
    fn is_operator(&self) -> bool {
        true
    }
}

struct AliasProjection {
    data: Box<dyn Projection>,
    alias: ColumnName,
}
impl Projection for AliasProjection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet, row: &Row) -> SmartReference<'a, Value> {
        self.data.get(results, row)
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        SmartReference::Borrowed(&self.alias)
    }
}
struct BinaryProjection {
    left: Box<dyn Projection>,
    right: Box<dyn Projection>,
    operator: Box<dyn BinaryFunction>,
}
impl Projection for BinaryProjection {
    fn name(&self) -> SmartReference<ColumnName> {
        let name = if self.operator.is_operator() {
            format!(
                "{} {} {}",
                self.left.name(),
                self.operator.name(),
                self.right.name()
            )
        } else {
            format!(
                "{}({}, {})",
                self.operator.name(),
                self.left.name(),
                self.right.name()
            )
        };
        ColumnName::simple(&name).into()
    }
    fn get<'a>(&'a self, results: &'a dyn ResultSet, row: &Row) -> SmartReference<'a, Value> {
        let left = self.left.get(results, row);
        let right = self.right.get(results, row);
        self.operator.calculate(left, right)
    }
}

impl<T: SingleConvert> Convert for T {
    fn convert(&self, parent: &dyn ResultSet) -> Result<Vec<Box<dyn Projection>>, CdvSqlError> {
        let result = self.convert_single(parent)?;
        Ok(vec![result])
    }
}
impl SingleConvert for Expr {
    fn convert_single(&self, parent: &dyn ResultSet) -> Result<Box<dyn Projection>, CdvSqlError> {
        match self {
            Expr::Identifier(ident) => {
                let name = ColumnName::simple(&ident.value);
                name.convert_single(parent)
            }
            Expr::CompoundIdentifier(idents) => {
                let mut root = ResultName::root();
                let mut names = idents.iter().peekable();
                while let Some(name) = names.next() {
                    if names.peek().is_none() {
                        let name = ColumnName::new(&Rc::new(root), &name.value);

                        return name.convert_single(parent);
                    } else {
                        root = root.append(&name.value);
                    }
                }
                Err(CdvSqlError::NoSelect)
            }
            Expr::BinaryOp { left, op, right } => {
                let left = left.convert_single(parent)?;
                let right = right.convert_single(parent)?;
                let operator = match op {
                    BinaryOperator::Plus => Plus {},
                    _ => {
                        return Err(CdvSqlError::ToDo(format!("Operator: {}", op)));
                    }
                };
                Ok(Box::new(BinaryProjection {
                    left,
                    right,
                    operator: Box::new(operator),
                }))
            }
            _ => Err(CdvSqlError::ToDo(format!(
                "Select expression like {}",
                self
            ))),
        }
    }
}
impl SingleConvert for ColumnName {
    fn convert_single(&self, parent: &dyn ResultSet) -> Result<Box<dyn Projection>, CdvSqlError> {
        let Some(column) = parent.column_index(self) else {
            return Err(CdvSqlError::NoSuchColumn(self.name().into()));
        };
        let Some(column_name) = parent.column_name(&column) else {
            return Err(CdvSqlError::NoSuchColumn(self.name().into()));
        };
        let projection = Box::new(ColumnProjection {
            column: column.clone(),
            column_name,
        });
        Ok(projection)
    }
}
