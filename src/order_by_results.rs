use std::cmp::Ordering;
use std::ops::Deref;

use crate::error::CvsSqlError;
use crate::group_by::{GroupRow, GroupedResultSet};
use crate::projections::Projection;
use crate::{engine::Engine, projections::SingleConvert};
use sqlparser::ast::{OrderBy, OrderByExpr, OrderByKind};

struct OrderByItem {
    by: Box<dyn Projection>,
    asc: bool,
    empty_first: bool,
}
impl OrderByItem {
    fn new(
        parent: &GroupedResultSet,
        engine: &Engine,
        expr: &OrderByExpr,
    ) -> Result<Self, CvsSqlError> {
        if expr.with_fill.is_some() {
            return Err(CvsSqlError::Unsupported("ORDER BY with fill".into()));
        }
        let by = expr.expr.convert_single(&parent.metadata, engine)?;
        let asc = expr.options.asc.unwrap_or(true);
        let empty_first = expr.options.nulls_first.unwrap_or(false);

        Ok(OrderByItem {
            by,
            asc,
            empty_first,
        })
    }

    fn compare(&self, left: &GroupRow, right: &GroupRow) -> Ordering {
        let ret = self.compare_as_is(left, right);
        if self.asc { ret } else { ret.reverse() }
    }
    fn compare_as_is(&self, left: &GroupRow, right: &GroupRow) -> Ordering {
        let left = self.by.get(left);
        let right = self.by.get(right);
        if left.is_empty() {
            if right.is_empty() {
                Ordering::Equal
            } else if self.empty_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        } else if right.is_empty() {
            if self.empty_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else {
            left.cmp(right.deref())
        }
    }
}
pub fn order_by(
    engine: &Engine,
    order_by: &Option<OrderBy>,
    results: &mut GroupedResultSet,
) -> Result<(), CvsSqlError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    if order_by.interpolate.is_some() {
        return Err(CvsSqlError::Unsupported("interpolate ORDER BY".into()));
    }
    let OrderByKind::Expressions(ref exprs) = order_by.kind else {
        return Err(CvsSqlError::Unsupported("ORDER BY all".into()));
    };
    let items = exprs
        .iter()
        .map(|expr| OrderByItem::new(results, engine, expr))
        .collect::<Result<Vec<_>, _>>()?;
    if items.is_empty() {
        return Ok(());
    }

    results.rows.sort_by(|left, right| {
        for item in &items {
            let order = item.compare(left, right);
            if order != Ordering::Equal {
                return order;
            }
        }
        Ordering::Equal
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{Interpolate, OrderByOptions, Statement, WithFill},
        parser::Parser,
    };

    use crate::{args::Args, dialect::FilesDialect, extractor::Extractor};

    use super::*;

    fn test_unsupported_order_by(change: fn(&mut OrderBy)) -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "SELECT * FROM tests.data.dates ORDER BY amount";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::Query(mut query)) = statement.into_iter().next() else {
            panic!("Not a select statement");
        };
        let Some(order_by) = &query.order_by else {
            panic!("No order by?");
        };
        let mut order_by = order_by.clone();
        change(&mut order_by);

        query.order_by = Some(order_by);
        let Err(err) = query.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }

    #[test]
    fn interpolate_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_order_by(|order_by| {
            order_by.interpolate = Some(Interpolate { exprs: None });
        })
    }

    #[test]
    fn all_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_order_by(|order_by| {
            order_by.kind = OrderByKind::All(OrderByOptions {
                asc: None,
                nulls_first: None,
            })
        })
    }

    #[test]
    fn with_fill_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_order_by(|order_by| {
            let OrderByKind::Expressions(exprs) = &order_by.kind else {
                panic!("Need expressions");
            };
            let mut expr = exprs.first().unwrap().clone();
            expr.with_fill = Some(WithFill {
                from: None,
                to: None,
                step: None,
            });
            order_by.kind = OrderByKind::Expressions(vec![expr]);
        })
    }
}
