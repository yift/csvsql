use std::cmp::Ordering;
use std::ops::Deref;

use crate::error::CvsSqlError;
use crate::intrim_result_set::IntrimResultSet;
use crate::projections::Projection;
use crate::results_data::DataRow;
use crate::{engine::Engine, projections::SingleConvert, results::ResultSet};
use sqlparser::ast::{OrderBy, OrderByExpr};

struct OrderByItem {
    by: Box<dyn Projection>,
    asc: bool,
    empty_first: bool,
}
impl OrderByItem {
    fn new(parent: &IntrimResultSet, engine: &Engine, expr: &OrderByExpr) -> Result<Self, CvsSqlError> {
        if expr.with_fill.is_some() {
            return Err(CvsSqlError::Unsupported("ORDER BY with fill".into()));
        }
        let by = expr.expr.convert_single(parent, engine)?;
        let asc = expr.asc.unwrap_or(true);
        let empty_first = expr.nulls_first.unwrap_or(false);

        Ok(OrderByItem {
            by,
            asc,
            empty_first,
        })
    }

    fn compare(&self, left: &DataRow, right: &DataRow) -> Ordering {
        let ret = self.compare_as_is(left, right);
        if self.asc {
            ret
        } else {
            ret.reverse()
        }
    }
    fn compare_as_is(&self, left: &DataRow, right: &DataRow) -> Ordering {
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
    results: &mut IntrimResultSet,
) -> Result<(), CvsSqlError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    if order_by.interpolate.is_some() {
        return Err(CvsSqlError::Unsupported("interpolate ORDER BY".into()));
    }
    let items = order_by
        .exprs
        .iter()
        .map(|expr| OrderByItem::new(results, engine, expr))
        .collect::<Result<Vec<_>, _>>()?;
    if items.is_empty() {
        return Ok(());
    }

    results.data.sort_by(|left, right| {
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
