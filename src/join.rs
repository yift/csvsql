use std::collections::HashSet;
use std::ops::Deref;
use std::rc::Rc;

use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableWithJoins};

use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::group_by::GroupRow;
use crate::projections::{Projection, SingleConvert};
use crate::result_set_metadata::Metadata;
use crate::results::{Column, Name, ResultSet};
use crate::results_data::{DataRow, ResultsData};
use crate::value::Value;

pub fn create_join(from: &[TableWithJoins], engine: &Engine) -> Result<ResultSet, CvsSqlError> {
    let mut result = None;

    for from in from {
        let mut res = from.relation.extract(engine)?;
        for j in &from.joins {
            res = join(res, j, engine)?;
        }
        result = match result {
            None => Some(res),
            Some(left) => {
                let joiner = Joiner {
                    joiner_type: JoinerType {
                        left_outer: false,
                        right_outer: false,
                    },
                    constraint: JoinerConstraint::All,
                };
                Some(product(left, res, joiner))
            }
        };
    }

    result.ok_or_else(|| CvsSqlError::Unsupported("SELECT without FROM".to_string()))
}
fn product(left: ResultSet, right: ResultSet, joiner: Joiner) -> ResultSet {
    let mut data = Vec::new();
    let mut missing_right_rows = HashSet::new();
    if joiner.joiner_type.right_outer {
        for (i, _) in right.data.iter().enumerate() {
            missing_right_rows.insert(i);
        }
    }
    for l in left.data.iter() {
        let mut right_added = false;
        for (r_index, r) in right.data.iter().enumerate() {
            let mut row = Vec::new();
            for left_column in left.columns() {
                row.push(l.get(&left_column).clone());
            }
            for right_column in right.columns() {
                row.push(r.get(&right_column).clone());
            }
            let mut row = DataRow::new(row);
            let use_row = match joiner.constraint {
                JoinerConstraint::All => true,
                JoinerConstraint::On(ref filter) => {
                    let grouped_row = GroupRow {
                        data: row,
                        group_rows: vec![],
                    };

                    let value = filter.get(&grouped_row);
                    let passed = value.deref() == &Value::Bool(true);

                    row = grouped_row.data;
                    passed
                }
                JoinerConstraint::Using(ref indices) => {
                    let mut ret = true;
                    for (left_col, right_col) in indices {
                        let left_value = l.get(left_col);
                        let right_value = r.get(right_col);
                        if left_value != right_value {
                            ret = false;
                            break;
                        }
                    }

                    ret
                }
            };
            if use_row {
                data.push(row);
                missing_right_rows.remove(&r_index);
                right_added = true;
            }
        }
        if !right_added && joiner.joiner_type.left_outer {
            let mut row = Vec::new();
            for left_column in left.columns() {
                row.push(l.get(&left_column).clone());
            }
            for _ in right.columns() {
                row.push(Value::Empty);
            }
            let row = DataRow::new(row);
            data.push(row);
        }
    }
    if joiner.joiner_type.right_outer {
        for index in missing_right_rows {
            let mut row = Vec::new();
            for _ in left.columns() {
                row.push(Value::Empty);
            }
            let empty = DataRow::new(vec![]);
            let r = right.data.get(index).unwrap_or(&empty);
            for right_column in right.columns() {
                row.push(r.get(&right_column).clone());
            }
            let row = DataRow::new(row);
            data.push(row);
        }
    }
    let metadata = Rc::new(Metadata::product(&left.metadata, &right.metadata));
    let data = ResultsData::new(data);
    ResultSet { data, metadata }
}

enum JoinerConstraint {
    On(Box<dyn Projection>),
    Using(Vec<(Column, Column)>),
    All,
}
impl JoinerConstraint {
    fn new(
        constraint: &JoinConstraint,
        engine: &Engine,
        left_metadata: &Rc<Metadata>,
        right_metadata: &Rc<Metadata>,
    ) -> Result<Self, CvsSqlError> {
        match constraint {
            JoinConstraint::Natural => Err(CvsSqlError::Unsupported("Natural join".into())),
            JoinConstraint::None => Ok(Self::All),
            JoinConstraint::On(expr) => {
                let metadata = Metadata::product(left_metadata, right_metadata);
                let on = expr.convert_single(&metadata, engine)?;
                Ok(Self::On(on))
            }
            JoinConstraint::Using(using) => {
                let mut indices = vec![];
                for name in using {
                    let name: Name = name.to_string().into();
                    let left_index = left_metadata.column_index(&name)?;
                    let right_index = right_metadata.column_index(&name)?;
                    indices.push((left_index.clone(), right_index.clone()));
                }

                Ok(Self::Using(indices))
            }
        }
    }
}
struct JoinerType {
    left_outer: bool,
    right_outer: bool,
}
struct Joiner {
    joiner_type: JoinerType,
    constraint: JoinerConstraint,
}
fn join(left: ResultSet, join: &Join, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
    if join.global {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... JOIN ... GLOBAL".to_string(),
        ));
    }
    let right = join.relation.extract(engine)?;
    let (left_outer, right_outer, constraint) = match &join.join_operator {
        JoinOperator::Join(c) => (false, false, c),
        JoinOperator::Inner(c) => (false, false, c),
        JoinOperator::Left(c) => (true, false, c),
        JoinOperator::LeftOuter(c) => (true, false, c),
        JoinOperator::Right(c) => (false, true, c),
        JoinOperator::RightOuter(c) => (false, true, c),
        JoinOperator::FullOuter(c) => (true, true, c),
        _ => {
            return Err(CvsSqlError::Unsupported(format!(
                "JOIN with {:?}",
                join.join_operator
            )));
        }
    };

    let constraint = JoinerConstraint::new(constraint, engine, &left.metadata, &right.metadata)?;

    let joiner = Joiner {
        joiner_type: JoinerType {
            left_outer,
            right_outer,
        },
        constraint,
    };

    Ok(product(left, right, joiner))
}
