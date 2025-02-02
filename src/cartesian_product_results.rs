use std::rc::Rc;

use crate::results::Name;
use crate::results::{Column, ColumnIndexError, ResultSet, ResultSetMetadata};
use crate::util::SmartReference;
use crate::value::Value;
struct ProductResults {
    left: Box<dyn ResultSet>,
    right: Box<dyn ResultSet>,
    metadata: Rc<dyn ResultSetMetadata>,
    row: usize,
}
struct ProductResultsMetadata {
    left: Rc<dyn ResultSetMetadata>,
    right: Rc<dyn ResultSetMetadata>,
}
impl ResultSetMetadata for ProductResultsMetadata {
    fn column_index(
        &self,
        name: &crate::results::Name,
    ) -> Result<SmartReference<Column>, ColumnIndexError> {
        let left_result = self.left.column_index(name);
        let right_result = self.right.column_index(name);
        match (&left_result, &right_result) {
            (Err(ColumnIndexError::AmbiguousColumnName(_)), _) => left_result,
            (_, Err(ColumnIndexError::AmbiguousColumnName(_))) => right_result,
            (Ok(_), Ok(_)) => Err(ColumnIndexError::AmbiguousColumnName(name.full_name())),
            (Ok(_), Err(ColumnIndexError::NoSuchColumn(_))) => left_result,
            (Err(ColumnIndexError::NoSuchColumn(_)), Ok(right_result)) => {
                let col =
                    Column::from_index(right_result.get_index() + self.left.number_of_columns());
                Ok(SmartReference::Owned(col))
            }
            (Err(ColumnIndexError::NoSuchColumn(_)), Err(ColumnIndexError::NoSuchColumn(_))) => {
                right_result
            }
        }
    }
    fn column_name(&self, column: &Column) -> Option<&Name> {
        self.left.column_name(column).or_else(|| {
            self.right.column_name(&Column::from_index(
                column.get_index() - self.left.number_of_columns(),
            ))
        })
    }
    fn number_of_columns(&self) -> usize {
        self.left.number_of_columns() + self.right.number_of_columns()
    }
    fn result_name(&self) -> Option<&Name> {
        None
    }
}
impl ResultSet for ProductResults {
    fn metadate(&self) -> &Rc<dyn ResultSetMetadata> {
        &self.metadata
    }
    fn get(&self, column: &Column) -> SmartReference<Value> {
        if column.get_index() < self.left.metadate().number_of_columns() {
            self.left.get(column)
        } else {
            let column =
                &Column::from_index(column.get_index() - self.left.metadate().number_of_columns());
            self.right.get(column)
        }
    }
    fn next_if_possible(&mut self) -> bool {
        if self.row == 0 {
            if self.left.next_if_possible() && self.right.next_if_possible() {
                self.row = 1;
                true
            } else {
                false
            }
        } else if self.right.next_if_possible() {
            self.row += 1;
            true
        } else {
            self.right.revert();
            if self.left.next_if_possible() && self.right.next_if_possible() {
                self.row += 1;
                true
            } else {
                false
            }
        }
    }
    fn revert(&mut self) {
        self.left.revert();
        self.right.revert();
    }
}

pub fn join(left: Box<dyn ResultSet>, right: Box<dyn ResultSet>) -> Box<dyn ResultSet> {
    let metadata = ProductResultsMetadata {
        left: left.metadate().clone(),
        right: right.metadate().clone(),
    };
    Box::new(ProductResults {
        left,
        right,
        row: 0,
        metadata: Rc::new(metadata),
    })
}
