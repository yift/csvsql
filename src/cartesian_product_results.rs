use crate::results::Column;
use crate::results::ColumnName;
use crate::results::ResultName;
use crate::results::ResultSet;
use crate::util::SmartReference;
use crate::value::Value;
use std::rc::Rc;
struct ProductResults {
    left: Box<dyn ResultSet>,
    right: Box<dyn ResultSet>,
    row: usize,
}
impl ResultSet for ProductResults {
    fn number_of_columns(&self) -> usize {
        self.left.number_of_columns() + self.right.number_of_columns()
    }
    fn column_name(&self, column: &Column) -> Option<ColumnName> {
        self.left.column_name(column).or_else(|| {
            self.right.column_name(&Column::from_index(
                column.get_index() - self.left.number_of_columns(),
            ))
        })
    }
    fn column_index(&self, name: &ColumnName) -> Option<Column> {
        self.left
            .column_index(name)
            .or_else(|| match self.right.column_index(name) {
                None => None,
                Some(c) => {
                    let col = Column::from_index(c.get_index() + self.left.number_of_columns());
                    Some(col)
                }
            })
    }
    fn get(&self, column: &Column) -> SmartReference<Value> {
        if column.get_index() < self.left.number_of_columns() {
            self.left.get(column)
        } else {
            let column = &Column::from_index(column.get_index() - self.left.number_of_columns());
            self.right.get(column)
        }
    }
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        None
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
    Box::new(ProductResults {
        left,
        right,
        row: 0,
    })
}
