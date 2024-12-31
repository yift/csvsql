use crate::results::Column;
use crate::results::ColumnName;
use crate::results::ResultName;
use crate::results::ResultSet;
use crate::results::Row;
use crate::value::Value;
use std::rc::Rc;
struct ProductResults {
    left: Box<dyn ResultSet>,
    right: Box<dyn ResultSet>,
}
impl ResultSet for ProductResults {
    fn number_of_rows(&self) -> usize {
        self.left.number_of_rows() * self.right.number_of_rows()
    }
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
    fn column_index(&self, name: &ColumnName) -> Option<&Column> {
        self.left
            .column_index(name)
            .or_else(|| self.right.column_index(name))
    }
    fn get(&self, row: &Row, column: &Column) -> &Value {
        if column.get_index() < self.left.number_of_columns() {
            let row = &Row::from_index(row.get_index() / self.right.number_of_rows());
            self.left.get(row, column)
        } else {
            let row = &Row::from_index(row.get_index() % self.right.number_of_rows());
            let column = &Column::from_index(column.get_index() - self.left.number_of_columns());
            self.right.get(row, column)
        }
    }
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        None
    }
}

pub fn join(left: Box<dyn ResultSet>, right: Box<dyn ResultSet>) -> Box<dyn ResultSet> {
    Box::new(ProductResults { left, right })
}
