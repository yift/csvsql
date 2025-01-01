use std::collections::HashMap;
use std::io::Error as IoError;
use std::path::Path;
use std::rc::Rc;

use csv::ReaderBuilder;
use sqlparser::ast::Ident;

use crate::results::Column;
use crate::results::ColumnName;
use crate::results::ResultName;
use crate::results::Row;
use crate::util::SmartReference;
use crate::{results::ResultSet, value::Value};

struct FileColumn {
    header: Option<String>,
    column_index: String,
}

impl FileColumn {
    fn new(header: Option<&str>, index: usize) -> Self {
        let column_index = FileColumn::get_default_header(index);
        let header = header.map(str::to_string);
        FileColumn {
            header,
            column_index,
        }
    }
    fn get_default_header(index: usize) -> String {
        let mut index = index;
        let mut title = String::new();
        let first = 'A' as usize;
        let size = 'Z' as usize - first + 1;
        loop {
            let chr = index % (size);
            index -= chr;
            title.insert(0, char::from((chr + first) as u8));
            if index == 0 {
                break;
            }
            index = index / size - 1;
        }
        title
    }
    fn name(&self) -> &str {
        match &self.header {
            None => &self.column_index,
            Some(name) => name,
        }
    }
}

#[derive(Default)]
struct Columns {
    columns: Vec<FileColumn>,
    names: HashMap<String, Column>,
}

impl Columns {
    fn push(&mut self, column: FileColumn) {
        let index = self.len();
        self.names
            .entry(column.column_index.to_string())
            .or_insert(Column::from_index(index));
        if let Some(name) = &column.header {
            self.names
                .entry(name.to_string())
                .or_insert(Column::from_index(index));
        }
        self.columns.push(column);
    }

    fn len(&self) -> usize {
        self.columns.len()
    }
}

struct FileResultSet {
    data: Vec<Vec<Value>>,
    file_name: Rc<ResultName>,
    columns: Columns,
}
impl ResultSet for FileResultSet {
    fn number_of_rows(&self) -> usize {
        self.data.len()
    }
    fn number_of_columns(&self) -> usize {
        self.columns.columns.len()
    }
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        Some(&self.file_name)
    }
    fn column_name(&self, column: &Column) -> Option<ColumnName> {
        self.columns
            .columns
            .get(column.get_index())
            .map(|col| col.name())
            .map(|name| ColumnName::new(&self.file_name, name))
    }
    fn column_index(&self, name: &ColumnName) -> Option<Column> {
        if self.file_name.matches(name.parent()) {
            self.columns.names.get(name.name()).cloned()
        } else {
            None
        }
    }
    fn get(&self, row: &Row, column: &Column) -> SmartReference<Value> {
        self.data
            .get(row.get_index())
            .and_then(|l| l.get(column.get_index()))
            .unwrap_or(&Value::Empty)
            .into()
    }
}

impl FileResultSet {
    fn new(file_name: &[Ident], root: &Path, first_line_as_name: bool) -> Result<Self, IoError> {
        let mut file_names = file_name.iter().peekable();
        let mut path = root.to_path_buf();
        let mut result_name = ResultName::root();
        while let Some(name) = file_names.next() {
            let name = name.to_string();
            result_name = result_name.append(&name);
            if file_names.peek().is_none() {
                path = path.join(format!("{}.csv", name));
            } else {
                path = path.join(name);
            }
        }

        let mut reader = ReaderBuilder::new()
            .flexible(true)
            .has_headers(first_line_as_name)
            .from_path(path)?;

        let mut columns = Columns::default();

        if first_line_as_name {
            let header = reader.headers()?;
            for (index, h) in header.iter().enumerate() {
                columns.push(FileColumn::new(Some(h), index));
            }
        }
        let mut data = Vec::new();
        for records in reader.records() {
            let mut values = Vec::new();
            let records = records?;
            for (index, record) in records.iter().enumerate() {
                let value = Value::from(record);
                values.push(value);
                if index >= columns.len() {
                    columns.push(FileColumn::new(None, index));
                }
            }
            data.push(values);
        }

        Ok(FileResultSet {
            data,
            file_name: Rc::new(result_name),
            columns,
        })
    }
}

pub fn read_file(
    file_name: &[Ident],
    root: &Path,
    first_line_as_name: bool,
) -> Result<Box<dyn ResultSet>, IoError> {
    Ok(Box::new(FileResultSet::new(
        file_name,
        root,
        first_line_as_name,
    )?))
}
