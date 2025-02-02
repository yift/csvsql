use std::io::Error as IoError;
use std::path::Path;
use std::rc::Rc;

use csv::ReaderBuilder;
use sqlparser::ast::Ident;

use crate::results::{Column, Name, ResultSetMetadata};
use crate::simple_result_set_metadata::SimpleResultSetMetadata;
use crate::util::SmartReference;
use crate::{results::ResultSet, value::Value};

fn get_default_header(index: usize) -> String {
    let mut index = index;
    let mut title = String::from("$");
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

struct FileResultSet {
    metadata: Rc<dyn ResultSetMetadata>,
    data: Vec<Vec<Value>>,
    index: usize,
}
impl ResultSet for FileResultSet {
    fn metadate(&self) -> &Rc<dyn ResultSetMetadata> {
        &self.metadata
    }
    fn next_if_possible(&mut self) -> bool {
        self.index += 1;
        self.index <= self.data.len()
    }
    fn revert(&mut self) {
        self.index = 0;
    }
    fn get<'a>(&'a self, column: &Column) -> SmartReference<'a, Value> {
        if self.index == 0 {
            return Value::Empty.into();
        }
        self.data
            .get(self.index - 1)
            .and_then(|l| l.get(column.get_index()))
            .unwrap_or(&Value::Empty)
            .into()
    }
}

trait AppendName {
    fn append(&self, name: &str) -> Self;
}
impl AppendName for Option<Name> {
    fn append(&self, name: &str) -> Self {
        match self {
            None => Some(name.into()),
            Some(parent) => Some(parent.append(name)),
        }
    }
}
impl FileResultSet {
    fn new(file_name: &[Ident], root: &Path, first_line_as_name: bool) -> Result<Self, IoError> {
        let mut file_names = file_name.iter().peekable();
        let mut path = root.to_path_buf();
        let mut result_name = None;
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

        let mut metadata = SimpleResultSetMetadata::new(result_name);

        if first_line_as_name {
            let header = reader.headers()?;
            for h in header {
                metadata.add_column(h);
            }
        }
        let mut data = Vec::new();
        for records in reader.records() {
            let mut values = Vec::new();
            let records = records?;
            for (index, record) in records.iter().enumerate() {
                let value = Value::from(record);
                values.push(value);
                if index >= metadata.number_of_columns() {
                    metadata.add_column(&get_default_header(index));
                }
            }
            data.push(values);
        }
        let metadata = Rc::new(metadata);

        Ok(FileResultSet {
            data,
            metadata,
            index: 0,
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
