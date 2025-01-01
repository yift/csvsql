use std::io::{self, Write};

use csv::WriterBuilder;
use thiserror::Error;

use crate::results::ResultSet;

pub trait Writer {
    fn write(&mut self, results: &dyn ResultSet) -> Result<(), WriterError>;
}

struct CsvWriter<W: Write> {
    writer: csv::Writer<W>,
}

impl<W: Write> Writer for CsvWriter<W> {
    fn write(&mut self, results: &dyn ResultSet) -> Result<(), WriterError> {
        let headers: Vec<_> = results
            .columns()
            .map(|column| results.column_name(&column))
            .map(|name| name.map(|c| c.to_string()).unwrap_or_default())
            .collect();
        self.writer.write_record(&headers)?;
        for row in results.rows() {
            let line: Vec<_> = results
                .columns()
                .map(|column| results.get(&row, &column))
                .map(|f| f.to_string())
                .collect();
            self.writer.write_record(line)?
        }
        self.writer.flush()?;
        Ok(())
    }
}

pub fn new_csv_writer<W: Write>(w: W) -> impl Writer {
    CsvWriter {
        writer: WriterBuilder::new().from_writer(w),
    }
}

#[derive(Error, Debug)]
pub enum WriterError {
    #[error("IO Error: `{0}`")]
    IoError(#[from] io::Error),
    #[error("CSV Error: `{0}`")]
    CsvError(#[from] csv::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::results::Column;
    use crate::results::ColumnName;
    use crate::results::ResultName;
    use crate::results::Row;
    use crate::util::SmartReference;
    use crate::value::Value;
    use std::rc::Rc;

    #[test]
    fn write_writes_csv_output() -> Result<(), WriterError> {
        struct Results {
            values: Vec<Vec<Value>>,
        }
        impl ResultSet for Results {
            fn number_of_rows(&self) -> usize {
                self.values.first().unwrap().len()
            }
            fn number_of_columns(&self) -> usize {
                self.values.len()
            }
            fn column_name(&self, column: &Column) -> Option<ColumnName> {
                let name = format!("col {}", column.get_index());
                Some(ColumnName::simple(&name))
            }
            fn column_index(&self, _: &ColumnName) -> Option<Column> {
                None
            }
            fn result_name(&self) -> Option<&Rc<ResultName>> {
                None
            }
            fn get(&self, row: &Row, column: &Column) -> SmartReference<Value> {
                self.values
                    .get(column.get_index())
                    .and_then(|v| v.get(row.get_index()))
                    .unwrap_or(&Value::Empty)
                    .into()
            }
        }
        let mut values = Vec::new();
        for col in 0..5 {
            let mut data = Vec::new();
            for row in 0..3 {
                data.push(Value::Int(col * 12 + row));
            }
            values.push(data);
        }
        let results = Results { values };
        let mut write = Vec::new();

        {
            let mut writer = new_csv_writer(&mut write);
            writer.write(&results)?;
        }

        let lines = String::from_utf8(write).unwrap();
        let lines: Vec<_> = lines.lines().collect();
        assert_eq!(lines[0], "col 0,col 1,col 2,col 3,col 4");
        assert_eq!(lines[1], "0,12,24,36,48");
        assert_eq!(lines[2], "1,13,25,37,49");
        assert_eq!(lines[3], "2,14,26,38,50");

        Ok(())
    }
}
