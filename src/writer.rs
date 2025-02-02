use std::io::{self, Write};

use csv::WriterBuilder;
use thiserror::Error;

use crate::results::ResultSet;

pub trait Writer {
    fn write(&mut self, results: &mut dyn ResultSet) -> Result<(), WriterError>;
}

struct CsvWriter<W: Write> {
    writer: csv::Writer<W>,
}

impl<W: Write> Writer for CsvWriter<W> {
    fn write(&mut self, results: &mut dyn ResultSet) -> Result<(), WriterError> {
        let headers: Vec<_> = results
            .columns()
            .map(|column| results.metadate().column_name(&column))
            .map(|name| name.map(|c| c.short_name()).unwrap_or_default())
            .collect();
        self.writer.write_record(&headers)?;
        while results.next_if_possible() {
            let line: Vec<_> = results
                .columns()
                .map(|column| results.get(&column))
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
    use std::rc::Rc;

    use bigdecimal::BigDecimal;
    use bigdecimal::FromPrimitive;

    use super::*;
    use crate::results::Column;
    use crate::results::ColumnIndexError;
    use crate::results::Name;
    use crate::results::ResultSetMetadata;
    use crate::util::SmartReference;
    use crate::value::Value;

    #[test]
    fn write_writes_csv_output() -> Result<(), WriterError> {
        struct Results {
            values: Vec<Vec<Value>>,
            index: usize,
            metadata: Rc<dyn ResultSetMetadata>,
        }
        #[derive(Default)]
        struct Metadata {
            names: Vec<Name>,
        }
        impl ResultSetMetadata for Metadata {
            fn column_index(
                &self,
                name: &Name,
            ) -> Result<SmartReference<Column>, ColumnIndexError> {
                Err(ColumnIndexError::NoSuchColumn(name.full_name()))
            }
            fn number_of_columns(&self) -> usize {
                self.names.len()
            }
            fn column_name(&self, column: &Column) -> Option<&Name> {
                self.names.get(column.get_index())
            }
            fn result_name(&self) -> Option<&Name> {
                None
            }
        }
        impl ResultSet for Results {
            fn metadate(&self) -> &Rc<dyn ResultSetMetadata> {
                &self.metadata
            }
            fn get(&self, column: &Column) -> SmartReference<Value> {
                self.values
                    .get(column.get_index())
                    .and_then(|v| v.get(self.index - 1))
                    .unwrap_or(&Value::Empty)
                    .into()
            }
            fn next_if_possible(&mut self) -> bool {
                self.index += 1;
                self.values.len() >= self.index
            }
            fn revert(&mut self) {
                self.index = 0;
            }
        }
        let mut values = Vec::new();
        let mut metadata = Metadata::default();
        for col in 0..5 {
            metadata.names.push(format!("col {}", col).into());
            let mut data = Vec::new();
            for row in 0..3 {
                data.push(Value::Number(BigDecimal::from_i32(col * 12 + row).unwrap()));
            }
            values.push(data);
        }
        let metadata = Rc::new(metadata);
        let mut results = Results {
            metadata,
            values,
            index: 0,
        };
        let mut write = Vec::new();

        {
            let mut writer = new_csv_writer(&mut write);
            writer.write(&mut results)?;
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
