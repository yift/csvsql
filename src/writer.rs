use std::io::{self, Write};

use csv::WriterBuilder;
use thiserror::Error;

use crate::results::ResultSet;

pub trait Writer {
    fn write(&mut self, results: &ResultSet) -> Result<(), WriterError>;
    fn append(&mut self, results: &ResultSet) -> Result<(), WriterError>;
}

struct CsvWriter<W: Write> {
    writer: csv::Writer<W>,
    with_headers: bool,
}

impl<W: Write> Writer for CsvWriter<W> {
    fn write(&mut self, results: &ResultSet) -> Result<(), WriterError> {
        if self.with_headers {
            let headers: Vec<_> = results
                .columns()
                .map(|column| results.metadata.column_title(&column))
                .collect();
            self.writer.write_record(&headers)?;
        }
        self.append(results)
    }
    fn append(&mut self, results: &ResultSet) -> Result<(), WriterError> {
        for row in results.data.iter() {
            let line: Vec<_> = results
                .columns()
                .map(|column| row.get(&column))
                .map(|f| f.to_string())
                .collect();
            self.writer.write_record(line)?
        }
        self.writer.flush()?;
        Ok(())
    }
}

pub fn new_csv_writer<W: Write>(w: W, with_headers: bool) -> impl Writer {
    CsvWriter {
        writer: WriterBuilder::new().from_writer(w),
        with_headers,
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
    use crate::result_set_metadata::SimpleResultSetMetadata;
    use crate::results_data::DataRow;
    use crate::results_data::ResultsData;
    use crate::value::Value;

    #[test]
    fn write_writes_csv_output() -> Result<(), WriterError> {
        let mut rows = Vec::new();
        let mut metadata = SimpleResultSetMetadata::new(None);
        for r in 0..3 {
            let mut row = Vec::new();
            for c in 0..5 {
                if r == 0 {
                    metadata.add_column(format!("col {}", c).as_str());
                }
                row.push(Value::Number(BigDecimal::from_i32(c * 12 + r).unwrap()));
            }
            let row = DataRow::new(row);
            rows.push(row);
        }
        let data = ResultsData::new(rows);
        let metadata = Rc::new(metadata.build());
        let results = ResultSet { metadata, data };
        let mut write = Vec::new();

        {
            let mut writer = new_csv_writer(&mut write, true);
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
    #[test]
    fn write_writes_csv_output_no_headers() -> Result<(), WriterError> {
        let mut rows = Vec::new();
        let mut metadata = SimpleResultSetMetadata::new(None);
        for r in 0..3 {
            let mut row = Vec::new();
            for c in 0..5 {
                if r == 0 {
                    metadata.add_column(format!("col {}", c).as_str());
                }
                row.push(Value::Number(BigDecimal::from_i32(c * 12 + r).unwrap()));
            }
            let row = DataRow::new(row);
            rows.push(row);
        }
        let data = ResultsData::new(rows);
        let metadata = Rc::new(metadata.build());
        let results = ResultSet { metadata, data };
        let mut write = Vec::new();

        {
            let mut writer = new_csv_writer(&mut write, false);
            writer.write(&results)?;
        }

        let lines = String::from_utf8(write).unwrap();
        let lines: Vec<_> = lines.lines().collect();
        assert_eq!(lines[0], "0,12,24,36,48");
        assert_eq!(lines[1], "1,13,25,37,49");
        assert_eq!(lines[2], "2,14,26,38,50");

        Ok(())
    }
}
