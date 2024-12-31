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
