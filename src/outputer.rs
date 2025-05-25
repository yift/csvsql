use chrono::NaiveTime;
use rust_xlsxwriter::workbook::Workbook;
use rust_xlsxwriter::{ExcelDateTime, Format, XlsxError};
use serde_json::{Map, Number, Value as JsonValue};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, IsTerminal};
use std::path::PathBuf;
use std::str::FromStr;

use csv::WriterBuilder;
use std::io::Write;

use crate::args::OutputFormat;
use crate::engine::CommandExecution;
use crate::value::Value;
use crate::writer::Writer;
use crate::{args::Args, error::CvsSqlError, table::draw_table, writer::new_csv_writer};
use bigdecimal::ToPrimitive;

pub trait Outputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError>;
}

pub fn create_outputer(args: &Args) -> Result<Box<dyn Outputer>, CvsSqlError> {
    let Some(output) = &args.output else {
        return Ok(create_console_output(args));
    };
    match args.output_format {
        OutputFormat::Csv => Ok(Box::new(CsvOutputer::new(output)?)),
        OutputFormat::Txt => Ok(Box::new(TxtOutputer::new(output)?)),
        OutputFormat::Html => Ok(Box::new(HtmlOutputer::new(output)?)),
        OutputFormat::Json => Ok(Box::new(JsonOutputer::new(output)?)),
        OutputFormat::Xls => Ok(Box::new(XlsxOutputer::new(output)?)),
    }
}

fn create_console_output(args: &Args) -> Box<dyn Outputer> {
    if !args.display_as_csv
        && !args.no_console
        && io::stdout().is_terminal()
        && io::stdin().is_terminal()
        && io::stderr().is_terminal()
    {
        Box::new(TableOutputer {})
    } else {
        Box::new(StdoutOutputer {})
    }
}
struct TableOutputer {}
impl Outputer for TableOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        draw_table(&results.results)?;
        Ok(None)
    }
}

struct StdoutOutputer {}
impl Outputer for StdoutOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        let stdout = io::stdout().lock();
        let mut writer = new_csv_writer(stdout, true);
        writer.write(&results.results).ok();
        Ok(None)
    }
}

fn create_root_file_in_dir(dir: &PathBuf, file_name: &str) -> Result<PathBuf, CvsSqlError> {
    if dir.exists() {
        if dir.is_file() {
            return Err(CvsSqlError::OutputCreationError(format!(
                "File {} is a file and can not be a directory",
                dir.to_str().unwrap_or_default()
            )));
        }
    } else {
        fs::create_dir_all(dir)?
    }
    let path = dir.join(file_name);
    if path.exists() {
        Err(CvsSqlError::OutputCreationError(format!(
            "File {} already exists",
            path.to_str().unwrap_or_default()
        )))
    } else {
        Ok(path)
    }
}

struct CsvOutputer {
    index: usize,
    root: PathBuf,
    all: PathBuf,
}
impl CsvOutputer {
    fn new(dir: &PathBuf) -> Result<Self, CvsSqlError> {
        let all = create_root_file_in_dir(dir, "all.csv")?;
        let header = vec!["index", "file", "sql"];
        let mut writer = WriterBuilder::new().from_path(&all)?;
        writer.write_record(header)?;
        writer.flush()?;

        Ok(Self {
            index: 0,
            root: dir.clone(),
            all,
        })
    }
}
impl Outputer for CsvOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        self.index += 1;
        let file_name = format!("{}.csv", self.index);

        let path = self.root.join(&file_name);
        let writer = File::create(&path)?;
        let mut writer = new_csv_writer(writer, true);
        writer.write(&results.results)?;

        let file = OpenOptions::new().append(true).open(&self.all)?;

        let saved = vec![format!("{}", self.index), file_name, results.sql.clone()];
        let mut writer = WriterBuilder::new().from_writer(file);
        writer.write_record(saved)?;
        writer.flush()?;

        Ok(Some(format!(
            "File {} created",
            path.to_str().unwrap_or_default()
        )))
    }
}

struct TxtOutputer {
    index: usize,
    root: PathBuf,
    all: PathBuf,
}
impl TxtOutputer {
    fn new(dir: &PathBuf) -> Result<Self, CvsSqlError> {
        let all = create_root_file_in_dir(dir, "all.txt")?;
        let header = vec!["index", "file", "sql"];
        let mut writer = WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(csv::QuoteStyle::Never)
            .from_path(&all)?;
        writer.write_record(header)?;
        writer.flush()?;

        Ok(Self {
            index: 0,
            root: dir.clone(),
            all,
        })
    }
}
impl Outputer for TxtOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        self.index += 1;
        let file_name = format!("{}.txt", self.index);
        let path = self.root.join(&file_name);
        let writer = File::create(&path)?;
        let mut writer = WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(csv::QuoteStyle::Never)
            .from_writer(writer);
        let headers: Vec<_> = results
            .results
            .columns()
            .map(|column| results.results.metadata.column_title(&column))
            .collect();
        writer.write_record(&headers)?;
        for row in results.results.data.iter() {
            let line: Vec<_> = results
                .results
                .columns()
                .map(|column| row.get(&column))
                .map(|f| f.to_string())
                .collect();
            writer.write_record(line)?
        }

        let saved = vec![format!("{}", self.index), file_name, results.sql.clone()];
        let file = OpenOptions::new().append(true).open(&self.all)?;
        let mut writer = WriterBuilder::new()
            .delimiter(b'\t')
            .quote_style(csv::QuoteStyle::Never)
            .from_writer(file);
        writer.write_record(saved)?;
        writer.flush()?;

        Ok(Some(format!(
            "File {} created",
            path.to_str().unwrap_or_default()
        )))
    }
}

struct HtmlOutputer {
    root: PathBuf,
    sqls: Vec<String>,
}
impl HtmlOutputer {
    fn new(dir: &PathBuf) -> Result<Self, CvsSqlError> {
        let index_file = create_root_file_in_dir(dir, "index.html")?;
        let mut writer = File::create(&index_file)?;
        writeln!(&mut writer, "<html>")?;
        writeln!(&mut writer, "</html>")?;

        Ok(Self {
            root: dir.clone(),
            sqls: Vec::new(),
        })
    }

    fn update_index(&self) -> Result<(), CvsSqlError> {
        let path = self.root.join("index.html");
        let writer = OpenOptions::new().write(true).truncate(true).open(path)?;
        let mut writer = BufWriter::new(&writer);
        writeln!(&mut writer, "<!DOCTYPE html>")?;
        writeln!(&mut writer, "<html lang='en'>")?;
        writeln!(&mut writer, "<head></head>")?;
        writeln!(&mut writer, "<body>")?;
        writeln!(&mut writer, "<table style=\"width:100%\">")?;
        writeln!(&mut writer, "<tr>")?;
        writeln!(&mut writer, "<th>index</th>")?;
        writeln!(&mut writer, "<th>sql</th>")?;
        writeln!(&mut writer, "<th>results</th>")?;
        writeln!(&mut writer, "</tr>")?;
        for (i, sql) in self.sqls.iter().enumerate() {
            writeln!(&mut writer, "<tr>")?;
            writeln!(&mut writer, "<td>{}</td>", i + 1)?;
            writeln!(
                &mut writer,
                "<td><code><pre>{}</pre></code></td>",
                html_escape::encode_text(sql)
            )?;
            writeln!(
                &mut writer,
                "<td><a href={}.html>{}.html</a></td>",
                i + 1,
                i + 1
            )?;
            writeln!(&mut writer, "</tr>")?;
        }
        writeln!(&mut writer, "</table>")?;
        writeln!(&mut writer, "</body>")?;
        writeln!(&mut writer, "</html>")?;

        Ok(())
    }
}
impl Outputer for HtmlOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        let file_name = format!("{}.html", self.sqls.len() + 1);
        let path = self.root.join(file_name);
        let writer = File::create(&path)?;
        let mut writer = BufWriter::new(&writer);
        writeln!(&mut writer, "<!DOCTYPE html>")?;
        writeln!(&mut writer, "<html lang='en'>")?;
        writeln!(&mut writer, "<head></head>")?;
        writeln!(&mut writer, "<body>")?;
        writeln!(&mut writer, "<table style=\"width:100%\">")?;
        writeln!(&mut writer, "<tr>")?;
        for col in results.results.columns() {
            let name = results.results.metadata.column_title(&col);
            writeln!(&mut writer, "<th>{}</th>", html_escape::encode_text(name))?
        }
        writeln!(&mut writer, "</tr>")?;
        for row in results.results.data.iter() {
            writeln!(&mut writer, "<tr>")?;
            for col in results.results.columns() {
                let data = row.get(&col).to_string();
                writeln!(&mut writer, "<td>{}</td>", html_escape::encode_text(&data))?
            }
            writeln!(&mut writer, "</tr>")?;
        }

        writeln!(&mut writer, "</table>")?;
        writeln!(&mut writer, "</body>")?;
        writeln!(&mut writer, "</html>")?;
        self.sqls.push(results.sql.clone());

        self.update_index()?;
        Ok(Some(format!(
            "File {} created",
            path.to_str().unwrap_or_default()
        )))
    }
}

struct JsonOutputer {
    index: usize,
    root: PathBuf,
}
impl JsonOutputer {
    fn new(dir: &PathBuf) -> Result<Self, CvsSqlError> {
        if dir.exists() {
            if dir.is_file() {
                return Err(CvsSqlError::OutputCreationError(format!(
                    "File {} is a file and can not be a directory",
                    dir.to_str().unwrap_or_default()
                )));
            }
        } else {
            fs::create_dir_all(dir)?
        }

        Ok(Self {
            index: 0,
            root: dir.clone(),
        })
    }
}
impl Outputer for JsonOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        let mut data_to_write = vec![];
        for row in results.results.data.iter() {
            let mut line = Map::new();
            for col in results.results.columns() {
                let name = results.results.metadata.column_title(&col);
                if !line.contains_key(name) {
                    let data = row.get(&col);
                    let data = match data {
                        Value::Empty => JsonValue::Null,
                        Value::Bool(b) => JsonValue::Bool(*b),
                        Value::Number(num) => match Number::from_str(&num.to_string()) {
                            Ok(num) => JsonValue::Number(num),
                            Err(_) => JsonValue::String(data.to_string()),
                        },
                        _ => JsonValue::String(data.to_string()),
                    };
                    line.insert(name.to_string(), data);
                }
            }
            data_to_write.push(JsonValue::Object(line));
        }

        self.index += 1;
        let file_name = format!("{}.json", self.index);
        let path = self.root.join(file_name);
        let writer = File::create(&path)?;
        let mut data_with_sql = HashMap::new();
        data_with_sql.insert("sql", JsonValue::String(results.sql.to_string()));
        data_with_sql.insert("results", JsonValue::Array(data_to_write));
        match serde_json::to_writer_pretty(writer, &data_with_sql) {
            Ok(_) => Ok(Some(format!(
                "File {} created",
                path.to_str().unwrap_or_default()
            ))),
            Err(e) => Err(CvsSqlError::OutputCreationError(format!(
                "Can not write json: {}",
                e
            ))),
        }
    }
}

struct XlsxOutputer {
    workbook: Workbook,
    path: PathBuf,
}
impl XlsxOutputer {
    fn new(file: &PathBuf) -> Result<Self, CvsSqlError> {
        let file = match file.extension() {
            Some(ext) => {
                if ext.to_str().unwrap_or_default() != "xlsx" {
                    return Err(CvsSqlError::OutputCreationError(format!(
                        "File {} must have xlsx extension",
                        file.to_str().unwrap_or_default()
                    )));
                }
                file
            }
            None => &file.with_extension("xlsx"),
        };
        let mut workbook = Workbook::new();

        let bold_format = Format::new().set_bold();
        let sqls = workbook.add_worksheet();
        sqls.set_name("sqls")?;
        sqls.write_string_with_format(0, 0, "SQL", &bold_format)?;
        sqls.set_column_width(0, 65)?;
        sqls.write_string_with_format(0, 1, "Sheet", &bold_format)?;

        Ok(Self {
            workbook,
            path: file.clone(),
        })
    }

    fn add_worksheet(&mut self, execution: &CommandExecution) -> Result<(), XlsxError> {
        let index = self.workbook.worksheets().len() as u32;
        let name = format!("Results {}", index);
        let sqls = self.workbook.worksheet_from_name("sqls").unwrap();
        let monospace = Format::new().set_font_name("Courier New");

        sqls.write_string_with_format(index, 0, &execution.sql, &monospace)?;
        sqls.write_string(index, 1, &name)?;

        let results = &execution.results;
        let worksheet = self.workbook.add_worksheet();
        worksheet.set_name(name)?;
        let bold_format = Format::new().set_bold();
        let date_format = Format::new().set_num_format("yyyy-mm-dd");
        let time_format = Format::new().set_num_format("yyyy-mm-dd HH:MM:SS");
        let mut widths = vec![];

        for col in results.columns() {
            let name = results.metadata.column_title(&col);
            worksheet.write_with_format(0, col.get_index() as u16, name, &bold_format)?;
            widths.push(name.len() as u32);
        }

        for (index, line) in results.data.iter().enumerate() {
            for col in results.columns() {
                let data = line.get(&col);
                let row = index as u32 + 1;
                let col = col.get_index() as u16;
                match data {
                    Value::Empty => {}
                    Value::Bool(b) => {
                        worksheet.write_boolean(row, col, *b)?;
                    }
                    Value::Number(num) => match num.to_f64() {
                        Some(num) => {
                            worksheet.write_number(row, col, num)?;
                        }
                        None => {
                            worksheet.write_string(row, col, data.to_string())?;
                        }
                    },
                    Value::Str(str) => {
                        worksheet.write_string(row, col, str)?;
                    }
                    Value::Date(date) => {
                        let date = ExcelDateTime::from_timestamp(
                            date.and_time(NaiveTime::default()).and_utc().timestamp(),
                        )?;
                        worksheet.write_datetime_with_format(row, col, date, &date_format)?;
                    }
                    Value::Timestamp(date) => {
                        let date = ExcelDateTime::from_timestamp(date.and_utc().timestamp())?;
                        worksheet.write_datetime_with_format(row, col, date, &time_format)?;
                    }
                };
                let w = data.to_string().len() as u32;
                if w > widths[col as usize] {
                    widths[col as usize] = w;
                }
            }
        }

        for (i, w) in widths.iter().enumerate() {
            if *w < 8 {
                worksheet.set_column_width(i as u16, 8)?;
            } else {
                worksheet.set_column_width(i as u16, *w)?;
            }
        }

        self.workbook
            .worksheets_mut()
            .swap((index - 1) as usize, index as usize);

        self.workbook.save(&self.path)?;
        Ok(())
    }
}
impl Outputer for XlsxOutputer {
    fn write(&mut self, results: &CommandExecution) -> Result<Option<String>, CvsSqlError> {
        match self.add_worksheet(results) {
            Ok(_) => Ok(Some(format!(
                "Sheet was added to {}",
                self.path.to_str().unwrap_or_default()
            ))),
            Err(err) => Err(CvsSqlError::OutputCreationError(format!(
                "Xlsx error: {}",
                err
            ))),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use bigdecimal::{BigDecimal, FromPrimitive};
    use calamine::{Data, Reader as XlsxReader, Xlsx, open_workbook};
    use csv::{Reader, ReaderBuilder};
    use scraper::Html;
    use tempfile::{NamedTempFile, tempdir};

    use crate::{engine::Engine, results::ResultSet};

    use super::*;

    fn run_commands_of_path(
        path: PathBuf,
        commands: &str,
        format: OutputFormat,
    ) -> Result<Vec<CommandExecution>, CvsSqlError> {
        let args = Args {
            output_format: format,
            output: Some(path),
            ..Args::default()
        };
        let mut outputer = create_outputer(&args)?;
        let engine = Engine::try_from(&args)?;
        let results = engine.execute_commands(commands)?;
        for results in &results {
            outputer.write(results)?;
        }
        Ok(results)
    }

    fn verify_csv(result: &ResultSet, path: &PathBuf) -> Result<(), CvsSqlError> {
        let mut reader = Reader::from_path(path)?;
        let headers = reader.headers()?;
        for col in result.columns() {
            let expected_header = result.metadata.column_title(&col);
            let actual_header = headers.get(col.get_index()).unwrap_or_default();
            assert_eq!(actual_header, expected_header);
        }

        for (index, line) in reader.records().enumerate() {
            let actual = line?;
            let expected = result.data.get(index).unwrap();

            for col in result.columns() {
                let actual_data = actual.get(col.get_index()).unwrap();
                let expected_data = expected.get(&col).to_string();
                assert_eq!(actual_data, expected_data);
            }
        }

        Ok(())
    }

    #[test]
    fn csv_outputter_test() -> Result<(), CvsSqlError> {
        let temp_dir = tempdir()?;
        let results = run_commands_of_path(
            temp_dir.path().to_path_buf(),
            "SELECT * FROM tests.data.artists; 
            SELECT COUNT(*) FROM tests.data.artists;
            SELECT id, price, \"delivered at\"  FROM tests.data.sales WHERE \"tax percentage\" > 10 AND \"tax percentage\" < 12; "            , 
            OutputFormat::Csv,
        )?;
        for (index, results) in results.iter().enumerate() {
            let file = temp_dir.path().join(format!("{}.csv", index + 1));
            verify_csv(&results.results, &file)?;
        }

        let all = temp_dir.path().join("all.csv");
        let mut reader = Reader::from_path(all)?;
        let headers = reader.headers()?;
        assert_eq!(&headers[0], "index");
        assert_eq!(&headers[1], "file");
        assert_eq!(&headers[2], "sql");
        for (index, line) in reader.records().enumerate() {
            let actual = line?;
            assert_eq!(actual[0], format!("{}", index + 1));
            assert_eq!(actual[1], format!("{}.csv", index + 1));
            assert_eq!(actual[2], results[index].sql);
        }

        Ok(())
    }

    fn verify_txt(result: &ResultSet, path: &PathBuf) -> Result<(), CvsSqlError> {
        let mut reader = ReaderBuilder::new().delimiter(b'\t').from_path(path)?;
        let headers = reader.headers()?;
        for col in result.columns() {
            let expected_header = result.metadata.column_title(&col);
            let actual_header = headers.get(col.get_index()).unwrap_or_default();
            assert_eq!(actual_header, expected_header);
        }

        for (index, line) in reader.records().enumerate() {
            let actual = line?;
            let expected = result.data.get(index).unwrap();

            for col in result.columns() {
                let actual_data = actual.get(col.get_index()).unwrap();
                let expected_data = expected.get(&col).to_string();
                assert_eq!(actual_data, expected_data);
            }
        }

        Ok(())
    }

    #[test]
    fn txt_outputter_test() -> Result<(), CvsSqlError> {
        let temp_dir = tempdir()?;
        let results = run_commands_of_path(
            temp_dir.path().to_path_buf(),
            "SELECT * FROM tests.data.artists; 
            SELECT COUNT(*) FROM tests.data.artists;
            SELECT id, price, \"delivered at\"  FROM tests.data.sales WHERE \"tax percentage\" > 10 AND \"tax percentage\" < 12; "            , 
            OutputFormat::Txt,
        )?;
        for (index, results) in results.iter().enumerate() {
            let file = temp_dir.path().join(format!("{}.txt", index + 1));
            verify_txt(&results.results, &file)?;
        }

        let all = temp_dir.path().join("all.txt");
        let mut reader = ReaderBuilder::new().delimiter(b'\t').from_path(&all)?;

        let headers = reader.headers()?;
        assert_eq!(&headers[0], "index");
        assert_eq!(&headers[1], "file");
        assert_eq!(&headers[2], "sql");
        for (index, line) in reader.records().enumerate() {
            let actual = line?;
            assert_eq!(actual[0], format!("{}", index + 1));
            assert_eq!(actual[1], format!("{}.txt", index + 1));
            assert_eq!(actual[2], results[index].sql);
        }

        Ok(())
    }

    fn verify_html(result: &ResultSet, path: &PathBuf) -> Result<(), CvsSqlError> {
        let html = fs::read_to_string(path)?;

        let document = Html::parse_document(&html);
        assert_eq!(0, document.errors.len());
        let root = document.root_element();
        assert_eq!("html", root.value().name());
        assert_eq!(2, root.child_elements().count());
        let mut kids = root.child_elements();
        let head = kids.next().unwrap();
        assert_eq!("head", head.value().name());
        let body = kids.next().unwrap();
        assert_eq!("body", body.value().name());
        assert_eq!(1, body.child_elements().count());
        let table = body.child_elements().next().unwrap();
        assert_eq!("table", table.value().name());
        let tbody = table.child_elements().next().unwrap();
        let mut rows = tbody.child_elements();
        let header = rows.next().unwrap();
        assert_eq!(
            result.metadata.number_of_columns(),
            header.child_elements().count()
        );
        let mut header_cells = header.child_elements();
        for col in result.columns() {
            let name = result.metadata.column_title(&col);
            let cell = header_cells.next().unwrap();
            assert_eq!("th", cell.value().name());
            assert_eq!(name, cell.text().next().unwrap());
        }

        for expected_row in result.data.iter() {
            let actual_row = rows.next().unwrap();
            assert_eq!(
                result.metadata.number_of_columns(),
                actual_row.child_elements().count()
            );
            let mut cells = actual_row.child_elements();
            for col in result.columns() {
                let value = expected_row.get(&col).to_string();
                let cell = cells.next().unwrap();
                assert_eq!("td", cell.value().name());
                assert_eq!(value, cell.text().next().unwrap_or_default());
            }
        }

        assert!(rows.next().is_none());

        Ok(())
    }

    fn verify_html_index(result: &[CommandExecution], path: &Path) -> Result<(), CvsSqlError> {
        let html = fs::read_to_string(path.join("index.html"))?;
        let document = Html::parse_document(&html);
        assert_eq!(0, document.errors.len());
        let root = document.root_element();
        assert_eq!("html", root.value().name());
        assert_eq!(2, root.child_elements().count());
        let mut kids = root.child_elements();
        let head = kids.next().unwrap();
        assert_eq!("head", head.value().name());
        let body = kids.next().unwrap();
        assert_eq!("body", body.value().name());
        assert_eq!(1, body.child_elements().count());
        let table = body.child_elements().next().unwrap();
        assert_eq!("table", table.value().name());
        let tbody = table.child_elements().next().unwrap();
        let mut rows = tbody.child_elements();
        let header = rows.next().unwrap();
        assert_eq!(3, header.child_elements().count());
        let mut header_cells = header.child_elements();
        let cell = header_cells.next().unwrap();
        assert_eq!("th", cell.value().name());
        assert_eq!("index", cell.text().next().unwrap());
        let cell = header_cells.next().unwrap();
        assert_eq!("th", cell.value().name());
        assert_eq!("sql", cell.text().next().unwrap());
        let cell = header_cells.next().unwrap();
        assert_eq!("th", cell.value().name());
        assert_eq!("results", cell.text().next().unwrap());
        assert!(header_cells.next().is_none());

        for (index, expected_row) in result.iter().enumerate() {
            let actual_row = rows.next().unwrap();
            assert_eq!(3, actual_row.child_elements().count());
            let mut cells = actual_row.child_elements();
            let cell = cells.next().unwrap();
            assert_eq!("td", cell.value().name());
            assert_eq!(
                format!("{}", index + 1),
                cell.text().next().unwrap_or_default()
            );
            let cell = cells.next().unwrap();
            assert_eq!("td", cell.value().name());
            assert_eq!(expected_row.sql, cell.text().next().unwrap_or_default());
            let cell = cells.next().unwrap();
            assert_eq!("td", cell.value().name());
            let a = cell.child_elements().next().unwrap();
            assert_eq!("a", a.value().name());
            assert_eq!(
                format!("{}.html", index + 1),
                a.text().next().unwrap_or_default()
            );
            assert_eq!(a.attr("href").unwrap(), format!("{}.html", index + 1))
        }

        assert!(rows.next().is_none());

        Ok(())
    }

    #[test]
    fn html_outputter_test() -> Result<(), CvsSqlError> {
        let temp_dir = tempdir()?;
        let results = run_commands_of_path(
            temp_dir.path().to_path_buf(),
            "SELECT * FROM tests.data.artists; 
            SELECT COUNT(*) FROM tests.data.artists;
            SELECT id, price, \"delivered at\"  FROM tests.data.sales WHERE \"tax percentage\" > 10 AND \"tax percentage\" < 12; "            , 
            OutputFormat::Html,
        )?;
        for (index, results) in results.iter().enumerate() {
            let file = temp_dir.path().join(format!("{}.html", index + 1));
            verify_html(&results.results, &file)?;
        }

        verify_html_index(&results, temp_dir.path())
    }

    #[test]
    fn json_outputter_test() -> Result<(), CvsSqlError> {
        let temp_dir = tempdir()?;
        let results = run_commands_of_path(
            temp_dir.path().to_path_buf(),
            "SELECT * FROM tests.data.artists; 
            SELECT COUNT(*) FROM tests.data.artists;
            SELECT id, price, \"delivered at\"  FROM tests.data.sales WHERE \"tax percentage\" > 10 AND \"tax percentage\" < 12; "            , 
            OutputFormat::Json,
        )?;

        for (i, result) in results.iter().enumerate() {
            let file = temp_dir.path().join(format!("{}.json", i + 1));
            let file = File::open(file)?;
            let json: JsonValue = serde_json::from_reader(file).unwrap();
            let json = json.as_object().unwrap();
            assert_eq!(json.get("sql").unwrap().as_str().unwrap(), result.sql);
            let actual_results = json.get("results").unwrap().as_array().unwrap();
            for (actual_row, expected_row) in actual_results.iter().zip(result.results.data.iter())
            {
                let actual_row = actual_row.as_object().unwrap();
                for col in result.results.columns() {
                    let expected_data = expected_row.get(&col);
                    let actual_data = actual_row
                        .get(result.results.metadata.column_title(&col))
                        .unwrap();
                    let expected_data = match expected_data {
                        Value::Empty => JsonValue::Null,
                        Value::Bool(b) => JsonValue::Bool(*b),
                        Value::Number(num) => match Number::from_str(&num.to_string()) {
                            Ok(num) => JsonValue::Number(num),
                            Err(_) => JsonValue::String(expected_data.to_string()),
                        },
                        _ => JsonValue::String(expected_data.to_string()),
                    };
                    assert_eq!(&expected_data, actual_data);
                }
            }
        }

        Ok(())
    }

    #[test]
    fn excel_outputter_test() -> Result<(), CvsSqlError> {
        let temp_file = NamedTempFile::with_suffix(".xlsx")?;
        let results = run_commands_of_path(
            temp_file.path().to_path_buf(),
            "SELECT * FROM tests.data.artists; 
            SELECT COUNT(*) FROM tests.data.artists;
            SELECT id, price * 100, \"delivered at\"  FROM tests.data.sales WHERE \"tax percentage\" > 10 AND \"tax percentage\" < 12; "            , 
            OutputFormat::Xls,
        )?;

        let mut workbook: Xlsx<_> = open_workbook(temp_file.path()).unwrap();

        let sqls = workbook.worksheet_range("sqls").unwrap();
        let (rows, cols) = sqls.get_size();
        assert_eq!(rows, results.len() + 1);
        assert_eq!(cols, 2);
        let cell = sqls.get_value((0, 0)).unwrap();
        let Data::String(sqls_title) = cell else {
            panic!("Expecting string cell");
        };
        assert_eq!(sqls_title, "SQL");
        let cell = sqls.get_value((0, 1)).unwrap();
        let Data::String(sheets_title) = cell else {
            panic!("Expecting string cell");
        };
        assert_eq!(sheets_title, "Sheet");

        for (index, results) in results.iter().enumerate() {
            let name = format!("Results {}", index + 1);
            let cell = sqls.get_value(((index + 1) as u32, 0)).unwrap();
            let Data::String(sql) = cell else {
                panic!("Expecting string cell");
            };
            assert_eq!(sql, &results.sql);
            let cell = sqls.get_value(((index + 1) as u32, 1)).unwrap();
            let Data::String(actual_name) = cell else {
                panic!("Expecting string cell");
            };
            assert_eq!(actual_name, &name);

            let sheet = workbook.worksheet_range(&name).unwrap();
            let (rows, cols) = sheet.get_size();
            assert_eq!(rows, results.results.data.iter().count() + 1);
            assert_eq!(cols, results.results.metadata.number_of_columns());
            for col in results.results.columns() {
                let expected_name = results.results.metadata.column_title(&col);
                let cell = sheet.get_value((0, col.get_index() as u32)).unwrap();
                let Data::String(actual_name) = cell else {
                    panic!("Expecting string cell");
                };
                assert_eq!(expected_name, actual_name);
            }
            for (row_index, actual_row) in results.results.data.iter().enumerate() {
                for col in results.results.columns() {
                    let cell = sheet
                        .get_value(((row_index + 1) as u32, col.get_index() as u32))
                        .unwrap();
                    let actual_value = actual_row.get(&col);
                    let expected_value = match cell {
                        Data::Bool(b) => Value::Bool(*b),
                        Data::Empty => Value::Empty,
                        Data::String(str) => Value::Str(str.to_string()),
                        Data::Float(f) => {
                            Value::Number(BigDecimal::from_f64(*f).unwrap().normalized())
                        }
                        Data::DateTime(tm) => Value::Timestamp(tm.as_datetime().unwrap()),
                        _ => {
                            panic!("Unexpected cell");
                        }
                    };
                    assert_eq!(actual_value, &expected_value);
                }
            }
        }
        Ok(())
    }
}
