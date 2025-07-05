use std::rc::Rc;

use sqlparser::ast::Values;

use crate::{
    engine::Engine,
    error::CvsSqlError,
    extractor::Extractor,
    group_by::GroupRow,
    projections::SingleConvert,
    result_set_metadata::SimpleResultSetMetadata,
    results::ResultSet,
    results_data::{DataRow, ResultsData},
};

impl Extractor for Values {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        let empty_metadat = SimpleResultSetMetadata::new(None).build();
        let empty_row = DataRow::new(vec![]);
        let empty_row = GroupRow {
            data: empty_row,
            group_rows: vec![],
        };
        let mut size = 0;
        let mut data = vec![];

        for row in &self.rows {
            if row.len() > size {
                size = row.len()
            }
            let mut line = vec![];
            for expr in row {
                let item = expr.convert_single(&empty_metadat, engine)?;
                let val = item.get(&empty_row).clone();
                line.push(val);
            }
            let row = DataRow::new(line);
            data.push(row);
        }

        let data = ResultsData::new(data);
        let mut metadata = SimpleResultSetMetadata::new(None);
        for i in 0..size {
            metadata.add_column(&format!("{i}"));
        }
        let metadata = Rc::new(metadata.build());

        Ok(ResultSet { data, metadata })
    }
}
