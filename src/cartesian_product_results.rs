use crate::result_set_metadata::Metadata;
use crate::results::ResultSet;
use crate::results_data::{DataRow, ResultsData};

pub fn join(left: ResultSet, right: ResultSet) -> ResultSet {
    let mut data = Vec::new();
    for l in left.data.iter() {
        for r in left.data.iter() {
            let mut row = Vec::new();
            for left_column in left.columns() {
                row.push(l.get(&left_column).clone());
            }
            for right_column in right.columns() {
                row.push(r.get(&right_column).clone());
            }
            let row = DataRow::new(row);
            data.push(row);
        }
    }
    let data = ResultsData::new(data);
    let metadata = Metadata::product(left.metadata, right.metadata);
    ResultSet { data, metadata }
}
