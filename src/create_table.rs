use std::fs::{self, File};
use std::rc::Rc;

use sqlparser::ast::CreateTable;

use crate::cast::AvailableDataTypes;
use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::file_results::read_file;
use crate::result_set_metadata::{Metadata, SimpleResultSetMetadata};
use crate::results::ResultSet;
use crate::results_data::{DataRow, ResultsData};
use crate::value::Value;
use crate::writer::{Writer, new_csv_writer};

impl Extractor for CreateTable {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.external {
            return Err(CvsSqlError::Unsupported("CREATE EXTERNAL TABLE".into()));
        }
        if self.global == Some(true) {
            return Err(CvsSqlError::Unsupported("CREATE GLOBAL TABLE".into()));
        }
        if self.or_replace {
            return Err(CvsSqlError::Unsupported("CREATE OR REPLACE".into()));
        }
        if self.transient {
            return Err(CvsSqlError::Unsupported("CREATE TRANSIENT TABLE".into()));
        }
        if self.volatile {
            return Err(CvsSqlError::Unsupported("CREATE VOLOTILE TABLE".into()));
        }
        if self.iceberg {
            return Err(CvsSqlError::Unsupported("CREATE ICEBERG table".into()));
        }
        if !self.constraints.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with constraints".into(),
            ));
        }
        if !self.table_properties.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with properties".into(),
            ));
        }
        if !self.with_options.is_empty() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with options".into()));
        }
        if self.file_format.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with format".into()));
        }
        if self.location.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with location".into(),
            ));
        }
        if self.engine.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with engine".into()));
        }
        if self.comment.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with comment".into()));
        }
        if self.auto_increment_offset.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with auto increment offset".into(),
            ));
        }
        if self.default_charset.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with default charset".into(),
            ));
        }
        if self.collation.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with collation".into(),
            ));
        }
        if self.on_commit.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with on commit".into(),
            ));
        }
        if self.on_cluster.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with on clustert".into(),
            ));
        }
        if self.primary_key.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with PRIMARY KEY".into(),
            ));
        }
        if self.order_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with order by".into(),
            ));
        }
        if self.partition_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with partition by".into(),
            ));
        }
        if self.cluster_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with cluster by".into(),
            ));
        }
        if self.clustered_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with clustered by".into(),
            ));
        }
        if self.options.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with options".into()));
        }
        if self.strict {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with strict".into()));
        }
        if self.copy_grants {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with copy grants".into(),
            ));
        }

        if self.enable_schema_evolution.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with enable_schema_evolution".into(),
            ));
        }
        if self.change_tracking.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with change_tracking".into(),
            ));
        }
        if self.data_retention_time_in_days.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with data_retention_time_in_days".into(),
            ));
        }
        if self.max_data_extension_time_in_days.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with max_data_extension_time_in_days".into(),
            ));
        }
        if self.default_ddl_collation.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with default_ddl_collation".into(),
            ));
        }
        if self.with_aggregation_policy.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with with_aggregation_policy".into(),
            ));
        }
        if self.with_row_access_policy.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with with_row_access_policy".into(),
            ));
        }
        if self.with_tags.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with with_tags".into(),
            ));
        }
        if self.external_volume.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with external_volume".into(),
            ));
        }
        if self.base_location.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with base_location".into(),
            ));
        }
        if self.catalog.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with catalog".into()));
        }
        if self.catalog_sync.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with catalog_sync".into(),
            ));
        }
        if self.storage_serialization_policy.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with storage_serialization_policy".into(),
            ));
        }
        if self.or_replace {
            return Err(CvsSqlError::Unsupported("CREATE OR REPLACE TABLE".into()));
        }

        let file = if self.temporary {
            engine.create_temp_file(&self.name)?
        } else {
            engine.file_name(&self.name)?
        };
        if file.read_only {
            return Err(CvsSqlError::ReadOnlyMode);
        }
        if file.is_temp && !self.temporary {
            return Err(CvsSqlError::TemporaryTableyExists(
                file.result_name.full_name(),
            ));
        }
        let data = if !self.columns.is_empty() {
            let mut metadata = SimpleResultSetMetadata::new(None);
            for col in &self.columns {
                AvailableDataTypes::try_from(&col.data_type)?;
                metadata.add_column(&col.name.to_string());
            }
            let metadata = Rc::new(metadata.build());
            ResultSet {
                metadata,
                data: ResultsData::new(vec![]),
            }
        } else if let Some(query) = &self.query {
            query.extract(engine)?
        } else if let Some(like) = &self.like {
            let data = read_file(engine, like)?;
            ResultSet {
                metadata: data.metadata.clone(),
                data: ResultsData::new(vec![]),
            }
        } else if let Some(clone) = &self.clone {
            read_file(engine, clone)?
        } else {
            return Err(CvsSqlError::NoTableStructuye(file.result_name.full_name()));
        };

        let file_name = engine.get_file_name(&file);
        let table_name = file.result_name.full_name();
        if file.exists {
            if !self.if_not_exists {
                return Err(CvsSqlError::TableAlreadyExists(table_name));
            }
        } else {
            if let Some(parent) = file.path.parent() {
                fs::create_dir_all(parent)?;
            }
            let writer = File::create(file.path)?;
            let mut writer = new_csv_writer(writer);
            if engine.first_line_as_name {
                writer.write(&data)?;
            } else {
                writer.append(&data)?;
            }
        }

        let mut metadata = SimpleResultSetMetadata::new(None);
        metadata.add_column("action");
        metadata.add_column("table");
        metadata.add_column("file");
        let metadata = Metadata::Simple(metadata);

        let row = vec![
            Value::Str("CREATED".to_string()),
            Value::Str(table_name),
            Value::Str(file_name),
        ];
        let row = DataRow::new(row);
        let data = vec![row];
        let data = ResultsData::new(data);
        let metadata = Rc::new(metadata);
        let results = ResultSet { metadata, data };

        Ok(results)
    }
}
