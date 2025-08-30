use std::fs::{self, File};
use std::rc::Rc;

use sqlparser::ast::{CreateTable, CreateTableOptions, HiveDistributionStyle};

use crate::cast::AvailableDataTypes;
use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::file_results::read_file;
use crate::result_set_metadata::SimpleResultSetMetadata;
use crate::results::ResultSet;
use crate::results_builder::build_simple_results;
use crate::results_data::ResultsData;
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
            return Err(CvsSqlError::Unsupported("CREATE VOLATILE TABLE".into()));
        }
        if self.iceberg {
            return Err(CvsSqlError::Unsupported("CREATE ICEBERG table".into()));
        }
        if !self.constraints.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with constraints".into(),
            ));
        }
        if self.table_options != CreateTableOptions::None {
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
        if self.comment.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with comment".into()));
        }
        if self.on_commit.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with on commit".into(),
            ));
        }
        if self.on_cluster.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with on cluster".into(),
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
        if self.hive_distribution != HiveDistributionStyle::NONE {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with hive distribution".into(),
            ));
        }
        if let Some(hive_format) = &self.hive_formats
            && (hive_format.location.is_some()
                || hive_format.row_format.is_some()
                || hive_format.serde_properties.is_some()
                || hive_format.storage.is_some())
        {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with hive format".into(),
            ));
        }
        if self.without_rowid {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE without rowid".into(),
            ));
        }
        if self.inherits.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with inherits".into(),
            ));
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
            let mut writer = new_csv_writer(writer, engine.first_line_as_name);
            writer.write(&data)?;
        }

        build_simple_results(vec![
            ("action", Value::Str("CREATED".to_string())),
            ("table", Value::Str(table_name)),
            ("file", Value::Str(file_name)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{
            ClusteredBy, CommentDef, Expr, FileFormat, HiveFormat, HiveIOFormat, HiveRowFormat,
            OneOrManyWithParens, RowAccessPolicy, Statement, StorageSerializationPolicy, Value,
            ValueWithSpan,
        },
        parser::Parser,
        tokenizer::Span,
    };

    use crate::{args::Args, dialect::FilesDialect};

    use super::*;

    fn test_unsupported(change: fn(&mut CreateTable)) -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "CREATE TABLE test_one(col TEXT)";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::CreateTable(mut create)) = statement.into_iter().next() else {
            panic!("Not a create statement");
        };
        change(&mut create);

        let Err(err) = create.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }

    #[test]
    fn external_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.external = true;
        })
    }

    #[test]
    fn volatile_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.volatile = true;
        })
    }

    #[test]
    fn iceberg_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.iceberg = true;
        })
    }

    #[test]
    fn table_options_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.table_options = CreateTableOptions::With(vec![]);
        })
    }

    #[test]
    fn hive_distribution_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.hive_distribution = HiveDistributionStyle::PARTITIONED { columns: vec![] }
        })
    }

    #[test]
    fn hive_formats_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.hive_formats = Some(HiveFormat {
                row_format: Some(HiveRowFormat::DELIMITED { delimiters: vec![] }),
                serde_properties: None,
                storage: None,
                location: None,
            })
        })?;
        test_unsupported(|create| {
            create.hive_formats = Some(HiveFormat {
                row_format: None,
                serde_properties: Some(vec![]),
                storage: None,
                location: None,
            })
        })?;
        test_unsupported(|create| {
            create.hive_formats = Some(HiveFormat {
                row_format: None,
                serde_properties: None,
                storage: Some(HiveIOFormat::FileFormat {
                    format: FileFormat::PARQUET,
                }),
                location: None,
            })
        })?;
        test_unsupported(|create| {
            create.hive_formats = Some(HiveFormat {
                row_format: None,
                serde_properties: None,
                storage: None,
                location: Some(String::default()),
            })
        })
    }

    #[test]
    fn without_rowid_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.without_rowid = true)
    }

    #[test]
    fn inherits_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.inherits = Some(vec![]))
    }

    #[test]
    fn table_with_file_format_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.file_format = Some(sqlparser::ast::FileFormat::JSONFILE))
    }

    #[test]
    fn table_location_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.location = Some("location".to_string()))
    }

    #[test]
    fn table_comment_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.comment = Some(CommentDef::WithEq("location".to_string())))
    }

    #[test]
    fn table_primary_key_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });
            let expr = Box::new(expr);

            create.primary_key = Some(expr)
        })
    }

    #[test]
    fn table_order_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });

            create.order_by = Some(OneOrManyWithParens::One(expr));
        })
    }

    #[test]
    fn table_partition_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });
            let expr = Box::new(expr);

            create.partition_by = Some(expr)
        })
    }

    #[test]
    fn table_cluster_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.cluster_by = Some(sqlparser::ast::WrappedCollection::NoWrapping(vec![]))
        })
    }

    #[test]
    fn table_clustered_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let cluster_by = ClusteredBy {
                columns: vec![],
                sorted_by: None,
                num_buckets: Value::Null,
            };

            create.clustered_by = Some(cluster_by)
        })
    }

    #[test]
    fn strict_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.strict = true;
        })
    }

    #[test]
    fn copy_grants_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.copy_grants = true;
        })
    }

    #[test]
    fn enable_schema_evolution_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.enable_schema_evolution = Some(true);
        })
    }

    #[test]
    fn change_tracking_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.change_tracking = Some(true);
        })
    }

    #[test]
    fn data_retention_time_in_days_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.data_retention_time_in_days = Some(2);
        })
    }

    #[test]
    fn max_data_extension_time_in_days_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.max_data_extension_time_in_days = Some(2);
        })
    }

    #[test]
    fn default_ddl_collation_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.default_ddl_collation = Some("one".into());
        })
    }

    #[test]
    fn with_aggregation_policy_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_aggregation_policy = Some(vec![].into());
        })
    }

    #[test]
    fn with_row_access_policy_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_row_access_policy = Some(RowAccessPolicy::new(vec![].into(), vec![]));
        })
    }

    #[test]
    fn with_tags_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_tags = Some(vec![]);
        })
    }

    #[test]
    fn base_location_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.base_location = Some("location".into());
        })
    }

    #[test]
    fn catalog_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.catalog = Some("location".into());
        })
    }

    #[test]
    fn catalog_sync_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.catalog_sync = Some("location".into());
        })
    }

    #[test]
    fn storage_serialization_policy_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.storage_serialization_policy = Some(StorageSerializationPolicy::Compatible);
        })
    }

    #[test]
    fn external_volume_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.external_volume = Some("location".into());
        })
    }
}
