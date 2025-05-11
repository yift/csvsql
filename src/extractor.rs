use sqlparser::ast::{
    Expr, GroupByExpr, LimitClause, OrderBy, Query, Select, SetExpr, Statement, TableFactor,
};

use crate::alter::alter;
use crate::drop::drop_table;
use crate::error::CvsSqlError;
use crate::file_results::read_file;
use crate::filter_results::{apply_having, make_filter};
use crate::group_by::{force_group_by, group_by};
use crate::join::create_join;
use crate::named_results::alias_results;
use crate::order_by_results::order_by;
use crate::projections::make_projection;
use crate::transaction::{commit_transaction, rollback_transaction, start_transaction};
use crate::trimmer::trim;
use crate::update::update_table;
use crate::{engine::Engine, results::ResultSet};
pub trait Extractor {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError>;
}

impl Extractor for Statement {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        match self {
            Statement::Query(query) => query.extract(engine),
            Statement::CreateTable(table) => table.extract(engine),
            Statement::Insert(insert) => insert.extract(engine),
            Statement::Update {
                table,
                assignments,
                from: _,
                selection,
                returning,
                or,
            } => update_table(engine, table, assignments, selection, returning, or),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
            } => drop_table(
                engine,
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
            ),
            Statement::Delete(delete) => delete.extract(engine),
            Statement::AlterTable {
                name,
                if_exists,
                only: _,
                operations,
                location,
                on_cluster,
            } => alter(engine, name, *if_exists, operations, location, on_cluster),
            Statement::StartTransaction {
                modes,
                begin: _,
                transaction,
                modifier,
                statements,
                exception_statements,
                has_end_keyword: _,
            } => start_transaction(
                engine,
                modes,
                transaction,
                modifier,
                statements,
                exception_statements,
            ),
            Statement::Commit {
                chain: _,
                end: _,
                modifier,
            } => commit_transaction(engine, modifier),
            Statement::Rollback {
                chain: _,
                savepoint,
            } => rollback_transaction(engine, savepoint),
            _ => Err(CvsSqlError::Unsupported(self.to_string())),
        }
    }
}

impl Extractor for Query {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.fetch.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... FETCH".to_string()));
        }
        if self.for_clause.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... FOR".to_string()));
        }
        if self.with.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... WITH".to_string()));
        }
        let (limit, offset) = match &self.limit_clause {
            None => (None, None),
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(offset), Some(limit)),
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(CvsSqlError::Unsupported("SELECT ... LIMIT BY".to_string()));
                }
                let offset = offset.as_ref().map(|o| &o.value);
                let limit = limit.as_ref();
                (limit, offset)
            }
        };
        if !self.locks.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "SELECT ... FOR UPDATE/SHARE".to_string(),
            ));
        }
        if self.settings.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... SETTINGS".to_string()));
        }
        if self.format_clause.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... FORMAT".to_string()));
        }

        match &*self.body {
            SetExpr::Select(select) => {
                extract(select, &self.order_by, limit, offset, engine, false)
            }
            SetExpr::Values(values) => values.extract(engine),
            _ => Err(CvsSqlError::Unsupported(format!("SELECT {}", self.body))),
        }
    }
}
impl Extractor for Select {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        extract(self, &None, None, None, engine, false)
    }
}
fn extract(
    select: &Select,
    order: &Option<OrderBy>,
    limit: Option<&Expr>,
    offset: Option<&Expr>,
    engine: &Engine,
    force_group: bool,
) -> Result<ResultSet, CvsSqlError> {
    if select.distinct.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT DISTINCT".to_string()));
    }
    if select.top.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT TOP".to_string()));
    }
    if select.top_before_distinct {
        return Err(CvsSqlError::Unsupported("SELECT ALL".to_string()));
    }
    if select.into.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT INTO".to_string()));
    }
    if !select.lateral_views.is_empty() {
        return Err(CvsSqlError::Unsupported("SELECT LATERAL VIEW".to_string()));
    }
    if select.prewhere.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT ... PREWHERE".to_string()));
    }
    if !select.cluster_by.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... CLUSTER BY".to_string(),
        ));
    }
    if !select.distribute_by.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... DISTRIBUTE BY".to_string(),
        ));
    }
    if !select.sort_by.is_empty() {
        return Err(CvsSqlError::Unsupported("SELECT ... SORT BY".to_string()));
    }
    if !select.named_window.is_empty() || select.window_before_qualify {
        return Err(CvsSqlError::Unsupported("SELECT ... WINDOW ".to_string()));
    }
    if select.qualify.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT ... QUALIFY".to_string()));
    }
    if select.value_table_mode.is_some() {
        return Err(CvsSqlError::Unsupported(
            "SELECT AS VALUE/STRUCT".to_string(),
        ));
    }
    if select.connect_by.is_some() {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... CONNECT BY".to_string(),
        ));
    }

    if select.from.is_empty() {
        return Err(CvsSqlError::Unsupported("SELECT without FROM".to_string()));
    }

    let product = create_join(&select.from, engine)?;

    let filter = make_filter(engine, &select.selection, product)?;

    let mut group_by = if force_group {
        force_group_by(filter)
    } else {
        match &select.group_by {
            GroupByExpr::All(_) => {
                return Err(CvsSqlError::Unsupported(
                    "SELECT ... GROUP BY ALL".to_string(),
                ));
            }
            GroupByExpr::Expressions(exp, mods) => {
                if !mods.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... GROUP BY WITH".to_string(),
                    ));
                }
                group_by(engine, exp, filter)?
            }
        }
    };
    apply_having(engine, &select.having, &mut group_by)?;

    order_by(engine, order, &mut group_by)?;
    trim(limit, offset, engine, &mut group_by)?;
    match make_projection(engine, group_by, &select.projection) {
        Ok(proj) => Ok(proj),
        Err(CvsSqlError::NoGroupBy) => {
            if !force_group {
                extract(select, order, limit, offset, engine, true)
            } else {
                Err(CvsSqlError::NoGroupBy)
            }
        }
        Err(e) => Err(e),
    }
}

impl Extractor for TableFactor {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if args.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with table arguments".to_string(),
                    ));
                }
                if !with_hints.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM  WITH".to_string(),
                    ));
                }
                if version.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM  with version".to_string(),
                    ));
                }
                if *with_ordinality {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM  WITH ORDINALITY".to_string(),
                    ));
                }
                if !partitions.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with partition".to_string(),
                    ));
                }
                if json_path.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with JSON path".to_string(),
                    ));
                }
                if sample.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with sample".to_string(),
                    ));
                }
                if !index_hints.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with index hints".to_string(),
                    ));
                }

                let results = read_file(engine, name)?;
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Err(CvsSqlError::Unsupported(
                            "SELECT ... FROM with subquery column alias".to_string(),
                        ));
                    }
                    Ok(alias_results(&alias.name, results))
                } else {
                    Ok(results)
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with lateral subquery".to_string(),
                    ));
                }
                let results = subquery.extract(engine)?;
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Err(CvsSqlError::Unsupported(
                            "SELECT ... FROM with subquery column alias".to_string(),
                        ));
                    }
                    Ok(alias_results(&alias.name, results))
                } else {
                    Ok(results)
                }
            }
            _ => Err(CvsSqlError::Unsupported(
                "SELECT ... FROM must be a table or sub query".to_string(),
            )),
        }
    }
}
