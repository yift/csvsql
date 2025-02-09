use sqlparser::ast::{GroupByExpr, Query, Select, SetExpr, Statement, TableFactor};

use crate::cartesian_product_results::join;
use crate::error::CvsSqlError;
use crate::file_results::read_file;
use crate::filter_results::make_filter;
use crate::named_results::alias_results;
use crate::projections::make_projection;
use crate::{engine::Engine, results::ResultSet};
pub trait Extractor {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError>;
}

impl Extractor for Statement {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        match self {
            Statement::Query(query) => query.extract(engine),
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
        if !self.limit_by.is_empty() {
            return Err(CvsSqlError::Unsupported("SELECT ... LIMIT BY".to_string()));
        }
        if !self.locks.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "SELECT ... FOR UPDATE/SHARE".to_string(),
            ));
        }
        if self.settings.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... SETTINGS".to_string()));
        }
        if self.format_clause.is_some() {
            return Err(CvsSqlError::ToDo("SELECT ... FORMAT".to_string()));
        }

        if self.order_by.is_some() {
            return Err(CvsSqlError::ToDo("SELECT ... ORDER BY".to_string()));
        }
        if self.limit.is_some() {
            return Err(CvsSqlError::ToDo("SELECT ... LIMIT".to_string()));
        }
        if self.offset.is_some() {
            return Err(CvsSqlError::ToDo("SELECT ... OFFSET".to_string()));
        }

        match &*self.body {
            SetExpr::Select(select) => select.extract(engine),
            SetExpr::Query(_) => Err(CvsSqlError::ToDo("SELECT (SELECT ...)".to_string())),
            SetExpr::Values(_) => Err(CvsSqlError::Unsupported("SELECT ... VALUES".to_string())),
            SetExpr::Insert(_) => Err(CvsSqlError::Unsupported("SELECT ... INSERT".to_string())),
            SetExpr::Table(_) => Err(CvsSqlError::Unsupported("SELECT ... TABLE".to_string())),
            SetExpr::Update(_) => Err(CvsSqlError::Unsupported("SELECT ... UPDATE".to_string())),
            SetExpr::SetOperation {
                op: _,
                set_quantifier: _,
                left: _,
                right: _,
            } => Err(CvsSqlError::Unsupported(
                "SELECT ... UNION/EXCEPT/INTERSECT".to_string(),
            )),
        }
    }
}

impl Extractor for Select {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.distinct.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT DISTINCT".to_string()));
        }
        if self.top.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT TOP".to_string()));
        }
        if self.top_before_distinct {
            return Err(CvsSqlError::Unsupported("SELECT ALL".to_string()));
        }
        if self.into.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT INTO".to_string()));
        }
        if !self.lateral_views.is_empty() {
            return Err(CvsSqlError::Unsupported("SELECT LATERAL VIEW".to_string()));
        }
        if self.prewhere.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... PREWHERE".to_string()));
        }
        if !self.cluster_by.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "SELECT ... CLUSTER BY".to_string(),
            ));
        }
        if !self.distribute_by.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "SELECT ... DISTRIBUTE BY".to_string(),
            ));
        }
        if !self.sort_by.is_empty() {
            return Err(CvsSqlError::Unsupported("SELECT ... SORT BY".to_string()));
        }
        if !self.named_window.is_empty() || self.window_before_qualify {
            return Err(CvsSqlError::Unsupported("SELECT ... WINDOW ".to_string()));
        }
        if self.qualify.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... QUALIFY".to_string()));
        }
        if self.value_table_mode.is_some() {
            return Err(CvsSqlError::Unsupported(
                "SELECT AS VALUE/STRUCT".to_string(),
            ));
        }
        if self.connect_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "SELECT ... CONNECT BY".to_string(),
            ));
        }

        if self.having.is_some() {
            return Err(CvsSqlError::ToDo("SELECT ... HAVING".to_string()));
        }
        match &self.group_by {
            GroupByExpr::All(_) => {
                return Err(CvsSqlError::Unsupported(
                    "SELECT ... GROUP BY ALL".to_string(),
                ))
            }
            GroupByExpr::Expressions(exp, mods) => {
                if !exp.is_empty() {
                    return Err(CvsSqlError::ToDo("SELECT ... GROUP BY".to_string()));
                }
                if !mods.is_empty() {
                    return Err(CvsSqlError::ToDo("SELECT ... GROUP BY".to_string()));
                }
            }
        }

        if self.from.is_empty() {
            return Err(CvsSqlError::Unsupported("SELECT without FROM".to_string()));
        }

        let mut product = None;

        for from in &self.from {
            if !from.joins.is_empty() {
                return Err(CvsSqlError::ToDo("SELECT ... JOIN".to_string()));
            }
            let from = from.relation.extract(engine)?;
            product = match product {
                None => Some(from),
                Some(left) => Some(join(left, from)),
            };
        }

        let Some(product) = product else {
            return Err(CvsSqlError::Unsupported("SELECT without FROM".to_string()));
        };

        let filter = make_filter(engine, &self.selection, product)?;

        make_projection(engine, filter, &self.projection)
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

                let results = read_file(&name.0, &engine.home, engine.first_line_as_name)?;
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
