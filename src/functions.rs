use std::{collections::HashSet, ops::Deref};

use bigdecimal::BigDecimal;
use sqlparser::ast::{
    DuplicateTreatment, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    group_by::GroupRow,
    projections::{Projection, SingleConvert},
    result_set_metadata::Metadata,
    util::SmartReference,
    value::Value,
};

impl SingleConvert for Function {
    fn convert_single(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Box<dyn Projection>, CvsSqlError> {
        if !self.within_group.is_empty() {
            return Err(CvsSqlError::Unsupported("WITHIN GROUP".into()));
        }

        if self.over.is_some() {
            return Err(CvsSqlError::Unsupported("OVER".into()));
        }

        if self.null_treatment.is_some() {
            return Err(CvsSqlError::Unsupported("IGNORE/RESPECT NULLS".into()));
        }

        if self.filter.is_some() {
            return Err(CvsSqlError::Unsupported("FILTER".into()));
        }
        if self.parameters != FunctionArguments::None {
            return Err(CvsSqlError::Unsupported("function parameters".into()));
        }

        let name = self.name.to_string().to_uppercase();
        match name.as_str() {
            "COUNT" => build_aggregator_function(metadata, engine, &self.args, Box::new(Count {})),
            "AVG" => build_aggregator_function(metadata, engine, &self.args, Box::new(Avg {})),
            "SUM" => build_aggregator_function(metadata, engine, &self.args, Box::new(Sum {})),
            "MIN" => build_aggregator_function(metadata, engine, &self.args, Box::new(Min {})),
            "MAX" => build_aggregator_function(metadata, engine, &self.args, Box::new(Max {})),
            _ => Err(CvsSqlError::Unsupported(format!("function {}", name))),
        }
    }
}

fn build_aggregator_function<D: Default + 'static>(
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
    operator: Box<dyn AggregateOperator<Data = D>>,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    let parent_metadata = match metadata {
        Metadata::Grouped { parent, this: _ } => parent,
        _ => return Err(CvsSqlError::ToDo("force aggreation".into())),
    };
    let lst = match &args {
        FunctionArguments::List(lst) => lst,
        FunctionArguments::Subquery(_) => {
            return Err(CvsSqlError::Unsupported(
                "function subquery arguments".into(),
            ));
        }
        FunctionArguments::None => {
            return Err(CvsSqlError::Unsupported(format!(
                "Function {} must have an argmeunt",
                operator.name()
            )));
        }
    };
    let distinct = matches!(lst.duplicate_treatment, Some(DuplicateTreatment::Distinct));

    if let Some(c) = lst.clauses.first() {
        return Err(CvsSqlError::Unsupported(format!("{}", c)));
    }
    let first = match lst.args.first() {
        Some(arg) => arg,
        None => {
            return Err(CvsSqlError::Unsupported(format!(
                "Function {} must have an argmeunt",
                operator.name()
            )));
        }
    };
    if lst.args.len() > 1 {
        return Err(CvsSqlError::Unsupported(format!(
            "Function {} must have a single argmeunt",
            operator.name()
        )));
    }
    let argument = match first {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
            e.convert_single(parent_metadata, engine)?
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            if operator.support_wildcard_argument() {
                if distinct {
                    return Err(CvsSqlError::Unsupported("DISTINCT with * argument".into()));
                }
                wildcard_operator()
            } else {
                return Err(CvsSqlError::Unsupported(format!(
                    "Function {} with * argument",
                    operator.name()
                )));
            }
        }
        _ => return Err(CvsSqlError::Unsupported(format!("{}", first))),
    };
    let name = format!("{}({})", operator.name(), argument.name());

    Ok(Box::new(AggregatedFunction {
        distinct,
        argument,
        operator,
        name,
    }))
}
trait AggregateOperator {
    type Data: Default;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>);
    fn to_value(&self, data: Self::Data) -> Value;
    fn name(&self) -> &str;
    fn support_wildcard_argument(&self) -> bool;
    fn init(&self) -> Self::Data {
        Self::Data::default()
    }
}

struct AggregatedFunction<D> {
    distinct: bool,
    argument: Box<dyn Projection>,
    operator: Box<dyn AggregateOperator<Data = D>>,
    name: String,
}

impl<D: Default> Projection for AggregatedFunction<D> {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let mut agg = self.operator.init();
        let mut found_items = HashSet::new();
        for row in row.group_rows.iter() {
            let value = self.argument.get(row);
            let to_add = if self.distinct {
                found_items.insert(value.clone())
            } else {
                true
            };
            if to_add {
                self.operator.aggregate(&mut agg, value);
            }
        }

        self.operator.to_value(agg).into()
    }
    fn name(&self) -> &str {
        &self.name
    }
}

struct Count {}

impl AggregateOperator for Count {
    type Data = u128;
    fn aggregate(&self, so_far: &mut Self::Data, _: SmartReference<'_, Value>) {
        *so_far += 1;
    }
    fn to_value(&self, data: Self::Data) -> Value {
        Value::Number(data.into())
    }
    fn name(&self) -> &str {
        "COUNT"
    }
    fn support_wildcard_argument(&self) -> bool {
        true
    }
}

struct Avg {}

#[derive(Default)]
struct AvgCalc {
    count: u128,
    total: BigDecimal,
}
impl AggregateOperator for Avg {
    type Data = AvgCalc;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>) {
        if let Value::Number(num) = value.deref() {
            so_far.count += 1;
            so_far.total += num;
        }
    }
    fn to_value(&self, data: Self::Data) -> Value {
        if data.count == 0 {
            Value::Empty
        } else {
            Value::Number(data.total / data.count)
        }
    }
    fn name(&self) -> &str {
        "AVG"
    }

    fn support_wildcard_argument(&self) -> bool {
        false
    }
}

struct Sum {}
impl AggregateOperator for Sum {
    type Data = BigDecimal;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>) {
        if let Value::Number(num) = value.deref() {
            *so_far += num
        }
    }
    fn to_value(&self, data: Self::Data) -> Value {
        Value::Number(data)
    }
    fn name(&self) -> &str {
        "SUM"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
}

struct Max {}
impl AggregateOperator for Max {
    type Data = Option<BigDecimal>;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>) {
        if let Value::Number(num) = value.deref() {
            match so_far {
                None => *so_far = Some(num.clone()),
                Some(max_so_far) => {
                    if num > max_so_far {
                        *so_far = Some(num.clone())
                    }
                }
            }
        }
    }
    fn to_value(&self, data: Self::Data) -> Value {
        match data {
            None => Value::Empty,
            Some(num) => Value::Number(num),
        }
    }
    fn name(&self) -> &str {
        "MAX"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
}
struct Min {}
impl AggregateOperator for Min {
    type Data = Option<BigDecimal>;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>) {
        if let Value::Number(num) = value.deref() {
            match so_far {
                None => *so_far = Some(num.clone()),
                Some(max_so_far) => {
                    if num < max_so_far {
                        *so_far = Some(num.clone())
                    }
                }
            }
        }
    }
    fn to_value(&self, data: Self::Data) -> Value {
        match data {
            None => Value::Empty,
            Some(num) => Value::Number(num),
        }
    }
    fn name(&self) -> &str {
        "MIN"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
}

struct Wildcard {}
impl Projection for Wildcard {
    fn get<'a>(&'a self, _: &'a GroupRow) -> SmartReference<'a, Value> {
        Value::Empty.into()
    }
    fn name(&self) -> &str {
        "*"
    }
}
fn wildcard_operator() -> Box<dyn Projection> {
    Box::new(Wildcard {})
}
