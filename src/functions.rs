use std::{collections::HashSet, ops::Deref};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    group_by::GroupRow,
    projections::{Projection, SingleConvert},
    result_set_metadata::Metadata,
    util::SmartReference,
    value::Value,
};
use bigdecimal::BigDecimal;
use bigdecimal::FromPrimitive;
use bigdecimal::ToPrimitive;
use chrono::{TimeZone, Utc, offset::LocalResult};
use itertools::Itertools;
use sqlparser::ast::{
    DuplicateTreatment, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
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

            "ABS" => build_function(metadata, engine, &self.args, Box::new(Abs {})),
            "ASCII" => build_function(metadata, engine, &self.args, Box::new(Ascii {})),
            "CHR" => build_function(metadata, engine, &self.args, Box::new(Chr {})),
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                build_function(metadata, engine, &self.args, Box::new(Length {}))
            }
            "COALESCE" => build_function(metadata, engine, &self.args, Box::new(Coalece {})),
            "CONCAT" => build_function(metadata, engine, &self.args, Box::new(Concat {})),
            "CONCAT_WS" => build_function(metadata, engine, &self.args, Box::new(ConcatWs {})),
            "CURRENT_DATE" => {
                build_function(metadata, engine, &self.args, Box::new(CurrentDate {}))
            }
            "NOW" | "CURRENT_TIME" | "CURRENT_TIMESTAMP" | "CURTIME" => {
                build_function(metadata, engine, &self.args, Box::new(Now {}))
            }
            "USER" | "CURRENT_USER" => {
                build_function(metadata, engine, &self.args, Box::new(User {}))
            }
            "FORMAT" | "DATE_FORMAT" | "TIME_FORMAT" | "TO_CHAR" => {
                build_function(metadata, engine, &self.args, Box::new(Format {}))
            }
            "TO_TIMESTAMP" | "FROM_UNIXTIME" => {
                build_function(metadata, engine, &self.args, Box::new(ToTimestamp {}))
            }
            "GREATEST" => build_function(metadata, engine, &self.args, Box::new(Greatest {})),
            "IF" => build_function(metadata, engine, &self.args, Box::new(If {})),
            "IFNULL" | "NULLIF" => {
                build_function(metadata, engine, &self.args, Box::new(NullIf {}))
            }
            "LOWER" | "LCASE" => build_function(metadata, engine, &self.args, Box::new(Lower {})),
            "LEAST" => build_function(metadata, engine, &self.args, Box::new(Least {})),
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
            if !value.is_empty() {
                let to_add = if self.distinct {
                    found_items.insert(value.clone())
                } else {
                    true
                };
                if to_add {
                    self.operator.aggregate(&mut agg, value);
                }
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
    type Data = Option<Value>;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>) {
        match so_far {
            None => *so_far = Some(value.clone()),
            Some(max_so_far) => {
                if value.deref() > max_so_far {
                    *so_far = Some(value.clone())
                }
            }
        }
    }
    fn to_value(&self, data: Self::Data) -> Value {
        match data {
            None => Value::Empty,
            Some(data) => data,
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
    type Data = Option<Value>;
    fn aggregate(&self, so_far: &mut Self::Data, value: SmartReference<'_, Value>) {
        match so_far {
            None => *so_far = Some(value.clone()),
            Some(max_so_far) => {
                if value.deref() < max_so_far {
                    *so_far = Some(value.clone())
                }
            }
        }
    }
    fn to_value(&self, data: Self::Data) -> Value {
        match data {
            None => Value::Empty,
            Some(data) => data,
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
        Value::Bool(true).into()
    }
    fn name(&self) -> &str {
        "*"
    }
}
fn wildcard_operator() -> Box<dyn Projection> {
    Box::new(Wildcard {})
}

trait Operator {
    fn name(&self) -> &str;
    fn min_args(&self) -> usize;
    fn max_args(&self) -> Option<usize>;
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value>;
}
struct SimpleFunction {
    arguments: Vec<Box<dyn Projection>>,
    operator: Box<dyn Operator>,
    name: String,
}
impl Projection for SimpleFunction {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let mut args = vec![];
        for a in &self.arguments {
            args.push(a.get(row));
        }
        self.operator.get(&args)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

fn build_function(
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
    operator: Box<dyn Operator>,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    let arguments = match &args {
        FunctionArguments::List(lst) => {
            if matches!(lst.duplicate_treatment, Some(DuplicateTreatment::Distinct)) {
                return Err(CvsSqlError::Unsupported(format!(
                    "Function {} with distinct argument",
                    operator.name()
                )));
            }
            if let Some(c) = lst.clauses.first() {
                return Err(CvsSqlError::Unsupported(format!("{}", c)));
            }
            let mut args = vec![];
            for a in &lst.args {
                let a = match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        e.convert_single(metadata, engine)?
                    }
                    _ => {
                        return Err(CvsSqlError::Unsupported(format!(
                            "{} as argment in function {}",
                            a,
                            operator.name()
                        )));
                    }
                };
                args.push(a);
            }
            args
        }
        FunctionArguments::Subquery(_) => {
            return Err(CvsSqlError::Unsupported(
                "function subquery arguments".into(),
            ));
        }
        FunctionArguments::None => vec![],
    };
    if arguments.len() < operator.min_args() {
        return Err(CvsSqlError::Unsupported(format!(
            "Function {} with {} argumnets or less",
            operator.name(),
            arguments.len()
        )));
    }
    if let Some(max) = operator.max_args() {
        if arguments.len() > max {
            return Err(CvsSqlError::Unsupported(format!(
                "Function {} with {} argumnets or more",
                operator.name(),
                arguments.len()
            )));
        }
    }
    let name = format!(
        "{}({})",
        operator.name(),
        arguments.iter().map(|f| f.name()).join(", ")
    );

    Ok(Box::new(SimpleFunction {
        arguments,
        operator,
        name,
    }))
}

struct Abs {}
impl Operator for Abs {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        match args.first() {
            Some(val) => match val.deref() {
                Value::Number(num) => Value::Number(num.abs()).into(),
                _ => Value::Empty.into(),
            },
            _ => Value::Empty.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "ABS"
    }
}

struct Ascii {}
impl Operator for Ascii {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let val = match args.first() {
            Some(val) => match val.deref() {
                Value::Str(str) => match str.chars().next() {
                    None => Value::Empty,
                    Some(l) => Value::Number((l as u32).into()),
                },
                _ => Value::Empty,
            },
            _ => Value::Empty,
        };
        val.into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "ASCII"
    }
}

struct Chr {}
impl Operator for Chr {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let val = match args.first() {
            Some(val) => match val.deref() {
                Value::Number(str) => match str.to_u32() {
                    None => Value::Empty,
                    Some(l) => match char::from_u32(l) {
                        None => Value::Empty,
                        Some(c) => Value::Str(c.to_string()),
                    },
                },
                _ => Value::Empty,
            },
            _ => Value::Empty,
        };
        val.into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "CHR"
    }
}

struct Length {}
impl Operator for Length {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let val = match args.first() {
            Some(val) => match val.deref() {
                Value::Str(str) => match BigDecimal::from_usize(str.len()) {
                    None => Value::Empty,
                    Some(num) => Value::Number(num),
                },
                _ => Value::Empty,
            },
            _ => Value::Empty,
        };
        val.into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LENGTH"
    }
}

struct Coalece {}
impl Operator for Coalece {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        for a in args.iter() {
            if !a.is_empty() {
                return a.deref().clone().into();
            }
        }
        Value::Empty.into()
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "COALESCE"
    }
}

struct Concat {}
impl Operator for Concat {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let str = args.iter().map(|f| f.to_string()).join("");
        Value::Str(str).into()
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "CONCAT"
    }
}

struct ConcatWs {}
impl Operator for ConcatWs {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(sep) = args.first() else {
            return Value::Empty.into();
        };
        let sep = sep.to_string();
        let str = args
            .iter()
            .skip(1)
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .join(sep.as_str());
        Value::Str(str).into()
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "CONCAT_WS"
    }
}

struct CurrentDate {}
impl Operator for CurrentDate {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        Value::Date(Utc::now().naive_utc().date()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "CURRENT_DATE"
    }
}
struct Now {}
impl Operator for Now {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        Value::Timestamp(Utc::now().naive_utc()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "NOW"
    }
}
struct User {}
impl Operator for User {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        Value::Str(whoami::username()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "CURRENT_USER"
    }
}

struct Format {}
impl Operator for Format {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(value) = args.first() else {
            return Value::Empty.into();
        };
        let Some(format) = args.get(1) else {
            return Value::Empty.into();
        };
        let formatted = match (value.deref(), format.deref()) {
            (Value::Date(date), Value::Str(format)) => date.format(format),
            (Value::Timestamp(ts), Value::Str(format)) => ts.format(format),
            _ => {
                return Value::Empty.into();
            }
        };
        let mut text = String::new();
        if formatted.write_to(&mut text).is_err() {
            return Value::Empty.into();
        }

        Value::Str(text).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "FORMAT"
    }
}

struct ToTimestamp {}
impl Operator for ToTimestamp {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(time) = args.first() else {
            return Value::Empty.into();
        };
        let Value::Number(time) = time.deref() else {
            return Value::Empty.into();
        };
        let Some(time) = time.to_i64() else {
            return Value::Empty.into();
        };

        let LocalResult::Single(time) = Utc.timestamp_opt(time, 0) else {
            return Value::Empty.into();
        };

        Value::Timestamp(time.naive_utc()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "TO_TIMESTAMP"
    }
}

struct Greatest {}
impl Operator for Greatest {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let mut greatest = None;
        for a in args.iter() {
            match &greatest {
                None => greatest = Some(a.deref().clone()),
                Some(val) => {
                    if a.deref() > val {
                        greatest = Some(a.deref().clone());
                    }
                }
            }
        }
        match greatest {
            None => Value::Empty.into(),
            Some(greatest) => greatest.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "GREATEST"
    }
}
struct If {}
impl Operator for If {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(condition) = args.first() else {
            return Value::Empty.into();
        };
        let Value::Bool(condition) = condition.deref() else {
            return Value::Empty.into();
        };
        let value = if *condition { args.get(1) } else { args.get(2) };
        match value {
            Some(v) => v.deref().clone().into(),
            None => Value::Empty.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "IF"
    }
}
struct NullIf {}
impl Operator for NullIf {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(value_one) = args.first() else {
            return Value::Empty.into();
        };
        let Some(value_two) = args.get(1) else {
            return Value::Empty.into();
        };
        if *value_one != *value_two {
            value_one.deref().clone().into()
        } else {
            Value::Empty.into()
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "NULLIF"
    }
}

struct Lower {}
impl Operator for Lower {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(text) = args.first() else {
            return Value::Empty.into();
        };
        let Value::Str(text) = text.deref() else {
            return Value::Empty.into();
        };
        Value::Str(text.to_lowercase()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LOWER"
    }
}
struct Least {}
impl Operator for Least {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let mut least = None;
        for a in args.iter() {
            if !a.is_empty() {
                match &least {
                    None => least = Some(a.deref().clone()),
                    Some(val) => {
                        if a.deref() < val {
                            least = Some(a.deref().clone());
                        }
                    }
                }
            }
        }
        match least {
            None => Value::Empty.into(),
            Some(least) => least.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "LEAST"
    }
}
