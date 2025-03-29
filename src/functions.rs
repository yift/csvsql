use std::{ops::Deref, str::FromStr};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    group_by::GroupRow,
    projections::{Projection, SingleConvert},
    result_set_metadata::Metadata,
    util::SmartReference,
    value::Value,
};
use bigdecimal::FromPrimitive;
use bigdecimal::ToPrimitive;
use bigdecimal::{BigDecimal, Zero};
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
        build_function_from_name(&name, metadata, engine, &self.args)
    }
}
fn build_function_from_name(
    name: &str,
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    match name {
        "COUNT" => build_aggregator_function(metadata, engine, args, Box::new(Count {})),
        "AVG" => build_aggregator_function(metadata, engine, args, Box::new(Avg {})),
        "SUM" => build_aggregator_function(metadata, engine, args, Box::new(Sum {})),
        "MIN" => build_aggregator_function(metadata, engine, args, Box::new(Min {})),
        "MAX" => build_aggregator_function(metadata, engine, args, Box::new(Max {})),

        "ABS" => build_function(metadata, engine, args, Box::new(Abs {})),
        "ASCII" => build_function(metadata, engine, args, Box::new(Ascii {})),
        "CHR" => build_function(metadata, engine, args, Box::new(Chr {})),
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            build_function(metadata, engine, args, Box::new(Length {}))
        }
        "COALESCE" => build_function(metadata, engine, args, Box::new(Coalece {})),
        "CONCAT" => build_function(metadata, engine, args, Box::new(Concat {})),
        "CONCAT_WS" => build_function(metadata, engine, args, Box::new(ConcatWs {})),
        "CURRENT_DATE" => build_function(metadata, engine, args, Box::new(CurrentDate {})),
        "NOW" | "CURRENT_TIME" | "CURRENT_TIMESTAMP" | "CURTIME" | "LOCALTIME"
        | "LOCALTIMESTAMP" => build_function(metadata, engine, args, Box::new(Now {})),
        "USER" | "CURRENT_USER" => build_function(metadata, engine, args, Box::new(User {})),
        "FORMAT" | "DATE_FORMAT" | "TIME_FORMAT" | "TO_CHAR" => {
            build_function(metadata, engine, args, Box::new(Format {}))
        }
        "TO_TIMESTAMP" | "FROM_UNIXTIME" => {
            build_function(metadata, engine, args, Box::new(ToTimestamp {}))
        }
        "GREATEST" => build_function(metadata, engine, args, Box::new(Greatest {})),
        "IF" => build_function(metadata, engine, args, Box::new(If {})),
        "NULLIF" => build_function(metadata, engine, args, Box::new(NullIf {})),
        "LOWER" | "LCASE" => build_function(metadata, engine, args, Box::new(Lower {})),
        "LEAST" => build_function(metadata, engine, args, Box::new(Least {})),
        "LEFT" => build_function(metadata, engine, args, Box::new(Left {})),
        "LPAD" => build_function(metadata, engine, args, Box::new(Lpad {})),
        "LTRIM" => build_function(metadata, engine, args, Box::new(Ltrim {})),
        "SUBSTRING" | "MID" => build_function(metadata, engine, args, Box::new(SubString {})),
        "PI" => build_function(metadata, engine, args, Box::new(Pi {})),
        _ => Err(CvsSqlError::Unsupported(format!("function {}", name))),
    }
}

fn build_aggregator_function(
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
    operator: Box<dyn AggregateOperator>,
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
    fn name(&self) -> &str;
    fn support_wildcard_argument(&self) -> bool;
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value;
}

trait Casters {
    fn to_number(self) -> Option<BigDecimal>;
}

impl Casters for Value {
    fn to_number(self) -> Option<BigDecimal> {
        match self {
            Value::Number(num) => Some(num),
            _ => None,
        }
    }
}

struct Count {}

impl AggregateOperator for Count {
    fn name(&self) -> &str {
        "COUNT"
    }
    fn support_wildcard_argument(&self) -> bool {
        true
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let count = data.count();
        Value::Number((count as u128).into())
    }
}
struct Avg {}

impl AggregateOperator for Avg {
    fn name(&self) -> &str {
        "AVG"
    }

    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let mut total = BigDecimal::zero();
        let mut count: u128 = 0;
        for num in data.filter_map(|f| f.to_number()) {
            count += 1;
            total += num;
        }
        if count == 0 {
            Value::Empty
        } else {
            Value::Number(total / count)
        }
    }
}

struct Sum {}

impl AggregateOperator for Sum {
    fn name(&self) -> &str {
        "SUM"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let total = data
            .filter_map(|f| f.to_number())
            .fold(BigDecimal::zero(), |a, b| a + b);
        Value::Number(total)
    }
}
struct Min {}

impl AggregateOperator for Min {
    fn name(&self) -> &str {
        "MIN"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let min = data.min();
        min.unwrap_or(Value::Empty)
    }
}

struct Max {}
impl AggregateOperator for Max {
    fn name(&self) -> &str {
        "MAX"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let min = data.max();
        min.unwrap_or(Value::Empty)
    }
}

struct AggregatedFunction {
    distinct: bool,
    argument: Box<dyn Projection>,
    operator: Box<dyn AggregateOperator>,
    name: String,
}

impl Projection for AggregatedFunction {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let mut iter = row
            .group_rows
            .iter()
            .map(|r| self.argument.get(r))
            .map(|v| v.clone());
        let value = if self.distinct {
            let mut unique = iter.unique();
            self.operator.aggreagate(&mut unique)
        } else {
            self.operator.aggreagate(&mut iter)
        };
        value.into()
    }
    fn name(&self) -> &str {
        &self.name
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
impl From<Option<BigDecimal>> for SmartReference<'_, Value> {
    fn from(val: Option<BigDecimal>) -> Self {
        match val {
            None => Value::Empty,
            Some(num) => Value::Number(num),
        }
        .into()
    }
}

impl From<Option<u32>> for SmartReference<'_, Value> {
    fn from(val: Option<u32>) -> Self {
        match val {
            None => Value::Empty,
            Some(num) => Value::Number(num.into()),
        }
        .into()
    }
}

impl From<Option<usize>> for SmartReference<'_, Value> {
    fn from(val: Option<usize>) -> Self {
        match val {
            None => Value::Empty,
            Some(num) => match BigDecimal::from_usize(num) {
                Some(num) => Value::Number(num),
                None => Value::Empty,
            },
        }
        .into()
    }
}

impl From<Option<String>> for SmartReference<'_, Value> {
    fn from(val: Option<String>) -> Self {
        match val {
            None => Value::Empty,
            Some(str) => Value::Str(str),
        }
        .into()
    }
}
impl From<Option<&str>> for SmartReference<'_, Value> {
    fn from(val: Option<&str>) -> Self {
        match val {
            None => Value::Empty,
            Some(str) => Value::Str(str.to_string()),
        }
        .into()
    }
}

trait Extractor {
    fn as_num(&self) -> Option<&BigDecimal>;
    fn as_string(&self) -> Option<&str>;
    fn as_bool(&self) -> Option<&bool>;
    fn as_u32(&self) -> Option<u32> {
        self.as_num().and_then(|s| s.to_u32())
    }
    fn as_i64(&self) -> Option<i64> {
        self.as_num().and_then(|s| s.to_i64())
    }
    fn as_usize(&self) -> Option<usize> {
        self.as_num().and_then(|s| s.to_usize())
    }
}
impl Extractor for Value {
    fn as_num(&self) -> Option<&BigDecimal> {
        match self {
            Value::Number(num) => Some(num),
            _ => None,
        }
    }
    fn as_string(&self) -> Option<&str> {
        match self {
            Value::Str(str) => Some(str),
            _ => None,
        }
    }
    fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }
}
impl<T: Extractor> Extractor for SmartReference<'_, T> {
    fn as_num(&self) -> Option<&BigDecimal> {
        self.deref().as_num()
    }
    fn as_string(&self) -> Option<&str> {
        self.deref().as_string()
    }
    fn as_bool(&self) -> Option<&bool> {
        self.deref().as_bool()
    }
}
impl<T: Extractor> Extractor for Option<&T> {
    fn as_num(&self) -> Option<&BigDecimal> {
        self.and_then(|t| t.as_num())
    }
    fn as_string(&self) -> Option<&str> {
        self.and_then(|s| s.as_string())
    }
    fn as_bool(&self) -> Option<&bool> {
        self.and_then(|s| s.as_bool())
    }
}

struct Abs {}
impl Operator for Abs {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first().as_num().map(|t| t.abs()).into()
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
        args.first()
            .as_string()
            .and_then(|s| s.chars().next())
            .map(|i| i as u32)
            .into()
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
        args.first()
            .as_u32()
            .and_then(char::from_u32)
            .map(|c| c.to_string())
            .into()
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
        args.first().as_string().map(|s| s.len()).into()
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
        let format = args.get(1);
        let Some(format) = format.as_string() else {
            return Value::Empty.into();
        };
        let formatted = match value.deref() {
            Value::Date(date) => date.format(format),
            Value::Timestamp(ts) => ts.format(format),
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
        let Some(time) = args.first().as_i64() else {
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
        let first = args.first();
        let Some(condition) = first.as_bool() else {
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
        args.first()
            .and_then(|f| f.as_string())
            .map(|f| f.to_lowercase())
            .into()
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

struct Left {}
impl Operator for Left {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };
        let length = args.get(1);
        let Some(length) = length.as_usize() else {
            return Value::Empty.into();
        };
        if text.len() < length {
            Value::Str(text.to_string()).into()
        } else {
            Value::Str(text[0..length].to_string()).into()
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "LEFT"
    }
}

struct Lpad {}
impl Operator for Lpad {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };
        let length = args.get(1);
        let Some(length) = length.as_usize() else {
            return Value::Empty.into();
        };
        let pad = args.get(2);

        let Some(pad) = pad.as_string() else {
            return Value::Empty.into();
        };

        if text.len() > length {
            Value::Str(text[0..length].to_string()).into()
        } else if pad.is_empty() {
            Value::Str(text.to_string()).into()
        } else {
            let mut str = String::new();
            let mut chars = pad.chars().cycle();
            for _ in 0..length - text.len() {
                let chr = chars.next().unwrap();
                str.push(chr);
            }
            str.push_str(text);
            Value::Str(str).into()
        }
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "LPAD"
    }
}

struct Ltrim {}
impl Operator for Ltrim {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .and_then(|f| f.as_string())
            .map(|f| f.trim_start())
            .into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LTRIM"
    }
}

struct SubString {}
impl Operator for SubString {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };

        let start = args.get(1);
        let Some(start) = start.as_usize() else {
            return Value::Empty.into();
        };

        if start > text.len() {
            return Value::Str(String::new()).into();
        }
        let text = &text[start - 1..];
        let length = match args.get(2) {
            None => text.len(),
            Some(length) => {
                let Some(length) = length.as_usize() else {
                    return Value::Empty.into();
                };
                length
            }
        };
        if length > text.len() {
            Value::Str(text.into())
        } else {
            Value::Str(text[..length].to_string())
        }
        .into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "SUBSTRING"
    }
}

struct Pi {}
impl Operator for Pi {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let pi = BigDecimal::from_str("3.1415926535897932384626433832795").unwrap();
        Value::Number(pi).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "PI"
    }
}
