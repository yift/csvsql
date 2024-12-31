use std::{fmt::Display, str::FromStr};

use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::NaiveDateTime;

pub enum Value {
    Str(String),
    Float(f64),
    Int(i64),
    BigDecimal(BigDecimal),
    Date(NaiveDateTime),
    Empty,
}

impl Display for Value {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => i.fmt(formatter),
            Value::Float(f) => f.fmt(formatter),
            Value::BigDecimal(b) => b.fmt(formatter),
            Value::Date(d) => d.format("%Y-%m-%d").fmt(formatter),
            Value::Str(str) => str.fmt(formatter),
            Value::Empty => write!(formatter, ""),
        }
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            return Value::Empty;
        }
        if let Ok(date) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d") {
            return Value::Date(date);
        }
        if let Ok(decimal) = BigDecimal::from_str(value) {
            if let Ok(i) = i64::from_str(value) {
                if let Some(other_decimal) = BigDecimal::from_i64(i) {
                    if decimal == other_decimal {
                        return Value::Int(i);
                    }
                }
            }
            if let Ok(f) = f64::from_str(value) {
                if let Some(other_decimal) = BigDecimal::from_f64(f) {
                    if decimal == other_decimal {
                        return Value::Float(f);
                    }
                }
            }
            return Value::BigDecimal(decimal);
        }
        Value::Str(value.to_string())
    }
}
