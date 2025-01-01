use std::{
    fmt::Display,
    hash::Hash,
    ops::{Add, Deref, Mul},
    str::FromStr,
};

use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use chrono::{NaiveDate, NaiveDateTime};
use thiserror::Error;

use crate::util::SmartReference;

#[derive(Debug)]
pub enum Value {
    Str(String),
    Float(f64),
    Int(i64),
    BigDecimal(BigDecimal),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Bool(bool),
    Empty,
}

impl Display for Value {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => i.fmt(formatter),
            Value::Float(f) => f.fmt(formatter),
            Value::BigDecimal(b) => b.fmt(formatter),
            Value::Date(d) => d.format("%Y-%m-%d").fmt(formatter),
            Value::Timestamp(d) => d.format("%Y-%m-%d %H:%M:%S%.f").fmt(formatter),
            Value::Str(str) => str.fmt(formatter),
            Value::Bool(b) => {
                if *b {
                    write!(formatter, "TRUE")
                } else {
                    write!(formatter, "FALSE")
                }
            }
            Value::Empty => write!(formatter, ""),
        }
    }
}
impl From<BigDecimal> for Value {
    fn from(decimal: BigDecimal) -> Self {
        let str = decimal.to_string();
        if let Ok(i) = i64::from_str(&str) {
            if let Some(other_decimal) = BigDecimal::from_i64(i) {
                if decimal == other_decimal {
                    return Value::Int(i);
                }
            }
        }
        if let Ok(f) = f64::from_str(&str) {
            if let Some(other_decimal) = BigDecimal::from_f64(f) {
                if decimal == other_decimal {
                    return Value::Float(f);
                }
            }
        }
        Value::BigDecimal(decimal)
    }
}

impl Value {
    fn as_number(&self) -> Option<SmartReference<'_, BigDecimal>> {
        match self {
            Value::BigDecimal(bc) => Some(bc.into()),
            Value::Float(f) => BigDecimal::from_f64(*f).map(|f| f.into()),
            Value::Int(i) => BigDecimal::from_i64(*i).map(|f| f.into()),
            _ => None,
        }
    }
}
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            return Value::Empty;
        }
        if value == "TRUE" {
            return Value::Bool(true);
        }
        if value == "FALSE" {
            return Value::Bool(false);
        }
        if let Ok(date) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
            return Value::Timestamp(date);
        }
        if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            return Value::Date(date);
        }
        if let Ok(decimal) = BigDecimal::from_str(value) {
            return decimal.into();
        }
        Value::Str(value.to_string())
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::Str(me) => match other {
                Value::Str(other) => me == other,
                _ => false,
            },
            Value::Int(me) => match other {
                Value::Int(other) => me == other,
                Value::BigDecimal(bc) => {
                    BigDecimal::from_i64(*me).map(|b| b == *bc).unwrap_or(false)
                }
                Value::Float(f) => me.to_f64().map(|b| b == *f).unwrap_or(false),
                _ => false,
            },
            Value::BigDecimal(me) => match other {
                Value::BigDecimal(other) => me == other,
                Value::Int(i) => BigDecimal::from_i64(*i).map(|i| i == *me).unwrap_or(false),
                Value::Float(f) => BigDecimal::from_f64(*f).map(|f| f == *me).unwrap_or(false),
                _ => false,
            },
            Value::Float(me) => match other {
                Value::Float(other) => me == other,
                Value::BigDecimal(bc) => {
                    BigDecimal::from_f64(*me).map(|b| b == *bc).unwrap_or(false)
                }
                Value::Int(i) => i.to_f64().map(|i| i == *me).unwrap_or(false),
                _ => false,
            },
            Value::Bool(me) => match other {
                Value::Bool(other) => me == other,
                _ => false,
            },
            Value::Timestamp(me) => match other {
                Value::Timestamp(other) => me == other,
                _ => false,
            },
            Value::Date(me) => match other {
                Value::Date(other) => me == other,
                _ => false,
            },
            Value::Empty => matches!(other, Value::Empty),
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Empty => state.write_i8(1),
            Value::Bool(true) => state.write_i8(2),
            Value::Bool(false) => state.write_i8(3),
            Value::Int(i) => {
                state.write_i8(4);
                i.hash(state);
            }
            Value::Str(str) => {
                state.write_i8(5);
                str.hash(state);
            }
            Value::BigDecimal(b) => {
                state.write_i8(6);
                b.hash(state);
            }
            Value::Date(b) => {
                state.write_i8(7);
                b.hash(state);
            }
            Value::Timestamp(b) => {
                state.write_i8(8);
                b.hash(state);
            }
            Value::Float(f) => {
                state.write_i8(8);
                state.write_u64(f.to_bits())
            }
        }
    }
}

impl Add for &Value {
    type Output = Value;
    fn add(self, rhs: Self) -> Self::Output {
        let Some(me) = self.as_number() else {
            return Value::Empty;
        };
        let Some(other) = rhs.as_number() else {
            return Value::Empty;
        };
        (me.deref() + other.deref()).into()
    }
}
impl Mul for &Value {
    type Output = Value;
    fn mul(self, rhs: Self) -> Self::Output {
        let Some(me) = self.as_number() else {
            return Value::Empty;
        };
        let Some(other) = rhs.as_number() else {
            return Value::Empty;
        };
        (me.deref() * other.deref()).into()
    }
}

#[derive(Error, Debug)]
pub enum ValueError {
    #[error("Not a number")]
    NotANumber,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_int_value() {
        let value = Value::Int(101);

        let str = format!("{}", value);

        assert_eq!(str, "101");
    }

    #[test]
    fn display_float_value() {
        let value = Value::Float(10.1);

        let str = format!("{}", value);

        assert_eq!(str, "10.1");
    }

    #[test]
    fn display_true_value() {
        let value = Value::Bool(true);

        let str = format!("{}", value);

        assert_eq!(str, "TRUE");
    }

    #[test]
    fn display_false_value() {
        let value = Value::Bool(false);

        let str = format!("{}", value);

        assert_eq!(str, "FALSE");
    }

    #[test]
    fn display_big_decimal_value() {
        let value =
            Value::BigDecimal(BigDecimal::from_str("12312312312312312312312312313123").unwrap());

        let str = format!("{}", value);

        assert_eq!(str, "12312312312312312312312312313123");
    }

    #[test]
    fn display_date_value() {
        let value = Value::Date(NaiveDate::parse_from_str("2018-04-21", "%Y-%m-%d").unwrap());

        let str = format!("{}", value);

        assert_eq!(str, "2018-04-21");
    }

    #[test]
    fn display_timestamp_value() {
        let value = Value::Timestamp(
            NaiveDateTime::parse_from_str("2018-04-21 10:12:40.011", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
        );

        let str = format!("{}", value);

        assert_eq!(str, "2018-04-21 10:12:40.011");
    }

    #[test]
    fn display_string_value() {
        let value = Value::Str("test".into());

        let str = format!("{}", value);

        assert_eq!(str, "test");
    }

    #[test]
    fn display_empty_value() {
        let value = Value::Empty;

        let str = format!("{}", value);

        assert_eq!(str, "");
    }

    #[test]
    fn from_str_empty() {
        let str = "";
        let value: Value = str.into();

        let is_empty = matches!(value, Value::Empty);
        assert!(is_empty);
    }

    #[test]
    fn from_str_string() {
        let str = "test";
        let value: Value = str.into();

        let str = match value {
            Value::Str(str) => Some(str),
            _ => None,
        };
        assert_eq!(str, Some("test".into()));
    }

    #[test]
    fn from_timestamp() {
        let str = "2018-04-21 10:12:40";
        let value: Value = str.into();

        let str = match value {
            Value::Timestamp(str) => Some(str),
            _ => None,
        };
        assert_eq!(
            str,
            Some(
                NaiveDateTime::parse_from_str("2018-04-21 10:12:40", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap()
            )
        );
    }

    #[test]
    fn from_date() {
        let str = "2018-04-21";
        let value: Value = str.into();

        let str = match value {
            Value::Date(str) => Some(str),
            _ => None,
        };
        assert_eq!(
            str,
            Some(NaiveDate::parse_from_str("2018-04-21", "%Y-%m-%d").unwrap())
        );
    }

    #[test]
    fn from_int() {
        let str = "-2001";
        let value: Value = str.into();

        let str = match value {
            Value::Int(str) => Some(str),
            _ => None,
        };
        assert_eq!(str, Some(-2001));
    }

    #[test]
    fn from_true() {
        let str = "TRUE";
        let value: Value = str.into();

        let str = match value {
            Value::Bool(str) => Some(str),
            _ => None,
        };
        assert_eq!(str, Some(true));
    }

    #[test]
    fn from_false() {
        let str = "FALSE";
        let value: Value = str.into();

        let str = match value {
            Value::Bool(str) => Some(str),
            _ => None,
        };
        assert_eq!(str, Some(false));
    }

    #[test]
    fn from_float() {
        let str = "3.25";
        let value: Value = str.into();

        let str = match value {
            Value::Float(str) => Some(str),
            _ => None,
        };
        assert_eq!(str, Some(3.25));
    }

    #[test]
    fn from_big_decimal() {
        let str = "325123142355765678123412453653.123412453456256456";
        let value: Value = str.into();

        let str = match value {
            Value::BigDecimal(str) => Some(str),
            _ => None,
        };
        assert_eq!(
            str,
            Some(
                BigDecimal::from_str("325123142355765678123412453653.123412453456256456").unwrap()
            )
        );
    }
}
