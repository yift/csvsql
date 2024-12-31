use std::collections::HashSet;

use chrono::NaiveDateTime;
use csvsql::{
    args::Args,
    engine::Engine,
    error::CdvSqlError,
    results::{Column, ColumnName, Row},
    value::Value,
};

fn get_expecte_data() -> Vec<Vec<(&'static str, Value)>> {
    vec![
        vec![
            ("id", Value::Int(562309979589523718)),
            ("company", Value::Str("Considine and Greenholt Inc".into())),
            ("name", Value::Str("Jayson Cummerata".into())),
            ("country", Value::Str("Kazakhstan".into())),
            ("email", Value::Str("laurianne@example.net".into())),
            ("active", Value::Bool(false)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-02-19 00:01:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-3272122301644320658)),
            ("company", Value::Str("Friesen LLC".into())),
            ("name", Value::Str("Ada McCullough".into())),
            ("country", Value::Str("Niue".into())),
            ("email", Value::Str("angus@example.com".into())),
            ("active", Value::Bool(true)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-07-11 11:33:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-4534989981740135395)),
            ("company", Value::Str("Powlowski and Witting LLC".into())),
            ("name", Value::Str("Taya Kessler".into())),
            ("country", Value::Str("Djibouti".into())),
            ("email", Value::Str("stone@example.net".into())),
            ("active", Value::Bool(true)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-04-05 13:54:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-6710777381220662304)),
            ("company", Value::Str("Tillman Inc".into())),
            ("name", Value::Str("Julie Nicolas".into())),
            ("country", Value::Str("Indonesia".into())),
            ("email", Value::Str("king@example.net".into())),
            ("active", Value::Bool(false)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-06-06 07:02:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-52047330704859071)),
            (
                "company",
                Value::Str("Runolfsdottir and Macejkovic Inc".into()),
            ),
            ("name", Value::Str("Roberta Effertz".into())),
            ("country", Value::Str("Cape Verde".into())),
            ("email", Value::Str("judge@example.com".into())),
            ("active", Value::Bool(true)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-07-02 08:37:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(8262312349476125604)),
            ("company", Value::Str("Gleason and Tromp and Sons".into())),
            ("name", Value::Str("Jamal Mills".into())),
            ("country", Value::Str("United Kingdom".into())),
            ("email", Value::Str("karli@example.org".into())),
            ("active", Value::Bool(true)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-01-22 03:25:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-6456289044355981069)),
            ("company", Value::Str("Bernier and Watsica Inc".into())),
            ("name", Value::Str("Mack Stark".into())),
            (
                "country",
                Value::Str("British Indian Ocean Territory (Chagos Archipelago)".into()),
            ),
            ("email", Value::Str("herminio@example.net".into())),
            ("active", Value::Bool(false)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-06-25 19:21:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-240257225591817487)),
            ("company", Value::Str("Herman Group".into())),
            ("name", Value::Str("Brandon Reinger".into())),
            ("country", Value::Str("South Africa".into())),
            ("email", Value::Str("norwood@example.net".into())),
            ("active", Value::Bool(false)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-02-10 16:16:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(-2619177609126860619)),
            ("company", Value::Str("Lehner and Sons".into())),
            ("name", Value::Str("Sasha Renner".into())),
            ("country", Value::Str("Australia".into())),
            ("email", Value::Str("kareem@example.org".into())),
            ("active", Value::Bool(true)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-08-10 10:42:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
        vec![
            ("id", Value::Int(6658516301522050359)),
            ("company", Value::Str("Moen LLC".into())),
            ("name", Value::Str("Troy Lynch".into())),
            ("country", Value::Str("Bermuda".into())),
            ("email", Value::Str("nichole@example.org".into())),
            ("active", Value::Bool(true)),
            (
                "last modified",
                Value::Timestamp(
                    NaiveDateTime::parse_from_str("2024-03-03 14:04:32", "%Y-%m-%d %H:%M:%S%.f")
                        .unwrap(),
                ),
            ),
        ],
    ]
}

#[test]
fn test_select_all() -> Result<(), CdvSqlError> {
    let args = Args {
        command: None,
        home: None,
        first_line_as_name: true,
    };
    let engine = Engine::try_from(&args)?;

    let results = engine.execute_commands("SELECT * FROM tests.data.customers")?;

    assert_eq!(results.len(), 1);
    let results = results.first().unwrap();
    assert_eq!(results.number_of_columns(), 7);
    assert_eq!(results.number_of_rows(), 10);

    assert_eq!(
        results.column_name(&Column::from_index(0)).unwrap().name(),
        "id"
    );
    assert_eq!(
        results.column_name(&Column::from_index(1)).unwrap().name(),
        "company"
    );
    assert_eq!(
        results.column_name(&Column::from_index(2)).unwrap().name(),
        "name"
    );
    assert_eq!(
        results.column_name(&Column::from_index(3)).unwrap().name(),
        "country"
    );
    assert_eq!(
        results.column_name(&Column::from_index(4)).unwrap().name(),
        "email"
    );
    assert_eq!(
        results.column_name(&Column::from_index(5)).unwrap().name(),
        "active"
    );
    assert_eq!(
        results.column_name(&Column::from_index(6)).unwrap().name(),
        "last modified"
    );

    let expected_data = get_expecte_data();
    for (row_index, data) in expected_data.iter().enumerate() {
        for (name, expected_value) in data {
            let row = Row::from_index(row_index);
            let name = ColumnName::simple(name);
            let actual_value = results.value(&row, &name);
            assert_eq!(expected_value, actual_value);
        }
    }
    Ok(())
}

#[test]
fn test_select_fields() -> Result<(), CdvSqlError> {
    let args = Args {
        command: None,
        home: None,
        first_line_as_name: true,
    };
    let engine = Engine::try_from(&args)?;

    let results = engine.execute_commands(
        "SELECT id, customers.name, active, tests.data.customers.email FROM tests.data.customers",
    )?;

    assert_eq!(results.len(), 1);
    let results = results.first().unwrap();
    assert_eq!(results.number_of_columns(), 4);
    assert_eq!(results.number_of_rows(), 10);

    assert_eq!(
        results.column_name(&Column::from_index(0)).unwrap().name(),
        "id"
    );
    assert_eq!(
        results.column_name(&Column::from_index(1)).unwrap().name(),
        "name"
    );
    assert_eq!(
        results.column_name(&Column::from_index(2)).unwrap().name(),
        "active"
    );
    assert_eq!(
        results.column_name(&Column::from_index(3)).unwrap().name(),
        "email"
    );

    let expected_data = get_expecte_data();
    for (row_index, data) in expected_data.iter().enumerate() {
        for (name, expected_value) in data {
            let row = Row::from_index(row_index);
            let name = ColumnName::simple(name);
            let actual_value = results.value(&row, &name);
            if name.name() == "id"
                || name.name() == "name"
                || name.name() == "active"
                || name.name() == "email"
            {
                assert_eq!(expected_value, actual_value);
            } else {
                assert_eq!(&Value::Empty, actual_value);
            }
        }
    }
    Ok(())
}
#[test]
fn test_cartesian_product() -> Result<(), CdvSqlError> {
    let args = Args {
        command: None,
        home: None,
        first_line_as_name: true,
    };
    let engine = Engine::try_from(&args)?;

    let results = engine.execute_commands(
        "SELECT A.id, B.name  FROM (SELECT * FROM tests.data.customers) A, (SELECT * FROM tests.data.customers) B",
    )?;

    assert_eq!(results.len(), 1);
    let results = results.first().unwrap();
    assert_eq!(results.number_of_columns(), 2);

    assert_eq!(
        results.column_name(&Column::from_index(0)).unwrap().name(),
        "id"
    );
    assert_eq!(
        results.column_name(&Column::from_index(1)).unwrap().name(),
        "name"
    );
    let data = get_expecte_data();
    let names: Vec<_> = data
        .iter()
        .flatten()
        .filter(|(name, _)| *name == "name")
        .map(|(_, data)| data)
        .collect();
    let ids: Vec<_> = data
        .iter()
        .flatten()
        .filter(|(name, _)| *name == "id")
        .map(|(_, data)| data)
        .collect();
    let mut expected_results = HashSet::new();
    for name in names {
        for id in &ids {
            expected_results.insert((name, *id));
        }
    }
    assert_eq!(results.number_of_rows(), expected_results.len());

    for row in results.rows() {
        let name = results.value(&row, &ColumnName::simple("name"));
        let id = results.value(&row, &ColumnName::simple("id"));
        assert!(expected_results.remove(&(name, id)));
    }
    assert!(expected_results.is_empty());

    Ok(())
}

/*


*/
