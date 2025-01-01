use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use bigdecimal::ToPrimitive;
use chrono::NaiveDateTime;
use csvsql::{
    args::Args,
    engine::Engine,
    error::CdvSqlError,
    results::{Column, ColumnName, Row},
    value::Value,
};

struct Customer {
    id: i64,
    company: String,
    name: String,
    country: String,
    email: String,
    active: bool,
    last_modified: NaiveDateTime,
}
impl Customer {
    fn to_values(&self) -> Vec<(String, Value)> {
        vec![
            ("id".into(), Value::Int(self.id)),
            ("company".into(), Value::Str(self.company.to_string())),
            ("name".into(), Value::Str(self.name.to_string())),
            ("country".into(), Value::Str(self.country.to_string())),
            ("email".into(), Value::Str(self.email.to_string())),
            ("active".into(), Value::Bool(self.active)),
            ("last modified".into(), Value::Timestamp(self.last_modified)),
        ]
    }
}
fn get_customers() -> Vec<Customer> {
    vec![
        Customer {
            id: -5783077230795473732,
            company: "Haley Inc".into(),
            name: "Amely Waelchi".into(),
            country: "Andorra".into(),
            email: "ericka@example.com".into(),
            active: true,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-01-13 09:59:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: -2357055618613761006,
            company: "Schinner and Sons".into(),
            name: "Enoch Rutherford".into(),
            country: "Timor-Leste".into(),
            email: "adaline@example.org".into(),
            active: true,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-08-28 10:09:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: 7832674597680560407,
            company: "Abshire and MacGyver Group".into(),
            name: "Shania Jaskolski".into(),
            country: "San Marino".into(),
            email: "carolyn@example.com".into(),
            active: true,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-03-14 11:11:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: -7997066339800540952,
            company: "Dach and Sons".into(),
            name: "Lindsey Von".into(),
            country: "Brunei Darussalam".into(),
            email: "gregoria@example.org".into(),
            active: true,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-04-03 22:02:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: 5667204520293600582,
            company: "Block and Nicolas and Sons".into(),
            name: "Lavina Bode".into(),
            country: "Grenada".into(),
            email: "violette@example.org".into(),
            active: true,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-06-08 09:23:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: 8181115030395395092,
            company: "Jerde and Treutel and Sons".into(),
            name: "Dusty Bosco".into(),
            country: "New Zealand".into(),
            email: "violet@example.com".into(),
            active: false,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-07-14 06:02:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: -1531692708764354477,
            company: "Leuschke Group".into(),
            name: "Hollis Fadel".into(),
            country: "Niger".into(),
            email: "colton@example.net".into(),
            active: false,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-01-31 09:18:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: -217192795646671883,
            company: "Schroeder and Dare Group".into(),
            name: "Christophe Waelchi".into(),
            country: "Honduras".into(),
            email: "kendra@example.org".into(),
            active: false,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-01-28 01:45:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: -8862786196595644070,
            company: "Yundt and Sons".into(),
            name: "Fernando Johnson".into(),
            country: "Seychelles".into(),
            email: "kendall@example.org".into(),
            active: true,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-07-19 09:40:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
        Customer {
            id: 7292867880167040642,
            company: "Schamberger and Durgan Inc".into(),
            name: "Mable Spencer".into(),
            country: "Montserrat".into(),
            email: "eino@example.net".into(),
            active: false,
            last_modified: NaiveDateTime::parse_from_str(
                "2024-09-12 07:33:32",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        },
    ]
}
struct Sale {
    id: String,
    sale_made: NaiveDateTime,
    delivered_at: Option<NaiveDateTime>,
    price: f64,
    delivery_cost: f64,
    tax_percentage: f64,
    customer_id: i64,
}
fn get_sales() -> Vec<Sale> {
    vec![
        Sale {
            id: "a69dde4e-6ec2-444e-9c7f-b1939d1a7538".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-10-13 11:29:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-25 04:59:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 52.45,
            delivery_cost: 1.10,
            tax_percentage: 17.2438,
            customer_id: -8862786196595644070,
        },
        Sale {
            id: "bb51cbae-44d4-40f5-8837-88db78216bd0".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-17 23:40:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-23 19:06:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 508.51,
            delivery_cost: 0.04,
            tax_percentage: 19.5775,
            customer_id: -8862786196595644070,
        },
        Sale {
            id: "e1b934d7-927a-498f-9e2c-d33a772cb27c".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-07 00:50:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-07 03:55:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 433.32,
            delivery_cost: 6.36,
            tax_percentage: 0.5377,
            customer_id: 8181115030395395092,
        },
        Sale {
            id: "294ceca1-bfd7-45c0-be2f-77775a27bfcd".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-10-18 21:59:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-20 19:51:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 558.50,
            delivery_cost: 17.21,
            tax_percentage: 4.6055,
            customer_id: -5783077230795473732,
        },
        Sale {
            id: "c6cbd01b-fbd9-4e61-a48a-5cfbf989ad1e".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-24 13:36:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-30 22:26:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 7.68,
            delivery_cost: 0.02,
            tax_percentage: 6.1023,
            customer_id: -217192795646671883,
        },
        Sale {
            id: "ad4e6f16-c651-44ce-bd06-11c6dca7687a".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-07-12 02:04:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-13 23:26:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 375.27,
            delivery_cost: 0.80,
            tax_percentage: 2.9127,
            customer_id: 7292867880167040642,
        },
        Sale {
            id: "cbd92d89-8de9-4860-9a03-ea171611b130".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-08-18 07:16:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-11 10:10:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 361.02,
            delivery_cost: 2.19,
            tax_percentage: 16.0907,
            customer_id: 8181115030395395092,
        },
        Sale {
            id: "6476a96e-d9a1-4843-9ccd-90afebc90ef5".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-12 15:59:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-23 13:46:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 37.96,
            delivery_cost: 0.98,
            tax_percentage: 10.9823,
            customer_id: 7292867880167040642,
        },
        Sale {
            id: "31f381fc-7543-40b7-9c6b-86d3b1df69aa".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-05-15 15:52:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-29 10:04:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 342.00,
            delivery_cost: 0.21,
            tax_percentage: 19.9554,
            customer_id: 7832674597680560407,
        },
        Sale {
            id: "501f01ae-22c3-496a-8e20-8914d437f7a7".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-05-20 20:23:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-07-31 19:37:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 10.58,
            delivery_cost: 0.32,
            tax_percentage: 7.3759,
            customer_id: 8181115030395395092,
        },
        Sale {
            id: "9e1f5858-7aa3-4d2c-810b-e6e5da6decb5".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-06-06 13:11:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-07-07 07:53:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 490.51,
            delivery_cost: 2.97,
            tax_percentage: 16.4209,
            customer_id: -5783077230795473732,
        },
        Sale {
            id: "88140a19-c101-45cd-a415-9e294ff9fa07".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-07-22 21:03:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-29 14:23:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 419.80,
            delivery_cost: 12.70,
            tax_percentage: 0.7270,
            customer_id: 7832674597680560407,
        },
        Sale {
            id: "d8c75a09-c8fb-44ab-ade6-7716631ac809".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-02-21 08:18:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-03-16 13:03:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 208.59,
            delivery_cost: 3.28,
            tax_percentage: 6.9612,
            customer_id: -1531692708764354477,
        },
        Sale {
            id: "17a280e3-a1bc-4f59-8dbe-01853d94f71c".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-03-03 13:03:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-06-14 13:49:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 548.39,
            delivery_cost: 1.59,
            tax_percentage: 9.0752,
            customer_id: -7997066339800540952,
        },
        Sale {
            id: "81cbf889-0191-4952-b663-4895bbe831cd".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-10 22:31:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-25 09:50:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 543.59,
            delivery_cost: 12.11,
            tax_percentage: 12.4391,
            customer_id: -1531692708764354477,
        },
        Sale {
            id: "237cb41c-ad78-4cb0-b9af-e6dbc5a7d481".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-02-24 16:47:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-16 08:02:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 495.54,
            delivery_cost: 2.94,
            tax_percentage: 2.8724,
            customer_id: -2357055618613761006,
        },
        Sale {
            id: "4dab28d9-d230-4db1-9ef4-e16d83093515".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-01-28 21:08:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-05-04 01:06:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 303.64,
            delivery_cost: 1.18,
            tax_percentage: 17.1970,
            customer_id: 7292867880167040642,
        },
        Sale {
            id: "3085ecb6-112f-488a-a4ac-9d128703fd3c".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-08-09 13:57:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-05 02:01:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 75.52,
            delivery_cost: 1.51,
            tax_percentage: 14.5107,
            customer_id: -7997066339800540952,
        },
        Sale {
            id: "d53bf311-a5bf-48d6-b829-99eea334868e".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-07-21 13:00:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: None,
            price: 511.86,
            delivery_cost: 1.92,
            tax_percentage: 3.0063,
            customer_id: -1531692708764354477,
        },
        Sale {
            id: "465404ae-946b-4118-b635-aae34e31e3ac".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-03-14 16:53:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: None,
            price: 177.80,
            delivery_cost: 2.00,
            tax_percentage: 5.5723,
            customer_id: 5667204520293600582,
        },
        Sale {
            id: "4c102d5b-2ad7-47c6-b792-aac61a01713d".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-01-17 05:30:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-03-20 19:29:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 42.48,
            delivery_cost: 0.18,
            tax_percentage: 4.1627,
            customer_id: -7997066339800540952,
        },
        Sale {
            id: "bf7017c9-0d7b-4d6a-87ed-b5511fb3d45c".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-02 23:44:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-28 07:50:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 505.51,
            delivery_cost: 15.27,
            tax_percentage: 8.5395,
            customer_id: 8181115030395395092,
        },
        Sale {
            id: "04d78c0f-0d5b-41e6-82d7-d03d97ec459c".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-10-28 22:47:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-29 20:21:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 527.85,
            delivery_cost: 8.38,
            tax_percentage: 18.5045,
            customer_id: -2357055618613761006,
        },
        Sale {
            id: "81582c5a-171c-4862-afd2-96e4f95638ce".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-08-31 03:22:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-24 14:53:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 172.03,
            delivery_cost: 2.61,
            tax_percentage: 16.5524,
            customer_id: 8181115030395395092,
        },
        Sale {
            id: "85206a75-588e-44d1-b55a-f878e0571993".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-05-26 07:30:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-09 20:38:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 157.71,
            delivery_cost: 1.97,
            tax_percentage: 2.7582,
            customer_id: -7997066339800540952,
        },
        Sale {
            id: "990f0c66-c738-44d1-80e4-8a18c210a84c".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-05-31 11:25:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-07-19 04:32:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 128.38,
            delivery_cost: 4.03,
            tax_percentage: 4.4871,
            customer_id: -217192795646671883,
        },
        Sale {
            id: "713239b0-42ca-4cd9-9d13-efe325c5b0f7".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-07-20 11:51:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-27 10:42:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 490.34,
            delivery_cost: 10.85,
            tax_percentage: 1.9361,
            customer_id: -5783077230795473732,
        },
        Sale {
            id: "31de9a1d-0c13-49a9-838c-a2b75d444b2e".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-05-26 16:36:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-07-12 23:37:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 438.74,
            delivery_cost: 8.94,
            tax_percentage: 1.9910,
            customer_id: -5783077230795473732,
        },
        Sale {
            id: "f173099f-77ce-471c-8ec0-3b8299b55bc8".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-08-02 20:05:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-08-08 22:45:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 313.58,
            delivery_cost: 0.48,
            tax_percentage: 18.4541,
            customer_id: -5783077230795473732,
        },
        Sale {
            id: "6b44a0c0-400c-4d6b-827c-f29a83b1c4c8".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-06-05 20:17:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: None,
            price: 524.56,
            delivery_cost: 8.39,
            tax_percentage: 11.6182,
            customer_id: -2357055618613761006,
        },
        Sale {
            id: "149ade13-ef5f-4c3e-8a6b-d0109c46c798".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-07-28 10:43:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: None,
            price: 150.86,
            delivery_cost: 2.70,
            tax_percentage: 0.8180,
            customer_id: 5667204520293600582,
        },
        Sale {
            id: "e5b1d405-f0ca-4c54-8004-4ea0e468c532".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-02-01 14:01:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-25 22:41:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 588.86,
            delivery_cost: 0.88,
            tax_percentage: 10.7028,
            customer_id: 7292867880167040642,
        },
        Sale {
            id: "1ff4bbad-6eac-423a-a8e4-a7253ee0bb51".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-02-24 10:20:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-06-26 21:17:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 496.23,
            delivery_cost: 5.80,
            tax_percentage: 14.4132,
            customer_id: -2357055618613761006,
        },
        Sale {
            id: "3e584d5c-08a8-49a0-82c5-0140c7b7c0ec".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-06-05 18:02:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: None,
            price: 57.11,
            delivery_cost: 0.84,
            tax_percentage: 9.8257,
            customer_id: -2357055618613761006,
        },
        Sale {
            id: "b26cb6dd-46e9-4e79-ac84-5978a4c41180".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-08-18 23:21:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-07 01:59:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 234.37,
            delivery_cost: 7.80,
            tax_percentage: 9.7815,
            customer_id: -8862786196595644070,
        },
        Sale {
            id: "cccee5a0-89d2-4196-b3c1-d8c311153aef".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-01-18 06:05:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-08-28 23:57:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 253.65,
            delivery_cost: 6.08,
            tax_percentage: 19.8160,
            customer_id: 5667204520293600582,
        },
        Sale {
            id: "b408e9a9-f616-409a-a776-3697381050fb".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-01-25 18:02:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-06-30 08:55:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 392.10,
            delivery_cost: 2.27,
            tax_percentage: 10.6329,
            customer_id: 8181115030395395092,
        },
        Sale {
            id: "0885c67f-bad2-412d-bad6-4144bb22da5d".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-07-10 14:03:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: None,
            price: 220.00,
            delivery_cost: 3.02,
            tax_percentage: 15.7206,
            customer_id: 7292867880167040642,
        },
        Sale {
            id: "82312859-b7c3-48db-93ba-515eb72e4a19".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-06-17 14:00:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-09-09 23:49:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 415.84,
            delivery_cost: 6.13,
            tax_percentage: 14.6032,
            customer_id: 7292867880167040642,
        },
        Sale {
            id: "6c32e71f-40ad-4787-9784-191fda404c53".into(),
            sale_made: NaiveDateTime::parse_from_str("2024-09-19 23:57:32", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
            delivered_at: Some(
                NaiveDateTime::parse_from_str("2024-10-04 18:17:32", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            price: 53.75,
            delivery_cost: 0.75,
            tax_percentage: 9.4016,
            customer_id: 7832674597680560407,
        },
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

    let expected_data = get_customers();
    for (row_index, data) in expected_data.iter().enumerate() {
        for (name, expected_value) in data.to_values() {
            let row = Row::from_index(row_index);
            let name = ColumnName::simple(&name);
            let actual_value = results.value(&row, &name);
            assert_eq!(expected_value, *actual_value);
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

    let expected_data = get_customers();
    for (row_index, data) in expected_data.iter().enumerate() {
        for (name, expected_value) in data.to_values() {
            let row = Row::from_index(row_index);
            let name = ColumnName::simple(&name);
            let actual_value = results.value(&row, &name);
            if name.name() == "id"
                || name.name() == "name"
                || name.name() == "active"
                || name.name() == "email"
            {
                assert_eq!(expected_value, *actual_value);
            } else {
                assert_eq!(Value::Empty, *actual_value);
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

    let mut expected_results = HashSet::new();
    for name in get_customers() {
        let name = name.name;
        for id in get_customers() {
            let id = id.id;
            expected_results.insert((name.clone(), id));
        }
    }

    assert_eq!(results.number_of_rows(), expected_results.len());

    for row in results.rows() {
        let name = results.value(&row, &ColumnName::simple("name")).to_string();
        let id = match *results.value(&row, &ColumnName::simple("id")) {
            Value::Int(i) => i,
            _ => -1,
        };
        assert!(expected_results.remove(&(name, id)));
    }
    assert!(expected_results.is_empty());

    Ok(())
}
#[test]
fn test_select_with_plus() -> Result<(), CdvSqlError> {
    let args = Args {
        command: None,
        home: None,
        first_line_as_name: true,
    };
    let engine = Engine::try_from(&args)?;

    let results = engine.execute_commands(
        "SELECT id, price + \"delivery cost\" as total_price  FROM tests.data.sales;",
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
        "total_price"
    );

    let mut prices = HashMap::new();
    for data in get_sales() {
        prices.insert(data.id.clone(), data.price + data.delivery_cost);
    }
    assert_eq!(results.number_of_rows(), prices.len());

    for row in results.rows() {
        let id = results.value(&row, &ColumnName::simple("id")).to_string();
        let expected_price = match results
            .value(&row, &ColumnName::simple("total_price"))
            .deref()
        {
            Value::BigDecimal(b) => b.to_f64().unwrap(),
            Value::Float(f) => *f,
            _ => 0.1,
        };
        let actual_price = prices.remove(&id).unwrap();
        let diff = (actual_price - expected_price).abs();
        assert!(diff < 0.01);
    }
    assert!(prices.is_empty());

    Ok(())
}
#[test]
fn test_use_literal() -> Result<(), CdvSqlError> {
    let args = Args {
        command: None,
        home: None,
        first_line_as_name: true,
    };
    let engine = Engine::try_from(&args)?;

    let results = engine.execute_commands(
        "SELECT id, \"tax percentage\", 100* \"tax percentage\"  FROM tests.data.sales;",
    )?;

    assert_eq!(results.len(), 1);
    let results = results.first().unwrap();

    assert_eq!(results.number_of_columns(), 3);

    assert_eq!(
        results.column_name(&Column::from_index(0)).unwrap().name(),
        "id"
    );
    assert_eq!(
        results.column_name(&Column::from_index(1)).unwrap().name(),
        "tax percentage"
    );
    assert_eq!(
        results.column_name(&Column::from_index(2)).unwrap().name(),
        "100 * tax percentage"
    );

    let mut taxes = HashMap::new();
    for data in get_sales() {
        taxes.insert(data.id.clone(), data.tax_percentage);
    }
    assert_eq!(results.number_of_rows(), taxes.len());

    for row in results.rows() {
        let id = results.value(&row, &ColumnName::simple("id")).to_string();
        let tax = match results
            .value(&row, &ColumnName::simple("tax percentage"))
            .deref()
        {
            Value::BigDecimal(b) => b.to_f64().unwrap(),
            Value::Float(f) => *f,
            _ => 0.1,
        };
        let tax_times_100 = match results
            .value(&row, &ColumnName::simple("100 * tax percentage"))
            .deref()
        {
            Value::BigDecimal(b) => b.to_f64().unwrap(),
            Value::Float(f) => *f,
            _ => 0.1,
        };

        let actual_tax = taxes.remove(&id).unwrap();
        let diff = (actual_tax - tax).abs();
        assert!(diff < 0.01);
        let diff_two = (actual_tax * 100.0 - tax_times_100).abs();
        assert!(diff_two < 0.01);
    }
    assert!(taxes.is_empty());

    Ok(())
}
