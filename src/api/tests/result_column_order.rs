// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use hawdb_core::ValueRef;

fn fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE catalog_order (id BIGINT PRIMARY KEY, name TEXT)")
        .unwrap();
    database
}

#[test]
fn catalog_projection_order_survives_empty_results_and_all_front_doors() {
    let mut database = fixture();
    for (sql, columns, values) in [
        (
            "SELECT data_type AS z_type, column_name AS a_name FROM information_schema.columns WHERE table_name = $1 AND column_name = 'id'",
            ["z_type", "a_name"],
            ["bigint", "id"],
        ),
        (
            "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE tablename = $1",
            ["tablename", "schemaname"],
            ["catalog_order", "public"],
        ),
        (
            "SELECT indexname, tablename FROM pg_indexes WHERE tablename = $1",
            ["indexname", "tablename"],
            ["catalog_order_pkey", "catalog_order"],
        ),
    ] {
        for (name, count) in [("catalog_order", 1), ("missing", 0)] {
            let parameters = [Value::String(name.into())];
            let mut outputs = vec![
                database.query_sql_with_params(sql, &parameters).unwrap(),
                database
                    .begin_read_transaction()
                    .unwrap()
                    .query_sql_with_params(sql, &parameters)
                    .unwrap(),
            ];
            outputs.push(
                database
                    .begin_transaction()
                    .unwrap()
                    .query_sql_with_params(sql, &parameters)
                    .unwrap(),
            );
            for output in outputs {
                assert_eq!(output.schema().columns(), columns, "{sql}");
                assert_eq!(output.rows.len(), count, "{sql}");
                if count != 0 {
                    for (ordinal, expected) in values.iter().enumerate() {
                        assert_eq!(
                            output.rows.value(0, ordinal),
                            Some(ValueRef::String(expected)),
                            "{sql}",
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn catalog_wildcards_expand_in_place_and_keep_declared_order() {
    let mut database = fixture();
    let table_columns = [
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
        "self_referencing_column_name",
        "reference_generation",
        "user_defined_type_catalog",
        "user_defined_type_schema",
        "user_defined_type_name",
        "is_insertable_into",
        "is_typed",
        "commit_action",
    ];
    for suffix in ["", " WHERE table_name = 'missing'", " LIMIT 0"] {
        let output = database
            .query_sql(&format!("SELECT * FROM information_schema.tables{suffix}"))
            .unwrap();
        assert_eq!(output.schema().columns(), table_columns);
    }
    for suffix in ["", " WHERE metric = 'missing'", " LIMIT 0"] {
        let output = database
            .query_sql(&format!(
                "SELECT value AS z_value, *, metric AS a_metric FROM system.plan_cache{suffix}"
            ))
            .unwrap();
        assert_eq!(
            output.schema().columns(),
            ["z_value", "metric", "value", "a_metric"]
        );
        for row in output.rows.iter() {
            assert_eq!(row.get("z_value"), row.get("value"));
            assert_eq!(row.get("a_metric"), row.get("metric"));
        }
    }
    for table in ["pg_tables", "pg_catalog.pg_tables"] {
        let output = database
            .query_sql(&format!("SELECT * FROM {table}"))
            .unwrap();
        assert_eq!(
            output.schema().columns(),
            [
                "schemaname",
                "tablename",
                "tableowner",
                "tablespace",
                "hasindexes",
                "hasrules",
                "hastriggers",
                "rowsecurity",
            ]
        );
    }
}

#[test]
fn catalog_duplicate_names_fail_even_without_rows() {
    let mut database = fixture();
    for projection in [
        "value, value",
        "value AS repeated, metric AS repeated",
        "*, value",
    ] {
        for suffix in ["", " WHERE metric = 'missing'", " LIMIT 0"] {
            let error = database
                .query_sql(&format!(
                    "SELECT {projection} FROM system.plan_cache{suffix}"
                ))
                .unwrap_err();
            assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
            assert!(error.to_string().contains("duplicate"), "{error}");
        }
    }
}

#[test]
fn catalog_order_preserves_row_and_payload_budget_refusal() {
    let mut database = fixture();
    let sql = "SELECT data_type, column_name FROM information_schema.columns WHERE table_name = 'catalog_order'";
    let output = database.query_sql(sql).unwrap();
    let payload = output
        .rows
        .iter()
        .map(|row| hawdb_executor::binding::map_payload_bytes(&row.to_owned_row()))
        .sum::<usize>();
    let exact = database
        .query_sql_with_params_options(
            sql,
            &[],
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: Some(payload),
            },
        )
        .unwrap();
    assert_eq!(exact.rows, output.rows);
    for (rows, bytes) in [(1, payload), (2, payload - 1)] {
        assert!(matches!(
            database.query_sql_with_params_options(
                sql,
                &[],
                QueryStreamOptions {
                    max_rows: Some(rows),
                    max_payload_bytes: Some(bytes),
                }
            ),
            Err(HawDBError::ReadBudgetExceeded(_))
        ));
    }
}
