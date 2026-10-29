// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::generator::DeterministicRng;

const INSERT: &str =
    "INSERT INTO @table (id, bucket, score, tag, enabled) VALUES ($1, $2, $3, $4, $5)";
const UPSERT: &str = "INSERT INTO @table (id, bucket, score, tag, enabled) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET bucket = EXCLUDED.bucket, score = EXCLUDED.score, tag = EXCLUDED.tag, enabled = EXCLUDED.enabled";
const AGGREGATES: &str = "COUNT(*) AS n, COUNT(score) AS present, SUM(score) AS total, MIN(score) AS minimum, MAX(score) AS maximum, MIN(tag) AS first_tag, MAX(tag) AS last_tag";

fn paired(sql: &str, parameters: Vec<Value>) -> Vec<SqlMutation> {
    ["sm_source", "sm_reference"]
        .iter()
        .map(|table| SqlMutation::data(sql.replace("@table", table), parameters.clone()))
        .collect()
}

fn step(name: &'static str, sql: &str, parameters: Vec<Value>, rollback: bool) -> Step {
    Step {
        name,
        mutations: paired(sql, parameters),
        rollback,
        boundary: Boundary::None,
        expected_error: None,
    }
}

fn row(rng: &mut DeterministicRng, id: i64) -> Vec<Value> {
    let bits = rng.next_u64();
    vec![
        Value::Int(id),
        if bits.is_multiple_of(5) {
            Value::Null
        } else {
            Value::Int((bits % 3) as i64)
        },
        if bits.is_multiple_of(4) {
            Value::Null
        } else {
            Value::Int(((bits >> 8) % 9) as i64 - 4)
        },
        match (bits >> 16) % 4 {
            0 => Value::Null,
            1 => Value::String("alpha".into()),
            2 => Value::String("zeta".into()),
            _ => Value::String("éclair".into()),
        },
        Value::Bool(bits & 8 != 0),
    ]
}

fn query(
    sql: String,
    parameters: Vec<Value>,
    result_semantics: ResultSemantics,
) -> SqlQueryInvocation {
    SqlQueryInvocation {
        sql,
        parameters,
        result_semantics,
    }
}

fn relation(name: &'static str, actual: String, reference: String) -> Relation {
    Relation {
        name,
        actual: query(actual, vec![], ResultSemantics::Bag),
        reference: query(reference, vec![], ResultSemantics::Bag),
    }
}

pub(super) fn generate(seed: u64, index: usize) -> Case {
    let mut rng = DeterministicRng::new(seed);
    let mut setup = paired("CREATE TABLE @table (id BIGINT PRIMARY KEY, bucket BIGINT, score BIGINT, tag TEXT, enabled BOOLEAN NOT NULL)", vec![]);
    setup.push(SqlMutation::required(
        "CREATE INDEX sm_source_order ON sm_source (bucket, score, id)",
    ));
    // Backfill includes duplicate extrema, a NULL-only group and projection
    // duplicates whose source identities must remain independent.
    setup.extend(paired("INSERT INTO @table (id, bucket, score, tag, enabled) VALUES (0, 0, -9, 'alpha', true), (1, 0, -9, 'alpha', true), (2, 0, 9, 'zeta', true), (3, 0, 9, 'zeta', true), (4, NULL, NULL, NULL, true), (5, 2, NULL, NULL, true)", vec![]));
    for id in 6..30 {
        setup.extend(paired(INSERT, row(&mut rng, id)));
    }
    let projection = "SELECT bucket, score, tag FROM sm_reference WHERE enabled = true";
    let grouped = format!(
        "SELECT bucket, {AGGREGATES} FROM sm_reference WHERE enabled = true GROUP BY bucket"
    );
    let global = format!("SELECT {AGGREGATES} FROM sm_reference WHERE enabled = true");
    for (name, select) in [
        ("sm_projection", projection),
        ("sm_grouped", grouped.as_str()),
        ("sm_global", global.as_str()),
    ] {
        setup.push(SqlMutation::required(format!(
            "CREATE MATERIALIZED VIEW {name} AS {}",
            select.replace("sm_reference", "sm_source")
        )));
    }
    setup.push(SqlMutation::required(
        "CREATE INDEX sm_view_order ON sm_projection (bucket, score)",
    ));
    // Schema changes checkpoint their rows. Append after the final DDL so the
    // initial reopen must replay data and incremental view maintenance from WAL.
    setup.extend(paired(
        INSERT,
        vec![
            Value::Int(30),
            Value::Int(0),
            Value::Int(0),
            Value::String("wal-tail".into()),
            Value::Bool(true),
        ],
    ));
    let mut relations = vec![
        relation("base", "SELECT * FROM sm_source".into(), "SELECT * FROM sm_reference".into()),
        relation("projection", "SELECT * FROM sm_projection".into(), projection.into()),
        relation("grouped", "SELECT * FROM sm_grouped".into(), grouped),
        relation("global", "SELECT * FROM sm_global".into(), global),
        relation("view_index", "SELECT score, tag FROM sm_projection WHERE bucket = 0 AND score >= -9".into(), "SELECT score, tag FROM sm_reference WHERE enabled = true AND bucket = 0 AND score >= -9".into()),
    ];
    let lower = if index & 1 == 0 { ">=" } else { ">" };
    let upper = if index & 2 == 0 { "<=" } else { "<" };
    let direction = if index & 4 == 0 { "ASC" } else { "DESC" };
    let limit = 1 + (seed % 5);
    let offset = (seed >> 3) % 3;
    for (name, bounds) in [
        ("range_interval", vec![Value::Int(-9), Value::Int(9)]),
        ("range_equal", vec![Value::Int(-9), Value::Int(-9)]),
        ("range_reversed", vec![Value::Int(9), Value::Int(-9)]),
        ("range_null", vec![Value::Null, Value::Int(9)]),
    ] {
        // Body is outside the index and the residual predicate must survive
        // both range derivation and LIMIT/OFFSET pushdown.
        let residual = if seed & 1 == 0 {
            " AND enabled = true"
        } else {
            ""
        };
        let sql = format!("SELECT id, score, tag FROM @table WHERE bucket = 0 AND score {lower} $1 AND score {upper} $2{residual} ORDER BY score {direction}, id {direction} LIMIT {limit} OFFSET {offset}");
        relations.push(Relation {
            name,
            actual: query(
                sql.replace("@table", "sm_source"),
                bounds.clone(),
                ResultSemantics::Ordered,
            ),
            reference: query(
                sql.replace("@table", "sm_reference"),
                bounds,
                ResultSemantics::Ordered,
            ),
        });
    }
    let mut steps = vec![
        step("delete", "DELETE FROM @table WHERE id = 0", vec![], false),
        step("delete", "DELETE FROM @table WHERE id = 1", vec![], false),
        step(
            "update",
            "UPDATE @table SET id = 80, bucket = NULL, score = NULL, tag = NULL WHERE id = 2",
            vec![],
            false,
        ),
        step(
            "update",
            "UPDATE @table SET bucket = 1, enabled = false WHERE id = 3",
            vec![],
            false,
        ),
        step(
            "rollback",
            "UPDATE @table SET enabled = false WHERE id >= 0",
            vec![],
            true,
        ),
    ];
    for position in 0..8 {
        let id = (rng.next_u64() % 16) as i64;
        let values = row(&mut rng, id);
        let next = match position % 4 {
            0 => step("upsert", UPSERT, values, false),
            1 => step("insert", INSERT, row(&mut rng, 100 + position), false),
            2 => step(
                "update",
                "UPDATE @table SET bucket = $2, score = $3, tag = $4, enabled = $5 WHERE id = $1",
                values,
                seed & 16 == 0,
            ),
            _ => step(
                "delete",
                "DELETE FROM @table WHERE id = $1",
                vec![Value::Int(id)],
                false,
            ),
        };
        steps.push(next);
    }
    steps.push(step(
        "clear",
        "DELETE FROM @table WHERE id >= 0",
        vec![],
        false,
    ));
    steps.push(step(
        "extreme_sum",
        INSERT,
        vec![
            Value::Int(500),
            Value::Null,
            Value::Int(i64::MAX),
            Value::Null,
            Value::Bool(true),
        ],
        false,
    ));
    // A valid earlier write in the same transaction must not leak when SUM
    // maintenance rejects a later source write. Do not mirror the invalid write.
    let mut rejected = step(
        "overflow_rejection",
        "UPDATE @table SET tag = 'staged' WHERE id = 500",
        vec![],
        false,
    );
    rejected.expected_error = Some("materialized aggregate BIGINT overflow");
    rejected.mutations.push(SqlMutation::data(
        INSERT.replace("@table", "sm_source"),
        vec![
            Value::Int(501),
            Value::Null,
            Value::Int(1),
            Value::Null,
            Value::Bool(true),
        ],
    ));
    steps.push(rejected);
    steps.push(step(
        "clear",
        "DELETE FROM @table WHERE id >= 0",
        vec![],
        false,
    ));
    // Recreate a previously empty group and a non-empty NULL-only aggregate.
    steps.push(step(
        "repopulate",
        INSERT,
        vec![
            Value::Int(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Bool(true),
        ],
        false,
    ));
    let storage = StorageProfile::for_case(index);
    if storage != StorageProfile::Memory {
        for (position, step) in steps.iter_mut().enumerate() {
            step.boundary = if position == 0 || step.name == "clear" {
                Boundary::Checkpoint
            } else if position == 1
                || step.rollback
                || step.expected_error.is_some()
                || matches!(step.name, "repopulate" | "extreme_sum")
                || rng.next_u64().is_multiple_of(4)
            {
                Boundary::Wal
            } else {
                Boundary::None
            };
        }
    }
    Case {
        seed,
        storage,
        setup,
        steps,
        relations,
    }
}
