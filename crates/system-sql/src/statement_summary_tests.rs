// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn assert_same_summary(actual: &StatementSummary, reference: &StatementSummary) {
    let without_clock = |summary: &StatementSummary| {
        let mut records = summary.snapshot();
        for record in &mut records {
            record.last_seen_unix_micros = 0;
        }
        records
    };
    assert_eq!(without_clock(actual), without_clock(reference));
}

#[test]
fn borrowed_completion_preserves_first_sample_and_error_recovery() {
    let first_text = "MATCH (n:Summary {id: 1}) RETURN n";
    let next_text = "match (n:Summary {id: 2}) return n;";
    let first = QueryIdentity::new("cypher", first_text);
    let next = QueryIdentity::new("CYPHER", next_text);
    assert_eq!(first.query_digest(), next.query_digest());
    assert_ne!(first.query_text_hash(), next.query_text_hash());
    let mut actual = StatementSummary::new(2);
    let mut reference = StatementSummary::new(2);
    actual.record_completed(" cypher ", first_text, "MATCH", &first, 5, 2);
    reference.record(StatementExecution::completed(
        " cypher ", first_text, "MATCH", &first, 5, 2,
    ));
    assert_same_summary(&actual, &reference);

    let failure = StatementExecution::failed(
        "CYPHER",
        next_text,
        "READ",
        &next,
        7,
        "\u{03bb}".repeat(MAX_STATEMENT_ERROR_BYTES),
    );
    actual.record(failure.clone());
    reference.record(failure);
    assert_same_summary(&actual, &reference);
    assert_eq!(
        actual.snapshot()[0].last_error.as_deref(),
        Some("\u{03bb}".repeat(MAX_STATEMENT_ERROR_BYTES / 2).as_str()),
    );

    actual.record_completed("CYPHER", next_text, "READ", &next, 11, 3);
    reference.record(StatementExecution::completed(
        "CYPHER", next_text, "READ", &next, 11, 3,
    ));
    assert_same_summary(&actual, &reference);
    let record = &actual.snapshot()[0];
    assert_eq!(record.query_language, " cypher ");
    assert_eq!(record.query_text, first_text);
    assert_eq!(record.statement_kind, "MATCH");
    assert_eq!(record.sample_query_text_hash, first.query_text_hash());
    assert_eq!(
        (
            record.execution_count,
            record.success_count,
            record.error_count
        ),
        (3, 2, 1)
    );
    assert_eq!(
        (record.total_elapsed_micros, record.max_elapsed_micros),
        (23, 11)
    );
    assert_eq!((record.total_row_count, record.last_row_count), (5, 3));
    assert_eq!(record.last_elapsed_micros, 11);
    assert!(record.last_success);
    assert_eq!(record.last_error, None);
}

#[test]
fn borrowed_completion_preserves_disabled_capacity_and_fifo_eviction() {
    let texts = [
        "MATCH (n:A) RETURN n",
        "MATCH (n:B) RETURN n",
        "match (n:A) return n;",
        "MATCH (n:C) RETURN n",
    ];
    for capacity in [0, 1, 2] {
        let mut actual = StatementSummary::new(capacity);
        let mut reference = StatementSummary::new(capacity);
        for (index, text) in texts.iter().enumerate() {
            let identity = QueryIdentity::new("cypher", text);
            actual.record_completed("cypher", text, "MATCH", &identity, index as u128, index);
            reference.record(StatementExecution::completed(
                "cypher",
                text,
                "MATCH",
                &identity,
                index as u128,
                index,
            ));
            assert_same_summary(&actual, &reference);
            assert!(actual.snapshot().len() <= capacity);
        }
        if capacity == 2 {
            // Updating A preserves its original FIFO position rather than
            // converting insertion-order eviction into LRU eviction.
            assert_eq!(
                actual
                    .snapshot()
                    .iter()
                    .map(|record| record.digest.as_str())
                    .collect::<Vec<_>>(),
                [
                    QueryIdentity::new("cypher", texts[1]).query_digest(),
                    QueryIdentity::new("cypher", texts[3]).query_digest(),
                ],
            );
        }
    }
}

#[test]
fn borrowed_completion_preserves_utf8_sample_truncation() {
    let text = format!("RETURN '{}'", "\u{03bb}".repeat(MAX_STATEMENT_TEXT_BYTES));
    let identity = QueryIdentity::new("cypher", &text);
    let mut actual = StatementSummary::new(1);
    let mut reference = StatementSummary::new(1);
    actual.record_completed("cypher", &text, "RETURN", &identity, 1, 1);
    reference.record(StatementExecution::completed(
        "cypher", &text, "RETURN", &identity, 1, 1,
    ));
    assert_same_summary(&actual, &reference);
    let record = &actual.snapshot()[0];
    assert!(record.query_text.len() <= MAX_STATEMENT_TEXT_BYTES);
    assert!(text.starts_with(&record.query_text));
    assert!(MAX_STATEMENT_TEXT_BYTES - record.query_text.len() < "\u{03bb}".len());
    assert_eq!(record.sample_query_text_hash, identity.query_text_hash());
}

#[test]
fn borrowed_completion_saturates_the_existing_counters() {
    let text = "MATCH (n) RETURN n";
    let identity = QueryIdentity::new("cypher", text);
    let mut actual = StatementSummary::new(1);
    actual.record_completed("cypher", text, "MATCH", &identity, 1, 1);
    let record = actual.records.get_mut(identity.query_digest()).unwrap();
    record.execution_count = i64::MAX;
    record.success_count = i64::MAX;
    record.error_count = i64::MAX;
    record.total_elapsed_micros = i64::MAX;
    record.total_row_count = i64::MAX;
    let mut reference = actual.clone();
    actual.record_completed("cypher", text, "MATCH", &identity, u128::MAX, usize::MAX);
    reference.record(StatementExecution::completed(
        "cypher",
        text,
        "MATCH",
        &identity,
        u128::MAX,
        usize::MAX,
    ));
    assert_same_summary(&actual, &reference);
    let record = &actual.snapshot()[0];
    assert_eq!(record.execution_count, i64::MAX);
    assert_eq!(record.success_count, i64::MAX);
    assert_eq!(record.error_count, i64::MAX);
    assert_eq!(record.total_elapsed_micros, i64::MAX);
    assert_eq!(record.total_row_count, i64::MAX);
    assert_eq!(record.max_elapsed_micros, i64::MAX);
    assert_eq!(record.last_elapsed_micros, i64::MAX);
}
