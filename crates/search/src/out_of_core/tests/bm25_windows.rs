// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::lexical_projection::QueryContext;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

#[test]
#[ignore = "manual allocation and retained-window measurement; excludes fixture construction"]
fn bm25_window_allocation_measurement() {
    let _serial = crate::test_allocation::serial();
    let path = test_dir("bm25-window-measurement");
    let mut first = document(0, "team");
    first.id = format!("a{}", "x".repeat(4096));
    let mut second = document(1, "team");
    second.id = format!("b{}", "x".repeat(4096));
    publish_two_artifact_manifest(&path, first, second);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let terms = reader.query_terms("graph", None).unwrap();
    let before = crate::test_allocation::live();
    let (scores, peak) = crate::test_allocation::measure(|| {
        reader
            .query_text_scores(
                &terms.values,
                Some(1),
                false,
                &CandidateSet::All(2),
                &mut Default::default(),
                None,
            )
            .unwrap()
    });
    assert_eq!(scores.matching_count, 2);
    assert!(scores.values.keys().any(|id| id.starts_with('b')));
    eprintln!("bm25 segments=2 window=1 matches={} retained={} retained_id_bytes={} peak_bytes={} read_bytes={}", scores.matching_count, scores.values.len(), scores.values.keys().map(String::len).sum::<usize>(), peak.saturating_sub(before), scores.bytes_read);
    drop(scores);
    assert_eq!(crate::test_allocation::live(), before);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_segment_scores_merge_into_one_global_window() {
    let path = test_dir("bm25-global-window");
    let documents = (0..32)
        .map(|n| {
            let mut doc = document(n % 16, "team");
            doc.id = format!("memory:{n:03}");
            doc
        })
        .collect::<Vec<_>>();
    publish_two_artifact_manifest_with_initial_documents(
        &path,
        documents,
        Default::default(),
        {
            let mut doc = document(0, "team");
            doc.id = "memory:032".into();
            doc
        },
        Default::default(),
    );
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let terms = reader.query_terms("graph", None).unwrap();
    let candidates = CandidateSet::All(33);
    let exhaustive = reader
        .query_text_scores(
            &terms.values,
            None,
            false,
            &candidates,
            &mut Default::default(),
            None,
        )
        .unwrap();
    assert_eq!(exhaustive.matching_count, 33);
    let mut expected = exhaustive
        .values
        .iter()
        .map(|(id, score)| (id.clone(), *score))
        .collect::<Vec<_>>();
    expected.sort_unstable_by(|(a, av), (b, bv)| bv.total_cmp(av).then_with(|| a.cmp(b)));
    let budget = NonZeroUsize::new(16 << 20).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let memory = ledger.account(
        QueryMemoryClass::BlockingState,
        "global text window",
        budget,
    );
    let task = crate::RuntimeTaskContext::default();
    let context = QueryContext {
        memory: &memory,
        task: &task,
        execution: None,
        io_scope: None,
    };
    for context in [None, Some(context)] {
        for limit in [0, 1, 7, 32, 64] {
            let scores = reader
                .query_text_scores(
                    &terms.values,
                    Some(limit),
                    false,
                    &candidates,
                    &mut Default::default(),
                    context,
                )
                .unwrap();
            assert_eq!(scores.matching_count, 33);
            assert_eq!(
                scores.values,
                expected.iter().take(limit).cloned().collect()
            );
            assert!(scores.values.len() <= limit);
            drop(scores);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
    let mut page = options(7, None);
    page.offset = 5;
    let actual = reader
        .search_with_options("graph", None, SearchMode::Text, page)
        .unwrap();
    assert_eq!(actual.result.total_hits, 33);
    assert_eq!(
        actual
            .result
            .hits
            .iter()
            .map(|hit| (hit.id.as_str(), hit.text_score.to_bits()))
            .collect::<Vec<_>>(),
        expected
            .iter()
            .skip(5)
            .take(7)
            .map(|(id, score)| (id.as_str(), score.to_bits()))
            .collect::<Vec<_>>()
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_segment_merge_still_rejects_duplicate_live_documents() {
    let path = test_dir("bm25-duplicate-window");
    publish_two_artifact_manifest(&path, document(0, "team"), document(0, "team"));
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let terms = reader.query_terms("graph", None).unwrap();
    let error = reader
        .query_text_scores(
            &terms.values,
            Some(1),
            false,
            &CandidateSet::All(2),
            &mut Default::default(),
            None,
        )
        .err()
        .expect("duplicate live layers must fail closed");
    assert!(error.to_string().contains("duplicate document"), "{error}");
    fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn bounded_hybrid_components_match_a_single_artifact() {
    let layered_path = test_dir("bm25-hybrid-layered");
    let single_path = test_dir("bm25-hybrid-single");
    let first = document(0, "team");
    let second = document(1, "team");
    let embedding = first.embedding.clone().unwrap();
    publish_two_artifact_manifest(&layered_path, first.clone(), second.clone());
    let mut writer =
        SearchOutOfCoreGenerationWriter::create(&single_path, Default::default()).unwrap();
    writer.push(first).unwrap();
    writer.push(second).unwrap();
    writer.finish().unwrap();
    let layered = SearchOutOfCoreReader::open(&layered_path).unwrap();
    let single = SearchOutOfCoreReader::open(&single_path).unwrap();
    for limit in [0, 1, 2] {
        let expected = single
            .search_with_options(
                "graph",
                Some(&embedding),
                SearchMode::Hybrid,
                options(10, Some(limit)),
            )
            .unwrap();
        let actual = layered
            .search_with_options(
                "graph",
                Some(&embedding),
                SearchMode::Hybrid,
                options(10, Some(limit)),
            )
            .unwrap();
        let signature = |output: &SearchOutOfCoreOutput| {
            output
                .result
                .hits
                .iter()
                .map(|hit| {
                    (
                        hit.id.clone(),
                        hit.score.to_bits(),
                        hit.text_score.to_bits(),
                        hit.vector_score.to_bits(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(signature(&actual), signature(&expected));
        assert_eq!(actual.result.total_hits, expected.result.total_hits);
    }
    fs::remove_dir_all(layered_path).unwrap();
    fs::remove_dir_all(single_path).unwrap();
}
