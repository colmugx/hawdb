// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn allocation_fixture(
    root: &OwnedDirectory,
    count: usize,
    id_suffix: &str,
) -> (Arc<LexicalProjectionReader>, Vec<SearchDocument>) {
    let documents = (0..count)
        .map(|index| document(&format!("document-{index:08}{id_suffix}"), "", "needle"))
        .collect::<Vec<_>>();
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig {
        target_block_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
        max_block_bytes: NonZeroU64::new(2 * 1024 * 1024).unwrap(),
        ..Default::default()
    })
    .write(
        &root.0,
        1,
        None,
        11,
        13,
        documents.iter(),
        &SearchAnalyzerLexicon::empty(),
    )
    .unwrap();
    (reader, documents)
}

#[test]
fn lexical_query_posting_allocations_grow_with_buffers_not_entries() {
    let _serial = crate::test_allocation::serial();
    for count in [8, 128, 4096] {
        let root = OwnedDirectory::new("posting-allocations");
        let (reader, _) = allocation_fixture(&root, count, "");
        let blocks = reader.posting_blocks("needle").count();
        let encoded = RefCell::new(PostingReadBuffer::default());
        let mut stream = TermPostingStream::new(&reader, "needle", &encoded);
        let (visited, allocations) = count_allocations(|| {
            let mut visited = 0;
            while let Some(ordinal) = stream.peek_ordinal().unwrap() {
                assert_eq!(ordinal, visited as u64);
                stream.take().unwrap();
                visited += 1;
            }
            visited
        });
        assert_eq!(visited, count);
        eprintln!("posting entries={count}, blocks={blocks}, allocations={allocations}");
        // These equal-sized blocks reuse one encoded and one decoded buffer.
        assert!(
            allocations <= 2,
            "posting buffers must survive block transitions"
        );
    }
}

#[test]
fn lexical_query_document_lookup_borrows_skipped_and_matching_ids() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedDirectory::new("document-lookup-allocations");
    let (reader, documents) =
        allocation_fixture(&root, 512, &format!("-\u{00e9}{}", "x".repeat(128)));
    assert_eq!(
        reader
            .manifest
            .blocks
            .iter()
            .filter(|block| block.kind == BlockKind::Documents)
            .count(),
        1
    );
    let mut lookup = DocumentLookup::new(&reader, None);
    assert_eq!(lookup.get(0).unwrap().0, documents[0].id);
    let bytes_read = lookup.bytes_read;
    let (_, allocations) = count_allocations(|| {
        // Both the skipped IDs and the requested IDs remain in the loaded block.
        for ordinal in [255, 256, 511] {
            let (id, length) = lookup.get(ordinal).unwrap();
            assert_eq!(id, documents[ordinal as usize].id);
            assert_eq!(length, 1);
        }
    });
    assert_eq!(lookup.bytes_read, bytes_read);
    eprintln!("document lookups=3, skipped IDs=508, allocations={allocations}");
    assert_eq!(allocations, 0);
}

#[test]
fn lexical_query_shares_a_physical_block_between_terms() {
    let root = OwnedDirectory::new("shared-posting-block");
    let documents = (0..8)
        .map(|index| document(&format!("doc-{index}"), "", "alpha beta gamma"))
        .collect::<Vec<_>>();
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig {
        target_block_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
        max_block_bytes: NonZeroU64::new(2 * 1024 * 1024).unwrap(),
        ..Default::default()
    })
    .write(
        &root.0,
        1,
        None,
        11,
        13,
        documents.iter(),
        &SearchAnalyzerLexicon::empty(),
    )
    .unwrap();
    let terms = BTreeSet::from(["alpha".to_owned(), "beta".to_owned(), "gamma".to_owned()]);
    let blocks = terms
        .iter()
        .flat_map(|term| reader.posting_blocks(term))
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), 3);
    assert!(blocks
        .iter()
        .all(|block| block.block_id == blocks[0].block_id));
    let report = reader
        .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
        .unwrap();
    let statistics = reader
        .query_statistics(&terms, reader.config.max_term_bytes)
        .unwrap();
    assert_eq!(report.matching_document_count, 8);
    assert_eq!(report.postings_visited, 24);
    assert_eq!(
        report.bytes_read - report.document_bytes_read - statistics.bytes_read(),
        blocks[0].length
    );
    let corpus =
        crate::TextCorpusStats::from_documents(documents.iter(), &SearchAnalyzerLexicon::empty());
    for doc in &documents {
        assert_eq!(
            report.scores[&doc.id].to_bits(),
            crate::bm25_score(&terms, doc, &corpus, &SearchAnalyzerLexicon::empty()).to_bits()
        );
    }
}

#[test]
fn shared_posting_buffer_refuses_cancelled_hits_and_does_not_cache_failed_reads() {
    let root = OwnedDirectory::new("shared-posting-errors");
    let (reader, _) = allocation_fixture(&root, 8, "");
    let block = reader.posting_blocks("needle").next().unwrap();
    let mut buffer = PostingReadBuffer::default();
    assert_eq!(buffer.read(&reader, block, None).unwrap().1, block.length);
    assert_eq!(buffer.read(&reader, block, None).unwrap().1, 0);
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    assert!(buffer.read(&reader, block, Some(&task)).is_err());
    let mut corrupt = block.clone();
    corrupt.checksum ^= 1;
    assert!(buffer.read(&reader, &corrupt, None).is_err());
    assert!(buffer.block.is_none());
    assert_eq!(buffer.read(&reader, block, None).unwrap().1, block.length);
}
