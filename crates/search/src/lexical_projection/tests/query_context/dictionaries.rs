// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn dictionary_fixture(
    root: &OwnedDirectory,
    count: usize,
    width: usize,
) -> (
    Arc<LexicalProjectionReader>,
    Vec<SearchDocument>,
    BTreeSet<String>,
) {
    let terms = (0..width)
        .map(|index| {
            format!(
                "word{}{}x",
                char::from(b'a' + (index / 26) as u8),
                char::from(b'a' + (index % 26) as u8)
            )
        })
        .collect::<BTreeSet<_>>();
    let words = terms.iter().collect::<Vec<_>>();
    let documents = (0..count)
        .map(|index| document(&format!("doc-{index:08}"), "", words[index % width]))
        .collect::<Vec<_>>();
    let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
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
    assert_eq!(
        reader
            .manifest
            .blocks
            .iter()
            .filter(|block| block.kind == BlockKind::Postings)
            .map(|block| u64::from(block.entry_count))
            .sum::<u64>(),
        count as u64,
        "fixture words must not produce analyzer aliases"
    );
    (reader, documents, terms)
}

fn verifications() -> usize {
    DICTIONARY_VERIFICATIONS.with(Cell::get)
}

#[test]
fn lexical_query_verifies_each_shared_dictionary_once() {
    for width in [1, 4, 32] {
        let root = OwnedDirectory::new("shared-dictionary");
        let (reader, documents, terms) = dictionary_fixture(&root, 128, width);
        let blocks = reader
            .manifest
            .blocks
            .iter()
            .filter(|block| block.kind == BlockKind::Postings)
            .collect::<Vec<_>>();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].entry_count, 128);
        let before = verifications();
        let full = reader
            .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
            .unwrap();
        // Statistics and scoring each read/verify the block once, irrespective
        // of the number of queried terms in its FST.
        assert_eq!(verifications() - before, 2);
        assert_eq!(full.matching_document_count, 128);
        assert_eq!(full.postings_visited, 128);
        assert_eq!(
            full.bytes_read - full.document_bytes_read,
            2 * blocks[0].length
        );
        let corpus = crate::TextCorpusStats::from_documents(
            documents.iter(),
            &SearchAnalyzerLexicon::empty(),
        );
        for doc in &documents {
            assert_eq!(
                full.scores[&doc.id].to_bits(),
                crate::bm25_score(&terms, doc, &corpus, &SearchAnalyzerLexicon::empty()).to_bits()
            );
        }
        let before = verifications();
        let window = reader
            .score(&terms, &LexicalMiniDelta::default(), Some(7), |_| Ok(true))
            .unwrap();
        assert_eq!(verifications() - before, 2);
        assert_eq!(window.matching_document_count, 128);
        let mut expected = full.scores.into_iter().collect::<Vec<_>>();
        expected.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        assert_eq!(window.scores, expected.into_iter().take(7).collect());
    }
}

#[test]
fn shared_dictionary_hits_allocate_nothing_and_eviction_reuses_the_block_vec() {
    let _serial = crate::test_allocation::serial();
    let root = OwnedDirectory::new("dictionary-eviction");
    let (reader, _, _) = dictionary_fixture(&root, 256, 4);
    let first = reader.posting_blocks("wordaax").next().unwrap();
    let second = reader.posting_blocks("wordacx").next().unwrap();
    assert_ne!(first.block_id, second.block_id);
    let mut buffer = PostingReadBuffer::default();
    let before = verifications();
    let (dictionary, bytes_read) = buffer.read(&reader, first, None).unwrap();
    assert_eq!(bytes_read, first.length);
    let pointer = dictionary.as_fst().as_inner().bytes.as_ptr();
    assert!(dictionary.get("wordaax").is_some());
    let (_, allocations) = count_allocations(|| {
        for _ in 0..32 {
            let (dictionary, bytes_read) = buffer.read(&reader, first, None).unwrap();
            assert_eq!(bytes_read, 0);
            assert_eq!(dictionary.as_fst().as_inner().bytes.as_ptr(), pointer);
            assert!(dictionary.get("wordabx").is_some());
            assert!(dictionary.get("unknown").is_none());
        }
    });
    assert_eq!(allocations, 0);
    assert_eq!(verifications() - before, 1);
    let capacity = buffer
        .dictionary
        .as_ref()
        .unwrap()
        .as_fst()
        .as_inner()
        .bytes
        .capacity();
    assert!(second.length as usize <= capacity);
    let (_, allocations) = count_allocations(|| {
        let (dictionary, bytes_read) = buffer.read(&reader, second, None).unwrap();
        assert_eq!(bytes_read, second.length);
        assert_eq!(dictionary.as_fst().as_inner().bytes.as_ptr(), pointer);
        assert!(dictionary.get("wordaax").is_none());
        assert!(dictionary.get("wordacx").is_some());
    });
    assert_eq!(allocations, 0);
    assert_eq!(verifications() - before, 2);
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    assert!(buffer.read(&reader, second, Some(&task)).is_err());
    assert_eq!(verifications() - before, 2);
    assert_eq!(buffer.read(&reader, first, None).unwrap().1, first.length);
    assert_eq!(verifications() - before, 3);
}

#[test]
fn shared_dictionary_failure_invalidates_the_view_and_retry_revalidates() {
    let root = OwnedDirectory::new("dictionary-failure");
    let (reader, _, _) = dictionary_fixture(&root, 128, 4);
    let block = reader.posting_blocks("wordaax").next().unwrap();
    let original = reader.read_block(block).unwrap();
    let mut corrupt_bytes = original.clone();
    let parts = split_posting_block(&original, reader.manifest.generation, block).unwrap();
    // Damage only the FST's internal checksum and recompute the physical-block
    // checksum, so the failure specifically exercises dictionary validation.
    let dictionary_end = original.len() - parts.payload.len();
    corrupt_bytes[dictionary_end - 1] ^= 1;
    let mut corrupt = block.clone();
    corrupt.checksum = checksum(&corrupt_bytes);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(root.0.join(&reader.manifest.artifact_file))
        .unwrap();
    let mut buffer = PostingReadBuffer::default();
    let before = verifications();
    buffer.read(&reader, block, None).unwrap();
    file.seek(SeekFrom::Start(block.offset)).unwrap();
    file.write_all(&corrupt_bytes).unwrap();
    let error = buffer.read(&reader, &corrupt, None).err().unwrap();
    assert!(error.to_string().contains("dictionary checksum"), "{error}");
    assert!(buffer.block.is_none());
    assert!(buffer.dictionary.is_none());
    assert_eq!(verifications() - before, 2);
    file.seek(SeekFrom::Start(block.offset)).unwrap();
    file.write_all(&original).unwrap();
    let (dictionary, bytes_read) = buffer.read(&reader, block, None).unwrap();
    assert_eq!(bytes_read, block.length);
    assert!(dictionary.get("wordaax").is_some());
    assert_eq!(verifications() - before, 3);
}

#[test]
#[ignore = "manual before/after work-count, allocation and warmed timing probe"]
fn dictionary_reuse_query_measurement() {
    let _serial = crate::test_allocation::serial();
    for (count, width) in [(128, 1), (128, 4), (128, 32), (4096, 32)] {
        let root = OwnedDirectory::new("dictionary-measurement");
        let (reader, _, terms) = dictionary_fixture(&root, count, width);
        let delta = LexicalMiniDelta::default();
        let query = || reader.score(&terms, &delta, Some(7), |_| Ok(true)).unwrap();
        let expected = query();
        for _ in 0..32 {
            assert_eq!(query(), expected);
        }
        let batch = if count > 128 { 4 } else { 32 };
        let mut micros = Vec::new();
        for _ in 0..21 {
            let start = std::time::Instant::now();
            for _ in 0..batch {
                std::hint::black_box(query());
            }
            micros.push(start.elapsed().as_secs_f64() * 1e6 / batch as f64);
        }
        micros.sort_by(f64::total_cmp);
        let before = verifications();
        let baseline = crate::test_allocation::live();
        let (result, peak) = crate::test_allocation::measure(query);
        assert_eq!(result, expected);
        eprintln!(
            "dictionary docs={count} terms={width} verifications={} read_bytes={} postings={} allocation_peak={} p50_us={:.3} p95_us={:.3}",
            verifications() - before,
            result.bytes_read,
            result.postings_visited,
            peak - baseline,
            micros[10],
            micros[19]
        );
        drop(result);
        assert_eq!(crate::test_allocation::live(), baseline);
    }
}
