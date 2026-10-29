// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Unregistered component probe; keep its baseline dependencies outside HawDB.

use hawdb_integrity::IntegrityHasher;
use jieba_rs::Jieba;
use std::path::Path;
use std::time::Instant;

#[path = "../../../crates/search/tests/support/live_allocation.rs"]
mod allocation;

#[path = "../../../crates/search/src/cjk_dictionary.rs"]
mod dictionary;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let source = Path::new(&args[2]);
    match args[1].as_str() {
        "compress" => {
            let raw = std::fs::read(source).unwrap();
            let encoded = zstd::stream::encode_all(raw.as_slice(), 0).unwrap();
            std::fs::create_dir_all(Path::new(&args[3]).parent().unwrap()).unwrap();
            std::fs::write(&args[3], &encoded).unwrap();
            println!(
                "{}",
                serde_json::json!({"source_bytes":raw.len(),"source_sha256":hawdb_integrity::sha256(&raw).to_string(),"encoded_bytes":encoded.len(),"encoded_sha256":hawdb_integrity::sha256(&encoded).to_string()})
            );
        }
        "default" | "streamed" => {
            let now = Instant::now();
            let (jieba, peak) = allocation::measure(|| {
                if args[1] == "default" {
                    Jieba::new()
                } else {
                    dictionary::load()
                }
            });
            let elapsed = now.elapsed();
            let retained = allocation::live();
            let state = format!("{jieba:?}");
            drop(jieba);
            println!(
                "{}",
                serde_json::json!({"mode":args[1],"elapsed_micros":elapsed.as_micros(),"rust_retained_bytes":retained,"rust_peak_bytes":peak,"rust_after_drop_bytes":allocation::live(),"state":state})
            );
        }
        "parity" => {
            let encoded = std::fs::read(source).unwrap();
            let raw = zstd::stream::decode_all(encoded.as_slice()).unwrap();
            let text = std::str::from_utf8(&raw).unwrap();
            let baseline = Jieba::new();
            let candidate = dictionary::load();
            assert_eq!(format!("{baseline:?}"), format!("{candidate:?}"));
            let mut digest = IntegrityHasher::new();
            let mut count = 0;
            for (index, line) in text.lines().enumerate() {
                let word = line.split_ascii_whitespace().next().unwrap();
                for hmm in [false, true] {
                    let expected = baseline.cut_for_search(word, hmm);
                    let actual = candidate.cut_for_search(word, hmm);
                    assert_eq!(expected, actual, "word index {index}, hmm={hmm}");
                    digest.update(word.as_bytes());
                    digest.update(&[u8::from(hmm)]);
                    for token in &expected {
                        digest.update(&(token.word.len() as u64).to_le_bytes());
                        digest.update(token.word.as_bytes());
                        digest.update(&(token.start as u64).to_le_bytes());
                        digest.update(&(token.end as u64).to_le_bytes());
                    }
                    count += 1;
                }
            }
            let words = text
                .lines()
                .map(|line| line.split_ascii_whitespace().next().unwrap())
                .collect::<Vec<_>>();
            let mut phrases = 0;
            for pair in words.windows(2).step_by(97) {
                let text = format!("{}{} HawDB0.17 \u{20000}\u{20bb7}", pair[0], pair[1]);
                for hmm in [false, true] {
                    assert_eq!(
                        baseline.cut_for_search(&text, hmm),
                        candidate.cut_for_search(&text, hmm),
                        "phrase index {phrases}, hmm={hmm}"
                    );
                    phrases += 1;
                }
            }
            println!(
                "{}",
                serde_json::json!({"word_cases":count,"phrase_cases":phrases,"token_fingerprint":digest.finish().sha256.to_string(),"state":format!("{candidate:?}")})
            );
        }
        "golden" => {
            let encoded = std::fs::read(source).unwrap();
            let raw = zstd::stream::decode_all(encoded.as_slice()).unwrap();
            let dictionary = std::str::from_utf8(&raw).unwrap();
            let baseline = Jieba::new();
            let mut inputs = vec![
                "\u{5317}\u{4eac}\u{5927}\u{5b66}\u{7684}\u{4e2d}\u{6587}\u{5206}\u{8bcd}"
                    .to_owned(),
                "\u{5357}\u{4eac}\u{5e02}\u{957f}\u{6c5f}\u{5927}\u{6865}".to_owned(),
                "\u{7814}\u{7a76}\u{751f}\u{547d}\u{8d77}\u{6e90}".to_owned(),
                "\u{20000}\u{20bb7} HawDB0.17".to_owned(),
            ];
            inputs.extend(dictionary.lines().step_by(17453).map(|line| {
                format!(
                    "{} HawDB\u{6570}\u{636e}\u{5e93}",
                    line.split_ascii_whitespace().next().unwrap()
                )
            }));
            let mut output = Vec::new();
            for text in inputs {
                for hmm in [false, true] {
                    let tokens = baseline.cut_for_search(&text,hmm).into_iter().map(|token|serde_json::json!({"word":token.word,"start":token.start,"end":token.end})).collect::<Vec<_>>();
                    output.push(serde_json::json!({"text":text,"hmm":hmm,"tokens":tokens}));
                }
            }
            std::fs::write(&args[3], serde_json::to_vec_pretty(&output).unwrap()).unwrap();
            println!("golden_cases={}", output.len());
        }
        _ => panic!("unknown proof mode"),
    }
}
