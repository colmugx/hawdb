// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The exact pinned Jieba dictionary, loaded through HawDB's patched Zstd.

use jieba_rs::Jieba;
use std::io::BufReader;

const EMBEDDED_DICTIONARY: &[u8] = include_bytes!("data/jieba-0.11.0.dict.txt.zst");

pub(crate) fn load() -> Jieba {
    // Stream directly into Jieba; retaining the decoded source would keep a
    // second copy alongside its immutable dictionary structures.
    let decoder = zstd::stream::read::Decoder::new(EMBEDDED_DICTIONARY)
        .expect("bundled Jieba dictionary must be a valid Zstd stream");
    Jieba::with_dict(&mut BufReader::new(decoder))
        .expect("bundled Jieba dictionary must contain valid entries")
}

#[cfg(test)]
#[path = "cjk_dictionary/tests.rs"]
mod tests;
