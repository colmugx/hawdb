# Pinned Jieba dictionary

`jieba-0.11.0.dict.txt.zst` contains the unmodified `dict.txt` from
[Jieba 0.11.0](https://github.com/messense/jieba-rs/blob/3a0d75cf7455e2006330a5b4e94156da272cd7b9/jieba/src/data/dict.txt).
The original MIT license is preserved in [LICENSE.jieba-rs](LICENSE.jieba-rs).
HawDB's own source remains Apache-2.0; the Search package declares both licenses
because this resource is distributed inside its archive.

| Property | Value |
| --- | --- |
| Original bytes | 5,071,843 |
| Distinct dictionary entries | 349,045 |
| Total entry frequency | 60,101,964 |
| Original SHA-256 | `139519822fe8ab9e10d9d07e68ea0451045380aedaf54ecc51e2a28c6b42a13f` |
| Compressed bytes | 2,126,841 |
| Compressed SHA-256 | `e7f58aba802185562a9314461a2a15fcb10df750f3f1470cb3c98eac677ce0df` |

The resource uses Zstd 0.14.3's streaming encoder at level `0` with bundled
Zstd 1.5.7. The decompressed bytes, including word order, frequencies and tags,
are the identity contract; do not normalize or edit them. Jieba 0.11.0's public
`with_dict` loader receives a streaming decoder over the embedded resource.
There is no runtime filesystem access, download, process or environment setting.
The decoded source is not retained alongside the dictionary structures.

Jieba's `default-dict` feature is disabled. That feature otherwise selects
include-flate's older Zstd dependency even when HawDB already uses a patched
version. No Cargo patch or advisory exclusion is needed, including for a
downstream consumer of the published Search crate.

The analyzer version remains unchanged: the Jieba release, dictionary and HMM
algorithms have not changed. Tests verify the complete decompressed digest and
48 token/offset cases recorded from the unmodified upstream loader. The
[manual component probe](../../../../docs/benchmarks/jieba_dictionary_loading_2026_10_27/README.md)
records the complete dictionary-word comparison, mixed phrases, initialization
allocations and regeneration command. Its baseline dependencies are isolated
outside the production workspace and package archives.
