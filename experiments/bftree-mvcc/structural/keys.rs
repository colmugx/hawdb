// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;

/// Preserve byte ordering, including proper prefixes and embedded zero bytes.
#[inline]
pub fn compare_head(left: &[u8], right: &[u8]) -> Ordering {
    if let (Ok(a), Ok(b)) = (<[u8; 4]>::try_from(left), <[u8; 4]>::try_from(right)) {
        u32::from_be_bytes(a).cmp(&u32::from_be_bytes(b))
    } else {
        left.cmp(right)
    }
}

/// An inclusive right fence separating two adjacent full keys. Retain at
/// least four bytes when possible, matching the existing inner preview width.
pub fn separator(left: &[u8], right: &[u8]) -> Option<Vec<u8>> {
    if left >= right {
        return None;
    }
    let common = left.iter().zip(right).take_while(|(a, b)| a == b).count();
    let keep = (common + 1).max(4).min(right.len());
    let candidate = right[..keep].to_vec();
    debug_assert!(left < candidate.as_slice() && candidate.as_slice() <= right);
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_heads_preserve_all_byte_values_and_prefix_order() {
        for byte in 0..=255 {
            for len in 0..=5 {
                let a = vec![byte; len];
                for other in [
                    vec![0; 4],
                    vec![255; 4],
                    a.clone(),
                    [a.clone(), vec![0]].concat(),
                ] {
                    assert_eq!(compare_head(&a, &other), a.cmp(&other));
                    assert_eq!(compare_head(&other, &a), other.cmp(&a));
                }
            }
        }
    }

    #[test]
    fn separators_cover_binary_prefixes_and_every_difference_position() {
        for len in 0..=160 {
            let left = vec![0; len];
            let right = [left.clone(), vec![0]].concat();
            let cut = separator(&left, &right).unwrap();
            assert!(left < cut && cut <= right);
            for position in 0..len {
                let mut right = left.clone();
                right[position] = 255;
                let cut = separator(&left, &right).unwrap();
                assert!(left < cut && cut <= right);
                assert_eq!(cut.len(), (position + 1).max(4).min(right.len()));
            }
        }
        assert!(separator(b"same", b"same").is_none());
        assert!(separator(b"z", b"a").is_none());
    }
}
