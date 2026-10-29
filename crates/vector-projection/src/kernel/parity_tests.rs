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
use crate::model::encoded_vector_bytes;

#[test]
fn packed_codes_match_scalar_bits_for_tails_and_unaligned_inputs() {
    verify_packed_code_parity();
}

pub(super) fn verify_packed_code_parity() {
    let selected = score_function(
        select_kernel(native_simd_preference().unwrap_or(KernelPreference::Scalar)).unwrap(),
    );
    let mut state = 0x83a9_175c_efd2_648bu64;
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for dimension in (0..=80).chain([384, 385, 768, 769, 1536, 1537]) {
            for offset in 0..8 {
                let mut codes = vec![0u8; offset + encoded_vector_bytes(dimension, width)];
                for byte in &mut codes {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    *byte = state as u8;
                }
                let codes = &codes[offset..];
                let mut query = vec![0.0; dimension + offset];
                for (index, value) in query.iter_mut().enumerate() {
                    let bits = match index % 8 {
                        0 => 0,
                        1 => 0x8000_0000,
                        2 => 1,
                        3 => 0x8000_0001,
                        _ => {
                            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                            // Finite values spanning cancellation and rounding boundaries.
                            (state as u32 & 0x807f_ffff) | (((state >> 32) as u32 % 180 + 20) << 23)
                        }
                    };
                    *value = f32::from_bits(bits);
                }
                let query = &mut query[offset..];
                for pattern in 0..3 {
                    if pattern > 0 {
                        query.fill(if pattern == 1 { -0.0 } else { 1.0 });
                    }
                    assert_eq!(
                        selected(codes, query, width).to_bits(),
                        score_codes(codes, query, width).to_bits(),
                        "width={width:?}, dimension={dimension}, offset={offset}, pattern={pattern}"
                    );
                }
            }
        }
    }
}

#[test]
fn every_code_and_lane_matches_scalar() {
    let score = score_function(
        select_kernel(native_simd_preference().unwrap_or(KernelPreference::Scalar)).unwrap(),
    );
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let mut codes = vec![0; encoded_vector_bytes(17, width)];
        for lane in 0..17 {
            let mut query = [0.0; 17];
            query[lane] = 1.0;
            for value in 0..(1u8 << width.bits()) {
                codes.fill(0);
                codes[lane / 8] |= (value >> width.refinement_bits()) << (lane % 8);
                for bit in 0..width.refinement_bits() {
                    let position = lane * width.refinement_bits() + bit;
                    codes[3 + position / 8] |= ((value >> bit) & 1) << (position % 8);
                }
                assert_eq!(score(&codes, &query, width), f32::from(value));
            }
        }
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[test]
#[should_panic]
fn avx2_rejects_short_code_slice_before_loading() {
    // The safe function pointer must not permit out-of-bounds SIMD loads.
    super::avx2::score_codes(&[0], &[1.0; 8], RaBitQBitWidth::Four);
}

#[cfg(target_arch = "aarch64")]
#[test]
#[should_panic]
fn neon_rejects_short_code_slice_before_loading() {
    super::neon::score_codes(&[0], &[1.0; 8], RaBitQBitWidth::Four);
}
