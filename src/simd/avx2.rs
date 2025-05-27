#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD-optimized triangular arbitrage calculation for x86_64
#[target_feature(enable = "avx2,fma")]
pub unsafe fn calculate_triangular_arbitrage_avx2(
    first_price: f64,
    second_price: f64,
    third_price: f64,
    first_is_base_to_quote: bool,
    second_is_base_to_quote: bool,
    third_is_base_to_quote: bool,
    fee_multiplier: f64,
    start_amount: f64
) -> f64 {
    // Load values into AVX2 registers
    let mut amount_vec = _mm256_set1_pd(start_amount);
    let fee_vec = _mm256_set1_pd(fee_multiplier);

    // First leg calculation
    let first_vec = _mm256_set1_pd(first_price);
    if first_is_base_to_quote {
        amount_vec = _mm256_mul_pd(amount_vec, first_vec);
    } else {
        amount_vec = _mm256_div_pd(amount_vec, first_vec);
    }
    amount_vec = _mm256_mul_pd(amount_vec, fee_vec);

    // Second leg calculation
    let second_vec = _mm256_set1_pd(second_price);
    if second_is_base_to_quote {
        amount_vec = _mm256_mul_pd(amount_vec, second_vec);
    } else {
        amount_vec = _mm256_div_pd(amount_vec, second_vec);
    }
    amount_vec = _mm256_mul_pd(amount_vec, fee_vec);

    // Third leg calculation
    let third_vec = _mm256_set1_pd(third_price);
    if third_is_base_to_quote {
        amount_vec = _mm256_mul_pd(amount_vec, third_vec);
    } else {
        amount_vec = _mm256_div_pd(amount_vec, third_vec);
    }
    amount_vec = _mm256_mul_pd(amount_vec, fee_vec);

    // Extract result
    let mut result = [0.0f64; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), amount_vec);
    result[0]
}

/// Batch process multiple triangular paths with AVX2
#[target_feature(enable = "avx2,fma")]
pub unsafe fn batch_calculate_arbitrage_avx2(
    prices: &[(f64, f64, f64)],
    directions: &[(bool, bool, bool)],
    fee_multiplier: f64,
    start_amount: f64,
    results: &mut [f64]
) -> usize {
    let len = prices.len().min(directions.len()).min(results.len());
    let simd_len = len & !3; // Process 4 at a time for AVX2

    let fee_vec = _mm256_set1_pd(fee_multiplier);
    let start_vec = _mm256_set1_pd(start_amount);

    let mut processed = 0;

    // Process 4 calculations at a time
    for i in (0..simd_len).step_by(4) {
        // Gather prices into vectors
        let first_prices = _mm256_set_pd(
            prices[i + 3].0,
            prices[i + 2].0,
            prices[i + 1].0,
            prices[i].0
        );
        let second_prices = _mm256_set_pd(
            prices[i + 3].1,
            prices[i + 2].1,
            prices[i + 1].1,
            prices[i].1
        );
        let third_prices = _mm256_set_pd(
            prices[i + 3].2,
            prices[i + 2].2,
            prices[i + 1].2,
            prices[i].2
        );

        let mut amounts = start_vec;

        // First leg - vectorized direction handling
        let first_mul_mask = _mm256_set_pd(
            if directions[i + 3].0 {
                -1.0
            } else {
                0.0
            },
            if directions[i + 2].0 {
                -1.0
            } else {
                0.0
            },
            if directions[i + 1].0 {
                -1.0
            } else {
                0.0
            },
            if directions[i].0 {
                -1.0
            } else {
                0.0
            }
        );

        let first_div_mask = _mm256_set_pd(
            if !directions[i + 3].0 {
                -1.0
            } else {
                0.0
            },
            if !directions[i + 2].0 {
                -1.0
            } else {
                0.0
            },
            if !directions[i + 1].0 {
                -1.0
            } else {
                0.0
            },
            if !directions[i].0 {
                -1.0
            } else {
                0.0
            }
        );

        let mul_result = _mm256_mul_pd(amounts, first_prices);
        let div_result = _mm256_div_pd(amounts, first_prices);

        // Blend results based on direction
        amounts = _mm256_blendv_pd(
            _mm256_blendv_pd(_mm256_setzero_pd(), div_result, first_div_mask),
            mul_result,
            first_mul_mask
        );

        amounts = _mm256_mul_pd(amounts, fee_vec);

        // Second leg (similar pattern)
        let second_mul_mask = _mm256_set_pd(
            if directions[i + 3].1 {
                -1.0
            } else {
                0.0
            },
            if directions[i + 2].1 {
                -1.0
            } else {
                0.0
            },
            if directions[i + 1].1 {
                -1.0
            } else {
                0.0
            },
            if directions[i].1 {
                -1.0
            } else {
                0.0
            }
        );

        let second_div_mask = _mm256_set_pd(
            if !directions[i + 3].1 {
                -1.0
            } else {
                0.0
            },
            if !directions[i + 2].1 {
                -1.0
            } else {
                0.0
            },
            if !directions[i + 1].1 {
                -1.0
            } else {
                0.0
            },
            if !directions[i].1 {
                -1.0
            } else {
                0.0
            }
        );

        let mul_result = _mm256_mul_pd(amounts, second_prices);
        let div_result = _mm256_div_pd(amounts, second_prices);

        amounts = _mm256_blendv_pd(
            _mm256_blendv_pd(_mm256_setzero_pd(), div_result, second_div_mask),
            mul_result,
            second_mul_mask
        );

        amounts = _mm256_mul_pd(amounts, fee_vec);

        // Third leg
        let third_mul_mask = _mm256_set_pd(
            if directions[i + 3].2 {
                -1.0
            } else {
                0.0
            },
            if directions[i + 2].2 {
                -1.0
            } else {
                0.0
            },
            if directions[i + 1].2 {
                -1.0
            } else {
                0.0
            },
            if directions[i].2 {
                -1.0
            } else {
                0.0
            }
        );

        let third_div_mask = _mm256_set_pd(
            if !directions[i + 3].2 {
                -1.0
            } else {
                0.0
            },
            if !directions[i + 2].2 {
                -1.0
            } else {
                0.0
            },
            if !directions[i + 1].2 {
                -1.0
            } else {
                0.0
            },
            if !directions[i].2 {
                -1.0
            } else {
                0.0
            }
        );

        let mul_result = _mm256_mul_pd(amounts, third_prices);
        let div_result = _mm256_div_pd(amounts, third_prices);

        amounts = _mm256_blendv_pd(
            _mm256_blendv_pd(_mm256_setzero_pd(), div_result, third_div_mask),
            mul_result,
            third_mul_mask
        );

        amounts = _mm256_mul_pd(amounts, fee_vec);

        // Store results
        _mm256_storeu_pd(results.as_mut_ptr().add(i), amounts);
        processed += 4;
    }

    processed
}

/// Fast parallel threshold comparison with AVX2
#[target_feature(enable = "avx2")]
pub unsafe fn find_profitable_avx2(
    profit_ratios: &[f64],
    min_threshold: f64,
    profitable_indices: &mut Vec<usize>
) {
    profitable_indices.clear();

    let threshold_vec = _mm256_set1_pd(min_threshold);
    let len = profit_ratios.len();
    let simd_len = len & !3; // Process 4 at a time

    for i in (0..simd_len).step_by(4) {
        let profit_vec = _mm256_loadu_pd(profit_ratios.as_ptr().add(i));
        let mask = _mm256_cmp_pd(profit_vec, threshold_vec, _CMP_GT_OQ);
        let mask_int = _mm256_movemask_pd(mask);

        // Check each bit in the mask
        if (mask_int & 1) != 0 {
            profitable_indices.push(i);
        }
        if (mask_int & 2) != 0 {
            profitable_indices.push(i + 1);
        }
        if (mask_int & 4) != 0 {
            profitable_indices.push(i + 2);
        }
        if (mask_int & 8) != 0 {
            profitable_indices.push(i + 3);
        }
    }

    // Handle remaining elements
    for i in simd_len..len {
        if profit_ratios[i] > min_threshold {
            profitable_indices.push(i);
        }
    }
}

// Platform-agnostic aliases for x86_64
pub use calculate_triangular_arbitrage_avx2 as calculate_triangular_arbitrage_neon;
pub use batch_calculate_arbitrage_avx2 as batch_calculate_arbitrage_neon;
pub use find_profitable_avx2 as find_profitable_neon;
