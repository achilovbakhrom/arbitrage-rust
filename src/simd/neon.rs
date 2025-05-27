//! ARM64 NEON SIMD optimizations for Mac M1
//! Complete implementation with correct NEON intrinsics

use std::arch::aarch64::*;

/// SIMD-optimized triangular arbitrage calculation for ARM64
#[target_feature(enable = "neon")]
pub unsafe fn calculate_triangular_arbitrage_neon(
    first_price: f64,
    second_price: f64,
    third_price: f64,
    first_is_base_to_quote: bool,
    second_is_base_to_quote: bool,
    third_is_base_to_quote: bool,
    fee_multiplier: f64,
    start_amount: f64
) -> f64 {
    // Simple scalar calculation optimized for single path
    // NEON is most beneficial for batch operations
    let mut amount = start_amount;

    // First leg calculation
    if first_is_base_to_quote {
        amount *= first_price;
    } else {
        amount /= first_price;
    }
    amount *= fee_multiplier;

    // Second leg calculation
    if second_is_base_to_quote {
        amount *= second_price;
    } else {
        amount /= second_price;
    }
    amount *= fee_multiplier;

    // Third leg calculation
    if third_is_base_to_quote {
        amount *= third_price;
    } else {
        amount /= third_price;
    }
    amount *= fee_multiplier;

    amount
}

/// Batch process multiple triangular paths with NEON
/// This is where NEON provides significant benefits
#[target_feature(enable = "neon")]
pub unsafe fn batch_calculate_arbitrage_neon(
    prices: &[(f64, f64, f64)], // (first, second, third)
    directions: &[(bool, bool, bool)], // (first_dir, second_dir, third_dir)
    fee_multiplier: f64,
    start_amount: f64,
    results: &mut [f64]
) -> usize {
    let len = prices.len().min(directions.len()).min(results.len());

    if len == 0 {
        return 0;
    }

    let simd_len = len & !1; // Process pairs for f64 NEON (2 lanes)
    let fee_vec = vdupq_n_f64(fee_multiplier);
    let start_vec = vdupq_n_f64(start_amount);

    let mut processed = 0;

    // Process 2 calculations at a time with NEON f64x2
    for i in (0..simd_len).step_by(2) {
        let price1 = prices[i];
        let price2 = prices[i + 1];
        let dir1 = directions[i];
        let dir2 = directions[i + 1];

        // Initialize amounts
        let mut amounts = start_vec;

        // First leg - handle directions individually since they may differ
        let mut first_results = [0.0f64; 2];
        vst1q_f64(first_results.as_mut_ptr(), amounts);

        if dir1.0 {
            first_results[0] *= price1.0;
        } else {
            first_results[0] /= price1.0;
        }
        if dir2.0 {
            first_results[1] *= price2.0;
        } else {
            first_results[1] /= price2.0;
        }

        amounts = vld1q_f64(first_results.as_ptr());
        amounts = vmulq_f64(amounts, fee_vec);

        // Second leg
        let mut second_results = [0.0f64; 2];
        vst1q_f64(second_results.as_mut_ptr(), amounts);

        if dir1.1 {
            second_results[0] *= price1.1;
        } else {
            second_results[0] /= price1.1;
        }
        if dir2.1 {
            second_results[1] *= price2.1;
        } else {
            second_results[1] /= price2.1;
        }

        amounts = vld1q_f64(second_results.as_ptr());
        amounts = vmulq_f64(amounts, fee_vec);

        // Third leg
        let mut third_results = [0.0f64; 2];
        vst1q_f64(third_results.as_mut_ptr(), amounts);

        if dir1.2 {
            third_results[0] *= price1.2;
        } else {
            third_results[0] /= price1.2;
        }
        if dir2.2 {
            third_results[1] *= price2.2;
        } else {
            third_results[1] /= price2.2;
        }

        amounts = vld1q_f64(third_results.as_ptr());
        amounts = vmulq_f64(amounts, fee_vec);

        // Store final results
        vst1q_f64(results.as_mut_ptr().add(i), amounts);
        processed += 2;
    }

    // Handle remaining single calculation
    for i in processed..len {
        let price = prices[i];
        let dir = directions[i];
        let mut amount = start_amount;

        // First leg
        if dir.0 {
            amount *= price.0;
        } else {
            amount /= price.0;
        }
        amount *= fee_multiplier;

        // Second leg
        if dir.1 {
            amount *= price.1;
        } else {
            amount /= price.1;
        }
        amount *= fee_multiplier;

        // Third leg
        if dir.2 {
            amount *= price.2;
        } else {
            amount /= price.2;
        }
        amount *= fee_multiplier;

        results[i] = amount;
        processed += 1;
    }

    processed
}

/// Optimized batch calculation for same-direction paths
/// When all paths have the same trading directions, we can use pure NEON vectorization
#[target_feature(enable = "neon")]
pub unsafe fn batch_calculate_same_direction_neon(
    first_prices: &[f64],
    second_prices: &[f64],
    third_prices: &[f64],
    first_is_base_to_quote: bool,
    second_is_base_to_quote: bool,
    third_is_base_to_quote: bool,
    fee_multiplier: f64,
    start_amount: f64,
    results: &mut [f64]
) -> usize {
    let len = first_prices
        .len()
        .min(second_prices.len())
        .min(third_prices.len())
        .min(results.len());

    if len == 0 {
        return 0;
    }

    let simd_len = len & !1; // Process pairs
    let fee_vec = vdupq_n_f64(fee_multiplier);
    let start_vec = vdupq_n_f64(start_amount);

    let mut processed = 0;

    // Process 2 calculations at a time
    for i in (0..simd_len).step_by(2) {
        // Load prices
        let first_vec = vld1q_f64(first_prices.as_ptr().add(i));
        let second_vec = vld1q_f64(second_prices.as_ptr().add(i));
        let third_vec = vld1q_f64(third_prices.as_ptr().add(i));

        let mut amounts = start_vec;

        // First leg - vectorized since direction is the same
        if first_is_base_to_quote {
            amounts = vmulq_f64(amounts, first_vec);
        } else {
            amounts = vdivq_f64(amounts, first_vec);
        }
        amounts = vmulq_f64(amounts, fee_vec);

        // Second leg
        if second_is_base_to_quote {
            amounts = vmulq_f64(amounts, second_vec);
        } else {
            amounts = vdivq_f64(amounts, second_vec);
        }
        amounts = vmulq_f64(amounts, fee_vec);

        // Third leg
        if third_is_base_to_quote {
            amounts = vmulq_f64(amounts, third_vec);
        } else {
            amounts = vdivq_f64(amounts, third_vec);
        }
        amounts = vmulq_f64(amounts, fee_vec);

        // Store results
        vst1q_f64(results.as_mut_ptr().add(i), amounts);
        processed += 2;
    }

    // Handle remaining elements
    for i in processed..len {
        let mut amount = start_amount;

        if first_is_base_to_quote {
            amount *= first_prices[i];
        } else {
            amount /= first_prices[i];
        }
        amount *= fee_multiplier;

        if second_is_base_to_quote {
            amount *= second_prices[i];
        } else {
            amount /= second_prices[i];
        }
        amount *= fee_multiplier;

        if third_is_base_to_quote {
            amount *= third_prices[i];
        } else {
            amount /= third_prices[i];
        }
        amount *= fee_multiplier;

        results[i] = amount;
        processed += 1;
    }

    processed
}

/// Fast parallel threshold comparison with NEON - simplified approach
#[target_feature(enable = "neon")]
pub unsafe fn find_profitable_neon(
    profit_ratios: &[f64],
    min_threshold: f64,
    profitable_indices: &mut Vec<usize>
) {
    profitable_indices.clear();

    if profit_ratios.is_empty() {
        return;
    }

    let threshold_vec = vdupq_n_f64(min_threshold);
    let len = profit_ratios.len();
    let simd_len = len & !1; // Process 2 at a time

    // Process 2 values at a time with NEON
    for i in (0..simd_len).step_by(2) {
        let profit_vec = vld1q_f64(profit_ratios.as_ptr().add(i));
        let comparison = vcgtq_f64(profit_vec, threshold_vec);

        // Extract values and check manually (avoiding complex reinterpret operations)
        let mut comparison_results = [0u64; 2];
        vst1q_u64(comparison_results.as_mut_ptr(), comparison);

        // Check if comparison was true (all bits set = 0xFFFFFFFFFFFFFFFF)
        if comparison_results[0] == 0xffffffffffffffff {
            profitable_indices.push(i);
        }
        if comparison_results[1] == 0xffffffffffffffff {
            profitable_indices.push(i + 1);
        }
    }

    // Handle remaining elements with scalar comparison
    for i in simd_len..len {
        if profit_ratios[i] > min_threshold {
            profitable_indices.push(i);
        }
    }
}

/// NEON-optimized vector operations for orderbook data
#[target_feature(enable = "neon")]
pub unsafe fn calculate_spreads_neon(
    bid_prices: &[f64],
    ask_prices: &[f64],
    spreads: &mut [f64]
) -> usize {
    let len = bid_prices.len().min(ask_prices.len()).min(spreads.len());

    if len == 0 {
        return 0;
    }

    let simd_len = len & !1; // Process 2 at a time
    let mut processed = 0;

    // Process 2 spreads at a time
    for i in (0..simd_len).step_by(2) {
        let bids = vld1q_f64(bid_prices.as_ptr().add(i));
        let asks = vld1q_f64(ask_prices.as_ptr().add(i));
        let spreads_vec = vsubq_f64(asks, bids);
        vst1q_f64(spreads.as_mut_ptr().add(i), spreads_vec);
        processed += 2;
    }

    // Handle remaining elements
    for i in processed..len {
        spreads[i] = ask_prices[i] - bid_prices[i];
    }

    len
}

/// Calculate mid prices using NEON
#[target_feature(enable = "neon")]
pub unsafe fn calculate_mid_prices_neon(
    bid_prices: &[f64],
    ask_prices: &[f64],
    mid_prices: &mut [f64]
) -> usize {
    let len = bid_prices.len().min(ask_prices.len()).min(mid_prices.len());

    if len == 0 {
        return 0;
    }

    let simd_len = len & !1;
    let half_vec = vdupq_n_f64(0.5);
    let mut processed = 0;

    // Process 2 mid prices at a time
    for i in (0..simd_len).step_by(2) {
        let bids = vld1q_f64(bid_prices.as_ptr().add(i));
        let asks = vld1q_f64(ask_prices.as_ptr().add(i));
        let sum = vaddq_f64(bids, asks);
        let mid = vmulq_f64(sum, half_vec);
        vst1q_f64(mid_prices.as_mut_ptr().add(i), mid);
        processed += 2;
    }

    // Handle remaining elements
    for i in processed..len {
        mid_prices[i] = (bid_prices[i] + ask_prices[i]) * 0.5;
    }

    len
}

/// Optimized profit ratio calculations with NEON
#[target_feature(enable = "neon")]
pub unsafe fn calculate_profit_ratios_neon(
    end_amounts: &[f64],
    start_amount: f64,
    profit_ratios: &mut [f64]
) -> usize {
    let len = end_amounts.len().min(profit_ratios.len());

    if len == 0 {
        return 0;
    }

    let simd_len = len & !1;
    let start_vec = vdupq_n_f64(start_amount);
    let mut processed = 0;

    // Process 2 ratios at a time
    for i in (0..simd_len).step_by(2) {
        let end_vec = vld1q_f64(end_amounts.as_ptr().add(i));
        let ratio_vec = vdivq_f64(end_vec, start_vec);
        vst1q_f64(profit_ratios.as_mut_ptr().add(i), ratio_vec);
        processed += 2;
    }

    // Handle remaining elements
    for i in processed..len {
        profit_ratios[i] = end_amounts[i] / start_amount;
    }

    len
}

/// Check if we should use NEON for a given batch size
#[inline]
pub fn should_use_neon_batch(batch_size: usize) -> bool {
    batch_size >= 4 && cfg!(target_arch = "aarch64")
}

/// Get optimal batch size for NEON processing
#[inline]
pub fn get_optimal_batch_size() -> usize {
    // NEON f64x2 processes 2 elements at a time
    // Optimal batch sizes are multiples of 2
    16 // Good balance between overhead and vectorization benefits
}
