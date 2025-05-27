#[cfg(target_arch = "aarch64")]
pub mod neon;

#[cfg(target_arch = "x86_64")]
pub mod avx2;

// Platform-agnostic interface
pub use self::platform::*;

#[cfg(target_arch = "aarch64")]
mod platform {
    pub use super::neon::*;
}

#[cfg(target_arch = "x86_64")]
mod platform {
    pub use super::avx2::*;
}

/// Minimum batch size for SIMD to be beneficial
pub const MIN_SIMD_BATCH_SIZE: usize = 4;

/// Check if SIMD is available on current platform
#[inline]
pub fn has_simd_support() -> bool {
    cfg!(any(target_arch = "aarch64", target_arch = "x86_64"))
}
