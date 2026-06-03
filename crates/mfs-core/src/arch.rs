//! Architecture-specific hot-path hints.
//!
//! All helpers fall back to a no-op on platforms without an intrinsic so that
//! the call sites stay portable.

/// Runtime CPU features that future SIMD dispatch tables can query.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CpuFeatures {
    pub avx2: bool,
    pub avx512: bool,
    pub sse42: bool,
}

impl CpuFeatures {
    /// Feature set for the scalar fallback implementation.
    pub const FALLBACK: Self = Self {
        avx2: false,
        avx512: false,
        sse42: false,
    };

    #[inline]
    pub fn detect() -> Self {
        Self {
            avx2: avx2_supported(),
            avx512: avx512_supported(),
            sse42: sse42_supported(),
        }
    }

    #[inline]
    pub fn preferred_dispatch(self) -> CpuDispatchPath {
        if self.avx512 {
            CpuDispatchPath::Avx512
        } else if self.avx2 {
            CpuDispatchPath::Avx2
        } else if self.sse42 {
            CpuDispatchPath::Sse42
        } else {
            CpuDispatchPath::Fallback
        }
    }
}

/// CPU implementation path for future hot loops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CpuDispatchPath {
    /// Always available scalar implementation.
    Fallback,
    Sse42,
    Avx2,
    Avx512,
}

impl CpuDispatchPath {
    #[inline]
    pub fn is_available(self, features: CpuFeatures) -> bool {
        match self {
            CpuDispatchPath::Fallback => true,
            CpuDispatchPath::Sse42 => features.sse42,
            CpuDispatchPath::Avx2 => features.avx2,
            CpuDispatchPath::Avx512 => features.avx512,
        }
    }
}

pub const CPU_FALLBACK_PATH: CpuDispatchPath = CpuDispatchPath::Fallback;

#[inline]
pub fn avx2_supported() -> bool {
    avx2_supported_impl()
}

#[inline]
pub fn avx512_supported() -> bool {
    avx512_supported_impl()
}

#[inline]
pub fn sse42_supported() -> bool {
    sse42_supported_impl()
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn avx2_supported_impl() -> bool {
    let detected: bool = std::is_x86_feature_detected!("avx2");
    detected
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn avx2_supported_impl() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn avx512_supported_impl() -> bool {
    let detected: bool = std::is_x86_feature_detected!("avx512f");
    detected
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn avx512_supported_impl() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn sse42_supported_impl() -> bool {
    let detected: bool = std::is_x86_feature_detected!("sse4.2");
    detected
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn sse42_supported_impl() -> bool {
    false
}

#[inline]
pub fn cpu_relax() {
    std::hint::spin_loop();
}

#[inline]
pub fn prefetch_read<T>(ptr: *const T) {
    if ptr.is_null() {
        return;
    }
    prefetch_read_impl(ptr.cast::<u8>());
}

#[inline]
pub fn prefetch_write<T>(ptr: *const T) {
    if ptr.is_null() {
        return;
    }
    prefetch_write_impl(ptr.cast::<u8>());
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn prefetch_read_impl(ptr: *const u8) {
    // SAFETY: `_mm_prefetch` is a cache hint. It does not dereference `ptr`, and
    // callers filter out null pointers before reaching this private helper.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), std::arch::x86_64::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn prefetch_write_impl(ptr: *const u8) {
    // SSE has no separate write-prefetch on most uarchs; T0 brings the line
    // into L1 in the M-equivalent state on Skylake, which is what we want
    // before a store. PREFETCHW (3DNow!) exists but is gated on a CPUID bit.
    // SAFETY: `_mm_prefetch` is a cache hint. It does not dereference `ptr`, and
    // callers filter out null pointers before reaching this private helper.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), std::arch::x86_64::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn prefetch_read_impl(ptr: *const u8) {
    // SAFETY: `prfm` is a cache hint. It has no architectural effect beyond a
    // possible prefetch, and callers filter out null pointers before this helper.
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{addr}]",
            addr = in(reg) ptr,
            options(nostack, readonly)
        );
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn prefetch_write_impl(ptr: *const u8) {
    // SAFETY: `prfm` is a cache hint. It has no architectural effect beyond a
    // possible prefetch, and callers filter out null pointers before this helper.
    unsafe {
        core::arch::asm!(
            "prfm pstl1keep, [{addr}]",
            addr = in(reg) ptr,
            options(nostack, readonly)
        );
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn prefetch_read_impl(_ptr: *const u8) {}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn prefetch_write_impl(_ptr: *const u8) {}
