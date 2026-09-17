//! Allocation-free estimates from the exact native codec used by the node.

/// Native streaming decoder allowance, without dictionaries, for a bounded
/// 8–128 MiB maximum window. This does not include the Rust input buffer.
pub fn decoder_bytes(window_log: u32) -> Option<usize> {
    if !(23..=27).contains(&window_log) {
        return None;
    }
    // SAFETY: these pure size/error queries take only scalar values, retain no
    // pointers, and allocate no context. The checked shift fits 32-bit usize.
    let bytes = unsafe { zstd_sys::ZSTD_estimateDStreamSize(1_usize << window_log) };
    if unsafe { zstd_sys::ZSTD_isError(bytes) } != 0 {
        None
    } else {
        Some(bytes)
    }
}
