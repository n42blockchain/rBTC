//! Budgeted native allocations for the pinned, allocation-failure-patched codec.
use std::{
    ffi::{CStr, c_void},
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::NonNull,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

const MAX_ALLOCATIONS: usize = 4096;

/// Supplies owned allocation permits. Implementations must not allocate while
/// acquiring a permit; dropping a permit refunds its allowance.
pub trait AllocationBudget: Send + Sync + 'static {
    /// The permit follows native allocation ownership, possibly across threads.
    type Lease: Send + 'static;
    /// Reserves bytes before allocation, or returns None under pressure.
    fn reserve(&self, bytes: usize) -> Option<Self::Lease>;
}

#[derive(Debug)]
struct AdmissionDenied;
impl std::fmt::Display for AdmissionDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native codec allocation admission failed")
    }
}
impl std::error::Error for AdmissionDenied {}
fn denied() -> io::Error {
    io::Error::other(AdmissionDenied)
}
/// Identifies a native allocation failure without parsing error messages.
pub fn is_admission_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|error| error.is::<AdmissionDenied>())
}

struct State<B: AllocationBudget> {
    budget: B,
    allocations: Mutex<Vec<(usize, B::Lease)>>,
    denied: AtomicBool,
    _reservation: B::Lease,
}
impl<B: AllocationBudget> State<B> {
    fn new(budget: B) -> io::Result<Box<Self>> {
        let bytes = MAX_ALLOCATIONS
            .checked_mul(std::mem::size_of::<(usize, B::Lease)>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(denied)?;
        let reservation = budget.reserve(bytes).ok_or_else(denied)?;
        Ok(Box::new(Self {
            budget,
            allocations: Mutex::new(Vec::with_capacity(MAX_ALLOCATIONS)),
            denied: AtomicBool::new(false),
            _reservation: reservation,
        }))
    }
}

unsafe extern "C" fn allocate<B: AllocationBudget>(
    opaque: *mut c_void,
    size: usize,
) -> *mut c_void {
    // SAFETY: opaque is the boxed State, retained until freeCCtx has joined all
    // workers. Only the synchronized allocation table is mutated by callbacks.
    let state = unsafe { &*(opaque as *const State<B>) };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let size = size.max(1);
        let lease = state.budget.reserve(size)?;
        let mut entries = state
            .allocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() == MAX_ALLOCATIONS {
            return None;
        }
        // SAFETY: malloc accepts any size and its null result is checked. C
        // owns successful pointers until its matching free callback.
        let pointer = NonNull::new(unsafe { libc::malloc(size) })?;
        entries.push((pointer.as_ptr() as usize, lease));
        Some(pointer.as_ptr())
    }));
    match result {
        Ok(Some(pointer)) => pointer,
        _ => {
            state.denied.store(true, Ordering::Release);
            std::ptr::null_mut()
        }
    }
}
unsafe extern "C" fn release<B: AllocationBudget>(opaque: *mut c_void, pointer: *mut c_void) {
    if pointer.is_null() {
        return;
    }
    // SAFETY: same live State as allocate. zstd frees each successful allocation
    // once. Pointer values in the table are never dereferenced by Rust.
    let state = unsafe { &*(opaque as *const State<B>) };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let mut entries = state
            .allocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let permit = entries
            .iter()
            .position(|(address, _)| *address == pointer as usize)
            .map(|index| entries.swap_remove(index).1);
        drop(entries);
        // SAFETY: the pointer was allocated by libc malloc, not Rust's global
        // allocator. Refund happens after freeing the allocation.
        unsafe {
            libc::free(pointer);
        }
        drop(permit);
    }));
}

struct Context<B: AllocationBudget> {
    pointer: NonNull<zstd_sys::ZSTD_CCtx>,
    state: Box<State<B>>,
}
impl<B: AllocationBudget> Drop for Context<B> {
    fn drop(&mut self) {
        // SAFETY: uniquely owned CCtx. Its free joins worker threads before
        // returning. State and its tracking-table permit drop afterward.
        unsafe {
            zstd_sys::ZSTD_freeCCtx(self.pointer.as_ptr());
        }
        debug_assert!(
            self.state
                .allocations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "native codec allocations survived context destruction"
        );
    }
}
struct OutputBuffer<L> {
    bytes: Vec<u8>,
    _reservation: L,
}

/// Streaming encoder whose native allocations use shared, fallible admission.
/// The caller must separately account for OS worker stacks and its input/output
/// writer. No unsafe native context is exposed by this API.
pub struct Encoder<W: Write, B: AllocationBudget> {
    context: Context<B>,
    output: OutputBuffer<B::Lease>,
    writer: Option<W>,
    failed: bool,
}
impl<W: Write, B: AllocationBudget> Encoder<W, B> {
    /// Creates a level-selected encoder with zero or 2–4 native workers.
    pub fn new(writer: W, budget: B, level: i32, workers: u32) -> io::Result<Self> {
        if workers == 1 || workers > 4 {
            return Err(io::Error::other("unsupported codec worker count"));
        }
        let mut state = State::new(budget)?;
        let callbacks = zstd_sys::ZSTD_customMem {
            customAlloc: Some(allocate::<B>),
            customFree: Some(release::<B>),
            opaque: (&mut *state as *mut State<B>).cast(),
        };
        // SAFETY: callbacks match the C ABI and state outlives the owned CCtx.
        let pointer = NonNull::new(unsafe { zstd_sys::ZSTD_createCCtx_advanced(callbacks) })
            .ok_or_else(denied)?;
        let context = Context { pointer, state };
        // SAFETY: allocation-free query from the same linked codec.
        let capacity = unsafe { zstd_sys::ZSTD_CStreamOutSize() };
        let reservation = context.state.budget.reserve(capacity).ok_or_else(denied)?;
        let mut encoder = Self {
            context,
            output: OutputBuffer {
                bytes: vec![0; capacity],
                _reservation: reservation,
            },
            writer: Some(writer),
            failed: false,
        };
        // SAFETY: a live unique context; these parameters accept scalar values.
        let code = unsafe {
            zstd_sys::ZSTD_CCtx_setParameter(
                encoder.context.pointer.as_ptr(),
                zstd_sys::ZSTD_cParameter::ZSTD_c_compressionLevel,
                level,
            )
        };
        encoder.check(code)?;
        let code = unsafe {
            zstd_sys::ZSTD_CCtx_loadDictionary(
                encoder.context.pointer.as_ptr(),
                std::ptr::null(),
                0,
            )
        };
        encoder.check(code)?;
        if workers != 0 {
            let code = unsafe {
                zstd_sys::ZSTD_CCtx_setParameter(
                    encoder.context.pointer.as_ptr(),
                    zstd_sys::ZSTD_cParameter::ZSTD_c_nbWorkers,
                    workers as i32,
                )
            };
            encoder.check(code)?;
        }
        Ok(encoder)
    }
    fn check(&mut self, code: usize) -> io::Result<usize> {
        if self.context.state.denied.load(Ordering::Acquire) {
            self.failed = true;
            return Err(denied());
        }
        // SAFETY: the codec's scalar error predicates and static error string.
        if unsafe { zstd_sys::ZSTD_isError(code) } != 0 {
            self.failed = true;
            let message = unsafe { CStr::from_ptr(zstd_sys::ZSTD_getErrorName(code)) };
            return Err(io::Error::other(message.to_string_lossy().into_owned()));
        }
        Ok(code)
    }
    fn step(
        &mut self,
        bytes: &[u8],
        mode: zstd_sys::ZSTD_EndDirective,
    ) -> io::Result<(usize, usize)> {
        if self.failed {
            return Err(io::Error::other("native codec stream already failed"));
        }
        let mut input = zstd_sys::ZSTD_inBuffer {
            src: bytes.as_ptr().cast(),
            size: bytes.len(),
            pos: 0,
        };
        let mut output = zstd_sys::ZSTD_outBuffer {
            dst: self.output.bytes.as_mut_ptr().cast(),
            size: self.output.bytes.len(),
            pos: 0,
        };
        // SAFETY: both slices remain live and exclusively borrowed for the call;
        // the codec receives their exact bounds and cannot retain these buffers.
        let code = unsafe {
            zstd_sys::ZSTD_compressStream2(
                self.context.pointer.as_ptr(),
                &mut output,
                &mut input,
                mode,
            )
        };
        let remaining = self.check(code)?;
        if let Err(error) = self
            .writer
            .as_mut()
            .unwrap()
            .write_all(&self.output.bytes[..output.pos])
        {
            self.failed = true;
            return Err(error);
        }
        Ok((input.pos, remaining))
    }
    /// Completes the frame and releases native workers/allocations before return.
    pub fn finish(mut self) -> io::Result<W> {
        while self.step(&[], zstd_sys::ZSTD_EndDirective::ZSTD_e_end)?.1 != 0 {}
        Ok(self.writer.take().unwrap())
    }
}
impl<W: Write, B: AllocationBudget> Write for Encoder<W, B> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            let consumed = self
                .step(bytes, zstd_sys::ZSTD_EndDirective::ZSTD_e_continue)?
                .0;
            if consumed != 0 {
                return Ok(consumed);
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        while self.step(&[], zstd_sys::ZSTD_EndDirective::ZSTD_e_flush)?.1 != 0 {}
        self.writer.as_mut().unwrap().flush()
    }
}
