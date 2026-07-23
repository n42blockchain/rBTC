#![no_main]

use std::sync::Arc;

use axum::{Router, body::Body, http::Request};
use libfuzzer_sys::fuzz_target;
use rbtc::api::{MemoryExplorerIndex, explorer_router};
use tokio::runtime::{Builder, Runtime};
use tower::ServiceExt;

thread_local! {
    static RUNTIME: Runtime = Builder::new_current_thread().build().expect("fuzz runtime");
    static ROUTER: Router = explorer_router(Arc::new(MemoryExplorerIndex::default()));
}

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
    let exercise = |input: &[u8]| {
        let uri = String::from_utf8_lossy(input);
        let Ok(request) = Request::builder().uri(uri.as_ref()).body(Body::empty()) else {
            return;
        };
        ROUTER.with(|router| {
            let service = router.clone();
            RUNTIME.with(|runtime| {
                let _ = runtime.block_on(service.oneshot(request));
            });
        });
    };
    exercise(input);
    if let Some(input) = input.strip_suffix(b"\n") {
        exercise(input);
    }
});
