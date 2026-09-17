//! Shared fixtures that keep loopback failures isolated from parallel tests.

use std::{
    io::ErrorKind,
    net::{SocketAddr, TcpListener},
    sync::mpsc::{self, Sender},
    thread::{self, JoinHandle},
    time::Duration,
};

/// Owns a failed peer's listener and stops its worker when the fixture is dropped.
pub(crate) struct FailedTcpEndpoint {
    stop: Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for FailedTcpEndpoint {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Reserves a loopback listener and immediately closes every accepted stream.
///
/// This models transport/handshake failure, not a TCP SYN refusal. On macOS a
/// bound, unlistened socket can silently hold connection attempts until their
/// timeout. Dropping that socket instead would let another test acquire its port.
/// The worker uses a standard thread so synchronous HTTPS tests and single-thread
/// Tokio tests can both observe prompt failure without driving another runtime.
pub(crate) fn failed_tcp_endpoint() -> (FailedTcpEndpoint, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let (stop, stopped) = mpsc::channel();
    let worker = thread::spawn(move || {
        loop {
            if stopped.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => drop(stream),
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if stopped.recv_timeout(Duration::from_millis(1)).is_ok() {
                        break;
                    }
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => panic!("failed endpoint accept: {error}"),
            }
        }
    });
    (
        FailedTcpEndpoint {
            stop,
            worker: Some(worker),
        },
        address,
    )
}

#[tokio::test]
async fn failed_endpoint_reserves_its_port_through_repeated_connections() {
    use tokio::{io::AsyncReadExt, net::TcpStream, time::timeout};

    let (reservation, address) = failed_tcp_endpoint();
    for _ in 0..3 {
        let bind_error = tokio::net::TcpListener::bind(address)
            .await
            .expect_err("another test must not acquire the failed peer's address");
        assert_eq!(bind_error.kind(), ErrorKind::AddrInUse);
        let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(address))
            .await
            .expect("the fixture must promptly accept TCP")
            .unwrap();
        let mut byte = [0];
        let result = timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .expect("a failed-peer fixture must promptly terminate the transport");
        match result {
            Ok(count) => assert_eq!(count, 0, "a failed peer must never send protocol bytes"),
            Err(error) => assert_eq!(error.kind(), ErrorKind::ConnectionReset),
        }
    }
    // Drop joins the worker, releasing the listener even on a current-thread runtime.
    drop(reservation);
    let _released = TcpListener::bind(address).expect("dropping the fixture releases its port");
}

/// Header provider that preserves identity but fails every storage lookup.
pub(crate) struct UnavailableHeaders(pub crate::headers::HeaderDag);
impl crate::headers::HeaderView for UnavailableHeaders {
    fn deployments(&self) -> &crate::deployments::DeploymentConfig {
        self.0.deployments()
    }
    fn active_tip(&self) -> crate::headers::HeaderInfo {
        self.0.active_tip()
    }
    fn header(
        &self,
        _: &bitcoin::BlockHash,
    ) -> Result<Option<crate::headers::HeaderInfo>, crate::headers::HeaderReadError> {
        Err(crate::headers::HeaderReadError::Unavailable(
            "injected header read failure".into(),
        ))
    }
    fn active_header(
        &self,
        _: u32,
    ) -> Result<Option<crate::headers::HeaderInfo>, crate::headers::HeaderReadError> {
        Err(crate::headers::HeaderReadError::Unavailable(
            "injected header read failure".into(),
        ))
    }
}
