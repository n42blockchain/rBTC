//! Shared fixtures that keep loopback failures isolated from parallel tests.

use std::net::SocketAddr;

use tokio::net::TcpSocket;

/// Reserves a loopback port without listening, so connections are refused.
/// Keep the returned socket alive until every attempted connection has finished.
/// Binding then dropping a listener leaves its port available to another test.
pub(crate) fn refused_tcp_endpoint() -> (TcpSocket, SocketAddr) {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = socket.local_addr().unwrap();
    (socket, address)
}

#[tokio::test]
async fn refused_endpoint_reserves_its_port_through_repeated_connections() {
    use std::{io::ErrorKind, time::Duration};

    use tokio::{net::TcpListener, net::TcpStream, time::timeout};

    let (reservation, address) = refused_tcp_endpoint();
    for _ in 0..3 {
        let bind_error = TcpListener::bind(address)
            .await
            .expect_err("another test must not acquire the failed peer's address");
        assert_eq!(bind_error.kind(), ErrorKind::AddrInUse);
        let connect_error = timeout(Duration::from_secs(5), TcpStream::connect(address))
            .await
            .expect("a reserved unlistened port must fail promptly")
            .expect_err("a failed-peer fixture must never accept a connection");
        assert_eq!(connect_error.kind(), ErrorKind::ConnectionRefused);
    }
    drop(reservation);
}
