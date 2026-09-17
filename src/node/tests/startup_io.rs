use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_startup_does_not_strand_the_bound_zmq_acceptor() {
    use std::io::Read as _;
    let (address, endpoint) = std::sync::mpsc::channel();
    let (finished, completion) = std::sync::mpsc::channel();
    // Independent of Tokio: this client still observes a bound port even if
    // the runtime strands the acceptor behind synchronous initialization.
    let subscriber = std::thread::spawn(move || {
        let endpoint = endpoint.recv_timeout(Duration::from_secs(5)).unwrap();
        let result = (|| -> std::io::Result<[u8; 64]> {
            let mut stream = std::net::TcpStream::connect(endpoint)?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut greeting = [0; 64];
            stream.read_exact(&mut greeting)?;
            Ok(greeting)
        })();
        // Always release startup, even when greeting times out, so a failed
        // regression cannot leave a blocked runtime worker behind.
        finished.send(()).unwrap();
        result
    });
    let node = tokio::spawn(async move {
        let publisher = ZmqPublisher::bind(
            "127.0.0.1:0".parse().unwrap(),
            ZmqPublisherConfig::default(),
        )
        .await
        .unwrap();
        address.send(publisher.local_addr()).unwrap();
        // Do not yield between spawning the acceptor and synchronous work.
        // This models the peer database open immediately after ZMQ binding.
        startup_io(|| completion.recv_timeout(Duration::from_secs(5))).unwrap();
        drop(publisher);
    });
    timeout(Duration::from_secs(5), node)
        .await
        .unwrap()
        .unwrap();
    let greeting = subscriber
        .join()
        .unwrap()
        .expect("bound ZMQ must greet during startup I/O");
    assert_eq!(greeting[0], 0xff);
    assert_eq!(greeting[10], 3);
    assert_eq!(&greeting[12..16], b"NULL");
}
