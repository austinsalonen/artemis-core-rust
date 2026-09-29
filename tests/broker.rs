//! Integration tests against a real Apache ActiveMQ Artemis broker running in Docker.
//!
//! The broker is started from the official `apache/activemq-artemis` image with a queue
//! pre-configured for the CORE protocol (`--queues rust.core.preconfigured`, anycast) on the
//! standard CORE acceptor (port 61616). The tests exercise the whole client: handshake,
//! session creation, queue management, send/receive, large messages, local transactions and
//! XA transactions.
//!
//! * If `ARTEMIS_ADDR=host:port` is set, that broker is used instead of Docker
//!   (`ARTEMIS_USER` / `ARTEMIS_PASSWORD` default to `artemis` / `artemis`).
//! * If Docker is not available the tests are skipped with a message.
//! * `ARTEMIS_IMAGE` overrides the image (default `apache/activemq-artemis:latest-alpine`).

use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use artemis_core::client::ConsumerOptions;
use artemis_core::xid::flags;
use artemis_core::{
    Connection, ConnectionOptions, Error, ExceptionType, Message, QueueConfiguration, RoutingType, SessionOptions,
    XaCode, Xid,
};

const PRECONFIGURED_QUEUE: &str = "rust.core.preconfigured";
const USER: &str = "artemis";
const PASSWORD: &str = "artemis";

// ---------------------------------------------------------------------------------------------
// Broker lifecycle
// ---------------------------------------------------------------------------------------------

struct Broker {
    addr: String,
    container: Option<String>,
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(name) = &self.container {
            let _ = Command::new("docker").args(["rm", "-f", name]).output();
        }
    }
}

static BROKER: OnceLock<Mutex<Option<std::sync::Arc<Broker>>>> = OnceLock::new();

/// Returns the broker address, starting a container on first use. `None` means "skip".
fn broker() -> Option<std::sync::Arc<Broker>> {
    let slot = BROKER.get_or_init(|| Mutex::new(None));
    let mut guard = slot.lock().unwrap();
    if let Some(b) = guard.as_ref() {
        return Some(b.clone());
    }
    let b = std::sync::Arc::new(start_broker()?);
    *guard = Some(b.clone());
    Some(b)
}

fn start_broker() -> Option<Broker> {
    if let Ok(addr) = std::env::var("ARTEMIS_ADDR") {
        return Some(Broker { addr, container: None });
    }
    let docker_ok = Command::new("docker").args(["info"]).output().map(|o| o.status.success()).unwrap_or(false);
    if !docker_ok {
        eprintln!("SKIPPING broker integration tests: docker is not available (set ARTEMIS_ADDR to use a broker)");
        return None;
    }
    let image =
        std::env::var("ARTEMIS_IMAGE").unwrap_or_else(|_| "apache/activemq-artemis:latest-alpine".to_string());
    let name = format!("artemis-core-rust-test-{}", std::process::id());
    let _ = Command::new("docker").args(["rm", "-f", &name]).output();
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &name,
            "-p",
            "127.0.0.1::61616",
            "-e",
            &format!("ARTEMIS_USER={USER}"),
            "-e",
            &format!("ARTEMIS_PASSWORD={PASSWORD}"),
            // Pre-configure an anycast queue for CORE clients (artemis create --queues).
            "-e",
            &format!("EXTRA_ARGS=--http-host 0.0.0.0 --relax-jolokia --queues {PRECONFIGURED_QUEUE}"),
            &image,
        ])
        .output()
        .expect("failed to run docker");
    assert!(out.status.success(), "docker run failed: {}", String::from_utf8_lossy(&out.stderr));
    let mut broker = Broker { addr: String::new(), container: Some(name.clone()) };

    // Resolve the published port.
    let mut addr = None;
    for _ in 0..50 {
        let out = Command::new("docker").args(["port", &name, "61616"]).output().expect("docker port");
        let s = String::from_utf8_lossy(&out.stdout);
        if let Some(line) = s.lines().find(|l| l.contains("127.0.0.1")) {
            addr = Some(line.trim().to_string());
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    broker.addr = addr.expect("could not determine the published CORE port");
    eprintln!("Artemis container {name} listening on {}", broker.addr);
    Some(broker)
}

/// Connects, retrying until the broker inside the container accepts CORE sessions.
async fn connect() -> Option<Connection> {
    let broker = broker()?;
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut last_err;
    loop {
        match Connection::connect(&broker.addr, ConnectionOptions::new().credentials(USER, PASSWORD)).await {
            Ok(conn) => match conn.create_session(SessionOptions::default()).await {
                Ok(session) => {
                    session.close().await.ok();
                    return Some(conn);
                }
                Err(e) => {
                    last_err = format!("create_session: {e}");
                    conn.close().await;
                }
            },
            Err(e) => last_err = format!("connect: {e}"),
        }
        if Instant::now() > deadline {
            panic!("broker did not become ready: {last_err}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn unique(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    format!("{prefix}.{}.{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst))
}

macro_rules! require_broker {
    () => {
        match connect().await {
            Some(c) => c,
            None => return,
        }
    };
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn handshake_and_version_negotiation() {
    let conn = require_broker!();
    assert!(conn.version() >= 129, "negotiated version {}", conn.version());
    let session = conn.create_session(SessionOptions::default()).await.unwrap();
    assert_eq!(session.server_version(), conn.version());
    conn.ping().await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn send_and_receive_on_preconfigured_core_queue() {
    let conn = require_broker!();
    let session = conn.create_session(SessionOptions::default()).await.unwrap();

    // The queue was created by the broker configuration, not by the client.
    let info = session.queue_query(PRECONFIGURED_QUEUE).await.unwrap();
    assert!(info.exists, "pre-configured queue must exist");
    assert_eq!(info.routing_type, Some(RoutingType::Anycast));
    let addr = session.address_query(PRECONFIGURED_QUEUE).await.unwrap();
    assert!(addr.exists);
    assert!(addr.queue_names.iter().any(|q| q == PRECONFIGURED_QUEUE));

    let producer = session.create_producer(Some(PRECONFIGURED_QUEUE)).await.unwrap();
    let marker = unique("marker");
    producer
        .send_blocking(Message::text("hello from rust").with_property("marker", marker.as_str()).with_routing_type(RoutingType::Anycast))
        .await
        .unwrap();

    session.start().await.unwrap();
    let consumer = session
        .create_consumer(PRECONFIGURED_QUEUE, ConsumerOptions::new().filter(&format!("marker = '{marker}'")))
        .await
        .unwrap();
    let msg = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("a message");
    assert_eq!(msg.text_body().unwrap().as_deref(), Some("hello from rust"));
    assert_eq!(msg.properties.get_string("marker").as_deref(), Some(marker.as_str()));
    assert_eq!(msg.address.as_ref().map(|a| a.to_string()).as_deref(), Some(PRECONFIGURED_QUEUE));
    assert_eq!(msg.delivery_count, 1);
    assert!(msg.message_id > 0);
    consumer.ack(&msg).await.unwrap();
    assert!(consumer.receive_timeout(Duration::from_millis(500)).await.unwrap().is_none());

    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn create_queue_send_receive_many_and_delete() {
    let conn = require_broker!();
    let session = conn.create_session(SessionOptions::default()).await.unwrap();
    let queue = unique("rust.core.q");

    session.create_address(&queue, &[RoutingType::Anycast], false).await.unwrap();
    session.create_queue(QueueConfiguration::new(queue.as_str()).routing_type(RoutingType::Anycast)).await.unwrap();
    let info = session.queue_query(&queue).await.unwrap();
    assert!(info.exists && info.durable);
    assert_eq!(info.message_count, 0);

    let producer = session.create_producer(Some(&queue)).await.unwrap();
    for i in 0..50i32 {
        producer.send(Message::text(&format!("m{i}")).with_property("i", i)).await.unwrap();
    }
    // Blocking send guarantees everything before it was processed by the broker.
    producer.send_blocking(Message::text("last")).await.unwrap();
    let info = session.queue_query(&queue).await.unwrap();
    assert_eq!(info.message_count, 51);

    session.start().await.unwrap();
    let consumer = session.create_consumer(&queue, ConsumerOptions::new()).await.unwrap();
    for i in 0..50 {
        let msg = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("message");
        assert_eq!(msg.text_body().unwrap().as_deref(), Some(format!("m{i}").as_str()));
        assert_eq!(msg.properties.get_i32("i"), Some(i));
        consumer.ack(&msg).await.unwrap();
    }
    let msg = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("last");
    assert_eq!(msg.text_body().unwrap().as_deref(), Some("last"));
    consumer.ack(&msg).await.unwrap();
    consumer.close().await.unwrap();

    // Acks were auto-committed: the queue is empty now.
    let info = session.queue_query(&queue).await.unwrap();
    assert_eq!(info.message_count, 0);

    session.delete_queue(&queue).await.unwrap();
    assert!(!session.queue_query(&queue).await.unwrap().exists);
    // Consuming from a deleted queue is a broker error.
    let err = session.create_consumer(&queue, ConsumerOptions::new()).await.err().expect("error");
    assert_eq!(err.exception_type(), Some(ExceptionType::QUEUE_DOES_NOT_EXIST), "{err}");

    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn bytes_message_and_large_message() {
    let conn = require_broker!();
    // Small threshold so a modest body is streamed as a large message.
    let session = conn.create_session(SessionOptions::new().min_large_message_size(10 * 1024)).await.unwrap();
    let queue = unique("rust.core.large");
    session.create_queue(QueueConfiguration::new(queue.as_str()).routing_type(RoutingType::Anycast)).await.unwrap();
    let producer = session.create_producer(Some(&queue)).await.unwrap();

    let small: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
    producer.send_blocking(Message::bytes(small.clone())).await.unwrap();
    let big: Vec<u8> = (0..300_000u32).map(|i| (i.wrapping_mul(31) % 253) as u8).collect();
    producer.send_blocking(Message::bytes(big.clone()).with_property("kind", "large")).await.unwrap();

    session.start().await.unwrap();
    let consumer = session.create_consumer(&queue, ConsumerOptions::new()).await.unwrap();
    let m1 = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("small");
    assert_eq!(m1.body(), &small[..]);
    assert!(!m1.is_large());
    let m2 = consumer.receive_timeout(Duration::from_secs(30)).await.unwrap().expect("large");
    assert!(m2.is_large(), "expected a large message");
    assert_eq!(m2.body().len(), big.len());
    assert_eq!(m2.body(), &big[..]);
    assert_eq!(m2.properties.get_string("kind").as_deref(), Some("large"));
    consumer.ack(&m2).await.unwrap();
    consumer.close().await.unwrap();
    session.delete_queue(&queue).await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn local_transactions_commit_and_rollback() {
    let conn = require_broker!();
    let session = conn.create_session(SessionOptions::transacted()).await.unwrap();
    let queue = unique("rust.core.tx");
    session.create_queue(QueueConfiguration::new(queue.as_str()).routing_type(RoutingType::Anycast)).await.unwrap();
    let producer = session.create_producer(Some(&queue)).await.unwrap();

    // Sends inside a transaction are invisible until commit and vanish on rollback.
    producer.send_blocking(Message::text("rolled back")).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);
    session.rollback().await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    for i in 0..3 {
        producer.send(Message::text(&format!("tx{i}"))).await.unwrap();
    }
    producer.send_blocking(Message::text("tx3")).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);
    session.commit().await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 4);

    // Acks inside a transaction: rollback redelivers with an incremented delivery count.
    session.start().await.unwrap();
    let consumer = session.create_consumer(&queue, ConsumerOptions::new()).await.unwrap();
    let first = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("tx0");
    assert_eq!(first.text_body().unwrap().as_deref(), Some("tx0"));
    assert_eq!(first.delivery_count, 1);
    consumer.ack(&first).await.unwrap();
    session.rollback().await.unwrap();
    consumer.drain_buffer().await;
    let again = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("tx0 redelivered");
    assert_eq!(again.text_body().unwrap().as_deref(), Some("tx0"));
    assert_eq!(again.delivery_count, 2);

    // Ack everything and commit.
    let mut last = again;
    for _ in 0..3 {
        last = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("more");
    }
    assert_eq!(last.text_body().unwrap().as_deref(), Some("tx3"));
    consumer.ack(&last).await.unwrap();
    session.commit().await.unwrap();
    assert!(consumer.receive_timeout(Duration::from_millis(500)).await.unwrap().is_none());
    consumer.close().await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    session.delete_queue(&queue).await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn xa_two_phase_commit_with_recovery() {
    let conn = require_broker!();
    let session = conn.create_session(SessionOptions::xa()).await.unwrap();
    assert!(session.is_xa());
    let queue = unique("rust.core.xa");
    session.create_queue(QueueConfiguration::new(queue.as_str()).routing_type(RoutingType::Anycast)).await.unwrap();
    let producer = session.create_producer(Some(&queue)).await.unwrap();

    // Sending outside an XA transaction on an XA session is rejected by the broker.
    let xid = Xid::random();
    session.xa_start(&xid, flags::TMNOFLAGS).await.unwrap();
    producer.send_blocking(Message::text("xa message")).await.unwrap();
    session.xa_end(&xid, flags::TMSUCCESS).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    let vote = session.xa_prepare(&xid).await.unwrap();
    assert_eq!(vote, XaCode::XA_OK.0);
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    // The prepared branch is reported by recovery from another XA session.
    let recovery = conn.create_session(SessionOptions::xa()).await.unwrap();
    let in_doubt = recovery.xa_recover().await.unwrap();
    assert!(in_doubt.contains(&xid), "prepared xid must be in doubt: {in_doubt:?}");
    recovery.close().await.unwrap();

    session.xa_commit(&xid, false).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 1);
    assert!(!session.xa_recover().await.unwrap().contains(&xid));

    // Consume inside a second XA transaction with one-phase commit.
    session.start().await.unwrap();
    let consumer = session.create_consumer(&queue, ConsumerOptions::new()).await.unwrap();
    let xid2 = Xid::random();
    session.xa_start(&xid2, flags::TMNOFLAGS).await.unwrap();
    let msg = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("xa message");
    assert_eq!(msg.text_body().unwrap().as_deref(), Some("xa message"));
    consumer.ack(&msg).await.unwrap();
    session.xa_end(&xid2, flags::TMSUCCESS).await.unwrap();
    session.xa_commit(&xid2, true).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    // Committing an unknown xid yields an XA error.
    match session.xa_commit(&Xid::random(), true).await {
        Err(Error::Xa { code, .. }) => assert_eq!(code, XaCode::XAER_NOTA),
        other => panic!("expected XAER_NOTA, got {other:?}"),
    }

    // Timeouts: the broker accepts a per-session timeout but reports its resource manager's
    // (global) timeout on get, exactly like the Java client observes.
    assert!(session.xa_set_transaction_timeout(120).await.unwrap());
    assert!(session.xa_get_transaction_timeout().await.unwrap() > 0);

    consumer.close().await.unwrap();
    session.delete_queue(&queue).await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn xa_rollback_discards_sends_and_redelivers_acks() {
    let conn = require_broker!();
    let session = conn.create_session(SessionOptions::xa()).await.unwrap();
    let queue = unique("rust.core.xarb");
    session.create_queue(QueueConfiguration::new(queue.as_str()).routing_type(RoutingType::Anycast)).await.unwrap();
    let producer = session.create_producer(Some(&queue)).await.unwrap();

    let xid = Xid::random();
    session.xa_start(&xid, flags::TMNOFLAGS).await.unwrap();
    producer.send_blocking(Message::text("discarded")).await.unwrap();
    session.xa_end(&xid, flags::TMFAIL).await.unwrap();
    session.xa_rollback(&xid).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    // Prepare then rollback.
    let xid = Xid::random();
    session.xa_start(&xid, flags::TMNOFLAGS).await.unwrap();
    producer.send_blocking(Message::text("prepared then rolled back")).await.unwrap();
    session.xa_end(&xid, flags::TMSUCCESS).await.unwrap();
    session.xa_prepare(&xid).await.unwrap();
    session.xa_rollback(&xid).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    // Commit one message, ack it in an XA branch, roll back: it is redelivered.
    let xid = Xid::random();
    session.xa_start(&xid, flags::TMNOFLAGS).await.unwrap();
    producer.send_blocking(Message::text("kept")).await.unwrap();
    session.xa_end(&xid, flags::TMSUCCESS).await.unwrap();
    session.xa_commit(&xid, true).await.unwrap();

    session.start().await.unwrap();
    let consumer = session.create_consumer(&queue, ConsumerOptions::new()).await.unwrap();
    let xid = Xid::random();
    session.xa_start(&xid, flags::TMNOFLAGS).await.unwrap();
    let m = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("kept");
    consumer.ack(&m).await.unwrap();
    session.xa_end(&xid, flags::TMSUCCESS).await.unwrap();
    session.xa_rollback(&xid).await.unwrap();
    consumer.drain_buffer().await;
    let m = consumer.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("kept redelivered");
    assert_eq!(m.text_body().unwrap().as_deref(), Some("kept"));
    assert!(m.delivery_count >= 2);

    // Suspend / resume a branch.
    let xid = Xid::random();
    session.xa_start(&xid, flags::TMNOFLAGS).await.unwrap();
    consumer.ack(&m).await.unwrap();
    session.xa_end(&xid, flags::TMSUSPEND).await.unwrap();
    session.xa_start(&xid, flags::TMRESUME).await.unwrap();
    session.xa_end(&xid, flags::TMSUCCESS).await.unwrap();
    session.xa_commit(&xid, true).await.unwrap();
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 0);

    consumer.close().await.unwrap();
    session.delete_queue(&queue).await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn multicast_address_with_two_subscriptions() {
    let conn = require_broker!();
    let session = conn.create_session(SessionOptions::default()).await.unwrap();
    let address = unique("rust.core.topic");
    session.create_address(&address, &[RoutingType::Multicast], false).await.unwrap();
    let q1 = format!("{address}.sub1");
    let q2 = format!("{address}.sub2");
    for q in [&q1, &q2] {
        session
            .create_queue(QueueConfiguration::new(q.as_str()).address(address.as_str()).routing_type(RoutingType::Multicast))
            .await
            .unwrap();
    }
    session.start().await.unwrap();
    let c1 = session.create_consumer(&q1, ConsumerOptions::new()).await.unwrap();
    let c2 = session.create_consumer(&q2, ConsumerOptions::new()).await.unwrap();

    // Anonymous producer: the address comes from the message.
    session.send_blocking(Message::text("fan out").with_address(address.as_str())).await.unwrap();
    for c in [&c1, &c2] {
        let m = c.receive_timeout(Duration::from_secs(10)).await.unwrap().expect("copy");
        assert_eq!(m.text_body().unwrap().as_deref(), Some("fan out"));
        c.ack(&m).await.unwrap();
    }
    c1.close().await.unwrap();
    c2.close().await.unwrap();
    session.delete_queue(&q1).await.unwrap();
    session.delete_queue(&q2).await.unwrap();
    session.close().await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn bad_credentials_are_rejected() {
    let Some(broker) = broker() else { return };
    let conn = Connection::connect(&broker.addr, ConnectionOptions::new().credentials("nobody", "wrong")).await;
    // The broker either rejects CONNECT (newer brokers) or CREATESESSION.
    let err = match conn {
        Ok(conn) => match conn.create_session(SessionOptions::default()).await {
            Ok(_) => panic!("session created with bad credentials"),
            Err(e) => e,
        },
        Err(e) => e,
    };
    match err {
        Error::Broker { code, .. } => assert_eq!(code, ExceptionType::SECURITY_EXCEPTION),
        Error::Closed(_) => {}
        other => panic!("unexpected error {other}"),
    }
}
