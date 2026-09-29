//! Sends and receives through a queue, then runs an XA transaction.
//!
//! ```sh
//! cargo run --example basic -- localhost:61616 artemis artemis
//! ```

use std::time::Duration;

use artemis_core::client::ConsumerOptions;
use artemis_core::xid::flags;
use artemis_core::{
    Connection, ConnectionOptions, Message, QueueConfiguration, Result, RoutingType, SessionOptions, XaCode, Xid,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "localhost:61616".to_string());
    let user = args.get(2).cloned().unwrap_or_else(|| "artemis".to_string());
    let pass = args.get(3).cloned().unwrap_or_else(|| "artemis".to_string());

    let conn = Connection::connect(&addr, ConnectionOptions::new().credentials(&user, &pass)).await?;
    println!("connected to {addr}, protocol version {}", conn.version());

    let session = conn.create_session(SessionOptions::default()).await?;
    let queue = "rust.example.queue";
    if !session.queue_query(queue).await?.exists {
        session.create_queue(QueueConfiguration::new(queue).routing_type(RoutingType::Anycast)).await?;
    }

    let producer = session.create_producer(Some(queue)).await?;
    for i in 0..3i32 {
        producer.send_blocking(Message::text(&format!("message {i}")).with_property("index", i)).await?;
    }

    session.start().await?;
    let consumer = session.create_consumer(queue, ConsumerOptions::new()).await?;
    while let Some(msg) = consumer.receive_timeout(Duration::from_secs(2)).await? {
        println!("received {:?} index={:?} id={}", msg.text_body()?, msg.properties.get_i32("index"), msg.message_id);
        consumer.ack(&msg).await?;
    }

    // XA: two-phase commit of one send.
    let xa = conn.create_session(SessionOptions::xa()).await?;
    let xid = Xid::random();
    xa.xa_start(&xid, flags::TMNOFLAGS).await?;
    xa.send_blocking(Message::text("sent in an XA transaction").with_address(queue)).await?;
    xa.xa_end(&xid, flags::TMSUCCESS).await?;
    let vote = xa.xa_prepare(&xid).await?;
    println!("prepare vote: {}", XaCode(vote));
    println!("in-doubt branches: {}", xa.xa_recover().await?.len());
    xa.xa_commit(&xid, false).await?;
    let msg = consumer.receive_timeout(Duration::from_secs(5)).await?.expect("xa message");
    println!("received after XA commit: {:?}", msg.text_body()?);
    consumer.ack(&msg).await?;

    consumer.close().await?;
    session.close().await?;
    xa.close().await?;
    conn.close().await;
    Ok(())
}
