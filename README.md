# artemis-core

A Rust port of the Apache ActiveMQ Artemis **CORE** wire protocol, including local and XA
transactions.

The port was made from the Artemis sources (`artemis-core-client`, `artemis-commons`,
`artemis-server`) at commit `b6be6a3` of <https://github.com/apache/activemq-artemis>
(2.58.0-SNAPSHOT, protocol version 137). It has two layers:

| Layer | Modules | Java counterpart |
|-------|---------|------------------|
| Codec | `buffer`, `simple_string`, `properties`, `message`, `xid`, `packet`, `queue` | `ActiveMQBuffer`, `SimpleString`, `TypedProperties`, `CoreMessage`, `XidCodecSupport`, `PacketImpl` + `wireformat/*`, `PacketDecoder` |
| Client | `client` (`Connection`, `Session`, `Producer`, `Consumer`) | `RemotingConnectionImpl`, `ChannelImpl`, `ActiveMQClientProtocolManager`, `ActiveMQSessionContext`, `ClientSessionImpl`, `ClientProducerImpl`, `ClientConsumerImpl` |

## What is implemented

* Framing (`int length`, `byte type`, `long channelID`), the `ARTEMIS` transport handshake,
  and the `CONNECT` / `CONNECT_RESPONSE` exchange.
* Every packet a client exchanges with a broker, with the version-dependent shapes
  (`_V2`/`_V3`/... variants) selected from the negotiated protocol version, exactly as
  `PacketDecoder` and `ChannelImpl.supports` do. Unknown packets (cluster topology, replication)
  decode to `Packet::Unknown` and are ignored.
* String encodings (`writeString` with its three size-dependent forms, `SimpleString` UTF-16LE,
  nullable variants, `BufferHelper` nullable numbers), `TypedProperties`, and the `CoreMessage`
  layout (`endOfBodyPosition`, body, headers, properties).
* Sessions: create (with version fallback like `VersionLoader.getClientVersions()`),
  start/stop/close, metadata, addresses and queues (create/delete/query, shared queues),
  producers (`CREATE_PRODUCER`/`REMOVE_PRODUCER`), consumers with credit based flow control,
  blocking and non-blocking sends, cumulative and individual acknowledgements, large messages
  (chunked send and reassembled receive), pings, blocking call correlation and timeouts,
  receive-side confirmation window.
* Transactions: local `commit` / `rollback` (`SESS_COMMIT`/`SESS_ROLLBACK`), and the full XA set
  `xa_start` (`TMNOFLAGS`/`TMJOIN`/`TMRESUME`), `xa_end` (`TMSUCCESS`/`TMFAIL`/`TMSUSPEND`),
  `xa_prepare`, `xa_commit` (one and two phase), `xa_rollback`, `xa_forget`, `xa_recover`,
  `xa_set_transaction_timeout`, `xa_get_transaction_timeout`, `xa_failed`, with XA error codes
  surfaced as `Error::Xa`.

Not implemented: the pre-2.0 (`< 129`) message layout, failover / reattach, topology
subscriptions, producer credit requests (the Java client blocks locally on credits; the broker
does not require them), compressed large messages, and the send-acknowledgement callback that
rides on `PACKETS_CONFIRMED`.

## Usage

```rust
use std::time::Duration;
use artemis_core::client::ConsumerOptions;
use artemis_core::xid::flags;
use artemis_core::*;

#[tokio::main]
async fn main() -> Result<()> {
    let conn = Connection::connect("localhost:61616",
        ConnectionOptions::new().credentials("artemis", "artemis")).await?;

    // Auto-commit session
    let session = conn.create_session(SessionOptions::default()).await?;
    session.create_queue(QueueConfiguration::new("orders").routing_type(RoutingType::Anycast)).await?;
    let producer = session.create_producer(Some("orders")).await?;
    producer.send_blocking(Message::text("hello").with_property("priority", 1i32)).await?;

    session.start().await?;
    let consumer = session.create_consumer("orders", ConsumerOptions::new()).await?;
    if let Some(msg) = consumer.receive_timeout(Duration::from_secs(5)).await? {
        println!("{:?}", msg.text_body()?);
        consumer.ack(&msg).await?;
    }

    // XA session
    let xa = conn.create_session(SessionOptions::xa()).await?;
    let xid = Xid::random();
    xa.xa_start(&xid, flags::TMNOFLAGS).await?;
    xa.send(Message::text("in xa").with_address("orders")).await?;
    xa.xa_end(&xid, flags::TMSUCCESS).await?;
    assert_eq!(xa.xa_prepare(&xid).await?, XaCode::XA_OK.0);
    xa.xa_commit(&xid, false).await?;

    consumer.close().await?;
    session.close().await?;
    xa.close().await?;
    conn.close().await;
    Ok(())
}
```

`examples/basic.rs` is a runnable version of this (`cargo run --example basic -- host:port user pass`).

## Tests

* `cargo test --lib` runs codec unit tests (byte-level layouts and round trips for every
  packet across protocol versions).
* `cargo test --test broker` runs integration tests against a real broker. By default it starts
  `apache/activemq-artemis:latest-alpine` with Docker, publishing the CORE acceptor (61616) on
  a random local port and pre-configuring an anycast queue (`rust.core.preconfigured`) through
  `artemis create --queues`. The tests cover handshake and version negotiation, queue and
  address management, send/receive on the pre-configured queue and on client-created queues,
  bytes and large messages, multicast fan-out, local transaction commit/rollback with
  redelivery, XA two-phase commit with recovery, XA rollback, suspend/resume and error codes,
  and authentication failures.
  * `ARTEMIS_ADDR=host:port` (with `ARTEMIS_USER`/`ARTEMIS_PASSWORD`) uses an existing broker.
  * `ARTEMIS_IMAGE` selects another image. Without Docker the tests print a notice and skip.

## Wire format notes

```text
frame      := int32 length, int8 type, int64 channelId, body
message    := int32 endOfBodyPosition (= 13 + bodyLen), body, int64 messageId,
              nullable SimpleString address, byte userIdFlag [16 bytes userId], int8 type,
              bool durable, int64 expiration, int64 timestamp, int8 priority, properties
properties := byte 0 | byte 1, int32 count, { SimpleString key, int8 type, value }*
xid        := int32 formatId, int32 bqLen, bq, int32 gtxLen, gtx
```

Blocking requests get a negative, decreasing correlation id per channel (`ChannelImpl`);
responses are matched on type and correlation id, and `EXCEPTION` always terminates the call.

## License

Released into the public domain under [The Unlicense](LICENSE). The port is derived from
Apache ActiveMQ Artemis (Apache License 2.0); see `NOTICE` for the required attribution.
