//! Async CORE client built on Tokio.
//!
//! The client mirrors the structure of the Java `artemis-core-client`:
//!
//! * a [`Connection`] owns the TCP transport, performs the `ARTEMIS` handshake, multiplexes
//!   packets over numbered *channels*, pings the broker and matches blocking requests with
//!   their responses (`RemotingConnectionImpl` / `ChannelImpl`);
//! * a [`Session`] is a server-side session bound to one channel; it sends messages, creates
//!   consumers and producers, manages queues/addresses and drives local and XA transactions
//!   (`ClientSessionImpl` / `ActiveMQSessionContext`);
//! * a [`Consumer`] receives messages for one server consumer and implements the credit based
//!   flow control (`ClientConsumerImpl`);
//! * a [`Producer`] sends regular and large messages (`ClientProducerImpl`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace, warn};

use crate::error::{Error, ExceptionType, Result};
use crate::message::{Message, RoutingType};
use crate::packet::{channels, plain_auth_data, split_frame, types, versions, Frame, Packet};
use crate::queue::{AddressQueryResult, QueueConfiguration, QueueQueryResult};
use crate::simple_string::SimpleString;
use crate::xid::{flags, Xid};

/// Bytes written on the socket before any packet to select the CORE protocol.
pub const HANDSHAKE: &[u8] = b"ARTEMIS";

/// Default consumer window size (bytes), `ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE`.
pub const DEFAULT_CONSUMER_WINDOW_SIZE: i32 = 1024 * 1024;
/// Default threshold above which messages are sent as large messages.
pub const DEFAULT_MIN_LARGE_MESSAGE_SIZE: i32 = 100 * 1024;
/// Default confirmation window (disabled).
pub const DEFAULT_CONFIRMATION_WINDOW_SIZE: i32 = -1;
/// Default blocking call timeout.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Default connection TTL announced in pings.
pub const DEFAULT_CONNECTION_TTL: Duration = Duration::from_secs(60);
/// Default ping interval (`clientFailureCheckPeriod`).
pub const DEFAULT_PING_PERIOD: Duration = Duration::from_secs(30);

// =============================================================================================
// Options
// =============================================================================================

/// Options for [`Connection::connect`].
#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub username: Option<String>,
    pub password: Option<String>,
    /// Client id sent with `CREATESESSION_V2` (broker >= 2.18).
    pub client_id: Option<String>,
    /// Timeout for blocking calls.
    pub call_timeout: Duration,
    /// Connection TTL announced to the broker in `PING` packets.
    pub connection_ttl: Duration,
    /// Interval between `PING` packets; `None` disables pinging.
    pub ping_period: Option<Duration>,
    /// Send the `CONNECT` handshake packet before creating sessions (as the Java client does).
    pub send_connect: bool,
    /// Protocol versions to try when creating the first session, newest first.
    pub client_versions: Vec<i32>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        ConnectionOptions {
            username: None,
            password: None,
            client_id: None,
            call_timeout: DEFAULT_CALL_TIMEOUT,
            connection_ttl: DEFAULT_CONNECTION_TTL,
            ping_period: Some(DEFAULT_PING_PERIOD),
            send_connect: true,
            client_versions: versions::CLIENT_VERSIONS.to_vec(),
        }
    }
}

impl ConnectionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn credentials(mut self, username: &str, password: &str) -> Self {
        self.username = Some(username.to_string());
        self.password = Some(password.to_string());
        self
    }

    pub fn client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    pub fn call_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeout = timeout;
        self
    }
}

/// Options for [`Connection::create_session`].
#[derive(Clone, Debug)]
pub struct SessionOptions {
    /// Session name; a random one is generated when `None`.
    pub name: Option<String>,
    /// Per-session credentials; fall back to the connection credentials.
    pub username: Option<String>,
    pub password: Option<String>,
    /// Create an XA session (transactions are controlled with the `xa_*` methods).
    pub xa: bool,
    /// Sends are committed immediately; `false` makes sends transactional.
    pub auto_commit_sends: bool,
    /// Acknowledgements are committed immediately; `false` makes acks transactional.
    pub auto_commit_acks: bool,
    /// Messages are acknowledged by the broker before delivery.
    pub pre_acknowledge: bool,
    /// Messages with a body larger than this are sent as large messages.
    pub min_large_message_size: i32,
    /// Confirmation window size for the session channel (`-1` disables).
    pub confirmation_window_size: i32,
    /// Default address used by the broker when a message has no address.
    pub default_address: Option<String>,
    /// Default consumer window size for consumers created on this session.
    pub consumer_window_size: i32,
    /// Block on non-transactional acknowledgements.
    pub block_on_acknowledge: bool,
}

impl Default for SessionOptions {
    fn default() -> Self {
        SessionOptions {
            name: None,
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            min_large_message_size: DEFAULT_MIN_LARGE_MESSAGE_SIZE,
            confirmation_window_size: DEFAULT_CONFIRMATION_WINDOW_SIZE,
            default_address: None,
            consumer_window_size: DEFAULT_CONSUMER_WINDOW_SIZE,
            block_on_acknowledge: false,
        }
    }
}

impl SessionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// A locally transacted session: sends and acks only take effect on [`Session::commit`].
    pub fn transacted() -> Self {
        SessionOptions { auto_commit_sends: false, auto_commit_acks: false, ..Default::default() }
    }

    /// An XA session.
    pub fn xa() -> Self {
        SessionOptions { xa: true, auto_commit_sends: false, auto_commit_acks: false, ..Default::default() }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn min_large_message_size(mut self, size: i32) -> Self {
        self.min_large_message_size = size;
        self
    }

    pub fn consumer_window_size(mut self, size: i32) -> Self {
        self.consumer_window_size = size;
        self
    }

    pub fn pre_acknowledge(mut self, pre_ack: bool) -> Self {
        self.pre_acknowledge = pre_ack;
        self
    }
}

/// Options for [`Session::create_consumer`].
#[derive(Clone, Debug, Default)]
pub struct ConsumerOptions {
    pub filter: Option<String>,
    pub priority: i32,
    pub browse_only: bool,
    /// Consumer window size in bytes; `None` uses the session default. `0` means
    /// "one message at a time" (slow consumer), `-1` disables flow control.
    pub window_size: Option<i32>,
}

impl ConsumerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filter(mut self, filter: &str) -> Self {
        self.filter = Some(filter.to_string());
        self
    }

    pub fn browse_only(mut self, browse: bool) -> Self {
        self.browse_only = browse;
        self
    }

    pub fn window_size(mut self, size: i32) -> Self {
        self.window_size = Some(size);
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

// =============================================================================================
// Connection internals
// =============================================================================================

struct Pending {
    expected: i8,
    correlation_id: i64,
    tx: oneshot::Sender<Packet>,
}

#[derive(Default)]
struct ChannelState {
    pending: Option<Pending>,
    handler: Option<mpsc::UnboundedSender<Delivery>>,
    next_blocking_correlation: i64,
    confirmation_window: i32,
    received_bytes: i32,
    last_confirmed_command_id: i32,
}

impl ChannelState {
    fn new(confirmation_window: i32) -> Self {
        ChannelState {
            pending: None,
            handler: None,
            next_blocking_correlation: -1,
            confirmation_window,
            received_bytes: 0,
            last_confirmed_command_id: -1,
        }
    }
}

/// A packet delivered to a channel handler together with its wire size.
struct Delivery {
    packet: Packet,
    packet_size: usize,
}

struct ConnectionInner {
    writer: tokio::sync::Mutex<Option<OwnedWriteHalf>>,
    version: AtomicI32,
    channels: Mutex<HashMap<i64, ChannelState>>,
    /// Serializes blocking calls per channel (`ChannelImpl.sendBlockingLock`).
    blocking_locks: Mutex<HashMap<i64, Arc<tokio::sync::Mutex<()>>>>,
    next_channel_id: AtomicI64,
    closed: AtomicBool,
    close_reason: Mutex<Option<String>>,
    options: ConnectionOptions,
}

impl ConnectionInner {
    fn version(&self) -> i32 {
        self.version.load(Ordering::SeqCst)
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            let reason = self.close_reason.lock().unwrap().clone().unwrap_or_else(|| "connection closed".into());
            Err(Error::Closed(reason))
        } else {
            Ok(())
        }
    }

    fn register_channel(&self, id: i64, confirmation_window: i32) {
        self.channels.lock().unwrap().entry(id).or_insert_with(|| ChannelState::new(confirmation_window));
    }

    fn remove_channel(&self, id: i64) {
        self.channels.lock().unwrap().remove(&id);
        self.blocking_locks.lock().unwrap().remove(&id);
    }

    fn set_handler(&self, id: i64, handler: mpsc::UnboundedSender<Delivery>) {
        if let Some(ch) = self.channels.lock().unwrap().get_mut(&id) {
            ch.handler = Some(handler);
        }
    }

    fn blocking_lock(&self, id: i64) -> Arc<tokio::sync::Mutex<()>> {
        self.blocking_locks.lock().unwrap().entry(id).or_default().clone()
    }

    /// Encodes and writes a packet. Returns the packet size on the wire.
    async fn write(&self, channel_id: i64, packet: &Packet) -> Result<usize> {
        self.check_open()?;
        let mut buf = BytesMut::with_capacity(64);
        let size = packet.encode(channel_id, self.version(), &mut buf);
        trace!(channel_id, ?packet, size, "-> send");
        let mut guard = self.writer.lock().await;
        match guard.as_mut() {
            Some(w) => {
                if let Err(e) = w.write_all(&buf).await {
                    drop(guard);
                    self.fail(format!("write failed: {e}"));
                    return Err(Error::Io(e));
                }
                Ok(size)
            }
            None => Err(Error::Closed("connection closed".into())),
        }
    }

    /// Sends a request and waits for its response (`ChannelImpl.sendBlocking`).
    async fn send_blocking(&self, channel_id: i64, mut packet: Packet, expected: i8) -> Result<Packet> {
        self.check_open()?;
        let lock = self.blocking_lock(channel_id);
        let _guard = lock.lock().await;

        let (tx, rx) = oneshot::channel();
        let correlation_id = {
            let mut channels = self.channels.lock().unwrap();
            let ch = channels
                .get_mut(&channel_id)
                .ok_or_else(|| Error::IllegalState(format!("channel {channel_id} is not open")))?;
            let correlation_id = if packet.supports_correlation(self.version()) {
                let id = ch.next_blocking_correlation;
                ch.next_blocking_correlation -= 1;
                id
            } else {
                -1
            };
            packet.set_correlation_id(correlation_id);
            ch.pending = Some(Pending { expected, correlation_id, tx });
            correlation_id
        };

        if let Err(e) = self.write(channel_id, &packet).await {
            self.clear_pending(channel_id);
            return Err(e);
        }

        let packet_type = packet.packet_type(self.version());
        let response = match tokio::time::timeout(self.options.call_timeout, rx).await {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => {
                self.clear_pending(channel_id);
                return Err(self.check_open().err().unwrap_or_else(|| Error::Closed("response channel dropped".into())));
            }
            Err(_) => {
                self.clear_pending(channel_id);
                return Err(Error::Timeout(self.options.call_timeout, packet_type));
            }
        };
        let _ = correlation_id;
        match response {
            Packet::Exception { code, message, .. } => Err(Error::broker(code, message)),
            other => {
                let got = other.packet_type(self.version());
                if got != expected {
                    return Err(Error::UnexpectedPacket { expected, got });
                }
                Ok(other)
            }
        }
    }

    fn clear_pending(&self, channel_id: i64) {
        if let Some(ch) = self.channels.lock().unwrap().get_mut(&channel_id) {
            ch.pending = None;
        }
    }

    fn fail(&self, reason: String) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        debug!("connection failed: {reason}");
        *self.close_reason.lock().unwrap() = Some(reason);
        // Dropping pending senders and handlers wakes up everybody waiting.
        let mut channels = self.channels.lock().unwrap();
        for ch in channels.values_mut() {
            ch.pending = None;
            ch.handler = None;
        }
    }

    /// Receive-side confirmation window handling (`ChannelImpl.confirm`).
    async fn confirm(&self, channel_id: i64, packet: &Packet, packet_size: usize) {
        let confirmed = {
            let mut channels = self.channels.lock().unwrap();
            let Some(ch) = channels.get_mut(&channel_id) else { return };
            if ch.confirmation_window < 0 || !packet.requires_confirmations() {
                return;
            }
            ch.last_confirmed_command_id += 1;
            ch.received_bytes = ch.received_bytes.saturating_add(packet_size as i32);
            if ch.received_bytes >= ch.confirmation_window {
                ch.received_bytes = 0;
                Some(ch.last_confirmed_command_id)
            } else {
                None
            }
        };
        if let Some(command_id) = confirmed {
            let _ = self.write(channel_id, &Packet::PacketsConfirmed { command_id }).await;
        }
    }

    /// Dispatches a decoded frame (`RemotingConnectionImpl.doBufferReceived` + `ChannelImpl.handlePacket`).
    async fn dispatch(&self, frame: Frame, packet_size: usize) {
        let Frame { channel_id, packet } = frame;
        trace!(channel_id, ?packet, packet_size, "<- recv");
        if let Packet::PacketsConfirmed { .. } = packet {
            // We keep no resend cache; nothing to clear.
            return;
        }
        if packet.is_response() {
            self.confirm(channel_id, &packet, packet_size).await;
            let pending = {
                let mut channels = self.channels.lock().unwrap();
                let Some(ch) = channels.get_mut(&channel_id) else {
                    warn!(channel_id, "response for unknown channel");
                    return;
                };
                let matches = match &ch.pending {
                    Some(p) => {
                        let ptype = packet.packet_type(self.version());
                        ptype == types::EXCEPTION || (ptype == p.expected && packet.correlation_id() == p.correlation_id)
                    }
                    None => false,
                };
                if matches {
                    ch.pending.take()
                } else {
                    None
                }
            };
            match pending {
                Some(p) => {
                    let _ = p.tx.send(packet);
                }
                None => debug!(channel_id, ?packet, "unmatched response packet dropped"),
            }
            return;
        }
        // Channel 0 carries pings and disconnects.
        if channel_id == channels::PING {
            match packet {
                Packet::Disconnect { .. } | Packet::DisconnectV2 { .. } | Packet::DisconnectV3 { .. } => {
                    self.fail(format!("broker disconnected: {packet:?}"));
                }
                Packet::Ping { .. } => {}
                other => debug!(?other, "ignoring channel 0 packet"),
            }
            return;
        }
        self.confirm(channel_id, &packet, packet_size).await;
        let handler = self.channels.lock().unwrap().get(&channel_id).and_then(|c| c.handler.clone());
        match handler {
            Some(h) => {
                let _ = h.send(Delivery { packet, packet_size });
            }
            None => debug!(channel_id, ?packet, "packet for channel without handler dropped"),
        }
    }
}

async fn reader_loop(inner: Arc<ConnectionInner>, mut reader: OwnedReadHalf) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        // Drain complete frames.
        loop {
            let frame_result = match split_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let decoded = Packet::decode_frame(frame, inner.version());
                    Some((decoded, consumed))
                }
                Ok(None) => None,
                Err(e) => {
                    inner.fail(format!("invalid frame: {e}"));
                    return;
                }
            };
            let Some((decoded, consumed)) = frame_result else { break };
            match decoded {
                Ok(frame) => inner.dispatch(frame, consumed).await,
                Err(e) => {
                    inner.fail(format!("packet decode error: {e}"));
                    return;
                }
            }
            let _ = buf.split_to(consumed);
        }
        match reader.read_buf(&mut buf).await {
            Ok(0) => {
                inner.fail("connection closed by peer".into());
                return;
            }
            Ok(_) => {}
            Err(e) => {
                inner.fail(format!("read error: {e}"));
                return;
            }
        }
    }
}

async fn ping_loop(inner: Arc<ConnectionInner>, period: Duration) {
    let ttl = inner.options.connection_ttl.as_millis() as i64;
    loop {
        tokio::time::sleep(period).await;
        if inner.closed.load(Ordering::SeqCst) {
            return;
        }
        if inner.write(channels::PING, &Packet::Ping { connection_ttl: ttl }).await.is_err() {
            return;
        }
    }
}

// =============================================================================================
// Connection
// =============================================================================================

/// A CORE protocol connection to a broker. Cheap to clone; all clones share the transport.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

impl Connection {
    /// Opens a TCP connection, sends the `ARTEMIS` handshake and (optionally) the `CONNECT`
    /// packet.
    pub async fn connect<A: ToSocketAddrs>(addr: A, options: ConnectionOptions) -> Result<Connection> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Self::from_stream(stream, options).await
    }

    /// Like [`Connection::connect`] but over an already established TCP stream.
    pub async fn from_stream(stream: TcpStream, options: ConnectionOptions) -> Result<Connection> {
        let (reader, mut writer) = stream.into_split();
        writer.write_all(HANDSHAKE).await?;

        let inner = Arc::new(ConnectionInner {
            writer: tokio::sync::Mutex::new(Some(writer)),
            version: AtomicI32::new(0),
            channels: Mutex::new(HashMap::new()),
            blocking_locks: Mutex::new(HashMap::new()),
            next_channel_id: AtomicI64::new(channels::USER),
            closed: AtomicBool::new(false),
            close_reason: Mutex::new(None),
            options: options.clone(),
        });
        inner.register_channel(channels::PING, -1);
        inner.register_channel(channels::SESSION, -1);
        tokio::spawn(reader_loop(inner.clone(), reader));

        let conn = Connection { inner };

        if options.send_connect {
            let client_version = options.client_versions.first().copied().unwrap_or(versions::CURRENT);
            let (auth_mechanism, auth_data) = match (&options.username, &options.password) {
                (Some(u), p) => (Some("PLAIN".to_string()), Some(plain_auth_data(u, p.as_deref().unwrap_or("")))),
                (None, _) => (None, None),
            };
            let packet = Packet::Connect { node_id: None, client_version, auth_mechanism, auth_data };
            match conn.inner.send_blocking(channels::SESSION, packet, types::CONNECT_RESPONSE).await? {
                Packet::ConnectResponse { server_version, .. } => {
                    if server_version > 0 {
                        conn.inner.version.store(server_version, Ordering::SeqCst);
                    }
                }
                other => return Err(Error::UnexpectedPacket { expected: types::CONNECT_RESPONSE, got: other.packet_type(0) }),
            }
        }

        if let Some(period) = options.ping_period {
            tokio::spawn(ping_loop(conn.inner.clone(), period));
        }
        Ok(conn)
    }

    /// Negotiated protocol version (0 until a session has been created or `CONNECT` answered).
    pub fn version(&self) -> i32 {
        self.inner.version()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Sends a `PING` immediately.
    pub async fn ping(&self) -> Result<()> {
        let ttl = self.inner.options.connection_ttl.as_millis() as i64;
        self.inner.write(channels::PING, &Packet::Ping { connection_ttl: ttl }).await.map(|_| ())
    }

    /// Creates a session (`CREATESESSION` on channel 1). Tries the configured client versions
    /// newest first until the broker accepts one.
    pub async fn create_session(&self, options: SessionOptions) -> Result<Session> {
        self.inner.check_open()?;
        let name = options.name.clone().unwrap_or_else(random_name);
        let username = options.username.clone().or_else(|| self.inner.options.username.clone());
        let password = options.password.clone().or_else(|| self.inner.options.password.clone());
        let channel_id = self.inner.next_channel_id.fetch_add(1, Ordering::SeqCst);

        let negotiated = self.inner.version();
        let mut candidates: Vec<i32> = self
            .inner
            .options
            .client_versions
            .iter()
            .copied()
            .filter(|v| negotiated <= 0 || *v <= negotiated)
            .collect();
        if candidates.is_empty() {
            candidates = self.inner.options.client_versions.clone();
        }

        let mut last_err = Error::IncompatibleVersion;
        for client_version in candidates {
            if client_version < versions::ADDRESSING_CHANGE_VERSION {
                break;
            }
            let client_id = if versions::supports_client_id(client_version) {
                Some(self.inner.options.client_id.clone())
            } else {
                None
            };
            let packet = Packet::CreateSession {
                name: name.clone(),
                session_channel_id: channel_id,
                version: client_version,
                username: username.clone(),
                password: password.clone(),
                min_large_message_size: options.min_large_message_size,
                xa: options.xa,
                auto_commit_sends: options.auto_commit_sends,
                auto_commit_acks: options.auto_commit_acks,
                pre_acknowledge: options.pre_acknowledge,
                window_size: options.confirmation_window_size,
                default_address: options.default_address.clone(),
                client_id,
            };
            match self.inner.send_blocking(channels::SESSION, packet, types::CREATESESSION_RESP).await {
                Ok(Packet::CreateSessionResponse { server_version }) => {
                    self.inner.version.store(server_version, Ordering::SeqCst);
                    return Ok(Session::new(self.inner.clone(), channel_id, name, options, server_version));
                }
                Ok(other) => {
                    return Err(Error::UnexpectedPacket { expected: types::CREATESESSION_RESP, got: other.packet_type(0) })
                }
                Err(e) if e.exception_type() == Some(ExceptionType::INCOMPATIBLE_CLIENT_SERVER_VERSIONS) => {
                    debug!(client_version, "broker rejected protocol version, trying an older one");
                    last_err = e;
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err)
    }

    /// Closes the transport. Sessions should be closed first with [`Session::close`].
    pub async fn close(&self) {
        self.inner.fail("closed by client".into());
        let mut guard = self.inner.writer.lock().await;
        if let Some(mut w) = guard.take() {
            let _ = w.shutdown().await;
        }
    }
}

fn random_name() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let a: u64 = rng.gen();
    let b: u64 = rng.gen();
    format!("{a:016x}-{b:016x}")
}

// =============================================================================================
// Session
// =============================================================================================

/// A received message together with bookkeeping for flow control.
struct Received {
    message: Message,
    packet_size: usize,
}

struct FlowState {
    /// `ClientConsumerImpl.clientWindowSize`: credits are sent back once this many bytes were
    /// consumed; `0` = slow consumer, `-1` = no flow control.
    client_window: i32,
    credits_to_send: i32,
}

struct LargeAssembly {
    message: Message,
    body: Vec<u8>,
    packet_size: usize,
}

struct ConsumerState {
    tx: mpsc::UnboundedSender<Received>,
    flow: Mutex<FlowState>,
    large: Mutex<Option<LargeAssembly>>,
}

struct SessionInner {
    conn: Arc<ConnectionInner>,
    channel_id: i64,
    name: String,
    options: SessionOptions,
    server_version: i32,
    consumers: Mutex<HashMap<i64, Arc<ConsumerState>>>,
    next_consumer_id: AtomicI64,
    next_producer_id: AtomicI32,
    default_producer: tokio::sync::Mutex<Option<Producer>>,
    started: AtomicBool,
    closed: AtomicBool,
}

impl SessionInner {
    fn version(&self) -> i32 {
        self.conn.version()
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed("session closed".into()));
        }
        self.conn.check_open()
    }

    async fn send(&self, packet: Packet) -> Result<usize> {
        self.check_open()?;
        self.conn.write(self.channel_id, &packet).await
    }

    async fn send_blocking(&self, packet: Packet, expected: i8) -> Result<Packet> {
        self.check_open()?;
        self.conn.send_blocking(self.channel_id, packet, expected).await
    }

    async fn send_blocking_null(&self, packet: Packet) -> Result<()> {
        self.send_blocking(packet, types::NULL_RESPONSE).await.map(|_| ())
    }

    async fn xa_call(&self, packet: Packet) -> Result<i32> {
        match self.send_blocking(packet, types::SESS_XA_RESP).await? {
            Packet::XaResponse { error, response_code, message, .. } => {
                if error {
                    Err(Error::xa(response_code, message))
                } else {
                    Ok(response_code)
                }
            }
            other => Err(Error::UnexpectedPacket { expected: types::SESS_XA_RESP, got: other.packet_type(0) }),
        }
    }

    /// `ClientConsumerImpl.flowControl`: sends credits back to the broker.
    async fn flow_control(&self, consumer_id: i64, state: &ConsumerState, bytes: usize) {
        let credits = {
            let mut flow = state.flow.lock().unwrap();
            if flow.client_window < 0 {
                return;
            }
            flow.credits_to_send = flow.credits_to_send.saturating_add(bytes as i32);
            if flow.credits_to_send >= flow.client_window {
                let credits = if flow.client_window == 0 { flow.credits_to_send - 1 } else { flow.credits_to_send };
                flow.credits_to_send = 0;
                if credits > 0 {
                    Some(credits)
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(credits) = credits {
            let _ = self.send(Packet::ConsumerFlowCredit { consumer_id, credits }).await;
        }
    }

    fn consumer(&self, id: i64) -> Option<Arc<ConsumerState>> {
        self.consumers.lock().unwrap().get(&id).cloned()
    }

    /// Session channel handler (`ActiveMQSessionContext.ClientSessionPacketHandler`).
    async fn handle(&self, delivery: Delivery) {
        let Delivery { packet, packet_size } = delivery;
        match packet {
            Packet::Receive { consumer_id, message, .. } => {
                if let Some(c) = self.consumer(consumer_id) {
                    let _ = c.tx.send(Received { message, packet_size });
                } else {
                    debug!(consumer_id, "message for unknown consumer dropped");
                }
            }
            Packet::ReceiveLarge { consumer_id, message, large_message_size, .. } => {
                if let Some(c) = self.consumer(consumer_id) {
                    let mut large = c.large.lock().unwrap();
                    *large = Some(LargeAssembly {
                        message,
                        body: Vec::with_capacity(large_message_size.max(0) as usize),
                        packet_size,
                    });
                }
            }
            Packet::ReceiveContinuation { consumer_id, body, continues } => {
                if let Some(c) = self.consumer(consumer_id) {
                    let complete = {
                        let mut large = c.large.lock().unwrap();
                        match large.as_mut() {
                            Some(asm) => {
                                asm.body.extend_from_slice(&body);
                                if continues {
                                    None
                                } else {
                                    large.take()
                                }
                            }
                            None => {
                                warn!(consumer_id, "continuation without a large message header");
                                None
                            }
                        }
                    };
                    // Java returns credits for every chunk as it arrives.
                    self.flow_control(consumer_id, &c, packet_size).await;
                    if let Some(asm) = complete {
                        let mut message = asm.message;
                        message.body = asm.body;
                        let _ = c.tx.send(Received { message, packet_size: asm.packet_size });
                    }
                }
            }
            Packet::DisconnectConsumer { consumer_id } => {
                self.consumers.lock().unwrap().remove(&consumer_id);
            }
            Packet::DisconnectConsumerWithKill { .. } => {
                warn!("broker killed this session's consumers (slow consumer policy)");
                self.consumers.lock().unwrap().clear();
            }
            Packet::ProducerCredits { .. } | Packet::ProducerCreditsFail { .. } => {
                // Producer flow control is not requested by this client.
            }
            Packet::Exception { code, message, .. } => {
                warn!(code, ?message, "asynchronous exception from broker");
            }
            other => debug!(?other, "unhandled session packet"),
        }
    }
}

async fn session_loop(inner: Arc<SessionInner>, mut rx: mpsc::UnboundedReceiver<Delivery>) {
    while let Some(delivery) = rx.recv().await {
        inner.handle(delivery).await;
    }
    // Connection gone: wake consumers.
    inner.consumers.lock().unwrap().clear();
}

/// A broker session. Cheap to clone.
#[derive(Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

impl Session {
    fn new(conn: Arc<ConnectionInner>, channel_id: i64, name: String, options: SessionOptions, server_version: i32) -> Session {
        conn.register_channel(channel_id, options.confirmation_window_size);
        let (tx, rx) = mpsc::unbounded_channel();
        conn.set_handler(channel_id, tx);
        let inner = Arc::new(SessionInner {
            conn,
            channel_id,
            name,
            options,
            server_version,
            consumers: Mutex::new(HashMap::new()),
            next_consumer_id: AtomicI64::new(0),
            next_producer_id: AtomicI32::new(0),
            default_producer: tokio::sync::Mutex::new(None),
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        tokio::spawn(session_loop(inner.clone(), rx));
        Session { inner }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn channel_id(&self) -> i64 {
        self.inner.channel_id
    }

    /// Protocol version reported by the broker when the session was created.
    pub fn server_version(&self) -> i32 {
        self.inner.server_version
    }

    pub fn is_xa(&self) -> bool {
        self.inner.options.xa
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::SeqCst)
    }

    // --- lifecycle ---------------------------------------------------------------------------

    /// Starts message delivery to this session's consumers (`SESS_START`).
    pub async fn start(&self) -> Result<()> {
        self.inner.send(Packet::SessionStart).await?;
        self.inner.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Stops message delivery (`SESS_STOP`, blocking).
    pub async fn stop(&self) -> Result<()> {
        self.inner.send_blocking_null(Packet::SessionStop).await?;
        self.inner.started.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Closes the session on the broker (`SESS_CLOSE`, blocking) and releases its channel.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let result = if self.inner.conn.check_open().is_ok() {
            self.inner.conn.send_blocking(self.inner.channel_id, Packet::SessionClose, types::NULL_RESPONSE).await.map(|_| ())
        } else {
            Ok(())
        };
        self.inner.conn.remove_channel(self.inner.channel_id);
        self.inner.consumers.lock().unwrap().clear();
        result
    }

    /// `SESS_ADD_METADATA2` (blocking).
    pub async fn add_metadata(&self, key: &str, data: &str) -> Result<()> {
        self.inner
            .send_blocking_null(Packet::AddMetaDataV2 { key: key.into(), data: data.into(), requires_confirmation: true })
            .await
    }

    /// `SESS_UNIQUE_ADD_METADATA` (blocking); fails with `DUPLICATE_METADATA` if taken.
    pub async fn add_unique_metadata(&self, key: &str, data: &str) -> Result<()> {
        self.inner
            .send_blocking_null(Packet::UniqueAddMetaData { key: key.into(), data: data.into(), requires_confirmation: true })
            .await
    }

    // --- queues and addresses ----------------------------------------------------------------

    pub async fn create_address(&self, address: &str, routing_types: &[RoutingType], auto_created: bool) -> Result<()> {
        if versions::before_address_change(self.inner.version()) {
            return Ok(());
        }
        self.inner
            .send_blocking_null(Packet::CreateAddress {
                address: address.into(),
                routing_types: routing_types.to_vec(),
                requires_response: true,
                auto_created,
            })
            .await
    }

    pub async fn create_queue(&self, config: QueueConfiguration) -> Result<()> {
        let mut config = config;
        if config.max_consumers.is_none() {
            config.max_consumers = Some(-1);
        }
        if config.purge_on_no_consumers.is_none() {
            config.purge_on_no_consumers = Some(false);
        }
        let v2 = !versions::before_address_change(self.inner.version());
        self.inner.send_blocking_null(Packet::CreateQueue { config, requires_response: true, v2 }).await
    }

    pub async fn create_shared_queue(&self, config: QueueConfiguration) -> Result<()> {
        self.inner.send_blocking_null(Packet::CreateSharedQueue { config, requires_response: true, v2: true }).await
    }

    pub async fn delete_queue(&self, queue_name: &str) -> Result<()> {
        self.inner.send_blocking_null(Packet::DeleteQueue { queue_name: queue_name.into() }).await
    }

    pub async fn queue_query(&self, queue_name: &str) -> Result<QueueQueryResult> {
        let expected = if versions::before_address_change(self.inner.version()) {
            types::SESS_QUEUEQUERY_RESP_V2
        } else {
            types::SESS_QUEUEQUERY_RESP_V3
        };
        match self.inner.send_blocking(Packet::QueueQuery { queue_name: queue_name.into() }, expected).await? {
            Packet::QueueQueryResponse { result, .. } => Ok(result),
            other => Err(Error::UnexpectedPacket { expected, got: other.packet_type(0) }),
        }
    }

    pub async fn address_query(&self, address: &str) -> Result<AddressQueryResult> {
        let v = self.inner.server_version;
        let expected = [
            types::SESS_BINDINGQUERY_RESP_V5,
            types::SESS_BINDINGQUERY_RESP_V4,
            types::SESS_BINDINGQUERY_RESP_V3,
            types::SESS_BINDINGQUERY_RESP_V2,
        ]
        .into_iter()
        .find(|t| versions::channel_supports(*t, v))
        .unwrap_or(types::SESS_BINDINGQUERY_RESP);
        match self.inner.send_blocking(Packet::BindingQuery { address: address.into() }, expected).await? {
            Packet::BindingQueryResponse { result, .. } => Ok(result),
            other => Err(Error::UnexpectedPacket { expected, got: other.packet_type(0) }),
        }
    }

    // --- producers and sending ---------------------------------------------------------------

    /// Creates a producer. `address` may be `None` for an anonymous producer that sends to the
    /// address carried by each message.
    pub async fn create_producer(&self, address: Option<&str>) -> Result<Producer> {
        self.inner.check_open()?;
        let id = self.inner.next_producer_id.fetch_add(1, Ordering::SeqCst);
        let address: Option<SimpleString> = address.map(Into::into);
        if !versions::before_producer_metrics(self.inner.version()) {
            self.inner.send(Packet::CreateProducer { id, address: address.clone() }).await?;
        }
        Ok(Producer { session: self.inner.clone(), id, address, closed: Arc::new(AtomicBool::new(false)) })
    }

    async fn default_producer(&self) -> Result<Producer> {
        let mut guard = self.inner.default_producer.lock().await;
        if let Some(p) = guard.as_ref() {
            return Ok(p.clone());
        }
        let p = self.create_producer(None).await?;
        *guard = Some(p.clone());
        Ok(p)
    }

    /// Sends a message through an anonymous producer without waiting for the broker.
    pub async fn send(&self, message: Message) -> Result<()> {
        self.default_producer().await?.send(message).await
    }

    /// Sends a message through an anonymous producer and waits for the broker's response.
    pub async fn send_blocking(&self, message: Message) -> Result<()> {
        self.default_producer().await?.send_blocking(message).await
    }

    // --- consumers ---------------------------------------------------------------------------

    /// Creates a consumer on `queue_name` (`SESS_CREATECONSUMER`, blocking) and grants it the
    /// initial window of credits.
    pub async fn create_consumer(&self, queue_name: &str, options: ConsumerOptions) -> Result<Consumer> {
        self.inner.check_open()?;
        let id = self.inner.next_consumer_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        let window_size = options.window_size.unwrap_or(self.inner.options.consumer_window_size);

        let expected = if versions::before_address_change(self.inner.version()) {
            types::SESS_QUEUEQUERY_RESP_V2
        } else {
            types::SESS_QUEUEQUERY_RESP_V3
        };
        let packet = Packet::CreateConsumer {
            id,
            queue_name: queue_name.into(),
            filter_string: options.filter.as_deref().map(Into::into),
            priority: options.priority,
            browse_only: options.browse_only,
            requires_response: true,
        };
        let queue_info = match self.inner.send_blocking(packet, expected).await? {
            Packet::QueueQueryResponse { result, .. } => result,
            other => return Err(Error::UnexpectedPacket { expected, got: other.packet_type(0) }),
        };

        // ActiveMQSessionContext: the queue's default consumer window applies when the client
        // uses the default.
        let window_size = if options.window_size.is_none() && self.inner.options.consumer_window_size == DEFAULT_CONSUMER_WINDOW_SIZE {
            queue_info.default_consumer_window_size.unwrap_or(window_size)
        } else {
            window_size
        };
        let client_window = calc_window_size(window_size)?;
        let state = Arc::new(ConsumerState {
            tx,
            flow: Mutex::new(FlowState { client_window, credits_to_send: 0 }),
            large: Mutex::new(None),
        });
        self.inner.consumers.lock().unwrap().insert(id, state.clone());

        if client_window != 0 {
            self.inner.send(Packet::ConsumerFlowCredit { consumer_id: id, credits: window_size }).await?;
        }

        Ok(Consumer {
            session: self.inner.clone(),
            id,
            queue_name: queue_name.into(),
            state,
            rx: tokio::sync::Mutex::new(rx),
            queue_info,
            closed: AtomicBool::new(false),
            last_delivered: Mutex::new(None),
        })
    }

    // --- local transactions ------------------------------------------------------------------

    /// Commits the current local transaction (`SESS_COMMIT`, blocking).
    pub async fn commit(&self) -> Result<()> {
        self.inner.send_blocking_null(Packet::Commit { correlation_id: -1 }).await
    }

    /// Rolls back the current local transaction (`SESS_ROLLBACK`, blocking).
    ///
    /// Like the Java client this stops delivery, discards locally buffered messages (the broker
    /// redelivers them), rolls back and restarts delivery.
    pub async fn rollback(&self) -> Result<()> {
        self.rollback_with(false).await
    }

    /// Rollback variant of `ClientSession.rollback(considerLastMessageAsDelivered)`.
    pub async fn rollback_with(&self, consider_last_message_as_delivered: bool) -> Result<()> {
        let was_started = self.is_started();
        if was_started {
            self.stop().await?;
        }
        self.clear_consumer_buffers();
        let result = self.inner.send_blocking_null(Packet::Rollback { consider_last_message_as_delivered }).await;
        if was_started {
            self.start().await?;
        }
        result
    }

    fn clear_consumer_buffers(&self) {
        // Buffered-but-unconsumed messages are dropped by the consumer side on the next receive
        // (they are redelivered by the broker with an incremented delivery count). We signal
        // this by resetting large-message assemblies; regular buffered messages are drained by
        // Consumer::drain_buffer which is invoked lazily.
        for c in self.inner.consumers.lock().unwrap().values() {
            *c.large.lock().unwrap() = None;
        }
    }

    // --- XA ----------------------------------------------------------------------------------

    fn check_xa(&self) -> Result<()> {
        if !self.inner.options.xa {
            return Err(Error::IllegalState("session is not XA".into()));
        }
        Ok(())
    }

    /// `XAResource.start(xid, flags)`: `TMNOFLAGS`, `TMJOIN` or `TMRESUME` (see [`flags`]).
    pub async fn xa_start(&self, xid: &Xid, xa_flags: i32) -> Result<()> {
        self.check_xa()?;
        let packet = match xa_flags {
            flags::TMJOIN => Packet::XaJoin { xid: xid.clone() },
            flags::TMRESUME => Packet::XaResume { xid: xid.clone() },
            flags::TMNOFLAGS => Packet::XaStart { xid: xid.clone() },
            _ => return Err(Error::xa(crate::error::XaCode::XAER_INVAL.0, Some("invalid start flags".into()))),
        };
        self.inner.xa_call(packet).await.map(|_| ())
    }

    /// `XAResource.end(xid, flags)`: `TMSUCCESS`, `TMFAIL` or `TMSUSPEND`.
    pub async fn xa_end(&self, xid: &Xid, xa_flags: i32) -> Result<()> {
        self.check_xa()?;
        let packet = match xa_flags {
            flags::TMSUSPEND => Packet::XaSuspend,
            flags::TMSUCCESS => Packet::XaEnd { xid: xid.clone(), failed: false },
            flags::TMFAIL => Packet::XaEnd { xid: xid.clone(), failed: true },
            _ => return Err(Error::xa(crate::error::XaCode::XAER_INVAL.0, Some("invalid end flags".into()))),
        };
        self.inner.xa_call(packet).await.map(|_| ())
    }

    /// `XAResource.prepare(xid)`; returns `XA_OK` (0) or `XA_RDONLY` (3).
    pub async fn xa_prepare(&self, xid: &Xid) -> Result<i32> {
        self.check_xa()?;
        self.inner.xa_call(Packet::XaPrepare { xid: xid.clone() }).await
    }

    /// `XAResource.commit(xid, onePhase)`.
    pub async fn xa_commit(&self, xid: &Xid, one_phase: bool) -> Result<()> {
        self.check_xa()?;
        self.inner.xa_call(Packet::XaCommit { xid: xid.clone(), one_phase }).await.map(|_| ())
    }

    /// `XAResource.rollback(xid)`.
    pub async fn xa_rollback(&self, xid: &Xid) -> Result<()> {
        self.check_xa()?;
        let was_started = self.is_started();
        if was_started {
            self.stop().await?;
        }
        self.clear_consumer_buffers();
        let result = self.inner.xa_call(Packet::XaRollback { xid: xid.clone() }).await.map(|_| ());
        if was_started {
            self.start().await?;
        }
        result
    }

    /// `XAResource.forget(xid)`.
    pub async fn xa_forget(&self, xid: &Xid) -> Result<()> {
        self.check_xa()?;
        self.inner.xa_call(Packet::XaForget { xid: xid.clone() }).await.map(|_| ())
    }

    /// `XAResource.recover()`: the broker's in-doubt (prepared) transaction branches.
    pub async fn xa_recover(&self) -> Result<Vec<Xid>> {
        self.check_xa()?;
        match self.inner.send_blocking(Packet::XaGetInDoubtXids, types::SESS_XA_INDOUBT_XIDS_RESP).await? {
            Packet::XaGetInDoubtXidsResponse { xids } => Ok(xids),
            other => Err(Error::UnexpectedPacket { expected: types::SESS_XA_INDOUBT_XIDS_RESP, got: other.packet_type(0) }),
        }
    }

    /// `XAResource.setTransactionTimeout(seconds)`.
    pub async fn xa_set_transaction_timeout(&self, seconds: i32) -> Result<bool> {
        self.check_xa()?;
        match self.inner.send_blocking(Packet::XaSetTimeout { timeout_seconds: seconds }, types::SESS_XA_SET_TIMEOUT_RESP).await? {
            Packet::XaSetTimeoutResponse { ok } => Ok(ok),
            other => Err(Error::UnexpectedPacket { expected: types::SESS_XA_SET_TIMEOUT_RESP, got: other.packet_type(0) }),
        }
    }

    /// `XAResource.getTransactionTimeout()`.
    pub async fn xa_get_transaction_timeout(&self) -> Result<i32> {
        self.check_xa()?;
        match self.inner.send_blocking(Packet::XaGetTimeout, types::SESS_XA_GET_TIMEOUT_RESP).await? {
            Packet::XaGetTimeoutResponse { timeout_seconds } => Ok(timeout_seconds),
            other => Err(Error::UnexpectedPacket { expected: types::SESS_XA_GET_TIMEOUT_RESP, got: other.packet_type(0) }),
        }
    }

    /// Notifies the broker that the transaction failed after a connection failure
    /// (`SESS_XA_FAILED`, non-blocking).
    pub async fn xa_failed(&self, xid: &Xid) -> Result<()> {
        self.check_xa()?;
        self.inner.send(Packet::XaAfterFailed { xid: xid.clone() }).await.map(|_| ())
    }
}

/// `ActiveMQSessionContext.calcWindowSize`.
fn calc_window_size(window_size: i32) -> Result<i32> {
    match window_size {
        -1 => Ok(-1),
        0 => Ok(0),
        1 => Ok(1),
        n if n > 1 => Ok(n >> 1),
        n => Err(Error::IllegalState(format!("invalid consumer window size {n}"))),
    }
}

// =============================================================================================
// Producer
// =============================================================================================

/// A message producer bound to a session.
#[derive(Clone)]
pub struct Producer {
    session: Arc<SessionInner>,
    id: i32,
    address: Option<SimpleString>,
    closed: Arc<AtomicBool>,
}

impl Producer {
    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn address(&self) -> Option<&SimpleString> {
        self.address.as_ref()
    }

    /// Sends without waiting for a broker response.
    pub async fn send(&self, message: Message) -> Result<()> {
        self.do_send(message, false).await
    }

    /// Sends and waits for the broker's `NULL_RESPONSE` (or exception).
    pub async fn send_blocking(&self, message: Message) -> Result<()> {
        self.do_send(message, true).await
    }

    async fn do_send(&self, mut message: Message, blocking: bool) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed("producer closed".into()));
        }
        self.session.check_open()?;
        if message.address.is_none() {
            message.address = self.address.clone();
        }
        if message.address.is_none() && self.session.options.default_address.is_none() {
            return Err(Error::IllegalState("message has no address and the producer is anonymous".into()));
        }
        let version = self.session.version();
        if versions::before_address_change(version) {
            return Err(Error::IllegalState(format!("broker protocol version {version} (< 2.0) is not supported")));
        }
        let min_large = self.session.options.min_large_message_size;
        if min_large > 0 && message.body.len() > min_large as usize {
            return self.send_large(message, blocking).await;
        }
        let packet = Packet::Send { message, requires_response: blocking, correlation_id: -1, sender_id: self.id };
        if blocking {
            self.session.send_blocking_null(packet).await
        } else {
            self.session.send(packet).await.map(|_| ())
        }
    }

    /// `ClientProducerImpl.largeMessageSend`: headers first, then body chunks.
    async fn send_large(&self, message: Message, blocking: bool) -> Result<()> {
        let min_large = self.session.options.min_large_message_size.max(1) as usize;
        let mut headers = message.clone();
        let body = std::mem::take(&mut headers.body);
        if headers.headers_and_properties_len() >= min_large {
            return Err(Error::IllegalState("message headers and properties exceed the large message size".into()));
        }
        self.session.send(Packet::SendLarge { message: headers }).await?;

        let total = body.len();
        let mut pos = 0usize;
        loop {
            let end = (pos + min_large).min(total);
            let chunk = body[pos..end].to_vec();
            let last = end >= total;
            let requires_response = last && blocking;
            let packet = Packet::SendContinuation {
                body: chunk,
                continues: !last,
                message_body_size: if last { total as i64 } else { -1 },
                requires_response,
                correlation_id: -1,
                sender_id: self.id,
            };
            if requires_response {
                self.session.send_blocking_null(packet).await?;
            } else {
                self.session.send(packet).await?;
            }
            if last {
                break;
            }
            pos = end;
        }
        Ok(())
    }

    /// Removes the producer from the broker (`REMOVE_PRODUCER`, non-blocking).
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if !versions::before_producer_metrics(self.session.version()) && self.session.check_open().is_ok() {
            self.session.send(Packet::RemoveProducer { id: self.id }).await?;
        }
        Ok(())
    }
}

// =============================================================================================
// Consumer
// =============================================================================================

/// A message consumer. Messages are buffered locally up to the consumer window; call
/// [`Consumer::receive`] to take them.
pub struct Consumer {
    session: Arc<SessionInner>,
    id: i64,
    queue_name: SimpleString,
    state: Arc<ConsumerState>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Received>>,
    queue_info: QueueQueryResult,
    closed: AtomicBool,
    last_delivered: Mutex<Option<i64>>,
}

impl Consumer {
    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn queue_name(&self) -> &SimpleString {
        &self.queue_name
    }

    /// Queue information returned by the broker when the consumer was created.
    pub fn queue_info(&self) -> &QueueQueryResult {
        &self.queue_info
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed("consumer closed".into()));
        }
        self.session.check_open()
    }

    async fn deliver(&self, received: Received) -> Message {
        *self.last_delivered.lock().unwrap() = Some(received.message.message_id);
        self.session.flow_control(self.id, &self.state, received.packet_size).await;
        received.message
    }

    async fn before_wait(&self) -> Result<()> {
        self.check_open()?;
        if !self.session.started.load(Ordering::SeqCst) {
            debug!("receive called on a session that has not been started");
        }
        // Slow consumer (window 0): ask for exactly one message.
        let slow = self.state.flow.lock().unwrap().client_window == 0;
        if slow {
            self.session.send(Packet::ConsumerFlowCredit { consumer_id: self.id, credits: 1 }).await?;
        }
        Ok(())
    }

    /// Waits for the next message.
    pub async fn receive(&self) -> Result<Message> {
        self.before_wait().await?;
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(r) => Ok(self.deliver(r).await),
            None => Err(self.check_open().err().unwrap_or_else(|| Error::Closed("consumer disconnected".into()))),
        }
    }

    /// Waits up to `timeout` for the next message; `Ok(None)` on timeout.
    pub async fn receive_timeout(&self, timeout: Duration) -> Result<Option<Message>> {
        self.before_wait().await?;
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(r)) => Ok(Some(self.deliver(r).await)),
            Ok(None) => Err(self.check_open().err().unwrap_or_else(|| Error::Closed("consumer disconnected".into()))),
            Err(_) => Ok(None),
        }
    }

    /// Returns a buffered message without waiting.
    pub async fn try_receive(&self) -> Result<Option<Message>> {
        self.check_open()?;
        let mut rx = self.rx.lock().await;
        match rx.try_recv() {
            Ok(r) => Ok(Some(self.deliver(r).await)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(Error::Closed("consumer disconnected".into())),
        }
    }

    /// Discards locally buffered messages (they remain unacknowledged on the broker).
    pub async fn drain_buffer(&self) -> usize {
        let mut rx = self.rx.lock().await;
        let mut n = 0;
        while let Ok(r) = rx.try_recv() {
            self.session.flow_control(self.id, &self.state, r.packet_size).await;
            n += 1;
        }
        n
    }

    /// Acknowledges `message` and every message delivered to this consumer before it
    /// (`SESS_ACKNOWLEDGE`). In a transacted or XA session the ack is part of the transaction.
    pub async fn ack(&self, message: &Message) -> Result<()> {
        self.ack_id(message.message_id).await
    }

    /// Cumulative acknowledgement by message id.
    pub async fn ack_id(&self, message_id: i64) -> Result<()> {
        self.check_open()?;
        if self.session.options.pre_acknowledge {
            return Ok(());
        }
        let block = self.session.options.block_on_acknowledge && self.session.options.auto_commit_acks;
        let packet = Packet::Acknowledge { consumer_id: self.id, message_id, requires_response: block };
        if block {
            self.session.send_blocking_null(packet).await
        } else {
            self.session.send(packet).await.map(|_| ())
        }
    }

    /// Acknowledges only `message` (`SESS_INDIVIDUAL_ACKNOWLEDGE`).
    pub async fn individual_ack(&self, message: &Message) -> Result<()> {
        self.check_open()?;
        if self.session.options.pre_acknowledge {
            return Ok(());
        }
        let block = self.session.options.block_on_acknowledge && self.session.options.auto_commit_acks;
        let packet = Packet::IndividualAcknowledge { consumer_id: self.id, message_id: message.message_id, requires_response: block };
        if block {
            self.session.send_blocking_null(packet).await
        } else {
            self.session.send(packet).await.map(|_| ())
        }
    }

    /// Acknowledges the last message delivered by [`Consumer::receive`], if any.
    pub async fn ack_last(&self) -> Result<()> {
        let last = *self.last_delivered.lock().unwrap();
        match last {
            Some(id) => self.ack_id(id).await,
            None => Ok(()),
        }
    }

    /// Marks a message as expired on the broker (`SESS_EXPIRED`).
    pub async fn expire(&self, message: &Message) -> Result<()> {
        self.check_open()?;
        self.session.send(Packet::Expire { consumer_id: self.id, message_id: message.message_id }).await.map(|_| ())
    }

    /// Asks the broker to deliver whatever is available (`SESS_FORCE_CONSUMER_DELIVERY`).
    pub async fn force_delivery(&self, sequence: i64) -> Result<()> {
        self.check_open()?;
        self.session.send(Packet::ForceConsumerDelivery { consumer_id: self.id, sequence }).await.map(|_| ())
    }

    /// Closes the consumer on the broker (`SESS_CONSUMER_CLOSE`, blocking).
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.session.consumers.lock().unwrap().remove(&self.id);
        if self.session.check_open().is_ok() {
            self.session.send_blocking_null(Packet::ConsumerClose { consumer_id: self.id }).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_size_calculation() {
        assert_eq!(calc_window_size(-1).unwrap(), -1);
        assert_eq!(calc_window_size(0).unwrap(), 0);
        assert_eq!(calc_window_size(1).unwrap(), 1);
        assert_eq!(calc_window_size(1024 * 1024).unwrap(), 512 * 1024);
        assert!(calc_window_size(-2).is_err());
    }

    #[test]
    fn random_names_differ() {
        assert_ne!(random_name(), random_name());
    }
}
