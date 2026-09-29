//! A Rust port of the Apache ActiveMQ Artemis **CORE** wire protocol.
//!
//! The crate has two layers:
//!
//! * a pure codec ([`packet`], [`message`], [`properties`], [`buffer`], [`xid`]) that encodes and
//!   decodes every packet exchanged between a CORE client and the broker, byte-for-byte
//!   compatible with the Java implementation in `artemis-core-client`;
//! * an async client ([`client`]) built on Tokio that performs the `ARTEMIS` handshake, creates
//!   sessions, sends and receives (large) messages, manages queues and addresses, and drives
//!   local and XA transactions.

pub mod buffer;
pub mod client;
pub mod error;
pub mod message;
pub mod packet;
pub mod properties;
pub mod queue;
pub mod simple_string;
pub mod xid;

pub use client::{Connection, ConnectionOptions, Consumer, Producer, Session, SessionOptions};
pub use error::{Error, ExceptionType, Result, XaCode};
pub use message::{Message, RoutingType};
pub use packet::{Frame, Packet};
pub use properties::{PropertyValue, TypedProperties};
pub use queue::{AddressQueryResult, QueueConfiguration, QueueQueryResult};
pub use simple_string::SimpleString;
pub use xid::Xid;
