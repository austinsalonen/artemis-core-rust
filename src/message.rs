//! Port of `CoreMessage` wire encoding.
//!
//! Inside a `SESS_SEND` / `SESS_RECEIVE_MSG` packet (after the 13-byte packet header) a
//! message is laid out as:
//!
//! ```text
//! int  endOfBodyPosition   -- PACKET_HEADERS_SIZE + body length
//! byte[] body
//! long messageID
//! nullable SimpleString address
//! byte userID-null-flag [+ 16 bytes userID]
//! byte type
//! boolean durable
//! long expiration
//! long timestamp
//! byte priority
//! TypedProperties properties
//! ```
//!
//! Large messages carry only the "headers and properties" part (everything from `messageID`
//! on) in `SESS_SEND_LARGE` / `SESS_RECEIVE_LARGE_MSG`; the body follows in continuation
//! packets.

use bytes::BytesMut;

use crate::buffer::{Reader, WriteExt, NOT_NULL, NULL, PACKET_HEADERS_SIZE};
use crate::error::DecodeError;
use crate::properties::{PropertyValue, TypedProperties};
use crate::simple_string::SimpleString;

/// Message body type constants (`Message.DEFAULT_TYPE` etc).
pub mod types {
    pub const DEFAULT: i8 = 0;
    pub const OBJECT: i8 = 2;
    pub const TEXT: i8 = 3;
    pub const BYTES: i8 = 4;
    pub const MAP: i8 = 5;
    pub const STREAM: i8 = 6;
    pub const EMBEDDED: i8 = 7;
    pub const LARGE_EMBEDDED: i8 = 8;
}

/// Well-known header property names (`Message.HDR_*`).
pub mod headers {
    pub const ROUTE_TO_IDS: &str = "_AMQ_ROUTE_TO";
    pub const SCALEDOWN_TO_IDS: &str = "_AMQ_SCALEDOWN_TO";
    pub const ROUTE_TO_ACK_IDS: &str = "_AMQ_ACK_ROUTE_TO";
    pub const BRIDGE_DUPLICATE_ID: &str = "_AMQ_BRIDGE_DUP";
    pub const ACTUAL_EXPIRY_TIME: &str = "_AMQ_ACTUAL_EXPIRY";
    pub const ORIGINAL_ADDRESS: &str = "_AMQ_ORIG_ADDRESS";
    pub const ORIGINAL_QUEUE: &str = "_AMQ_ORIG_QUEUE";
    pub const ORIG_MESSAGE_ID: &str = "_AMQ_ORIG_MESSAGE_ID";
    pub const GROUP_ID: &str = "_AMQ_GROUP_ID";
    pub const GROUP_SEQUENCE: &str = "_AMQ_GROUP_SEQUENCE";
    pub const LARGE_COMPRESSED: &str = "_AMQ_LARGE_COMPRESSED";
    pub const LARGE_BODY_SIZE: &str = "_AMQ_LARGE_SIZE";
    pub const SCHEDULED_DELIVERY_TIME: &str = "_AMQ_SCHED_DELIVERY";
    pub const DUPLICATE_DETECTION_ID: &str = "_AMQ_DUPL_ID";
    pub const LAST_VALUE_NAME: &str = "_AMQ_LVQ_NAME";
    pub const CONTENT_TYPE: &str = "_AMQ_CONTENT_TYPE";
    pub const VALIDATED_USER: &str = "_AMQ_VALIDATED_USER";
    pub const ROUTING_TYPE: &str = "_AMQ_ROUTING_TYPE";
    pub const ORIG_ROUTING_TYPE: &str = "_AMQ_ORIG_ROUTING_TYPE";
    pub const INGRESS_TIMESTAMP: &str = "_AMQ_INGRESS_TIMESTAMP";
    pub const PREFIX: &str = "_AMQ_PREFIX";
}

/// Address routing type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RoutingType {
    Multicast,
    Anycast,
}

impl RoutingType {
    pub fn to_byte(self) -> i8 {
        match self {
            RoutingType::Multicast => 0,
            RoutingType::Anycast => 1,
        }
    }

    pub fn from_byte(b: i8) -> Option<RoutingType> {
        match b {
            0 => Some(RoutingType::Multicast),
            1 => Some(RoutingType::Anycast),
            _ => None,
        }
    }
}

/// A CORE message.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub message_id: i64,
    pub address: Option<SimpleString>,
    pub user_id: Option<[u8; 16]>,
    /// One of [`types`].
    pub message_type: i8,
    pub durable: bool,
    /// Absolute expiration time in milliseconds since the epoch, `0` for never.
    pub expiration: i64,
    /// Creation timestamp in milliseconds since the epoch.
    pub timestamp: i64,
    pub priority: i8,
    pub properties: TypedProperties,
    pub body: Vec<u8>,
    /// Delivery count as reported by the broker on receive (not part of the message encoding).
    pub delivery_count: i32,
    /// Set on receive for messages that arrived as large messages.
    pub large_message_size: Option<i64>,
}

impl Default for Message {
    fn default() -> Self {
        Message {
            message_id: 0,
            address: None,
            user_id: None,
            message_type: types::DEFAULT,
            durable: true,
            expiration: 0,
            timestamp: now_millis(),
            priority: 4,
            properties: TypedProperties::new(),
            body: Vec::new(),
            delivery_count: 0,
            large_message_size: None,
        }
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Message {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a `TEXT` message. The body is a nullable `SimpleString`, matching the JMS
    /// `TextMessage` layout used by Artemis.
    pub fn text(text: &str) -> Self {
        let mut m = Message::new();
        m.message_type = types::TEXT;
        m.set_text(text);
        m
    }

    /// Creates a `BYTES` message with a raw body.
    pub fn bytes(body: impl Into<Vec<u8>>) -> Self {
        let mut m = Message::new();
        m.message_type = types::BYTES;
        m.body = body.into();
        m
    }

    pub fn with_address(mut self, address: impl Into<SimpleString>) -> Self {
        self.address = Some(address.into());
        self
    }

    pub fn with_durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    pub fn with_priority(mut self, priority: i8) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_expiration(mut self, expiration_millis: i64) -> Self {
        self.expiration = expiration_millis;
        self
    }

    pub fn with_property(mut self, key: &str, value: impl Into<PropertyValue>) -> Self {
        self.properties.put(key, value);
        self
    }

    pub fn with_routing_type(self, routing_type: RoutingType) -> Self {
        self.with_property(headers::ROUTING_TYPE, PropertyValue::Byte(routing_type.to_byte()))
    }

    pub fn with_group_id(self, group: &str) -> Self {
        self.with_property(headers::GROUP_ID, group)
    }

    pub fn with_duplicate_id(self, id: &str) -> Self {
        self.with_property(headers::DUPLICATE_DETECTION_ID, id)
    }

    pub fn with_scheduled_delivery_time(self, millis: i64) -> Self {
        self.with_property(headers::SCHEDULED_DELIVERY_TIME, millis)
    }

    /// Replaces the body with a nullable `SimpleString` and marks the message as `TEXT`.
    pub fn set_text(&mut self, text: &str) {
        let mut b = BytesMut::new();
        b.write_nullable_simple_string(Some(&SimpleString::new(text)));
        self.body = b.to_vec();
        self.message_type = types::TEXT;
    }

    /// Reads the body as a nullable `SimpleString` (the `TEXT` message layout).
    pub fn text_body(&self) -> Result<Option<String>, DecodeError> {
        if self.body.is_empty() {
            return Ok(None);
        }
        let mut r = Reader::new(&self.body);
        Ok(r.read_nullable_simple_string()?.map(|s| s.to_string_lossy()))
    }

    /// Returns the raw body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn routing_type(&self) -> Option<RoutingType> {
        match self.properties.get(headers::ROUTING_TYPE)? {
            PropertyValue::Byte(b) => RoutingType::from_byte(*b),
            _ => None,
        }
    }

    pub fn is_large(&self) -> bool {
        self.large_message_size.is_some()
    }

    // -----------------------------------------------------------------------------------------
    // Encoding
    // -----------------------------------------------------------------------------------------

    pub fn headers_and_properties_len(&self) -> usize {
        8 // messageID
            + 1 + self.address.as_ref().map(|a| a.wire_size()).unwrap_or(0)
            + 1 + if self.user_id.is_some() { 16 } else { 0 }
            + 1 // type
            + 1 // durable
            + 8 // expiration
            + 8 // timestamp
            + 1 // priority
            + self.properties.encoded_len()
    }

    /// Total encoded size of the message (as `CoreMessage.getEncodeSize()`).
    pub fn encoded_len(&self) -> usize {
        4 + self.body.len() + self.headers_and_properties_len()
    }

    pub fn encode_headers_and_properties(&self, out: &mut BytesMut) {
        out.write_i64(self.message_id);
        out.write_nullable_simple_string(self.address.as_ref());
        match &self.user_id {
            None => out.write_u8(NULL),
            Some(uid) => {
                out.write_u8(NOT_NULL);
                out.write_bytes(uid);
            }
        }
        out.write_i8(self.message_type);
        out.write_bool(self.durable);
        out.write_i64(self.expiration);
        out.write_i64(self.timestamp);
        out.write_i8(self.priority);
        self.properties.encode(out);
    }

    pub fn decode_headers_and_properties(r: &mut Reader<'_>) -> Result<Message, DecodeError> {
        let message_id = r.read_i64()?;
        let address = r.read_nullable_simple_string()?;
        let user_id = if r.read_u8()? == NULL {
            None
        } else {
            let b = r.read_bytes(16)?;
            let mut uid = [0u8; 16];
            uid.copy_from_slice(b);
            Some(uid)
        };
        let message_type = r.read_i8()?;
        let durable = r.read_bool()?;
        let expiration = r.read_i64()?;
        let timestamp = r.read_i64()?;
        let priority = r.read_i8()?;
        let properties = TypedProperties::decode(r)?;
        Ok(Message {
            message_id,
            address,
            user_id,
            message_type,
            durable,
            expiration,
            timestamp,
            priority,
            properties,
            body: Vec::new(),
            delivery_count: 0,
            large_message_size: None,
        })
    }

    /// Full message encoding (body + headers + properties) as embedded in send/receive packets.
    pub fn encode(&self, out: &mut BytesMut) {
        let end_of_body = PACKET_HEADERS_SIZE + self.body.len();
        out.write_i32(end_of_body as i32);
        out.write_bytes(&self.body);
        self.encode_headers_and_properties(out);
    }

    /// Decodes a full message; leaves the reader positioned after the properties.
    pub fn decode(r: &mut Reader<'_>) -> Result<Message, DecodeError> {
        let end_of_body = r.read_i32()?;
        let body_len = end_of_body as i64 - PACKET_HEADERS_SIZE as i64;
        if body_len < 0 {
            return Err(DecodeError::Invalid(format!("invalid endOfBodyPosition {end_of_body}")));
        }
        let body = r.read_bytes(body_len as usize)?.to_vec();
        let mut m = Message::decode_headers_and_properties(r)?;
        m.body = body;
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_message_round_trip() {
        let mut m = Message::text("hello").with_address("myqueue").with_property("k", 5i32);
        m.message_id = 42;
        m.user_id = Some([7u8; 16]);
        let mut b = BytesMut::new();
        m.encode(&mut b);
        assert_eq!(b.len(), m.encoded_len());
        let mut r = Reader::new(&b);
        let d = Message::decode(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(d, m);
        assert_eq!(d.text_body().unwrap().as_deref(), Some("hello"));
        assert_eq!(d.properties.get_i32("k"), Some(5));
    }

    #[test]
    fn empty_body_layout() {
        let mut m = Message::new();
        m.timestamp = 0;
        let mut b = BytesMut::new();
        m.encode(&mut b);
        // endOfBodyPosition == PACKET_HEADERS_SIZE for an empty body
        assert_eq!(&b[..4], &[0, 0, 0, 13]);
        let mut r = Reader::new(&b);
        let d = Message::decode(&mut r).unwrap();
        assert!(d.body.is_empty());
    }
}
