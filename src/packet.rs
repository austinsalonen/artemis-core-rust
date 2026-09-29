//! CORE protocol packets: type constants, versions, and the encode/decode of every packet a
//! client exchanges with the broker (port of `PacketImpl` + `wireformat/*` + `PacketDecoder`).
//!
//! Frame layout on the wire:
//!
//! ```text
//! int  length      -- number of bytes that follow (not counting this int)
//! byte type
//! long channelID
//! ...  packet-specific fields
//! ```
//!
//! Several packets changed shape over time. The broker version negotiated on the connection
//! (`CreateSessionResponse.server_version`) decides which shape is used; see [`versions`].

#![allow(clippy::field_reassign_with_default)]

use bytes::BytesMut;

use crate::buffer::{Reader, WriteExt, PACKET_HEADERS_SIZE};
use crate::error::DecodeError;
use crate::message::{Message, RoutingType};
use crate::queue::{AddressQueryResult, QueueConfiguration, QueueQueryResult, TransportConfiguration, TransportParam};
use crate::simple_string::SimpleString;
use crate::xid::Xid;

/// Packet type constants (`PacketImpl.*`).
#[allow(non_upper_case_globals)]
pub mod types {
    pub const PING: i8 = 10;
    pub const DISCONNECT: i8 = 11;
    pub const DISCONNECT_CONSUMER: i8 = 12;
    pub const DISCONNECT_CONSUMER_KILL: i8 = 13;
    pub const EXCEPTION: i8 = 20;
    pub const NULL_RESPONSE: i8 = 21;
    pub const PACKETS_CONFIRMED: i8 = 22;
    pub const CREATESESSION: i8 = 30;
    pub const CREATESESSION_RESP: i8 = 31;
    pub const REATTACH_SESSION: i8 = 32;
    pub const REATTACH_SESSION_RESP: i8 = 33;
    pub const CREATE_QUEUE: i8 = 34;
    pub const DELETE_QUEUE: i8 = 35;
    pub const CREATE_SHARED_QUEUE: i8 = 36;
    pub const SESS_XA_FAILED: i8 = 39;
    pub const SESS_CREATECONSUMER: i8 = 40;
    pub const SESS_ACKNOWLEDGE: i8 = 41;
    pub const SESS_EXPIRED: i8 = 42;
    pub const SESS_COMMIT: i8 = 43;
    pub const SESS_ROLLBACK: i8 = 44;
    pub const SESS_QUEUEQUERY: i8 = 45;
    pub const SESS_QUEUEQUERY_RESP: i8 = 46;
    pub const SESS_BINDINGQUERY: i8 = 49;
    pub const SESS_BINDINGQUERY_RESP: i8 = 50;
    pub const SESS_XA_START: i8 = 51;
    pub const SESS_XA_END: i8 = 52;
    pub const SESS_XA_COMMIT: i8 = 53;
    pub const SESS_XA_PREPARE: i8 = 54;
    pub const SESS_XA_RESP: i8 = 55;
    pub const SESS_XA_ROLLBACK: i8 = 56;
    pub const SESS_XA_JOIN: i8 = 57;
    pub const SESS_XA_SUSPEND: i8 = 58;
    pub const SESS_XA_RESUME: i8 = 59;
    pub const SESS_XA_FORGET: i8 = 60;
    pub const SESS_XA_INDOUBT_XIDS: i8 = 61;
    pub const SESS_XA_INDOUBT_XIDS_RESP: i8 = 62;
    pub const SESS_XA_SET_TIMEOUT: i8 = 63;
    pub const SESS_XA_SET_TIMEOUT_RESP: i8 = 64;
    pub const SESS_XA_GET_TIMEOUT: i8 = 65;
    pub const SESS_XA_GET_TIMEOUT_RESP: i8 = 66;
    pub const SESS_START: i8 = 67;
    pub const SESS_STOP: i8 = 68;
    pub const SESS_CLOSE: i8 = 69;
    pub const SESS_FLOWTOKEN: i8 = 70;
    pub const SESS_SEND: i8 = 71;
    pub const SESS_SEND_LARGE: i8 = 72;
    pub const SESS_SEND_CONTINUATION: i8 = 73;
    pub const SESS_CONSUMER_CLOSE: i8 = 74;
    pub const SESS_RECEIVE_MSG: i8 = 75;
    pub const SESS_RECEIVE_LARGE_MSG: i8 = 76;
    pub const SESS_RECEIVE_CONTINUATION: i8 = 77;
    pub const SESS_FORCE_CONSUMER_DELIVERY: i8 = 78;
    pub const SESS_PRODUCER_REQUEST_CREDITS: i8 = 79;
    pub const SESS_PRODUCER_CREDITS: i8 = 80;
    pub const SESS_INDIVIDUAL_ACKNOWLEDGE: i8 = 81;
    pub const SESS_PRODUCER_FAIL_CREDITS: i8 = 82;
    pub const REPLICATION_RESPONSE: i8 = 90;
    pub const REPLICATION_APPEND: i8 = 91;
    pub const REPLICATION_APPEND_TX: i8 = 92;
    pub const REPLICATION_DELETE: i8 = 93;
    pub const REPLICATION_DELETE_TX: i8 = 94;
    pub const REPLICATION_PREPARE: i8 = 95;
    pub const REPLICATION_COMMIT_ROLLBACK: i8 = 96;
    pub const REPLICATION_PAGE_WRITE: i8 = 97;
    pub const REPLICATION_PAGE_EVENT: i8 = 98;
    pub const REPLICATION_LARGE_MESSAGE_BEGIN: i8 = 99;
    pub const REPLICATION_LARGE_MESSAGE_END: i8 = 100;
    pub const REPLICATION_LARGE_MESSAGE_WRITE: i8 = 101;
    pub const REPLICATION_SYNC_FILE: i8 = 103;
    pub const SESS_ADD_METADATA: i8 = 104;
    pub const SESS_ADD_METADATA2: i8 = 105;
    pub const SESS_UNIQUE_ADD_METADATA: i8 = 106;
    pub const CLUSTER_TOPOLOGY: i8 = 110;
    pub const NODE_ANNOUNCE: i8 = 111;
    pub const SUBSCRIBE_TOPOLOGY: i8 = 112;
    pub const SUBSCRIBE_TOPOLOGY_V2: i8 = 113;
    pub const CLUSTER_TOPOLOGY_V2: i8 = 114;
    pub const BACKUP_REGISTRATION: i8 = 115;
    pub const BACKUP_REGISTRATION_FAILED: i8 = 116;
    pub const REPLICATION_START_FINISH_SYNC: i8 = 120;
    pub const REPLICATION_SCHEDULED_FAILOVER: i8 = 121;
    pub const CLUSTER_TOPOLOGY_V3: i8 = 122;
    pub const DISCONNECT_V2: i8 = 124;
    pub const CLUSTER_CONNECT: i8 = 125;
    pub const CLUSTER_CONNECT_REPLY: i8 = 126;
    pub const BACKUP_REQUEST: i8 = 127;
    pub const BACKUP_REQUEST_RESPONSE: i8 = -1;
    pub const QUORUM_VOTE: i8 = -2;
    pub const QUORUM_VOTE_REPLY: i8 = -3;
    /// Formerly `CHECK_FOR_FAILOVER`.
    pub const CONNECT: i8 = -4;
    /// Formerly `CHECK_FOR_FAILOVER_REPLY`.
    pub const CONNECT_RESPONSE: i8 = -5;
    pub const SCALEDOWN_ANNOUNCEMENT: i8 = -6;
    pub const SESS_QUEUEQUERY_RESP_V2: i8 = -7;
    pub const SESS_BINDINGQUERY_RESP_V2: i8 = -8;
    pub const REPLICATION_RESPONSE_V2: i8 = -9;
    pub const SESS_BINDINGQUERY_RESP_V3: i8 = -10;
    pub const CREATE_ADDRESS: i8 = -11;
    pub const CREATE_QUEUE_V2: i8 = -12;
    pub const CREATE_SHARED_QUEUE_V2: i8 = -13;
    pub const SESS_QUEUEQUERY_RESP_V3: i8 = -14;
    pub const SESS_BINDINGQUERY_RESP_V4: i8 = -15;
    pub const FEDERATION_DOWNSTREAM_CONNECT: i8 = -16;
    pub const CLUSTER_TOPOLOGY_V4: i8 = -17;
    pub const CREATESESSION_V2: i8 = -18;
    pub const DISCONNECT_V3: i8 = -19;
    pub const CREATE_PRODUCER: i8 = -20;
    pub const REMOVE_PRODUCER: i8 = -21;
    pub const SESS_BINDINGQUERY_RESP_V5: i8 = -22;
}

/// Protocol "incrementing version" constants and the gates that depend on them.
pub mod versions {
    /// 2.0.0
    pub const ADDRESSING_CHANGE_VERSION: i32 = 129;
    /// 2.7.0
    pub const ARTEMIS_2_7_0_VERSION: i32 = 130;
    pub const ASYNC_RESPONSE_CHANGE_VERSION: i32 = ARTEMIS_2_7_0_VERSION;
    pub const CONSUMER_PRIORITY_CHANGE_VERSION: i32 = ARTEMIS_2_7_0_VERSION;
    pub const FQQN_CHANGE_VERSION: i32 = ARTEMIS_2_7_0_VERSION;
    /// 2.18.0
    pub const ARTEMIS_2_18_0_VERSION: i32 = 131;
    /// 2.21.0
    pub const ARTEMIS_2_21_0_VERSION: i32 = 132;
    /// 2.24.0
    pub const ARTEMIS_2_24_0_VERSION: i32 = 133;
    /// 2.28.0
    pub const ARTEMIS_2_28_0_VERSION: i32 = 134;
    /// 2.29.0
    pub const ARTEMIS_2_29_0_VERSION: i32 = 135;
    /// 2.37.0
    pub const ARTEMIS_2_37_0_VERSION: i32 = 136;
    /// 2.58.0
    pub const ARTEMIS_2_58_0_VERSION: i32 = 137;

    /// The newest protocol version this crate speaks.
    pub const CURRENT: i32 = ARTEMIS_2_58_0_VERSION;

    /// Client versions to try when creating a session, newest first (as the Java client does).
    /// Versions before [`ADDRESSING_CHANGE_VERSION`] use the 1.x message layout and are not
    /// supported by this port.
    pub const CLIENT_VERSIONS: &[i32] = &[137, 136, 135, 134, 133, 132, 131, 130, 129];

    pub fn before_address_change(v: i32) -> bool {
        v > 0 && v < ADDRESSING_CHANGE_VERSION
    }
    pub fn before_async_response_change(v: i32) -> bool {
        v > 0 && v < ASYNC_RESPONSE_CHANGE_VERSION
    }
    pub fn supports_consumer_priority(v: i32) -> bool {
        v >= CONSUMER_PRIORITY_CHANGE_VERSION
    }
    pub fn supports_client_id(v: i32) -> bool {
        v >= ARTEMIS_2_18_0_VERSION
    }
    pub fn supports_commit_v2(v: i32) -> bool {
        v >= ARTEMIS_2_21_0_VERSION
    }
    pub fn before_producer_metrics(v: i32) -> bool {
        v < ARTEMIS_2_28_0_VERSION
    }

    /// `ChannelImpl.supports(packetType, version)`.
    pub fn channel_supports(packet_type: i8, v: i32) -> bool {
        use super::types::*;
        match packet_type {
            CLUSTER_TOPOLOGY_V2 => v >= 122,
            DISCONNECT_CONSUMER => v >= 124,
            CLUSTER_TOPOLOGY_V3 | DISCONNECT_V2 => v >= 125,
            SESS_QUEUEQUERY_RESP_V2 | SESS_BINDINGQUERY_RESP_V2 => v >= 126,
            SESS_BINDINGQUERY_RESP_V3 => v >= 127,
            SESS_QUEUEQUERY_RESP_V3 | SESS_BINDINGQUERY_RESP_V4 => v >= ADDRESSING_CHANGE_VERSION,
            CLUSTER_TOPOLOGY_V4 | CREATESESSION_V2 | DISCONNECT_V3 => v >= ARTEMIS_2_18_0_VERSION,
            SESS_BINDINGQUERY_RESP_V5 => v >= ARTEMIS_2_29_0_VERSION,
            _ => true,
        }
    }
}

/// Well-known channel ids (`ChannelImpl.CHANNEL_ID`).
pub mod channels {
    pub const PING: i64 = 0;
    pub const SESSION: i64 = 1;
    pub const REPLICATION: i64 = 2;
    pub const CLUSTER: i64 = 3;
    pub const FEDERATION: i64 = 4;
    /// First id used for user (session) channels.
    pub const USER: i64 = 10;
}

/// Reasons carried by `DISCONNECT_V3` (`DisconnectReason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisconnectReason {
    Redirect,
    RedirectOnUnavailable,
    ScaleDown,
    ScaleDownOnUnavailable,
    ShutDown,
    ShutDownOnUnavailable,
    Unknown(i8),
}

impl DisconnectReason {
    pub fn from_byte(b: i8) -> Self {
        match b {
            0 => DisconnectReason::Redirect,
            1 => DisconnectReason::RedirectOnUnavailable,
            2 => DisconnectReason::ScaleDown,
            3 => DisconnectReason::ScaleDownOnUnavailable,
            4 => DisconnectReason::ShutDown,
            5 => DisconnectReason::ShutDownOnUnavailable,
            other => DisconnectReason::Unknown(other),
        }
    }

    pub fn to_byte(self) -> i8 {
        match self {
            DisconnectReason::Redirect => 0,
            DisconnectReason::RedirectOnUnavailable => 1,
            DisconnectReason::ScaleDown => 2,
            DisconnectReason::ScaleDownOnUnavailable => 3,
            DisconnectReason::ShutDown => 4,
            DisconnectReason::ShutDownOnUnavailable => 5,
            DisconnectReason::Unknown(b) => b,
        }
    }
}

/// Any CORE packet (payload only; the channel id lives in [`Frame`]).
#[derive(Clone, Debug, PartialEq)]
pub enum Packet {
    // --- connection level -------------------------------------------------------------------
    Ping { connection_ttl: i64 },
    Disconnect { node_id: Option<SimpleString> },
    DisconnectV2 { node_id: Option<SimpleString>, scale_down_node_id: Option<SimpleString> },
    DisconnectV3 {
        node_id: Option<SimpleString>,
        reason: Option<DisconnectReason>,
        target_node_id: Option<SimpleString>,
        target_connector: Option<TransportConfiguration>,
    },
    DisconnectConsumer { consumer_id: i64 },
    DisconnectConsumerWithKill { node_id: Option<SimpleString> },
    /// `ActiveMQExceptionMessage` (V2 adds `correlation_id` when the version supports it).
    Exception { code: i32, message: Option<String>, correlation_id: i64 },
    NullResponse { correlation_id: i64 },
    PacketsConfirmed { command_id: i32 },
    Connect {
        node_id: Option<String>,
        client_version: i32,
        auth_mechanism: Option<String>,
        auth_data: Option<Vec<u8>>,
    },
    ConnectResponse { ok_to_failover: bool, server_version: i32 },
    CreateSession {
        name: String,
        session_channel_id: i64,
        version: i32,
        username: Option<String>,
        password: Option<String>,
        min_large_message_size: i32,
        xa: bool,
        auto_commit_sends: bool,
        auto_commit_acks: bool,
        pre_acknowledge: bool,
        window_size: i32,
        default_address: Option<String>,
        /// When `Some`, the `CREATESESSION_V2` shape (with client id) is used.
        client_id: Option<Option<String>>,
    },
    CreateSessionResponse { server_version: i32 },
    ReattachSession { name: String, last_confirmed_command_id: i32 },
    ReattachSessionResponse { last_confirmed_command_id: i32, reattached: bool },

    // --- queues / addresses ------------------------------------------------------------------
    CreateAddress { address: SimpleString, routing_types: Vec<RoutingType>, requires_response: bool, auto_created: bool },
    /// `CREATE_QUEUE` (v2 = false) or `CREATE_QUEUE_V2`.
    CreateQueue { config: QueueConfiguration, requires_response: bool, v2: bool },
    /// `CREATE_SHARED_QUEUE` (v2 = false) or `CREATE_SHARED_QUEUE_V2`.
    CreateSharedQueue { config: QueueConfiguration, requires_response: bool, v2: bool },
    DeleteQueue { queue_name: SimpleString },
    QueueQuery { queue_name: SimpleString },
    /// `SESS_QUEUEQUERY_RESP` (version 1), `_V2` (2) or `_V3` (3).
    QueueQueryResponse { version: u8, result: QueueQueryResult },
    BindingQuery { address: SimpleString },
    /// `SESS_BINDINGQUERY_RESP` (version 1) up to `_V5` (5).
    BindingQueryResponse { version: u8, result: AddressQueryResult },

    // --- session -----------------------------------------------------------------------------
    CreateConsumer {
        id: i64,
        queue_name: SimpleString,
        filter_string: Option<SimpleString>,
        priority: i32,
        browse_only: bool,
        requires_response: bool,
    },
    Acknowledge { consumer_id: i64, message_id: i64, requires_response: bool },
    IndividualAcknowledge { consumer_id: i64, message_id: i64, requires_response: bool },
    Expire { consumer_id: i64, message_id: i64 },
    Commit { correlation_id: i64 },
    Rollback { consider_last_message_as_delivered: bool },
    SessionStart,
    SessionStop,
    SessionClose,
    ConsumerFlowCredit { consumer_id: i64, credits: i32 },
    ConsumerClose { consumer_id: i64 },
    ForceConsumerDelivery { consumer_id: i64, sequence: i64 },
    RequestProducerCredits { credits: i32, address: SimpleString },
    ProducerCredits { credits: i32, address: SimpleString },
    ProducerCreditsFail { credits: i32, address: SimpleString },
    CreateProducer { id: i32, address: Option<SimpleString> },
    RemoveProducer { id: i32 },
    AddMetaData { key: String, data: String },
    AddMetaDataV2 { key: String, data: String, requires_confirmation: bool },
    UniqueAddMetaData { key: String, data: String, requires_confirmation: bool },

    // --- messages ----------------------------------------------------------------------------
    /// `SESS_SEND` (V1/V2/V3 depending on version).
    Send { message: Message, requires_response: bool, correlation_id: i64, sender_id: i32 },
    /// `SESS_SEND_LARGE`: headers and properties only.
    SendLarge { message: Message },
    /// `SESS_SEND_CONTINUATION` (V1/V2/V3 depending on version).
    SendContinuation {
        body: Vec<u8>,
        continues: bool,
        /// Only encoded on the last chunk (`continues == false`).
        message_body_size: i64,
        requires_response: bool,
        correlation_id: i64,
        sender_id: i32,
    },
    Receive { consumer_id: i64, delivery_count: i32, message: Message },
    ReceiveLarge { consumer_id: i64, delivery_count: i32, large_message_size: i64, message: Message },
    ReceiveContinuation { body: Vec<u8>, continues: bool, consumer_id: i64 },

    // --- XA ----------------------------------------------------------------------------------
    XaStart { xid: Xid },
    XaEnd { xid: Xid, failed: bool },
    XaCommit { xid: Xid, one_phase: bool },
    XaPrepare { xid: Xid },
    XaRollback { xid: Xid },
    XaJoin { xid: Xid },
    XaResume { xid: Xid },
    XaForget { xid: Xid },
    XaAfterFailed { xid: Xid },
    XaSuspend,
    XaResponse { error: bool, response_code: i32, message: Option<String>, correlation_id: i64 },
    XaGetInDoubtXids,
    XaGetInDoubtXidsResponse { xids: Vec<Xid> },
    XaSetTimeout { timeout_seconds: i32 },
    XaSetTimeoutResponse { ok: bool },
    XaGetTimeout,
    XaGetTimeoutResponse { timeout_seconds: i32 },

    /// A packet type this crate does not model (e.g. cluster topology); payload kept verbatim.
    Unknown { packet_type: i8, payload: Vec<u8> },
}

/// A packet together with the channel it travels on.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    pub channel_id: i64,
    pub packet: Packet,
}

impl Packet {
    /// The type byte this packet is encoded with for a given negotiated protocol version.
    pub fn packet_type(&self, _version: i32) -> i8 {
        use types::*;
        match self {
            Packet::Ping { .. } => PING,
            Packet::Disconnect { .. } => DISCONNECT,
            Packet::DisconnectV2 { .. } => DISCONNECT_V2,
            Packet::DisconnectV3 { .. } => DISCONNECT_V3,
            Packet::DisconnectConsumer { .. } => DISCONNECT_CONSUMER,
            Packet::DisconnectConsumerWithKill { .. } => DISCONNECT_CONSUMER_KILL,
            Packet::Exception { .. } => EXCEPTION,
            Packet::NullResponse { .. } => NULL_RESPONSE,
            Packet::PacketsConfirmed { .. } => PACKETS_CONFIRMED,
            Packet::Connect { .. } => CONNECT,
            Packet::ConnectResponse { .. } => CONNECT_RESPONSE,
            Packet::CreateSession { client_id: None, .. } => CREATESESSION,
            Packet::CreateSession { client_id: Some(_), .. } => CREATESESSION_V2,
            Packet::CreateSessionResponse { .. } => CREATESESSION_RESP,
            Packet::ReattachSession { .. } => REATTACH_SESSION,
            Packet::ReattachSessionResponse { .. } => REATTACH_SESSION_RESP,
            Packet::CreateAddress { .. } => CREATE_ADDRESS,
            Packet::CreateQueue { v2: false, .. } => CREATE_QUEUE,
            Packet::CreateQueue { v2: true, .. } => CREATE_QUEUE_V2,
            Packet::CreateSharedQueue { v2: false, .. } => CREATE_SHARED_QUEUE,
            Packet::CreateSharedQueue { v2: true, .. } => CREATE_SHARED_QUEUE_V2,
            Packet::DeleteQueue { .. } => DELETE_QUEUE,
            Packet::QueueQuery { .. } => SESS_QUEUEQUERY,
            Packet::QueueQueryResponse { version, .. } => match version {
                1 => SESS_QUEUEQUERY_RESP,
                2 => SESS_QUEUEQUERY_RESP_V2,
                _ => SESS_QUEUEQUERY_RESP_V3,
            },
            Packet::BindingQuery { .. } => SESS_BINDINGQUERY,
            Packet::BindingQueryResponse { version, .. } => match version {
                1 => SESS_BINDINGQUERY_RESP,
                2 => SESS_BINDINGQUERY_RESP_V2,
                3 => SESS_BINDINGQUERY_RESP_V3,
                4 => SESS_BINDINGQUERY_RESP_V4,
                _ => SESS_BINDINGQUERY_RESP_V5,
            },
            Packet::CreateConsumer { .. } => SESS_CREATECONSUMER,
            Packet::Acknowledge { .. } => SESS_ACKNOWLEDGE,
            Packet::IndividualAcknowledge { .. } => SESS_INDIVIDUAL_ACKNOWLEDGE,
            Packet::Expire { .. } => SESS_EXPIRED,
            Packet::Commit { .. } => SESS_COMMIT,
            Packet::Rollback { .. } => SESS_ROLLBACK,
            Packet::SessionStart => SESS_START,
            Packet::SessionStop => SESS_STOP,
            Packet::SessionClose => SESS_CLOSE,
            Packet::ConsumerFlowCredit { .. } => SESS_FLOWTOKEN,
            Packet::ConsumerClose { .. } => SESS_CONSUMER_CLOSE,
            Packet::ForceConsumerDelivery { .. } => SESS_FORCE_CONSUMER_DELIVERY,
            Packet::RequestProducerCredits { .. } => SESS_PRODUCER_REQUEST_CREDITS,
            Packet::ProducerCredits { .. } => SESS_PRODUCER_CREDITS,
            Packet::ProducerCreditsFail { .. } => SESS_PRODUCER_FAIL_CREDITS,
            Packet::CreateProducer { .. } => CREATE_PRODUCER,
            Packet::RemoveProducer { .. } => REMOVE_PRODUCER,
            Packet::AddMetaData { .. } => SESS_ADD_METADATA,
            Packet::AddMetaDataV2 { .. } => SESS_ADD_METADATA2,
            Packet::UniqueAddMetaData { .. } => SESS_UNIQUE_ADD_METADATA,
            Packet::Send { .. } => SESS_SEND,
            Packet::SendLarge { .. } => SESS_SEND_LARGE,
            Packet::SendContinuation { .. } => SESS_SEND_CONTINUATION,
            Packet::Receive { .. } => SESS_RECEIVE_MSG,
            Packet::ReceiveLarge { .. } => SESS_RECEIVE_LARGE_MSG,
            Packet::ReceiveContinuation { .. } => SESS_RECEIVE_CONTINUATION,
            Packet::XaStart { .. } => SESS_XA_START,
            Packet::XaEnd { .. } => SESS_XA_END,
            Packet::XaCommit { .. } => SESS_XA_COMMIT,
            Packet::XaPrepare { .. } => SESS_XA_PREPARE,
            Packet::XaRollback { .. } => SESS_XA_ROLLBACK,
            Packet::XaJoin { .. } => SESS_XA_JOIN,
            Packet::XaResume { .. } => SESS_XA_RESUME,
            Packet::XaForget { .. } => SESS_XA_FORGET,
            Packet::XaAfterFailed { .. } => SESS_XA_FAILED,
            Packet::XaSuspend => SESS_XA_SUSPEND,
            Packet::XaResponse { .. } => SESS_XA_RESP,
            Packet::XaGetInDoubtXids => SESS_XA_INDOUBT_XIDS,
            Packet::XaGetInDoubtXidsResponse { .. } => SESS_XA_INDOUBT_XIDS_RESP,
            Packet::XaSetTimeout { .. } => SESS_XA_SET_TIMEOUT,
            Packet::XaSetTimeoutResponse { .. } => SESS_XA_SET_TIMEOUT_RESP,
            Packet::XaGetTimeout => SESS_XA_GET_TIMEOUT,
            Packet::XaGetTimeoutResponse { .. } => SESS_XA_GET_TIMEOUT_RESP,
            Packet::Unknown { packet_type, .. } => *packet_type,
        }
    }

    /// `Packet.isResponse()`.
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            Packet::ConnectResponse { .. }
                | Packet::CreateSessionResponse { .. }
                | Packet::ReattachSessionResponse { .. }
                | Packet::NullResponse { .. }
                | Packet::Exception { .. }
                | Packet::QueueQueryResponse { .. }
                | Packet::BindingQueryResponse { .. }
                | Packet::XaResponse { .. }
                | Packet::XaGetInDoubtXidsResponse { .. }
                | Packet::XaSetTimeoutResponse { .. }
                | Packet::XaGetTimeoutResponse { .. }
        )
    }

    /// `Packet.isRequiresConfirmations()`: whether this packet counts towards the
    /// confirmation window of its channel.
    pub fn requires_confirmations(&self) -> bool {
        match self {
            Packet::Ping { .. }
            | Packet::Disconnect { .. }
            | Packet::DisconnectV2 { .. }
            | Packet::DisconnectV3 { .. }
            | Packet::PacketsConfirmed { .. }
            | Packet::CreateSession { .. }
            | Packet::CreateSessionResponse { .. }
            | Packet::ReattachSession { .. }
            | Packet::ReattachSessionResponse { .. }
            | Packet::AddMetaData { .. } => false,
            Packet::AddMetaDataV2 { requires_confirmation, .. }
            | Packet::UniqueAddMetaData { requires_confirmation, .. } => *requires_confirmation,
            _ => true,
        }
    }

    /// Whether the packet carries a correlation id in the given protocol version.
    pub fn supports_correlation(&self, version: i32) -> bool {
        match self {
            Packet::NullResponse { .. } | Packet::Exception { .. } | Packet::XaResponse { .. } => {
                !versions::before_async_response_change(version)
            }
            Packet::Send { .. } | Packet::SendContinuation { .. } => {
                !versions::before_async_response_change(version) && !versions::before_address_change(version)
            }
            Packet::Commit { .. } => versions::supports_commit_v2(version),
            _ => false,
        }
    }

    /// `Packet.isResponseAsync()`: requests whose response is matched by correlation id.
    pub fn is_response_async(&self, version: i32) -> bool {
        self.supports_correlation(version)
    }

    /// `Packet.isRequiresResponse()` for request packets.
    pub fn requires_response(&self) -> bool {
        match self {
            Packet::Send { requires_response, .. }
            | Packet::SendContinuation { requires_response, .. }
            | Packet::CreateConsumer { requires_response, .. }
            | Packet::Acknowledge { requires_response, .. }
            | Packet::IndividualAcknowledge { requires_response, .. }
            | Packet::CreateAddress { requires_response, .. }
            | Packet::CreateQueue { requires_response, .. }
            | Packet::CreateSharedQueue { requires_response, .. } => *requires_response,
            _ => false,
        }
    }

    /// `Packet.getCorrelationID()`; `-1` when unsupported.
    pub fn correlation_id(&self) -> i64 {
        match self {
            Packet::NullResponse { correlation_id }
            | Packet::Exception { correlation_id, .. }
            | Packet::XaResponse { correlation_id, .. }
            | Packet::Send { correlation_id, .. }
            | Packet::SendContinuation { correlation_id, .. }
            | Packet::Commit { correlation_id } => *correlation_id,
            _ => -1,
        }
    }

    /// `Packet.setCorrelationID()`; no-op for packets without a correlation id.
    pub fn set_correlation_id(&mut self, id: i64) {
        match self {
            Packet::NullResponse { correlation_id }
            | Packet::Exception { correlation_id, .. }
            | Packet::XaResponse { correlation_id, .. }
            | Packet::Send { correlation_id, .. }
            | Packet::SendContinuation { correlation_id, .. }
            | Packet::Commit { correlation_id } => *correlation_id = id,
            _ => {}
        }
    }

    // -----------------------------------------------------------------------------------------
    // Encoding
    // -----------------------------------------------------------------------------------------

    /// Encodes a complete frame (including the 4 byte length prefix) into `out`.
    /// Returns the total number of bytes written (the "packet size").
    pub fn encode(&self, channel_id: i64, version: i32, out: &mut BytesMut) -> usize {
        let start = out.len();
        out.write_i32(0); // length placeholder
        out.write_i8(self.packet_type(version));
        out.write_i64(channel_id);
        self.encode_rest(version, out);
        let total = out.len() - start;
        let len = (total - 4) as i32;
        out[start..start + 4].copy_from_slice(&len.to_be_bytes());
        total
    }

    fn encode_rest(&self, version: i32, out: &mut BytesMut) {
        match self {
            Packet::Ping { connection_ttl } => out.write_i64(*connection_ttl),
            Packet::Disconnect { node_id } => out.write_nullable_simple_string(node_id.as_ref()),
            Packet::DisconnectV2 { node_id, scale_down_node_id } => {
                out.write_nullable_simple_string(node_id.as_ref());
                out.write_nullable_simple_string(scale_down_node_id.as_ref());
            }
            Packet::DisconnectV3 { node_id, reason, target_node_id, target_connector } => {
                out.write_nullable_simple_string(node_id.as_ref());
                out.write_i8(reason.map(|r| r.to_byte()).unwrap_or(-1));
                out.write_nullable_simple_string(target_node_id.as_ref());
                match target_connector {
                    Some(tc) => {
                        out.write_bool(true);
                        encode_transport_configuration(tc, out);
                    }
                    None => out.write_bool(false),
                }
            }
            Packet::DisconnectConsumer { consumer_id } => out.write_i64(*consumer_id),
            Packet::DisconnectConsumerWithKill { node_id } => out.write_nullable_simple_string(node_id.as_ref()),
            Packet::Exception { code, message, correlation_id } => {
                out.write_i32(*code);
                out.write_nullable_string(message.as_deref());
                if self.supports_correlation(version) {
                    out.write_i64(*correlation_id);
                }
            }
            Packet::NullResponse { correlation_id } => {
                if self.supports_correlation(version) {
                    out.write_i64(*correlation_id);
                }
            }
            Packet::PacketsConfirmed { command_id } => out.write_i32(*command_id),
            Packet::Connect { node_id, client_version, auth_mechanism, auth_data } => {
                out.write_nullable_string(node_id.as_deref());
                encode_connect_map(*client_version, auth_mechanism.as_deref(), auth_data.as_deref(), out);
            }
            Packet::ConnectResponse { ok_to_failover, server_version } => {
                out.write_bool(*ok_to_failover);
                // AbstractMapPersister record: int recordSize, unsigned short entries, entries...
                let fields = if *server_version > 0 { 1 } else { 0 };
                let record_size = 4 + 2 + if fields == 1 { 2 + 1 + 4 } else { 0 };
                out.write_i32(record_size);
                out.write_u16(fields);
                if fields == 1 {
                    out.write_i16(1); // FIELD_SERVER_VERSION
                    out.write_u8(MAP_TYPE_INTEGER);
                    out.write_i32(*server_version);
                }
            }
            Packet::CreateSession {
                name,
                session_channel_id,
                version: client_version,
                username,
                password,
                min_large_message_size,
                xa,
                auto_commit_sends,
                auto_commit_acks,
                pre_acknowledge,
                window_size,
                default_address,
                client_id,
            } => {
                out.write_string(name);
                out.write_i64(*session_channel_id);
                out.write_i32(*client_version);
                out.write_nullable_string(username.as_deref());
                out.write_nullable_string(password.as_deref());
                out.write_i32(*min_large_message_size);
                out.write_bool(*xa);
                out.write_bool(*auto_commit_sends);
                out.write_bool(*auto_commit_acks);
                out.write_i32(*window_size);
                out.write_bool(*pre_acknowledge);
                out.write_nullable_string(default_address.as_deref());
                if let Some(cid) = client_id {
                    out.write_nullable_string(cid.as_deref());
                }
            }
            Packet::CreateSessionResponse { server_version } => out.write_i32(*server_version),
            Packet::ReattachSession { name, last_confirmed_command_id } => {
                out.write_string(name);
                out.write_i32(*last_confirmed_command_id);
            }
            Packet::ReattachSessionResponse { last_confirmed_command_id, reattached } => {
                out.write_i32(*last_confirmed_command_id);
                out.write_bool(*reattached);
            }
            Packet::CreateAddress { address, routing_types, requires_response, auto_created } => {
                out.write_simple_string(address);
                out.write_i32(routing_types.len() as i32);
                for rt in routing_types {
                    out.write_i8(rt.to_byte());
                }
                out.write_bool(*requires_response);
                out.write_bool(*auto_created);
            }
            Packet::CreateQueue { config, requires_response, v2 } => {
                out.write_simple_string(&config.address);
                out.write_simple_string(&config.name);
                out.write_nullable_simple_string(config.filter_string.as_ref());
                out.write_bool(config.durable);
                out.write_bool(config.temporary);
                out.write_bool(*requires_response);
                if *v2 {
                    out.write_bool(config.auto_created);
                    out.write_i8(config.routing_type.map(|r| r.to_byte()).unwrap_or(-1));
                    out.write_i32(config.max_consumers.unwrap_or(-1));
                    out.write_bool(config.purge_on_no_consumers.unwrap_or(false));
                    encode_queue_config_tail(config, out);
                }
            }
            Packet::CreateSharedQueue { config, requires_response, v2 } => {
                out.write_simple_string(&config.address);
                out.write_simple_string(&config.name);
                out.write_nullable_simple_string(config.filter_string.as_ref());
                out.write_bool(config.durable);
                if *v2 {
                    out.write_i8(config.routing_type.map(|r| r.to_byte()).unwrap_or(-1));
                    out.write_bool(*requires_response);
                    out.write_nullable_i32(config.max_consumers);
                    out.write_nullable_bool(config.purge_on_no_consumers);
                    encode_queue_config_tail(config, out);
                } else {
                    out.write_bool(*requires_response);
                }
            }
            Packet::DeleteQueue { queue_name } => out.write_simple_string(queue_name),
            Packet::QueueQuery { queue_name } => out.write_simple_string(queue_name),
            Packet::QueueQueryResponse { version: v, result } => encode_queue_query_response(*v, result, out),
            Packet::BindingQuery { address } => out.write_simple_string(address),
            Packet::BindingQueryResponse { version: v, result } => encode_binding_query_response(*v, result, out),
            Packet::CreateConsumer { id, queue_name, filter_string, priority, browse_only, requires_response } => {
                out.write_i64(*id);
                out.write_simple_string(queue_name);
                out.write_nullable_simple_string(filter_string.as_ref());
                out.write_bool(*browse_only);
                out.write_bool(*requires_response);
                if versions::supports_consumer_priority(version) {
                    out.write_i32(*priority);
                }
            }
            Packet::Acknowledge { consumer_id, message_id, requires_response }
            | Packet::IndividualAcknowledge { consumer_id, message_id, requires_response } => {
                out.write_i64(*consumer_id);
                out.write_i64(*message_id);
                out.write_bool(*requires_response);
            }
            Packet::Expire { consumer_id, message_id } => {
                out.write_i64(*consumer_id);
                out.write_i64(*message_id);
            }
            Packet::Commit { correlation_id } => {
                if self.supports_correlation(version) {
                    out.write_i64(*correlation_id);
                }
            }
            Packet::Rollback { consider_last_message_as_delivered } => out.write_bool(*consider_last_message_as_delivered),
            Packet::SessionStart | Packet::SessionStop | Packet::SessionClose => {}
            Packet::ConsumerFlowCredit { consumer_id, credits } => {
                out.write_i64(*consumer_id);
                out.write_i32(*credits);
            }
            Packet::ConsumerClose { consumer_id } => out.write_i64(*consumer_id),
            Packet::ForceConsumerDelivery { consumer_id, sequence } => {
                out.write_i64(*consumer_id);
                out.write_i64(*sequence);
            }
            Packet::RequestProducerCredits { credits, address }
            | Packet::ProducerCredits { credits, address }
            | Packet::ProducerCreditsFail { credits, address } => {
                out.write_i32(*credits);
                out.write_simple_string(address);
            }
            Packet::CreateProducer { id, address } => {
                out.write_i32(*id);
                out.write_nullable_simple_string(address.as_ref());
            }
            Packet::RemoveProducer { id } => out.write_i32(*id),
            Packet::AddMetaData { key, data } => {
                out.write_string(key);
                out.write_string(data);
            }
            Packet::AddMetaDataV2 { key, data, requires_confirmation }
            | Packet::UniqueAddMetaData { key, data, requires_confirmation } => {
                out.write_string(key);
                out.write_string(data);
                out.write_bool(*requires_confirmation);
            }
            Packet::Send { message, requires_response, correlation_id, sender_id } => {
                message.encode(out);
                out.write_bool(*requires_response);
                if self.supports_correlation(version) {
                    out.write_i64(*correlation_id);
                    if !versions::before_producer_metrics(version) {
                        out.write_i32(*sender_id);
                    }
                }
            }
            Packet::SendLarge { message } => message.encode_headers_and_properties(out),
            Packet::SendContinuation { body, continues, message_body_size, requires_response, correlation_id, sender_id } => {
                out.write_sized_bytes(body);
                out.write_bool(*continues);
                if !*continues {
                    out.write_i64(*message_body_size);
                }
                out.write_bool(*requires_response);
                if self.supports_correlation(version) {
                    out.write_i64(*correlation_id);
                    if !versions::before_producer_metrics(version) {
                        out.write_i32(*sender_id);
                    }
                }
            }
            Packet::Receive { consumer_id, delivery_count, message } => {
                message.encode(out);
                out.write_i64(*consumer_id);
                out.write_i32(*delivery_count);
            }
            Packet::ReceiveLarge { consumer_id, delivery_count, large_message_size, message } => {
                out.write_i64(*consumer_id);
                out.write_i32(*delivery_count);
                out.write_i64(*large_message_size);
                message.encode_headers_and_properties(out);
            }
            Packet::ReceiveContinuation { body, continues, consumer_id } => {
                out.write_sized_bytes(body);
                out.write_bool(*continues);
                out.write_i64(*consumer_id);
            }
            Packet::XaStart { xid }
            | Packet::XaPrepare { xid }
            | Packet::XaRollback { xid }
            | Packet::XaJoin { xid }
            | Packet::XaResume { xid }
            | Packet::XaForget { xid }
            | Packet::XaAfterFailed { xid } => xid.encode(out),
            Packet::XaEnd { xid, failed } => {
                xid.encode(out);
                out.write_bool(*failed);
            }
            Packet::XaCommit { xid, one_phase } => {
                xid.encode(out);
                out.write_bool(*one_phase);
            }
            Packet::XaSuspend | Packet::XaGetInDoubtXids | Packet::XaGetTimeout => {}
            Packet::XaResponse { error, response_code, message, correlation_id } => {
                out.write_bool(*error);
                out.write_i32(*response_code);
                out.write_nullable_string(message.as_deref());
                if self.supports_correlation(version) {
                    out.write_i64(*correlation_id);
                }
            }
            Packet::XaGetInDoubtXidsResponse { xids } => {
                out.write_i32(xids.len() as i32);
                for x in xids {
                    x.encode(out);
                }
            }
            Packet::XaSetTimeout { timeout_seconds } => out.write_i32(*timeout_seconds),
            Packet::XaSetTimeoutResponse { ok } => out.write_bool(*ok),
            Packet::XaGetTimeoutResponse { timeout_seconds } => out.write_i32(*timeout_seconds),
            Packet::Unknown { payload, .. } => out.write_bytes(payload),
        }
    }

    // -----------------------------------------------------------------------------------------
    // Decoding
    // -----------------------------------------------------------------------------------------

    /// Decodes a frame whose 4-byte length prefix has already been stripped:
    /// `[type][channelID][rest]`.
    pub fn decode_frame(frame: &[u8], version: i32) -> Result<Frame, DecodeError> {
        let mut r = Reader::new(frame);
        let packet_type = r.read_i8()?;
        let channel_id = r.read_i64()?;
        let packet = Packet::decode_body(packet_type, &mut r, version)?;
        Ok(Frame { channel_id, packet })
    }

    fn decode_body(packet_type: i8, r: &mut Reader<'_>, version: i32) -> Result<Packet, DecodeError> {
        use types::*;
        let has_correlation = !versions::before_async_response_change(version);
        let packet = match packet_type {
            PING => Packet::Ping { connection_ttl: r.read_i64()? },
            DISCONNECT => Packet::Disconnect { node_id: r.read_nullable_simple_string()? },
            DISCONNECT_V2 => Packet::DisconnectV2 {
                node_id: r.read_nullable_simple_string()?,
                scale_down_node_id: r.read_nullable_simple_string()?,
            },
            DISCONNECT_V3 => {
                let node_id = r.read_nullable_simple_string()?;
                let reason_byte = r.read_i8()?;
                let reason = if reason_byte < 0 { None } else { Some(DisconnectReason::from_byte(reason_byte)) };
                let target_node_id = r.read_nullable_simple_string()?;
                let target_connector = if r.read_bool()? { Some(decode_transport_configuration(r)?) } else { None };
                Packet::DisconnectV3 { node_id, reason, target_node_id, target_connector }
            }
            DISCONNECT_CONSUMER => Packet::DisconnectConsumer { consumer_id: r.read_i64()? },
            DISCONNECT_CONSUMER_KILL => Packet::DisconnectConsumerWithKill { node_id: r.read_nullable_simple_string()? },
            EXCEPTION => {
                let code = r.read_i32()?;
                let message = r.read_nullable_string()?;
                let correlation_id = if has_correlation && r.remaining() >= 8 { r.read_i64()? } else { -1 };
                Packet::Exception { code, message, correlation_id }
            }
            NULL_RESPONSE => {
                let correlation_id = if has_correlation && r.remaining() >= 8 { r.read_i64()? } else { -1 };
                Packet::NullResponse { correlation_id }
            }
            PACKETS_CONFIRMED => Packet::PacketsConfirmed { command_id: r.read_i32()? },
            CONNECT => {
                let node_id = r.read_nullable_string()?;
                let (client_version, auth_mechanism, auth_data) =
                    if r.has_remaining() { decode_connect_map(r)? } else { (0, None, None) };
                Packet::Connect { node_id, client_version, auth_mechanism, auth_data }
            }
            CONNECT_RESPONSE => {
                let ok_to_failover = r.read_bool()?;
                let mut server_version = 0;
                if r.has_remaining() {
                    for entry in decode_map_record(r)? {
                        if entry.key == 1 {
                            if let MapValue::Integer(v) = entry.value {
                                server_version = v;
                            }
                        }
                    }
                }
                Packet::ConnectResponse { ok_to_failover, server_version }
            }
            CREATESESSION | CREATESESSION_V2 => {
                let name = r.read_string()?;
                let session_channel_id = r.read_i64()?;
                let client_version = r.read_i32()?;
                let username = r.read_nullable_string()?;
                let password = r.read_nullable_string()?;
                let min_large_message_size = r.read_i32()?;
                let xa = r.read_bool()?;
                let auto_commit_sends = r.read_bool()?;
                let auto_commit_acks = r.read_bool()?;
                let window_size = r.read_i32()?;
                let pre_acknowledge = r.read_bool()?;
                let default_address = r.read_nullable_string()?;
                let client_id = if packet_type == CREATESESSION_V2 { Some(r.read_nullable_string()?) } else { None };
                Packet::CreateSession {
                    name,
                    session_channel_id,
                    version: client_version,
                    username,
                    password,
                    min_large_message_size,
                    xa,
                    auto_commit_sends,
                    auto_commit_acks,
                    pre_acknowledge,
                    window_size,
                    default_address,
                    client_id,
                }
            }
            CREATESESSION_RESP => Packet::CreateSessionResponse { server_version: r.read_i32()? },
            REATTACH_SESSION => Packet::ReattachSession { name: r.read_string()?, last_confirmed_command_id: r.read_i32()? },
            REATTACH_SESSION_RESP => Packet::ReattachSessionResponse {
                last_confirmed_command_id: r.read_i32()?,
                reattached: r.read_bool()?,
            },
            CREATE_ADDRESS => {
                let address = r.read_simple_string()?;
                let n = r.read_i32()?;
                let mut routing_types = Vec::new();
                for _ in 0..n.max(0) {
                    if let Some(rt) = RoutingType::from_byte(r.read_i8()?) {
                        routing_types.push(rt);
                    }
                }
                let requires_response = r.read_bool()?;
                let auto_created = r.read_bool()?;
                Packet::CreateAddress { address, routing_types, requires_response, auto_created }
            }
            CREATE_QUEUE | CREATE_QUEUE_V2 => {
                let mut config = QueueConfiguration::default();
                config.address = r.read_simple_string()?;
                config.name = r.read_simple_string()?;
                config.filter_string = r.read_nullable_simple_string()?;
                config.durable = r.read_bool()?;
                config.temporary = r.read_bool()?;
                let requires_response = r.read_bool()?;
                let v2 = packet_type == CREATE_QUEUE_V2;
                if v2 {
                    config.auto_created = r.read_bool()?;
                    config.routing_type = RoutingType::from_byte(r.read_i8()?);
                    config.max_consumers = Some(r.read_i32()?);
                    config.purge_on_no_consumers = Some(r.read_bool()?);
                    decode_queue_config_tail(&mut config, r)?;
                }
                Packet::CreateQueue { config, requires_response, v2 }
            }
            CREATE_SHARED_QUEUE | CREATE_SHARED_QUEUE_V2 => {
                let mut config = QueueConfiguration::default();
                config.address = r.read_simple_string()?;
                config.name = r.read_simple_string()?;
                config.filter_string = r.read_nullable_simple_string()?;
                config.durable = r.read_bool()?;
                let v2 = packet_type == CREATE_SHARED_QUEUE_V2;
                let requires_response;
                if v2 {
                    config.routing_type = RoutingType::from_byte(r.read_i8()?);
                    requires_response = r.read_bool()?;
                    if r.has_remaining() {
                        config.max_consumers = r.read_nullable_i32()?;
                        config.purge_on_no_consumers = r.read_nullable_bool()?;
                        decode_queue_config_tail(&mut config, r)?;
                    }
                } else {
                    requires_response = r.read_bool()?;
                }
                Packet::CreateSharedQueue { config, requires_response, v2 }
            }
            DELETE_QUEUE => Packet::DeleteQueue { queue_name: r.read_simple_string()? },
            SESS_QUEUEQUERY => Packet::QueueQuery { queue_name: r.read_simple_string()? },
            SESS_QUEUEQUERY_RESP => Packet::QueueQueryResponse { version: 1, result: decode_queue_query_response(1, r)? },
            SESS_QUEUEQUERY_RESP_V2 => Packet::QueueQueryResponse { version: 2, result: decode_queue_query_response(2, r)? },
            SESS_QUEUEQUERY_RESP_V3 => Packet::QueueQueryResponse { version: 3, result: decode_queue_query_response(3, r)? },
            SESS_BINDINGQUERY => Packet::BindingQuery { address: r.read_simple_string()? },
            SESS_BINDINGQUERY_RESP => Packet::BindingQueryResponse { version: 1, result: decode_binding_query_response(1, r)? },
            SESS_BINDINGQUERY_RESP_V2 => Packet::BindingQueryResponse { version: 2, result: decode_binding_query_response(2, r)? },
            SESS_BINDINGQUERY_RESP_V3 => Packet::BindingQueryResponse { version: 3, result: decode_binding_query_response(3, r)? },
            SESS_BINDINGQUERY_RESP_V4 => Packet::BindingQueryResponse { version: 4, result: decode_binding_query_response(4, r)? },
            SESS_BINDINGQUERY_RESP_V5 => Packet::BindingQueryResponse { version: 5, result: decode_binding_query_response(5, r)? },
            SESS_CREATECONSUMER => {
                let id = r.read_i64()?;
                let queue_name = r.read_simple_string()?;
                let filter_string = r.read_nullable_simple_string()?;
                let browse_only = r.read_bool()?;
                let requires_response = r.read_bool()?;
                let priority = if r.has_remaining() { r.read_i32()? } else { 0 };
                Packet::CreateConsumer { id, queue_name, filter_string, priority, browse_only, requires_response }
            }
            SESS_ACKNOWLEDGE => Packet::Acknowledge {
                consumer_id: r.read_i64()?,
                message_id: r.read_i64()?,
                requires_response: r.read_bool()?,
            },
            SESS_INDIVIDUAL_ACKNOWLEDGE => Packet::IndividualAcknowledge {
                consumer_id: r.read_i64()?,
                message_id: r.read_i64()?,
                requires_response: r.read_bool()?,
            },
            SESS_EXPIRED => Packet::Expire { consumer_id: r.read_i64()?, message_id: r.read_i64()? },
            SESS_COMMIT => {
                let correlation_id =
                    if versions::supports_commit_v2(version) && r.remaining() >= 8 { r.read_i64()? } else { -1 };
                Packet::Commit { correlation_id }
            }
            SESS_ROLLBACK => Packet::Rollback { consider_last_message_as_delivered: r.read_bool()? },
            SESS_START => Packet::SessionStart,
            SESS_STOP => Packet::SessionStop,
            SESS_CLOSE => Packet::SessionClose,
            SESS_FLOWTOKEN => Packet::ConsumerFlowCredit { consumer_id: r.read_i64()?, credits: r.read_i32()? },
            SESS_CONSUMER_CLOSE => Packet::ConsumerClose { consumer_id: r.read_i64()? },
            SESS_FORCE_CONSUMER_DELIVERY => Packet::ForceConsumerDelivery { consumer_id: r.read_i64()?, sequence: r.read_i64()? },
            SESS_PRODUCER_REQUEST_CREDITS => Packet::RequestProducerCredits { credits: r.read_i32()?, address: r.read_simple_string()? },
            SESS_PRODUCER_CREDITS => Packet::ProducerCredits { credits: r.read_i32()?, address: r.read_simple_string()? },
            SESS_PRODUCER_FAIL_CREDITS => Packet::ProducerCreditsFail { credits: r.read_i32()?, address: r.read_simple_string()? },
            CREATE_PRODUCER => Packet::CreateProducer { id: r.read_i32()?, address: r.read_nullable_simple_string()? },
            REMOVE_PRODUCER => Packet::RemoveProducer { id: r.read_i32()? },
            SESS_ADD_METADATA => Packet::AddMetaData { key: r.read_string()?, data: r.read_string()? },
            SESS_ADD_METADATA2 => Packet::AddMetaDataV2 {
                key: r.read_string()?,
                data: r.read_string()?,
                requires_confirmation: r.read_bool()?,
            },
            SESS_UNIQUE_ADD_METADATA => Packet::UniqueAddMetaData {
                key: r.read_string()?,
                data: r.read_string()?,
                requires_confirmation: r.read_bool()?,
            },
            SESS_SEND => {
                // Trailer fields sit at the very end of the frame (see SessionSendMessage.decodeRest).
                let with_corr = !versions::before_async_response_change(version) && !versions::before_address_change(version);
                let with_sender = with_corr && !versions::before_producer_metrics(version);
                let trailer = 1 + if with_corr { 8 } else { 0 } + if with_sender { 4 } else { 0 };
                let total = r.as_slice().len();
                if total < r.position() + trailer {
                    return Err(DecodeError::Underflow { needed: trailer, offset: r.position() });
                }
                let msg_end = total - trailer;
                let mut mr = Reader::new(&r.as_slice()[r.position()..msg_end]);
                let message = Message::decode(&mut mr)?;
                r.set_position(msg_end)?;
                let requires_response = r.read_bool()?;
                let correlation_id = if with_corr { r.read_i64()? } else { -1 };
                let sender_id = if with_sender { r.read_i32()? } else { 0 };
                Packet::Send { message, requires_response, correlation_id, sender_id }
            }
            SESS_SEND_LARGE => Packet::SendLarge { message: Message::decode_headers_and_properties(r)? },
            SESS_SEND_CONTINUATION => {
                let body = r.read_sized_bytes()?.to_vec();
                let continues = r.read_bool()?;
                let message_body_size = if !continues { r.read_i64()? } else { -1 };
                let requires_response = r.read_bool()?;
                let correlation_id = if has_correlation && r.remaining() >= 8 { r.read_i64()? } else { -1 };
                let sender_id =
                    if has_correlation && !versions::before_producer_metrics(version) && r.remaining() >= 4 { r.read_i32()? } else { 0 };
                Packet::SendContinuation { body, continues, message_body_size, requires_response, correlation_id, sender_id }
            }
            SESS_RECEIVE_MSG => {
                let total = r.as_slice().len();
                let trailer = 8 + 4;
                if total < r.position() + trailer {
                    return Err(DecodeError::Underflow { needed: trailer, offset: r.position() });
                }
                let msg_end = total - trailer;
                let mut mr = Reader::new(&r.as_slice()[r.position()..msg_end]);
                let mut message = Message::decode(&mut mr)?;
                r.set_position(msg_end)?;
                let consumer_id = r.read_i64()?;
                let delivery_count = r.read_i32()?;
                message.delivery_count = delivery_count;
                Packet::Receive { consumer_id, delivery_count, message }
            }
            SESS_RECEIVE_LARGE_MSG => {
                let consumer_id = r.read_i64()?;
                let delivery_count = r.read_i32()?;
                let large_message_size = r.read_i64()?;
                let mut message = Message::decode_headers_and_properties(r)?;
                message.delivery_count = delivery_count;
                message.large_message_size = Some(large_message_size);
                Packet::ReceiveLarge { consumer_id, delivery_count, large_message_size, message }
            }
            SESS_RECEIVE_CONTINUATION => Packet::ReceiveContinuation {
                body: r.read_sized_bytes()?.to_vec(),
                continues: r.read_bool()?,
                consumer_id: r.read_i64()?,
            },
            SESS_XA_START => Packet::XaStart { xid: Xid::decode(r)? },
            SESS_XA_END => Packet::XaEnd { xid: Xid::decode(r)?, failed: r.read_bool()? },
            SESS_XA_COMMIT => Packet::XaCommit { xid: Xid::decode(r)?, one_phase: r.read_bool()? },
            SESS_XA_PREPARE => Packet::XaPrepare { xid: Xid::decode(r)? },
            SESS_XA_ROLLBACK => Packet::XaRollback { xid: Xid::decode(r)? },
            SESS_XA_JOIN => Packet::XaJoin { xid: Xid::decode(r)? },
            SESS_XA_RESUME => Packet::XaResume { xid: Xid::decode(r)? },
            SESS_XA_FORGET => Packet::XaForget { xid: Xid::decode(r)? },
            SESS_XA_FAILED => Packet::XaAfterFailed { xid: Xid::decode(r)? },
            SESS_XA_SUSPEND => Packet::XaSuspend,
            SESS_XA_RESP => {
                let error = r.read_bool()?;
                let response_code = r.read_i32()?;
                let message = r.read_nullable_string()?;
                let correlation_id = if has_correlation && r.remaining() >= 8 { r.read_i64()? } else { -1 };
                Packet::XaResponse { error, response_code, message, correlation_id }
            }
            SESS_XA_INDOUBT_XIDS => Packet::XaGetInDoubtXids,
            SESS_XA_INDOUBT_XIDS_RESP => {
                let n = r.read_i32()?;
                let mut xids = Vec::new();
                for _ in 0..n.max(0) {
                    xids.push(Xid::decode(r)?);
                }
                Packet::XaGetInDoubtXidsResponse { xids }
            }
            SESS_XA_SET_TIMEOUT => Packet::XaSetTimeout { timeout_seconds: r.read_i32()? },
            SESS_XA_SET_TIMEOUT_RESP => Packet::XaSetTimeoutResponse { ok: r.read_bool()? },
            SESS_XA_GET_TIMEOUT => Packet::XaGetTimeout,
            SESS_XA_GET_TIMEOUT_RESP => Packet::XaGetTimeoutResponse { timeout_seconds: r.read_i32()? },
            other => Packet::Unknown { packet_type: other, payload: r.peek_remaining().to_vec() },
        };
        Ok(packet)
    }
}

// ---------------------------------------------------------------------------------------------
// Helpers: queue configuration tails, query responses, transport configuration, map records
// ---------------------------------------------------------------------------------------------

/// The optional attributes appended to `CREATE_QUEUE_V2` / `CREATE_SHARED_QUEUE_V2`.
fn encode_queue_config_tail(c: &QueueConfiguration, out: &mut BytesMut) {
    out.write_nullable_bool(c.exclusive);
    out.write_nullable_bool(c.last_value);
    out.write_nullable_simple_string(c.last_value_key.as_ref());
    out.write_nullable_bool(c.non_destructive);
    out.write_nullable_i32(c.consumers_before_dispatch);
    out.write_nullable_i64(c.delay_before_dispatch);
    out.write_nullable_bool(c.group_rebalance);
    out.write_nullable_i32(c.group_buckets);
    out.write_nullable_bool(c.auto_delete);
    out.write_nullable_i64(c.auto_delete_delay);
    out.write_nullable_i64(c.auto_delete_message_count);
    out.write_nullable_simple_string(c.group_first_key.as_ref());
    out.write_nullable_i64(c.ring_size);
    out.write_nullable_bool(c.enabled);
    out.write_nullable_bool(c.group_rebalance_pause_dispatch);
}

fn decode_queue_config_tail(c: &mut QueueConfiguration, r: &mut Reader<'_>) -> Result<(), DecodeError> {
    if r.has_remaining() {
        c.exclusive = r.read_nullable_bool()?;
        c.last_value = r.read_nullable_bool()?;
    }
    if r.has_remaining() {
        c.last_value_key = r.read_nullable_simple_string()?;
        c.non_destructive = r.read_nullable_bool()?;
        c.consumers_before_dispatch = r.read_nullable_i32()?;
        c.delay_before_dispatch = r.read_nullable_i64()?;
        c.group_rebalance = r.read_nullable_bool()?;
        c.group_buckets = r.read_nullable_i32()?;
        c.auto_delete = r.read_nullable_bool()?;
        c.auto_delete_delay = r.read_nullable_i64()?;
        c.auto_delete_message_count = r.read_nullable_i64()?;
    }
    if r.has_remaining() {
        c.group_first_key = r.read_nullable_simple_string()?;
    }
    if r.has_remaining() {
        c.ring_size = r.read_nullable_i64()?;
    }
    if r.has_remaining() {
        c.enabled = r.read_nullable_bool()?;
    }
    if r.has_remaining() {
        c.group_rebalance_pause_dispatch = r.read_nullable_bool()?;
    }
    Ok(())
}

fn encode_queue_query_response(version: u8, q: &QueueQueryResult, out: &mut BytesMut) {
    out.write_bool(q.exists);
    out.write_bool(q.durable);
    out.write_bool(q.temporary);
    out.write_i32(q.consumer_count);
    out.write_i64(q.message_count);
    out.write_nullable_simple_string(q.filter_string.as_ref());
    out.write_nullable_simple_string(q.address.as_ref());
    out.write_nullable_simple_string(q.name.as_ref());
    if version >= 2 {
        out.write_bool(q.auto_create_queues);
    }
    if version >= 3 {
        out.write_bool(q.auto_created);
        out.write_bool(q.purge_on_no_consumers);
        out.write_i8(q.routing_type.map(|r| r.to_byte()).unwrap_or(-1));
        out.write_i32(q.max_consumers);
        out.write_nullable_bool(q.exclusive);
        out.write_nullable_bool(q.last_value);
        out.write_nullable_i32(q.default_consumer_window_size);
        out.write_nullable_simple_string(q.last_value_key.as_ref());
        out.write_nullable_bool(q.non_destructive);
        out.write_nullable_i32(q.consumers_before_dispatch);
        out.write_nullable_i64(q.delay_before_dispatch);
        out.write_nullable_bool(q.group_rebalance);
        out.write_nullable_i32(q.group_buckets);
        out.write_nullable_bool(q.auto_delete);
        out.write_nullable_i64(q.auto_delete_delay);
        out.write_nullable_i64(q.auto_delete_message_count);
        out.write_nullable_simple_string(q.group_first_key.as_ref());
        out.write_nullable_i64(q.ring_size);
        out.write_nullable_bool(q.enabled);
        out.write_nullable_bool(q.group_rebalance_pause_dispatch);
        out.write_nullable_bool(q.configuration_managed);
    }
}

fn decode_queue_query_response(version: u8, r: &mut Reader<'_>) -> Result<QueueQueryResult, DecodeError> {
    let mut q = QueueQueryResult::default();
    q.exists = r.read_bool()?;
    q.durable = r.read_bool()?;
    q.temporary = r.read_bool()?;
    q.consumer_count = r.read_i32()?;
    q.message_count = r.read_i64()?;
    q.filter_string = r.read_nullable_simple_string()?;
    q.address = r.read_nullable_simple_string()?;
    q.name = r.read_nullable_simple_string()?;
    if version >= 2 {
        q.auto_create_queues = r.read_bool()?;
    }
    if version >= 3 {
        q.auto_created = r.read_bool()?;
        q.purge_on_no_consumers = r.read_bool()?;
        q.routing_type = RoutingType::from_byte(r.read_i8()?);
        q.max_consumers = r.read_i32()?;
        if r.has_remaining() {
            q.exclusive = r.read_nullable_bool()?;
            q.last_value = r.read_nullable_bool()?;
        }
        if r.has_remaining() {
            q.default_consumer_window_size = r.read_nullable_i32()?;
        }
        if r.has_remaining() {
            q.last_value_key = r.read_nullable_simple_string()?;
            q.non_destructive = r.read_nullable_bool()?;
            q.consumers_before_dispatch = r.read_nullable_i32()?;
            q.delay_before_dispatch = r.read_nullable_i64()?;
            q.group_rebalance = r.read_nullable_bool()?;
            q.group_buckets = r.read_nullable_i32()?;
            q.auto_delete = r.read_nullable_bool()?;
            q.auto_delete_delay = r.read_nullable_i64()?;
            q.auto_delete_message_count = r.read_nullable_i64()?;
        }
        if r.has_remaining() {
            q.group_first_key = r.read_nullable_simple_string()?;
        }
        if r.has_remaining() {
            q.ring_size = r.read_nullable_i64()?;
        }
        if r.has_remaining() {
            q.enabled = r.read_nullable_bool()?;
        }
        if r.has_remaining() {
            q.group_rebalance_pause_dispatch = r.read_nullable_bool()?;
        }
        if r.has_remaining() {
            q.configuration_managed = r.read_nullable_bool()?;
        }
    }
    Ok(q)
}

fn encode_binding_query_response(version: u8, a: &AddressQueryResult, out: &mut BytesMut) {
    out.write_bool(a.exists);
    out.write_i32(a.queue_names.len() as i32);
    for q in &a.queue_names {
        out.write_simple_string(q);
    }
    if version >= 2 {
        out.write_bool(a.auto_create_queues);
    }
    if version >= 3 {
        out.write_bool(a.auto_create_addresses);
    }
    if version >= 4 {
        out.write_bool(a.default_purge_on_no_consumers);
        out.write_i32(a.default_max_consumers);
        out.write_nullable_bool(a.default_exclusive);
        out.write_nullable_bool(a.default_last_value);
        out.write_nullable_simple_string(a.default_last_value_key.as_ref());
        out.write_nullable_bool(a.default_non_destructive);
        out.write_nullable_i32(a.default_consumers_before_dispatch);
        out.write_nullable_i64(a.default_delay_before_dispatch);
    }
    if version >= 5 {
        out.write_bool(a.supports_multicast);
        out.write_bool(a.supports_anycast);
    }
}

fn decode_binding_query_response(version: u8, r: &mut Reader<'_>) -> Result<AddressQueryResult, DecodeError> {
    let mut a = AddressQueryResult::default();
    a.exists = r.read_bool()?;
    let n = r.read_i32()?;
    for _ in 0..n.max(0) {
        a.queue_names.push(r.read_simple_string()?);
    }
    if version >= 2 {
        a.auto_create_queues = r.read_bool()?;
    }
    if version >= 3 {
        a.auto_create_addresses = r.read_bool()?;
    }
    if version >= 4 {
        a.default_purge_on_no_consumers = r.read_bool()?;
        a.default_max_consumers = r.read_i32()?;
        if r.has_remaining() {
            a.default_exclusive = r.read_nullable_bool()?;
            a.default_last_value = r.read_nullable_bool()?;
        }
        if r.has_remaining() {
            a.default_last_value_key = r.read_nullable_simple_string()?;
            a.default_non_destructive = r.read_nullable_bool()?;
            a.default_consumers_before_dispatch = r.read_nullable_i32()?;
            a.default_delay_before_dispatch = r.read_nullable_i64()?;
        }
    } else {
        a.supports_multicast = true;
        a.supports_anycast = true;
    }
    if version >= 5 && r.has_remaining() {
        a.supports_multicast = r.read_bool()?;
        a.supports_anycast = r.read_bool()?;
    } else if version == 4 {
        a.supports_multicast = true;
        a.supports_anycast = true;
    }
    Ok(a)
}

const TRANSPORT_TYPE_BOOLEAN: u8 = 0;
const TRANSPORT_TYPE_INT: u8 = 1;
const TRANSPORT_TYPE_LONG: u8 = 2;
const TRANSPORT_TYPE_STRING: u8 = 3;

fn encode_transport_configuration(tc: &TransportConfiguration, out: &mut BytesMut) {
    out.write_string(&tc.name);
    out.write_string(&tc.factory_class_name);
    out.write_i32(tc.params.len() as i32);
    for (k, v) in &tc.params {
        out.write_string(k);
        match v {
            TransportParam::Boolean(b) => {
                out.write_u8(TRANSPORT_TYPE_BOOLEAN);
                out.write_bool(*b);
            }
            TransportParam::Int(i) => {
                out.write_u8(TRANSPORT_TYPE_INT);
                out.write_i32(*i);
            }
            TransportParam::Long(l) => {
                out.write_u8(TRANSPORT_TYPE_LONG);
                out.write_i64(*l);
            }
            TransportParam::String(s) => {
                out.write_u8(TRANSPORT_TYPE_STRING);
                out.write_string(s);
            }
        }
    }
}

fn decode_transport_configuration(r: &mut Reader<'_>) -> Result<TransportConfiguration, DecodeError> {
    let name = r.read_string()?;
    let factory_class_name = r.read_string()?;
    let n = r.read_i32()?;
    let mut params = Vec::new();
    for _ in 0..n.max(0) {
        let key = r.read_string()?;
        let value = match r.read_u8()? {
            TRANSPORT_TYPE_BOOLEAN => TransportParam::Boolean(r.read_bool()?),
            TRANSPORT_TYPE_INT => TransportParam::Int(r.read_i32()?),
            TRANSPORT_TYPE_LONG => TransportParam::Long(r.read_i64()?),
            TRANSPORT_TYPE_STRING => TransportParam::String(r.read_string()?),
            other => return Err(DecodeError::Invalid(format!("invalid transport param type {other}"))),
        };
        params.push((key, value));
    }
    Ok(TransportConfiguration { name, factory_class_name, params })
}

// `AbstractMapPersister` datatypes
const MAP_TYPE_BOOLEAN: u8 = 0;
const MAP_TYPE_STRING: u8 = 1;
const MAP_TYPE_INTEGER: u8 = 2;
const MAP_TYPE_LONG: u8 = 3;
const MAP_TYPE_BYTE: u8 = 4;
const MAP_TYPE_BYTE_ARRAY: u8 = 5;

#[derive(Debug)]
#[allow(dead_code)]
enum MapValue {
    Boolean(bool),
    String(String),
    Integer(i32),
    Long(i64),
    Byte(i8),
    ByteArray(Vec<u8>),
}

#[derive(Debug)]
struct MapEntry {
    key: i16,
    value: MapValue,
}

/// Decodes an `AbstractMapPersister` record: `int recordSize, ushort entries, entries...`.
fn decode_map_record(r: &mut Reader<'_>) -> Result<Vec<MapEntry>, DecodeError> {
    let start = r.position();
    let size = r.read_i32()?;
    if size < 6 {
        return Err(DecodeError::Invalid(format!("invalid map record size {size}")));
    }
    let end = start + size as usize;
    if end > r.as_slice().len() {
        return Err(DecodeError::Underflow { needed: end - r.as_slice().len(), offset: r.position() });
    }
    let entries = r.read_u16()?;
    let mut out = Vec::with_capacity(entries as usize);
    for _ in 0..entries {
        let key = r.read_i16()?;
        let value = match r.read_u8()? {
            MAP_TYPE_BOOLEAN => MapValue::Boolean(r.read_bool()?),
            MAP_TYPE_STRING => MapValue::String(r.read_string()?),
            MAP_TYPE_INTEGER => MapValue::Integer(r.read_i32()?),
            MAP_TYPE_LONG => MapValue::Long(r.read_i64()?),
            MAP_TYPE_BYTE => MapValue::Byte(r.read_i8()?),
            MAP_TYPE_BYTE_ARRAY => MapValue::ByteArray(r.read_sized_bytes()?.to_vec()),
            other => return Err(DecodeError::Invalid(format!("unknown map datatype {other}"))),
        };
        out.push(MapEntry { key, value });
    }
    if r.position() != end {
        return Err(DecodeError::Invalid(format!(
            "map record position mismatch: expected {end}, at {}",
            r.position()
        )));
    }
    Ok(out)
}

const CONNECT_FIELD_CLIENT_VERSION: i16 = 1;
const CONNECT_FIELD_AUTH_MECHANISM: i16 = 2;
const CONNECT_FIELD_AUTH_DATA: i16 = 3;

fn encode_connect_map(client_version: i32, auth_mechanism: Option<&str>, auth_data: Option<&[u8]>, out: &mut BytesMut) {
    let header_pos = out.len();
    out.write_i32(0);
    out.write_u16(0);
    let mut fields: u16 = 0;
    if client_version > 0 {
        out.write_i16(CONNECT_FIELD_CLIENT_VERSION);
        out.write_u8(MAP_TYPE_INTEGER);
        out.write_i32(client_version);
        fields += 1;
    }
    if let Some(m) = auth_mechanism {
        out.write_i16(CONNECT_FIELD_AUTH_MECHANISM);
        out.write_u8(MAP_TYPE_STRING);
        out.write_string(m);
        fields += 1;
    }
    if let Some(d) = auth_data {
        out.write_i16(CONNECT_FIELD_AUTH_DATA);
        out.write_u8(MAP_TYPE_BYTE_ARRAY);
        out.write_sized_bytes(d);
        fields += 1;
    }
    let record_size = (out.len() - header_pos) as i32;
    out[header_pos..header_pos + 4].copy_from_slice(&record_size.to_be_bytes());
    out[header_pos + 4..header_pos + 6].copy_from_slice(&fields.to_be_bytes());
}

/// `(client_version, auth_mechanism, auth_data)` carried by a `CONNECT` packet.
type ConnectFields = (i32, Option<String>, Option<Vec<u8>>);

fn decode_connect_map(r: &mut Reader<'_>) -> Result<ConnectFields, DecodeError> {
    let mut client_version = 0;
    let mut mechanism = None;
    let mut data = None;
    for e in decode_map_record(r)? {
        match (e.key, e.value) {
            (CONNECT_FIELD_CLIENT_VERSION, MapValue::Integer(v)) => client_version = v,
            (CONNECT_FIELD_AUTH_MECHANISM, MapValue::String(s)) => mechanism = Some(s),
            (CONNECT_FIELD_AUTH_DATA, MapValue::ByteArray(b)) => data = Some(b),
            _ => {}
        }
    }
    Ok((client_version, mechanism, data))
}

/// Builds the SASL-PLAIN style auth data used by `ConnectMessage.withPlainCredentials`.
pub fn plain_auth_data(username: &str, password: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(username.len() + password.len() + 2);
    v.push(0);
    v.extend_from_slice(username.as_bytes());
    v.push(0);
    v.extend_from_slice(password.as_bytes());
    v
}

/// Helper to split a stream of bytes into frames. Returns the frame (without the length prefix)
/// and the total number of bytes consumed, or `None` if the buffer does not yet hold a full frame.
pub fn split_frame(buf: &[u8]) -> Result<Option<(&[u8], usize)>, DecodeError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len < (PACKET_HEADERS_SIZE - 4) as i32 {
        return Err(DecodeError::Invalid(format!("invalid frame length {len}")));
    }
    let len = len as usize;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    Ok(Some((&buf[4..4 + len], 4 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::types as msg_types;

    fn round_trip(p: Packet, version: i32) -> Packet {
        let mut out = BytesMut::new();
        let size = p.encode(77, version, &mut out);
        assert_eq!(size, out.len());
        let (frame, consumed) = split_frame(&out).unwrap().expect("full frame");
        assert_eq!(consumed, out.len());
        let decoded = Packet::decode_frame(frame, version).unwrap();
        assert_eq!(decoded.channel_id, 77);
        assert_eq!(decoded.packet, p, "version {version}");
        decoded.packet
    }

    #[test]
    fn header_layout() {
        let mut out = BytesMut::new();
        Packet::Ping { connection_ttl: 60000 }.encode(0, versions::CURRENT, &mut out);
        // length = 1 (type) + 8 (channel) + 8 (ttl) = 17
        assert_eq!(&out[..4], &[0, 0, 0, 17]);
        assert_eq!(out[4], types::PING as u8);
        assert_eq!(&out[5..13], &[0; 8]);
        assert_eq!(out.len(), 21);
    }

    #[test]
    fn connection_packets() {
        for v in [versions::CURRENT, 129] {
            round_trip(Packet::Ping { connection_ttl: 12345 }, v);
            round_trip(Packet::Disconnect { node_id: Some("node".into()) }, v);
            round_trip(Packet::DisconnectV2 { node_id: None, scale_down_node_id: Some("x".into()) }, v);
            round_trip(
                Packet::DisconnectV3 {
                    node_id: Some("n".into()),
                    reason: Some(DisconnectReason::ShutDown),
                    target_node_id: None,
                    target_connector: Some(TransportConfiguration {
                        name: "netty".into(),
                        factory_class_name: "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory".into(),
                        params: vec![
                            ("host".into(), TransportParam::String("localhost".into())),
                            ("port".into(), TransportParam::Int(61616)),
                            ("ssl".into(), TransportParam::Boolean(false)),
                            ("t".into(), TransportParam::Long(9)),
                        ],
                    }),
                },
                v,
            );
            round_trip(Packet::DisconnectConsumer { consumer_id: 5 }, v);
            round_trip(Packet::DisconnectConsumerWithKill { node_id: None }, v);
            round_trip(Packet::PacketsConfirmed { command_id: 9 }, v);
            round_trip(
                Packet::Connect {
                    node_id: Some("nid".into()),
                    client_version: 137,
                    auth_mechanism: Some("PLAIN".into()),
                    auth_data: Some(plain_auth_data("user", "pass")),
                },
                v,
            );
            round_trip(Packet::Connect { node_id: None, client_version: 0, auth_mechanism: None, auth_data: None }, v);
            round_trip(Packet::ConnectResponse { ok_to_failover: true, server_version: 137 }, v);
            round_trip(Packet::ConnectResponse { ok_to_failover: false, server_version: 0 }, v);
            round_trip(Packet::CreateSessionResponse { server_version: 135 }, v);
            round_trip(Packet::ReattachSession { name: "s".into(), last_confirmed_command_id: 3 }, v);
            round_trip(Packet::ReattachSessionResponse { last_confirmed_command_id: 1, reattached: true }, v);
        }
        let session = |client_id| Packet::CreateSession {
            name: "session-name-longer-than-eight".into(),
            session_channel_id: 10,
            version: 137,
            username: Some("admin".into()),
            password: None,
            min_large_message_size: 102400,
            xa: true,
            auto_commit_sends: false,
            auto_commit_acks: true,
            pre_acknowledge: false,
            window_size: -1,
            default_address: None,
            client_id,
        };
        round_trip(session(None), 129);
        round_trip(session(Some(Some("cid".into()))), versions::CURRENT);
        round_trip(session(Some(None)), versions::CURRENT);
    }

    #[test]
    fn correlation_packets_follow_version() {
        // With correlation support
        let v = versions::CURRENT;
        let p = round_trip(Packet::NullResponse { correlation_id: -3 }, v);
        assert_eq!(p.correlation_id(), -3);
        round_trip(Packet::Exception { code: 105, message: Some("denied".into()), correlation_id: 4 }, v);
        round_trip(Packet::XaResponse { error: true, response_code: -4, message: Some("XAER_NOTA".into()), correlation_id: 7 }, v);
        round_trip(Packet::Commit { correlation_id: -8 }, v);

        // Without: correlation is dropped on the wire and decodes as -1
        let v = 129;
        let mut out = BytesMut::new();
        Packet::NullResponse { correlation_id: 5 }.encode(1, v, &mut out);
        assert_eq!(out.len(), 4 + 9);
        let f = Packet::decode_frame(&out[4..], v).unwrap();
        assert_eq!(f.packet, Packet::NullResponse { correlation_id: -1 });
        let mut out = BytesMut::new();
        Packet::Commit { correlation_id: 5 }.encode(1, 131, &mut out);
        assert_eq!(out.len(), 13);

        // A V1 NullResponse (no trailing long) decodes fine on a V2 connection
        let f = Packet::decode_frame(&out[4..], versions::CURRENT).unwrap();
        assert_eq!(f.packet, Packet::Commit { correlation_id: -1 });
    }

    #[test]
    fn queue_and_address_packets() {
        let v = versions::CURRENT;
        round_trip(
            Packet::CreateAddress {
                address: "addr".into(),
                routing_types: vec![RoutingType::Anycast, RoutingType::Multicast],
                requires_response: true,
                auto_created: false,
            },
            v,
        );
        let config = QueueConfiguration::new("q1")
            .address("addr")
            .routing_type(RoutingType::Anycast)
            .filter("color = 'red'")
            .max_consumers(-1)
            .purge_on_no_consumers(false)
            .exclusive(true)
            .ring_size(10);
        round_trip(Packet::CreateQueue { config: config.clone(), requires_response: true, v2: true }, v);
        round_trip(Packet::CreateSharedQueue { config: config.clone(), requires_response: true, v2: true }, v);
        // V1 packets do not carry the extended attributes
        let mut v1 = config.clone();
        v1.routing_type = None;
        v1.max_consumers = None;
        v1.purge_on_no_consumers = None;
        v1.exclusive = None;
        v1.ring_size = None;
        round_trip(Packet::CreateQueue { config: v1.clone(), requires_response: false, v2: false }, v);
        round_trip(Packet::CreateSharedQueue { config: v1, requires_response: false, v2: false }, v);
        round_trip(Packet::DeleteQueue { queue_name: "q1".into() }, v);
        round_trip(Packet::QueueQuery { queue_name: "q1".into() }, v);
        round_trip(Packet::BindingQuery { address: "addr".into() }, v);

        let mut q = QueueQueryResult { exists: true, durable: true, consumer_count: 2, message_count: 99, ..Default::default() };
        q.name = Some("q1".into());
        q.address = Some("addr".into());
        round_trip(Packet::QueueQueryResponse { version: 1, result: q.clone() }, v);
        q.auto_create_queues = true;
        round_trip(Packet::QueueQueryResponse { version: 2, result: q.clone() }, v);
        q.routing_type = Some(RoutingType::Multicast);
        q.max_consumers = -1;
        q.exclusive = Some(false);
        q.last_value = None;
        q.default_consumer_window_size = Some(1024);
        q.ring_size = Some(-1);
        q.enabled = Some(true);
        q.configuration_managed = Some(false);
        round_trip(Packet::QueueQueryResponse { version: 3, result: q.clone() }, v);

        let mut a = AddressQueryResult { exists: true, queue_names: vec!["a".into(), "b".into()], ..Default::default() };
        a.supports_multicast = true;
        a.supports_anycast = true;
        round_trip(Packet::BindingQueryResponse { version: 1, result: a.clone() }, v);
        a.auto_create_queues = true;
        round_trip(Packet::BindingQueryResponse { version: 2, result: a.clone() }, v);
        a.auto_create_addresses = true;
        round_trip(Packet::BindingQueryResponse { version: 3, result: a.clone() }, v);
        a.default_max_consumers = -1;
        a.default_exclusive = Some(true);
        a.default_last_value_key = Some("k".into());
        a.default_delay_before_dispatch = Some(5);
        round_trip(Packet::BindingQueryResponse { version: 4, result: a.clone() }, v);
        a.supports_anycast = false;
        round_trip(Packet::BindingQueryResponse { version: 5, result: a.clone() }, v);
    }

    #[test]
    fn session_packets() {
        let v = versions::CURRENT;
        let consumer = Packet::CreateConsumer {
            id: 1,
            queue_name: "q".into(),
            filter_string: None,
            priority: 3,
            browse_only: false,
            requires_response: true,
        };
        round_trip(consumer.clone(), v);
        // Before 130 the priority is not sent and decodes as 0
        let mut out = BytesMut::new();
        consumer.encode(1, 129, &mut out);
        let f = Packet::decode_frame(&out[4..], 129).unwrap();
        match f.packet {
            Packet::CreateConsumer { priority, .. } => assert_eq!(priority, 0),
            other => panic!("{other:?}"),
        }
        round_trip(Packet::Acknowledge { consumer_id: 1, message_id: 2, requires_response: true }, v);
        round_trip(Packet::IndividualAcknowledge { consumer_id: 1, message_id: 2, requires_response: false }, v);
        round_trip(Packet::Expire { consumer_id: 1, message_id: 2 }, v);
        round_trip(Packet::Rollback { consider_last_message_as_delivered: true }, v);
        round_trip(Packet::SessionStart, v);
        round_trip(Packet::SessionStop, v);
        round_trip(Packet::SessionClose, v);
        round_trip(Packet::ConsumerFlowCredit { consumer_id: 3, credits: 1000 }, v);
        round_trip(Packet::ConsumerClose { consumer_id: 3 }, v);
        round_trip(Packet::ForceConsumerDelivery { consumer_id: 3, sequence: 8 }, v);
        round_trip(Packet::RequestProducerCredits { credits: 10, address: "a".into() }, v);
        round_trip(Packet::ProducerCredits { credits: 10, address: "a".into() }, v);
        round_trip(Packet::ProducerCreditsFail { credits: 10, address: "a".into() }, v);
        round_trip(Packet::CreateProducer { id: 4, address: None }, v);
        round_trip(Packet::RemoveProducer { id: 4 }, v);
        round_trip(Packet::AddMetaData { key: "k".into(), data: "d".into() }, v);
        round_trip(Packet::AddMetaDataV2 { key: "k".into(), data: "d".into(), requires_confirmation: false }, v);
        round_trip(Packet::UniqueAddMetaData { key: "k".into(), data: "d".into(), requires_confirmation: true }, v);
    }

    #[test]
    fn message_packets_all_versions() {
        let mut m = Message::text("payload").with_address("dest").with_property("p", 1i64);
        m.timestamp = 1;
        for v in [129, 130, 133, 134, versions::CURRENT] {
            let sent = Packet::Send { message: m.clone(), requires_response: true, correlation_id: -2, sender_id: 3 };
            let mut out = BytesMut::new();
            sent.encode(10, v, &mut out);
            let f = Packet::decode_frame(&out[4..], v).unwrap();
            match f.packet {
                Packet::Send { message, requires_response, correlation_id, sender_id } => {
                    assert_eq!(message, m);
                    assert!(requires_response);
                    if v >= 130 {
                        assert_eq!(correlation_id, -2);
                    } else {
                        assert_eq!(correlation_id, -1);
                    }
                    if v >= 134 {
                        assert_eq!(sender_id, 3);
                    } else {
                        assert_eq!(sender_id, 0);
                    }
                }
                other => panic!("{other:?}"),
            }
            let mut recv_m = m.clone();
            recv_m.delivery_count = 2;
            round_trip(Packet::Receive { consumer_id: 9, delivery_count: 2, message: recv_m }, v);
            let mut headers_only = m.clone();
            headers_only.body.clear();
            round_trip(Packet::SendLarge { message: headers_only.clone() }, v);
            let mut large = headers_only.clone();
            large.large_message_size = Some(5000);
            round_trip(Packet::ReceiveLarge { consumer_id: 1, delivery_count: 0, large_message_size: 5000, message: large }, v);
            round_trip(Packet::ReceiveContinuation { body: vec![1, 2, 3], continues: true, consumer_id: 1 }, v);
            let cont = Packet::SendContinuation {
                body: vec![4, 5],
                continues: false,
                message_body_size: 2,
                requires_response: true,
                correlation_id: -4,
                sender_id: 1,
            };
            let mut out = BytesMut::new();
            cont.encode(10, v, &mut out);
            let f = Packet::decode_frame(&out[4..], v).unwrap();
            if v >= 134 {
                assert_eq!(f.packet, cont);
            }
            round_trip(
                Packet::SendContinuation {
                    body: vec![],
                    continues: true,
                    message_body_size: -1,
                    requires_response: false,
                    correlation_id: if v >= 130 { 5 } else { -1 },
                    sender_id: if v >= 134 { 2 } else { 0 },
                },
                v,
            );
        }
        let _ = msg_types::TEXT;
    }

    #[test]
    fn xa_packets() {
        let v = versions::CURRENT;
        let xid = Xid::new(1, vec![1, 2], vec![3]);
        round_trip(Packet::XaStart { xid: xid.clone() }, v);
        round_trip(Packet::XaEnd { xid: xid.clone(), failed: true }, v);
        round_trip(Packet::XaCommit { xid: xid.clone(), one_phase: true }, v);
        round_trip(Packet::XaPrepare { xid: xid.clone() }, v);
        round_trip(Packet::XaRollback { xid: xid.clone() }, v);
        round_trip(Packet::XaJoin { xid: xid.clone() }, v);
        round_trip(Packet::XaResume { xid: xid.clone() }, v);
        round_trip(Packet::XaForget { xid: xid.clone() }, v);
        round_trip(Packet::XaAfterFailed { xid: xid.clone() }, v);
        round_trip(Packet::XaSuspend, v);
        round_trip(Packet::XaGetInDoubtXids, v);
        round_trip(Packet::XaGetInDoubtXidsResponse { xids: vec![xid.clone(), Xid::random()] }, v);
        round_trip(Packet::XaSetTimeout { timeout_seconds: 30 }, v);
        round_trip(Packet::XaSetTimeoutResponse { ok: true }, v);
        round_trip(Packet::XaGetTimeout, v);
        round_trip(Packet::XaGetTimeoutResponse { timeout_seconds: 30 }, v);
        // A V1 XA response on a V2 connection decodes with correlation -1
        let mut out = BytesMut::new();
        Packet::XaResponse { error: false, response_code: 0, message: None, correlation_id: 0 }.encode(1, 129, &mut out);
        let f = Packet::decode_frame(&out[4..], v).unwrap();
        assert_eq!(f.packet, Packet::XaResponse { error: false, response_code: 0, message: None, correlation_id: -1 });
    }

    #[test]
    fn unknown_packets_keep_payload() {
        let mut out = BytesMut::new();
        out.write_i32(0);
        out.write_i8(types::CLUSTER_TOPOLOGY_V4);
        out.write_i64(0);
        out.write_bytes(&[1, 2, 3, 4]);
        let f = Packet::decode_frame(&out[4..], versions::CURRENT).unwrap();
        assert_eq!(f.packet, Packet::Unknown { packet_type: types::CLUSTER_TOPOLOGY_V4, payload: vec![1, 2, 3, 4] });
        round_trip(f.packet, versions::CURRENT);
    }

    #[test]
    fn frame_splitting() {
        let mut out = BytesMut::new();
        Packet::Ping { connection_ttl: 1 }.encode(0, versions::CURRENT, &mut out);
        Packet::SessionStart.encode(10, versions::CURRENT, &mut out);
        let (f1, c1) = split_frame(&out).unwrap().unwrap();
        assert_eq!(f1.len(), 17);
        let (f2, c2) = split_frame(&out[c1..]).unwrap().unwrap();
        assert_eq!(f2.len(), 9);
        assert_eq!(c1 + c2, out.len());
        assert!(split_frame(&out[..3]).unwrap().is_none());
        assert!(split_frame(&out[..10]).unwrap().is_none());
    }
}
