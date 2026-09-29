//! Error types for the CORE protocol codec and client.

use std::fmt;

/// Error codes carried by `ActiveMQException` packets (see `ActiveMQExceptionType` in Artemis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExceptionType(pub i32);

macro_rules! exception_types {
    ($($name:ident = $code:expr),* $(,)?) => {
        impl ExceptionType {
            $(pub const $name: ExceptionType = ExceptionType($code);)*

            /// Symbolic name of the code if it is a known Artemis exception type.
            pub fn name(self) -> Option<&'static str> {
                match self.0 {
                    $($code => Some(stringify!($name)),)*
                    _ => None,
                }
            }
        }
    };
}

exception_types! {
    INTERNAL_ERROR = 0,
    UNSUPPORTED_PACKET = 1,
    NOT_CONNECTED = 2,
    CONNECTION_TIMEDOUT = 3,
    DISCONNECTED = 4,
    UNBLOCKED = 5,
    IO_ERROR = 6,
    QUEUE_DOES_NOT_EXIST = 100,
    QUEUE_EXISTS = 101,
    OBJECT_CLOSED = 102,
    INVALID_FILTER_EXPRESSION = 103,
    ILLEGAL_STATE = 104,
    SECURITY_EXCEPTION = 105,
    ADDRESS_DOES_NOT_EXIST = 106,
    ADDRESS_EXISTS = 107,
    INCOMPATIBLE_CLIENT_SERVER_VERSIONS = 108,
    LARGE_MESSAGE_ERROR_BODY = 110,
    TRANSACTION_ROLLED_BACK = 111,
    SESSION_CREATION_REJECTED = 112,
    DUPLICATE_ID_REJECTED = 113,
    DUPLICATE_METADATA = 114,
    TRANSACTION_OUTCOME_UNKNOWN = 115,
    ALREADY_REPLICATING = 116,
    INTERCEPTOR_REJECTED_PACKET = 117,
    INVALID_TRANSIENT_QUEUE_USE = 118,
    REMOTE_DISCONNECT = 119,
    TRANSACTION_TIMEOUT = 120,
    NATIVE_ERROR_INTERNAL = 200,
    NATIVE_ERROR_INVALID_BUFFER = 201,
    NATIVE_ERROR_NOT_ALIGNED = 202,
    NATIVE_ERROR_CANT_INITIALIZE_AIO = 203,
    NATIVE_ERROR_CANT_RELEASE_AIO = 204,
    NATIVE_ERROR_CANT_OPEN_CLOSE_FILE = 205,
    NATIVE_ERROR_CANT_ALLOCATE_QUEUE = 206,
    NATIVE_ERROR_PREALLOCATE_FILE = 208,
    NATIVE_ERROR_ALLOCATE_MEMORY = 209,
    ADDRESS_FULL = 210,
    LARGE_MESSAGE_INTERRUPTED = 211,
    CLUSTER_SECURITY_EXCEPTION = 212,
    NOT_IMPLEMENTED_EXCEPTION = 213,
    MAX_CONSUMER_LIMIT_EXCEEDED = 214,
    UNEXPECTED_ROUTING_TYPE_FOR_ADDRESS = 215,
    INVALID_QUEUE_CONFIGURATION = 216,
    DELETE_ADDRESS_ERROR = 217,
    NULL_REF = 218,
    SHUTDOWN_ERROR = 219,
    REPLICATION_TIMEOUT_ERROR = 220,
    DIVERT_DOES_NOT_EXIST = 221,
    ROUTING_EXCEPTION = 222,
    TIMEOUT_EXCEPTION = 223,
    GENERIC_EXCEPTION = 999,
}

impl fmt::Display for ExceptionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(n) => write!(f, "{n}({})", self.0),
            None => write!(f, "UNKNOWN({})", self.0),
        }
    }
}

/// XA error / return codes as defined by `javax.transaction.xa.XAException` and `XAResource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct XaCode(pub i32);

macro_rules! xa_codes {
    ($($name:ident = $code:expr),* $(,)?) => {
        impl XaCode {
            $(pub const $name: XaCode = XaCode($code);)*

            pub fn name(self) -> Option<&'static str> {
                match self.0 {
                    $($code => Some(stringify!($name)),)*
                    _ => None,
                }
            }
        }
    };
}

xa_codes! {
    // XA_RBBASE == XA_RBROLLBACK (100) and XA_RBEND == XA_RBTRANSIENT (107)
    XA_RBROLLBACK = 100,
    XA_RBCOMMFAIL = 101,
    XA_RBDEADLOCK = 102,
    XA_RBINTEGRITY = 103,
    XA_RBOTHER = 104,
    XA_RBPROTO = 105,
    XA_RBTIMEOUT = 106,
    XA_RBTRANSIENT = 107,
    XA_NOMIGRATE = 9,
    XA_HEURHAZ = 8,
    XA_HEURCOM = 7,
    XA_HEURRB = 6,
    XA_HEURMIX = 5,
    XA_RETRY = 4,
    XA_RDONLY = 3,
    XA_OK = 0,
    XAER_ASYNC = -2,
    XAER_RMERR = -3,
    XAER_NOTA = -4,
    XAER_INVAL = -5,
    XAER_PROTO = -6,
    XAER_RMFAIL = -7,
    XAER_DUPID = -8,
    XAER_OUTSIDE = -9,
}

impl fmt::Display for XaCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(n) => write!(f, "{n}({})", self.0),
            None => write!(f, "XA({})", self.0),
        }
    }
}

/// Errors produced while decoding wire data.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("buffer underflow: needed {needed} more bytes at offset {offset}")]
    Underflow { needed: usize, offset: usize },
    #[error("invalid data: {0}")]
    Invalid(String),
    #[error("unknown packet type {0}")]
    UnknownPacketType(i8),
}

/// Top level error type of the crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("encode error: {0}")]
    Encode(String),
    /// An `ActiveMQException` returned by the broker.
    #[error("broker exception {code}: {message:?}")]
    Broker { code: ExceptionType, message: Option<String> },
    /// An XA operation failed on the broker.
    #[error("XA error {code}: {message:?}")]
    Xa { code: XaCode, message: Option<String> },
    #[error("timed out after {0:?} waiting for a response to packet type {1}")]
    Timeout(std::time::Duration, i8),
    #[error("connection closed: {0}")]
    Closed(String),
    #[error("unexpected packet type {got} (expected {expected})")]
    UnexpectedPacket { expected: i8, got: i8 },
    #[error("incompatible client/server versions")]
    IncompatibleVersion,
    #[error("illegal state: {0}")]
    IllegalState(String),
}

impl Error {
    pub fn broker(code: i32, message: Option<String>) -> Self {
        Error::Broker { code: ExceptionType(code), message }
    }

    pub fn xa(code: i32, message: Option<String>) -> Self {
        Error::Xa { code: XaCode(code), message }
    }

    /// Returns the Artemis exception type when this is a broker-side error.
    pub fn exception_type(&self) -> Option<ExceptionType> {
        match self {
            Error::Broker { code, .. } => Some(*code),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
