//! Port of `org.apache.activemq.artemis.api.core.SimpleString`.
//!
//! A `SimpleString` is a string stored as UTF-16 code units in little-endian byte order
//! (low byte first, then high byte). On the wire it is encoded as a 4-byte big-endian
//! byte length followed by the raw bytes.

use std::borrow::Borrow;
use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct SimpleString {
    data: Vec<u8>,
}

impl SimpleString {
    /// Creates a `SimpleString` from a Rust string.
    pub fn new(s: &str) -> Self {
        let mut data = Vec::with_capacity(s.len() * 2);
        for unit in s.encode_utf16() {
            data.push((unit & 0xFF) as u8);
            data.push((unit >> 8) as u8);
        }
        SimpleString { data }
    }

    /// Wraps raw UTF-16LE bytes as received from the wire.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        SimpleString { data }
    }

    /// The raw UTF-16LE bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    /// Number of UTF-16 code units.
    pub fn len(&self) -> usize {
        self.data.len() / 2
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Size of the wire encoding (4 byte length prefix plus data).
    pub fn wire_size(&self) -> usize {
        4 + self.data.len()
    }

    pub fn starts_with(&self, prefix: &SimpleString) -> bool {
        self.data.starts_with(&prefix.data)
    }

    pub fn concat(&self, other: &SimpleString) -> SimpleString {
        let mut data = self.data.clone();
        data.extend_from_slice(&other.data);
        SimpleString { data }
    }

    /// Converts back to a Rust `String`; unpaired surrogates are replaced with U+FFFD.
    pub fn to_string_lossy(&self) -> String {
        let units: Vec<u16> = self.data.chunks(2).map(|c| u16::from_le_bytes([c[0], c[1]])).collect();
        String::from_utf16_lossy(&units)
    }
}

impl fmt::Debug for SimpleString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimpleString({:?})", self.to_string_lossy())
    }
}

impl fmt::Display for SimpleString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        SimpleString::new(s)
    }
}

impl From<String> for SimpleString {
    fn from(s: String) -> Self {
        SimpleString::new(&s)
    }
}

impl From<&String> for SimpleString {
    fn from(s: &String) -> Self {
        SimpleString::new(s)
    }
}

impl From<SimpleString> for String {
    fn from(s: SimpleString) -> Self {
        s.to_string_lossy()
    }
}

impl PartialEq<str> for SimpleString {
    fn eq(&self, other: &str) -> bool {
        self.to_string_lossy() == other
    }
}

impl PartialEq<&str> for SimpleString {
    fn eq(&self, other: &&str) -> bool {
        self.to_string_lossy() == *other
    }
}

impl Borrow<[u8]> for SimpleString {
    fn borrow(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_ascii_and_unicode() {
        for s in ["", "a", "hello world", "héllo", "日本語", "emoji 😀 pair"] {
            let ss = SimpleString::new(s);
            assert_eq!(ss.to_string_lossy(), s);
            assert_eq!(ss.len(), s.encode_utf16().count());
        }
    }

    #[test]
    fn byte_layout_is_utf16le() {
        let ss = SimpleString::new("AB");
        assert_eq!(ss.as_bytes(), &[0x41, 0x00, 0x42, 0x00]);
        let ss = SimpleString::new("é");
        assert_eq!(ss.as_bytes(), &[0xE9, 0x00]);
    }
}
