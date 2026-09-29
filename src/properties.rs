//! Port of `org.apache.activemq.artemis.utils.collections.TypedProperties`.
//!
//! Wire format:
//! ```text
//! byte NULL                      -- if there are no properties
//! byte NOT_NULL, int count, then for each entry:
//!     int keyLen, key bytes (UTF-16LE), byte type, value
//! ```

use std::collections::BTreeMap;

use bytes::BytesMut;

use crate::buffer::{Reader, WriteExt, NOT_NULL, NULL};
use crate::error::DecodeError;
use crate::simple_string::SimpleString;

pub const TYPE_NULL: u8 = 0;
pub const TYPE_BOOLEAN: u8 = 2;
pub const TYPE_BYTE: u8 = 3;
pub const TYPE_BYTES: u8 = 4;
pub const TYPE_SHORT: u8 = 5;
pub const TYPE_INT: u8 = 6;
pub const TYPE_LONG: u8 = 7;
pub const TYPE_FLOAT: u8 = 8;
pub const TYPE_DOUBLE: u8 = 9;
pub const TYPE_STRING: u8 = 10;
pub const TYPE_CHAR: u8 = 11;

/// A typed property value.
#[derive(Clone, Debug, PartialEq)]
pub enum PropertyValue {
    Null,
    Boolean(bool),
    Byte(i8),
    Bytes(Vec<u8>),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(SimpleString),
    /// A UTF-16 code unit (Java `char`).
    Char(u16),
}

impl PropertyValue {
    pub fn type_id(&self) -> u8 {
        match self {
            PropertyValue::Null => TYPE_NULL,
            PropertyValue::Boolean(_) => TYPE_BOOLEAN,
            PropertyValue::Byte(_) => TYPE_BYTE,
            PropertyValue::Bytes(_) => TYPE_BYTES,
            PropertyValue::Short(_) => TYPE_SHORT,
            PropertyValue::Int(_) => TYPE_INT,
            PropertyValue::Long(_) => TYPE_LONG,
            PropertyValue::Float(_) => TYPE_FLOAT,
            PropertyValue::Double(_) => TYPE_DOUBLE,
            PropertyValue::String(_) => TYPE_STRING,
            PropertyValue::Char(_) => TYPE_CHAR,
        }
    }

    /// Encoded size including the leading type byte.
    pub fn encoded_len(&self) -> usize {
        1 + match self {
            PropertyValue::Null => 0,
            PropertyValue::Boolean(_) | PropertyValue::Byte(_) => 1,
            PropertyValue::Bytes(b) => 4 + b.len(),
            PropertyValue::Short(_) | PropertyValue::Char(_) => 2,
            PropertyValue::Int(_) | PropertyValue::Float(_) => 4,
            PropertyValue::Long(_) | PropertyValue::Double(_) => 8,
            PropertyValue::String(s) => s.wire_size(),
        }
    }

    pub fn encode(&self, out: &mut BytesMut) {
        out.write_u8(self.type_id());
        match self {
            PropertyValue::Null => {}
            PropertyValue::Boolean(b) => out.write_bool(*b),
            PropertyValue::Byte(b) => out.write_i8(*b),
            PropertyValue::Bytes(b) => out.write_sized_bytes(b),
            PropertyValue::Short(s) => out.write_i16(*s),
            PropertyValue::Int(i) => out.write_i32(*i),
            PropertyValue::Long(l) => out.write_i64(*l),
            PropertyValue::Float(f) => out.write_f32(*f),
            PropertyValue::Double(d) => out.write_f64(*d),
            PropertyValue::String(s) => out.write_simple_string(s),
            PropertyValue::Char(c) => out.write_u16(*c),
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<PropertyValue, DecodeError> {
        let t = r.read_u8()?;
        Ok(match t {
            TYPE_NULL => PropertyValue::Null,
            TYPE_BOOLEAN => PropertyValue::Boolean(r.read_bool()?),
            TYPE_BYTE => PropertyValue::Byte(r.read_i8()?),
            TYPE_BYTES => PropertyValue::Bytes(r.read_sized_bytes()?.to_vec()),
            TYPE_SHORT => PropertyValue::Short(r.read_i16()?),
            TYPE_INT => PropertyValue::Int(r.read_i32()?),
            TYPE_LONG => PropertyValue::Long(r.read_i64()?),
            TYPE_FLOAT => PropertyValue::Float(r.read_f32()?),
            TYPE_DOUBLE => PropertyValue::Double(r.read_f64()?),
            TYPE_STRING => PropertyValue::String(r.read_simple_string()?),
            TYPE_CHAR => PropertyValue::Char(r.read_u16()?),
            other => return Err(DecodeError::Invalid(format!("invalid property type {other}"))),
        })
    }

    /// Renders the value as a string, following Java's `toString` conventions loosely.
    pub fn as_string(&self) -> Option<String> {
        match self {
            PropertyValue::Null => None,
            PropertyValue::Boolean(b) => Some(b.to_string()),
            PropertyValue::Byte(b) => Some(b.to_string()),
            PropertyValue::Bytes(b) => Some(format!("{b:?}")),
            PropertyValue::Short(s) => Some(s.to_string()),
            PropertyValue::Int(i) => Some(i.to_string()),
            PropertyValue::Long(l) => Some(l.to_string()),
            PropertyValue::Float(f) => Some(f.to_string()),
            PropertyValue::Double(d) => Some(d.to_string()),
            PropertyValue::String(s) => Some(s.to_string_lossy()),
            PropertyValue::Char(c) => Some(String::from_utf16_lossy(&[*c])),
        }
    }
}

macro_rules! from_value {
    ($t:ty, $variant:ident) => {
        impl From<$t> for PropertyValue {
            fn from(v: $t) -> Self {
                PropertyValue::$variant(v)
            }
        }
    };
}
from_value!(bool, Boolean);
from_value!(i8, Byte);
from_value!(Vec<u8>, Bytes);
from_value!(i16, Short);
from_value!(i32, Int);
from_value!(i64, Long);
from_value!(f32, Float);
from_value!(f64, Double);
from_value!(SimpleString, String);

impl From<&str> for PropertyValue {
    fn from(v: &str) -> Self {
        PropertyValue::String(SimpleString::new(v))
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        PropertyValue::String(SimpleString::new(&v))
    }
}

/// An ordered map of typed properties.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TypedProperties {
    map: BTreeMap<SimpleString, PropertyValue>,
}

impl TypedProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn put(&mut self, key: impl Into<SimpleString>, value: impl Into<PropertyValue>) -> &mut Self {
        self.map.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&PropertyValue> {
        self.map.get(&SimpleString::new(key))
    }

    pub fn get_ss(&self, key: &SimpleString) -> Option<&PropertyValue> {
        self.map.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<PropertyValue> {
        self.map.remove(&SimpleString::new(key))
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(&SimpleString::new(key))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SimpleString, &PropertyValue)> {
        self.map.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &SimpleString> {
        self.map.keys()
    }

    /// Convenience accessor returning the property as a string if it is a string.
    pub fn get_string(&self, key: &str) -> Option<String> {
        match self.get(key) {
            Some(PropertyValue::String(s)) => Some(s.to_string_lossy()),
            Some(other) => other.as_string(),
            None => None,
        }
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.get(key)? {
            PropertyValue::Long(l) => Some(*l),
            PropertyValue::Int(i) => Some(*i as i64),
            PropertyValue::Short(s) => Some(*s as i64),
            PropertyValue::Byte(b) => Some(*b as i64),
            _ => None,
        }
    }

    pub fn get_i32(&self, key: &str) -> Option<i32> {
        match self.get(key)? {
            PropertyValue::Int(i) => Some(*i),
            PropertyValue::Short(s) => Some(*s as i32),
            PropertyValue::Byte(b) => Some(*b as i32),
            _ => None,
        }
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.get(key)? {
            PropertyValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn get_bytes(&self, key: &str) -> Option<&[u8]> {
        match self.get(key)? {
            PropertyValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn encoded_len(&self) -> usize {
        if self.map.is_empty() {
            1
        } else {
            1 + 4 + self.map.iter().map(|(k, v)| k.wire_size() + v.encoded_len()).sum::<usize>()
        }
    }

    pub fn encode(&self, out: &mut BytesMut) {
        if self.map.is_empty() {
            out.write_u8(NULL);
            return;
        }
        out.write_u8(NOT_NULL);
        out.write_i32(self.map.len() as i32);
        for (k, v) in &self.map {
            out.write_simple_string(k);
            v.encode(out);
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<TypedProperties, DecodeError> {
        let mut props = TypedProperties::new();
        if r.read_u8()? == NULL {
            return Ok(props);
        }
        let n = r.read_i32()?;
        if n < 0 {
            return Err(DecodeError::Invalid(format!("negative property count {n}")));
        }
        for _ in 0..n {
            let key = r.read_simple_string()?;
            let value = PropertyValue::decode(r)?;
            props.map.insert(key, value);
        }
        Ok(props)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_encodes_as_single_null_byte() {
        let p = TypedProperties::new();
        let mut b = BytesMut::new();
        p.encode(&mut b);
        assert_eq!(&b[..], &[0]);
        assert_eq!(p.encoded_len(), 1);
        let mut r = Reader::new(&b);
        assert_eq!(TypedProperties::decode(&mut r).unwrap(), p);
    }

    #[test]
    fn all_types_round_trip() {
        let mut p = TypedProperties::new();
        p.put("null", PropertyValue::Null)
            .put("bool", true)
            .put("byte", -3i8)
            .put("bytes", vec![1u8, 2, 3])
            .put("short", -300i16)
            .put("int", 123456i32)
            .put("long", -9_000_000_000i64)
            .put("float", 1.5f32)
            .put("double", -2.25f64)
            .put("string", "hello ünïcödé")
            .put("char", PropertyValue::Char('x' as u16));
        let mut b = BytesMut::new();
        p.encode(&mut b);
        assert_eq!(b.len(), p.encoded_len());
        let mut r = Reader::new(&b);
        let decoded = TypedProperties::decode(&mut r).unwrap();
        assert_eq!(decoded, p);
        assert_eq!(r.remaining(), 0);
        assert_eq!(decoded.get_string("string").as_deref(), Some("hello ünïcödé"));
        assert_eq!(decoded.get_i64("long"), Some(-9_000_000_000));
        assert_eq!(decoded.get_i32("short"), Some(-300));
    }
}
