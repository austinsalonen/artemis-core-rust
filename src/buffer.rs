//! Byte-level readers and writers matching `ActiveMQBuffer` / `ChannelBufferWrapper` semantics.
//!
//! All multi-byte integers are big-endian (Netty `ByteBuf` default). Strings have three
//! encodings on the wire (see `UTF8Util.writeString` in Artemis):
//!
//! * `writeString`: `int charLength` then
//!   * `charLength < 9`  : each UTF-16 code unit as a big-endian `short`
//!   * `charLength < 0xfff`: `unsigned short byteLength` + "UTF-8" where each UTF-16 unit is
//!     encoded separately (1, 2 or 3 bytes; surrogates are encoded individually)
//!   * otherwise: a `SimpleString` (`int byteLength` + UTF-16LE bytes)
//! * `writeNullableString`: `byte NULL/NOT_NULL` then `writeString`
//! * `writeSimpleString`: `int byteLength` + UTF-16LE bytes

use bytes::{BufMut, BytesMut};

use crate::error::DecodeError;
use crate::simple_string::SimpleString;

pub const NULL: u8 = 0;
pub const NOT_NULL: u8 = 1;

/// Number of bytes of the fixed packet header: `int size` + `byte type` + `long channelID`.
pub const PACKET_HEADERS_SIZE: usize = 4 + 1 + 8;

// ---------------------------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------------------------

/// Extension methods for writing CORE-encoded values into a `BytesMut`.
pub trait WriteExt {
    fn write_bool(&mut self, v: bool);
    fn write_i8(&mut self, v: i8);
    fn write_u8(&mut self, v: u8);
    fn write_i16(&mut self, v: i16);
    fn write_u16(&mut self, v: u16);
    fn write_i32(&mut self, v: i32);
    fn write_i64(&mut self, v: i64);
    fn write_f32(&mut self, v: f32);
    fn write_f64(&mut self, v: f64);
    fn write_bytes(&mut self, v: &[u8]);
    /// `int length` + raw bytes.
    fn write_sized_bytes(&mut self, v: &[u8]);
    fn write_string(&mut self, v: &str);
    fn write_nullable_string(&mut self, v: Option<&str>);
    fn write_simple_string(&mut self, v: &SimpleString);
    fn write_nullable_simple_string(&mut self, v: Option<&SimpleString>);
    /// `BufferHelper.writeNullableBoolean`: presence flag then value.
    fn write_nullable_bool(&mut self, v: Option<bool>);
    fn write_nullable_i32(&mut self, v: Option<i32>);
    fn write_nullable_i64(&mut self, v: Option<i64>);
    /// Java modified UTF ("`writeUTF`"): `unsigned short byteLength` + per-code-unit UTF-8.
    fn write_utf(&mut self, v: &str);
}

/// Number of bytes `write_utf` produces for a string (excluding the length prefix).
pub fn utf_size(s: &str) -> usize {
    s.encode_utf16()
        .map(|c| if c <= 0x7f { 1 } else if c >= 0x800 { 3 } else { 2 })
        .sum()
}

impl WriteExt for BytesMut {
    fn write_bool(&mut self, v: bool) {
        self.put_u8(if v { 1 } else { 0 });
    }
    fn write_i8(&mut self, v: i8) {
        self.put_i8(v);
    }
    fn write_u8(&mut self, v: u8) {
        self.put_u8(v);
    }
    fn write_i16(&mut self, v: i16) {
        self.put_i16(v);
    }
    fn write_u16(&mut self, v: u16) {
        self.put_u16(v);
    }
    fn write_i32(&mut self, v: i32) {
        self.put_i32(v);
    }
    fn write_i64(&mut self, v: i64) {
        self.put_i64(v);
    }
    fn write_f32(&mut self, v: f32) {
        self.put_f32(v);
    }
    fn write_f64(&mut self, v: f64) {
        self.put_f64(v);
    }
    fn write_bytes(&mut self, v: &[u8]) {
        self.put_slice(v);
    }
    fn write_sized_bytes(&mut self, v: &[u8]) {
        self.put_i32(v.len() as i32);
        self.put_slice(v);
    }
    fn write_string(&mut self, v: &str) {
        let char_len = v.encode_utf16().count();
        self.put_i32(char_len as i32);
        if char_len < 9 {
            for unit in v.encode_utf16() {
                self.put_u16(unit);
            }
        } else if char_len < 0xfff {
            self.write_utf(v);
        } else {
            self.write_simple_string(&SimpleString::new(v));
        }
    }
    fn write_nullable_string(&mut self, v: Option<&str>) {
        match v {
            None => self.put_u8(NULL),
            Some(s) => {
                self.put_u8(NOT_NULL);
                self.write_string(s);
            }
        }
    }
    fn write_simple_string(&mut self, v: &SimpleString) {
        self.put_i32(v.as_bytes().len() as i32);
        self.put_slice(v.as_bytes());
    }
    fn write_nullable_simple_string(&mut self, v: Option<&SimpleString>) {
        match v {
            None => self.put_u8(NULL),
            Some(s) => {
                self.put_u8(NOT_NULL);
                self.write_simple_string(s);
            }
        }
    }
    fn write_nullable_bool(&mut self, v: Option<bool>) {
        self.write_bool(v.is_some());
        if let Some(b) = v {
            self.write_bool(b);
        }
    }
    fn write_nullable_i32(&mut self, v: Option<i32>) {
        self.write_bool(v.is_some());
        if let Some(i) = v {
            self.put_i32(i);
        }
    }
    fn write_nullable_i64(&mut self, v: Option<i64>) {
        self.write_bool(v.is_some());
        if let Some(i) = v {
            self.put_i64(i);
        }
    }
    fn write_utf(&mut self, v: &str) {
        let len = utf_size(v);
        assert!(len <= 0xffff, "string too long for writeUTF: {len} bytes");
        self.put_u16(len as u16);
        for c in v.encode_utf16() {
            if c <= 0x7f {
                self.put_u8(c as u8);
            } else if c >= 0x800 {
                self.put_u8(0xE0 | ((c >> 12) & 0x0F) as u8);
                self.put_u8(0x80 | ((c >> 6) & 0x3F) as u8);
                self.put_u8(0x80 | (c & 0x3F) as u8);
            } else {
                self.put_u8(0xC0 | ((c >> 6) & 0x1F) as u8);
                self.put_u8(0x80 | (c & 0x3F) as u8);
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------------------------

/// A bounds-checked cursor over a byte slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) -> Result<(), DecodeError> {
        if pos > self.buf.len() {
            return Err(DecodeError::Underflow { needed: pos - self.buf.len(), offset: self.pos });
        }
        self.pos = pos;
        Ok(())
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    /// The whole underlying slice.
    pub fn as_slice(&self) -> &'a [u8] {
        self.buf
    }

    /// Slice of all remaining bytes (does not advance).
    pub fn peek_remaining(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    fn need(&self, n: usize) -> Result<(), DecodeError> {
        if self.remaining() < n {
            Err(DecodeError::Underflow { needed: n - self.remaining(), offset: self.pos })
        } else {
            Ok(())
        }
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        self.need(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn skip(&mut self, n: usize) -> Result<(), DecodeError> {
        self.need(n)?;
        self.pos += n;
        Ok(())
    }

    pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.read_bytes(1)?[0])
    }

    pub fn read_i8(&mut self) -> Result<i8, DecodeError> {
        Ok(self.read_u8()? as i8)
    }

    pub fn read_bool(&mut self) -> Result<bool, DecodeError> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_i16(&mut self) -> Result<i16, DecodeError> {
        let b = self.read_bytes(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let b = self.read_bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    pub fn read_i32(&mut self) -> Result<i32, DecodeError> {
        let b = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn read_i64(&mut self) -> Result<i64, DecodeError> {
        let b = self.read_bytes(8)?;
        Ok(i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    pub fn read_f32(&mut self) -> Result<f32, DecodeError> {
        Ok(f32::from_bits(self.read_i32()? as u32))
    }

    pub fn read_f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_bits(self.read_i64()? as u64))
    }

    fn read_len(&mut self) -> Result<usize, DecodeError> {
        let n = self.read_i32()?;
        if n < 0 {
            return Err(DecodeError::Invalid(format!("negative length {n}")));
        }
        let n = n as usize;
        self.need(n)?;
        Ok(n)
    }

    /// `int length` + raw bytes.
    pub fn read_sized_bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.read_len()?;
        self.read_bytes(n)
    }

    pub fn read_simple_string(&mut self) -> Result<SimpleString, DecodeError> {
        let n = self.read_len()?;
        if n % 2 != 0 {
            return Err(DecodeError::Invalid(format!("odd SimpleString byte length {n}")));
        }
        Ok(SimpleString::from_bytes(self.read_bytes(n)?.to_vec()))
    }

    pub fn read_nullable_simple_string(&mut self) -> Result<Option<SimpleString>, DecodeError> {
        if self.read_u8()? == NULL {
            Ok(None)
        } else {
            Ok(Some(self.read_simple_string()?))
        }
    }

    pub fn read_utf(&mut self) -> Result<String, DecodeError> {
        let size = self.read_u16()? as usize;
        let bytes = self.read_bytes(size)?;
        decode_utf(bytes)
    }

    pub fn read_string(&mut self) -> Result<String, DecodeError> {
        let len = self.read_i32()?;
        if len < 0 {
            return Err(DecodeError::Invalid(format!("negative string length {len}")));
        }
        let len = len as usize;
        if len < 9 {
            let mut units = Vec::with_capacity(len);
            for _ in 0..len {
                units.push(self.read_u16()?);
            }
            Ok(String::from_utf16_lossy(&units))
        } else if len < 0xfff {
            self.read_utf()
        } else {
            Ok(self.read_simple_string()?.to_string_lossy())
        }
    }

    pub fn read_nullable_string(&mut self) -> Result<Option<String>, DecodeError> {
        if self.read_u8()? == NULL {
            Ok(None)
        } else {
            Ok(Some(self.read_string()?))
        }
    }

    pub fn read_nullable_bool(&mut self) -> Result<Option<bool>, DecodeError> {
        if self.read_bool()? {
            Ok(Some(self.read_bool()?))
        } else {
            Ok(None)
        }
    }

    pub fn read_nullable_i32(&mut self) -> Result<Option<i32>, DecodeError> {
        if self.read_bool()? {
            Ok(Some(self.read_i32()?))
        } else {
            Ok(None)
        }
    }

    pub fn read_nullable_i64(&mut self) -> Result<Option<i64>, DecodeError> {
        if self.read_bool()? {
            Ok(Some(self.read_i64()?))
        } else {
            Ok(None)
        }
    }
}

/// Decodes the per-code-unit UTF-8 variant used by `UTF8Util.readUTF`.
pub fn decode_utf(bytes: &[u8]) -> Result<String, DecodeError> {
    let mut units: Vec<u16> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b1 = bytes[i];
        i += 1;
        if b1 < 0x80 {
            units.push(b1 as u16);
        } else {
            match b1 >> 4 {
                0xC | 0xD => {
                    let b2 = *bytes.get(i).ok_or(DecodeError::Underflow { needed: 1, offset: i })?;
                    i += 1;
                    units.push((((b1 & 0x1F) as u16) << 6) | (b2 & 0x3F) as u16);
                }
                0xE => {
                    if i + 1 >= bytes.len() {
                        return Err(DecodeError::Underflow { needed: 2, offset: i });
                    }
                    let b2 = bytes[i];
                    let b3 = bytes[i + 1];
                    i += 2;
                    units.push(
                        (((b1 & 0x0F) as u16) << 12) | (((b2 & 0x3F) as u16) << 6) | (b3 & 0x3F) as u16,
                    );
                }
                _ => return Err(DecodeError::Invalid(format!("unhandled utf8 byte {b1:#x}"))),
            }
        }
    }
    Ok(String::from_utf16_lossy(&units))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt_string(s: &str) {
        let mut b = BytesMut::new();
        b.write_string(s);
        let mut r = Reader::new(&b);
        assert_eq!(r.read_string().unwrap(), s);
        assert_eq!(r.remaining(), 0);

        let mut b = BytesMut::new();
        b.write_nullable_string(Some(s));
        let mut r = Reader::new(&b);
        assert_eq!(r.read_nullable_string().unwrap().as_deref(), Some(s));
    }

    #[test]
    fn string_encodings_round_trip() {
        rt_string("");
        rt_string("short");
        rt_string("exactly8");
        rt_string("nine char");
        rt_string("a medium length string with ünïcödé and 日本語 and 😀 emoji");
        rt_string(&"x".repeat(0xffe));
        rt_string(&"y".repeat(0xfff));
        rt_string(&"日".repeat(5000));
    }

    #[test]
    fn short_strings_use_shorts() {
        let mut b = BytesMut::new();
        b.write_string("ab");
        assert_eq!(&b[..], &[0, 0, 0, 2, 0, b'a', 0, b'b']);
    }

    #[test]
    fn medium_strings_use_utf() {
        let mut b = BytesMut::new();
        b.write_string("123456789");
        assert_eq!(&b[..6], &[0, 0, 0, 9, 0, 9]);
        assert_eq!(&b[6..], b"123456789");
    }

    #[test]
    fn nullable_helpers() {
        let mut b = BytesMut::new();
        b.write_nullable_bool(None);
        b.write_nullable_bool(Some(true));
        b.write_nullable_i32(Some(-5));
        b.write_nullable_i64(None);
        b.write_nullable_simple_string(None);
        b.write_nullable_simple_string(Some(&"q".into()));
        let mut r = Reader::new(&b);
        assert_eq!(r.read_nullable_bool().unwrap(), None);
        assert_eq!(r.read_nullable_bool().unwrap(), Some(true));
        assert_eq!(r.read_nullable_i32().unwrap(), Some(-5));
        assert_eq!(r.read_nullable_i64().unwrap(), None);
        assert_eq!(r.read_nullable_simple_string().unwrap(), None);
        assert_eq!(r.read_nullable_simple_string().unwrap().unwrap(), "q");
        assert!(r.read_u8().is_err());
    }
}
