//! Minimal bencode decoder.
//!
//! Returns a typed `Value` tree and, as a special case, can track the raw byte
//! range of the "info" dict so the caller can compute its SHA-1.

use crate::error::{Error, Result};
use std::collections::BTreeMap;

/// A decoded bencode value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Raw byte string.
    Bytes(Vec<u8>),
    /// Signed 64-bit integer.
    Int(i64),
    /// Ordered list of values.
    List(Vec<Value>),
    /// Dictionary with byte-string keys (iteration order is sorted by key).
    Dict(BTreeMap<Vec<u8>, Value>),
}

impl Value {
    /// Return the inner byte slice if this is a `Bytes` variant, otherwise `None`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Value::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }

    /// Interpret this value as a UTF-8 string slice, or `None` if it is not valid UTF-8 bytes.
    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Return the inner integer if this is an `Int` variant, otherwise `None`.
    pub fn as_int(&self) -> Option<i64> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    /// Return a slice of the inner list if this is a `List` variant, otherwise `None`.
    pub fn as_list(&self) -> Option<&[Value]> {
        if let Value::List(l) = self {
            Some(l)
        } else {
            None
        }
    }

    /// Return a reference to the inner map if this is a `Dict` variant, otherwise `None`.
    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        if let Value::Dict(d) = self {
            Some(d)
        } else {
            None
        }
    }

    /// Convenience: look up a key in a `Value::Dict`.
    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.as_dict()?.get(key)
    }
}

// Parser
struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Parser { data, pos: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    /// Parse digits up to `delim`, consume delimiter, return integer value.
    fn parse_int_digits(&mut self, delim: u8) -> Result<i64> {
        let start = self.pos;
        loop {
            match self.peek() {
                None => return Err(Error::Bencode("unterminated integer".into())),
                Some(b) if b == delim => {
                    self.pos += 1;
                    break;
                }
                Some(_) => {
                    self.pos += 1;
                }
            }
        }
        let s = std::str::from_utf8(&self.data[start..self.pos - 1])
            .map_err(|e| Error::Bencode(format!("non-UTF8 integer: {e}")))?;
        s.parse::<i64>()
            .map_err(|e| Error::Bencode(format!("invalid integer '{s}': {e}")))
    }

    /// Parse a bencoded byte string: `<length>:<bytes>`
    fn parse_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.parse_int_digits(b':')?;
        if len < 0 {
            return Err(Error::Bencode("negative byte-string length".into()));
        }
        let len = len as usize;
        let end = self
            .pos
            .checked_add(len)
            .filter(|&e| e <= self.data.len())
            .ok_or_else(|| {
                Error::Bencode(format!(
                    "byte-string of length {len} exceeds input at offset {}",
                    self.pos
                ))
            })?;
        let bytes = self.data[self.pos..end].to_vec();
        self.pos = end;
        Ok(bytes)
    }

    fn parse_value(&mut self) -> Result<Value> {
        match self.peek() {
            Some(b'i') => {
                self.pos += 1;
                Ok(Value::Int(self.parse_int_digits(b'e')?))
            }
            Some(b'l') => {
                self.pos += 1;
                let mut list = Vec::new();
                while self.peek() != Some(b'e') {
                    if self.peek().is_none() {
                        return Err(Error::Bencode("unterminated list".into()));
                    }
                    list.push(self.parse_value()?);
                }
                self.pos += 1; // consume 'e'
                Ok(Value::List(list))
            }
            Some(b'd') => {
                self.pos += 1;
                let mut dict = BTreeMap::new();
                while self.peek() != Some(b'e') {
                    if self.peek().is_none() {
                        return Err(Error::Bencode("unterminated dict".into()));
                    }
                    let key = self.parse_bytes()?;
                    let val = self.parse_value()?;
                    dict.insert(key, val);
                }
                self.pos += 1; // consume 'e'
                Ok(Value::Dict(dict))
            }
            Some(b'0'..=b'9') => Ok(Value::Bytes(self.parse_bytes()?)),
            Some(b) => Err(Error::Bencode(format!(
                "unexpected byte 0x{b:02x} at offset {}",
                self.pos
            ))),
            None => Err(Error::Bencode("unexpected end of input".into())),
        }
    }
}

// Public API
/// Decode bencode data.
pub fn decode(data: &[u8]) -> Result<Value> {
    Parser::new(data).parse_value()
}

/// Decode a `.torrent` file and return both the parsed value tree **and** the
/// byte range `[start, end)` of the raw bencoded "info" dict (needed to
/// compute the info_hash without re-encoding).
pub fn decode_torrent(data: &[u8]) -> Result<(Value, Option<(usize, usize)>)> {
    let mut p = Parser::new(data);

    // Top-level must be a dict.
    match p.peek() {
        Some(b'd') => {
            p.pos += 1;
        }
        _ => return Err(Error::Bencode("torrent must start with a dict".into())),
    }

    let mut dict = BTreeMap::new();
    let mut info_range: Option<(usize, usize)> = None;

    while p.peek() != Some(b'e') {
        if p.peek().is_none() {
            return Err(Error::Bencode("unterminated top-level dict".into()));
        }
        let key = p.parse_bytes()?;
        if key == b"info" {
            let start = p.pos;
            let val = p.parse_value()?;
            info_range = Some((start, p.pos));
            dict.insert(key, val);
        } else {
            let val = p.parse_value()?;
            dict.insert(key, val);
        }
    }
    // Consume final top-level dict 'e' (pos not used further)
    let _ = p.pos;

    Ok((Value::Dict(dict), info_range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_integer_positive() {
        assert_eq!(decode(b"i42e").unwrap(), Value::Int(42));
    }

    #[test]
    fn decode_integer_negative() {
        assert_eq!(decode(b"i-1e").unwrap(), Value::Int(-1));
    }

    #[test]
    fn decode_integer_zero() {
        assert_eq!(decode(b"i0e").unwrap(), Value::Int(0));
    }

    #[test]
    fn decode_byte_string() {
        assert_eq!(decode(b"4:spam").unwrap(), Value::Bytes(b"spam".to_vec()));
    }

    #[test]
    fn decode_empty_byte_string() {
        assert_eq!(decode(b"0:").unwrap(), Value::Bytes(vec![]));
    }

    #[test]
    fn decode_list() {
        let v = decode(b"l4:spami42ee").unwrap();
        let list = v.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].as_bytes(), Some(b"spam".as_ref()));
        assert_eq!(list[1].as_int(), Some(42));
    }

    #[test]
    fn decode_empty_list() {
        assert_eq!(decode(b"le").unwrap(), Value::List(vec![]));
    }

    #[test]
    fn decode_dict() {
        let v = decode(b"d3:bari1e3:fooi2ee").unwrap();
        let d = v.as_dict().unwrap();
        assert_eq!(d.get(b"bar".as_ref()).and_then(|v| v.as_int()), Some(1));
        assert_eq!(d.get(b"foo".as_ref()).and_then(|v| v.as_int()), Some(2));
    }

    #[test]
    fn decode_empty_dict() {
        let v = decode(b"de").unwrap();
        assert_eq!(v.as_dict().unwrap().len(), 0);
    }

    #[test]
    fn decode_nested_structure() {
        let v = decode(b"d3:keyli1ei2ei3eee").unwrap();
        let list = v.get(b"key").unwrap().as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[2].as_int(), Some(3));
    }

    #[test]
    fn value_as_str_valid_utf8() {
        assert_eq!(decode(b"5:hello").unwrap().as_str(), Some("hello"));
    }

    #[test]
    fn value_as_str_non_utf8_returns_none() {
        let v = decode(b"3:\xff\xfe\xfd").unwrap();
        assert!(v.as_str().is_none());
    }

    #[test]
    fn value_get_missing_key_returns_none() {
        let v = decode(b"d3:keyi1ee").unwrap();
        assert!(v.get(b"other").is_none());
    }

    #[test]
    fn value_get_on_non_dict_returns_none() {
        let v = decode(b"i42e").unwrap();
        assert!(v.get(b"key").is_none());
    }

    #[test]
    fn decode_error_unterminated_integer() {
        assert!(decode(b"i42").is_err());
    }

    #[test]
    fn decode_error_truncated_byte_string() {
        assert!(decode(b"5:hi").is_err());
    }

    #[test]
    fn decode_error_unterminated_list() {
        assert!(decode(b"l4:spam").is_err());
    }

    #[test]
    fn decode_error_unterminated_dict() {
        assert!(decode(b"d4:name4:test").is_err());
    }

    #[test]
    fn decode_error_empty_input() {
        assert!(decode(b"").is_err());
    }

    #[test]
    fn decode_torrent_extracts_info_range() {
        // dict with "info" key — range should span the raw info-dict bytes.
        let data = b"d4:infod4:name4:testee";
        let (val, range) = decode_torrent(data).unwrap();
        assert!(val.as_dict().is_some());
        let (start, end) = range.expect("should have info range");
        // The slice should re-decode as a valid dict with the same contents.
        let info_val = decode(&data[start..end]).unwrap();
        assert_eq!(info_val.get(b"name").and_then(|v| v.as_str()), Some("test"));
    }

    #[test]
    fn decode_torrent_without_info_key_yields_none_range() {
        let data = b"d4:name4:teste";
        let (_, range) = decode_torrent(data).unwrap();
        assert!(range.is_none());
    }

    #[test]
    fn decode_torrent_non_dict_errors() {
        assert!(decode_torrent(b"l4:infoe").is_err());
    }
}
