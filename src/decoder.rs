use std::str::from_utf8;
use serde_json::Map;

#[allow(dead_code)]
pub fn decode_bencoded_value(encoded_value: Vec<u8>) -> (serde_json::Value, Vec<u8>) {
    match encoded_value.first() {
        Some(b'i') => {
            match encoded_value
                            .split_at(1)
                            .1
                            .split_at(encoded_value[1..].iter().position(|&b| b == b'e').unwrap_or(encoded_value.len() - 1)) {
                (digits, rest) => {
                    let n: String = std::str::from_utf8(digits).expect("Invalid integer format").parse().expect("Invalid integer format");
                    return (n.into(), rest[1..].to_vec());
                }
                _ => {
                            panic!("Invalid integer format");
                        }
            }
        }
        Some(b'd') => {
            let mut values = Map::new();
            let mut rest = encoded_value.split_at(1).1.to_vec();
            while !rest.is_empty() && !rest.starts_with(&[b'e']) {
                let (key, remainder) = decode_bencoded_value(rest.to_vec());
                let key = match key {
                    serde_json::Value::String(key) => key,
                    key => {
                        panic!("Dict keys must be strings, not {key:?}");
                    }
                };
                let (value, remainder) = decode_bencoded_value(remainder.to_vec());
                values.insert(key.to_string(), value);
                rest = remainder;
            }

            return (values.into(), rest[1..].to_vec())

        },
        Some(b'l') => {
            let mut values = Vec::new();
            let mut rest = encoded_value.split_at(1).1.to_vec();
            while !rest.is_empty() && !rest.starts_with(&[b'e']) {
                let (value, remainder) = decode_bencoded_value(rest.to_vec());
                values.push(value);
                rest = remainder;
            }

            return (values.into(), rest[1..].to_vec());
        },
        Some(b'0'..=b'9') => {
            let colon_position = encoded_value.iter().position(|&byte| byte == b':').unwrap_or(encoded_value.len() - 1);
            let (len, rest) = encoded_value.split_at(colon_position);

            let len: String = from_utf8(&len).expect("Invalid string length").parse().expect("Invalid string length");
            let len = match len.parse::<usize>() {
                Ok(len) => len,
                Err(_) => panic!("Invalid string length"),
            };
        
            if rest.len() < len {
                panic!("String length exceeds available data");
            }
            return (rest[..len].into(), rest[len..].to_vec());
        },
        _ => panic!("Unhandled encoded value: {:?}", encoded_value)
    }
}