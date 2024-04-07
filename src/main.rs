use serde_json::{self, Map};
use std::{collections::HashMap, env};

// Available if you need it!
// use serde_bencode

#[allow(dead_code)]
fn decode_bencoded_value(encoded_value: &str) -> (serde_json::Value, &str) {
    match encoded_value.chars().next() {
        Some('i') => {
            if let Some((n, rest)) =
                encoded_value
                    .split_at(1)
                    .1
                    .split_once('e')
                    .and_then(|(digits, rest)| {
                        let n = digits.parse::<i64>().ok()?;
                        Some((n, rest))
                    })
            {
                return (n.into(), rest);
            } else {
                panic!("Invalid integer format");
            }
        },
        Some('d') => {
            let mut values = Map::new();
            let mut rest = encoded_value.split_at(1).1;
            while !rest.is_empty() && !rest.starts_with('e') {
                let (key, remainder) = decode_bencoded_value(rest);
                let key = match key {
                    serde_json::Value::String(key) => key,
                    key => {
                        panic!("Dict keys must be strings, not {key:?}");
                    }
                };
                let (value, remainder) = decode_bencoded_value(remainder);
                values.insert(key.to_string(), value);
                rest = remainder;
            }

            return (values.into(), &rest[1..])

        },
        Some('l') => {
            let mut values = Vec::new();
            let mut rest = encoded_value.split_at(1).1;
            while !rest.is_empty() && !rest.starts_with('e') {
                let (value, remainder) = decode_bencoded_value(rest);
                values.push(value);
                rest = remainder;
            }

            return (values.into(), &rest[1..]);
        },
        Some('0'..='9') => {
            let (len, rest) = match encoded_value.split_once(':') {
                Some((len, rest)) => (len, rest),
                None => panic!("Invalid string format")
            };

            let len = match len.parse::<usize>() {
                Ok(len) => len,
                Err(_) => panic!("Invalid string length"),
            };
        
            if rest.len() < len {
                panic!("String length exceeds available data");
            }
            return (rest[..len].into(), &rest[len..]);
        },
        _ => panic!("Unhandled encoded value: {}", encoded_value)
    }
}

// Usage: your_bittorrent.sh decode "<encoded_value>"
fn main() {
    let args: Vec<String> = env::args().collect();
    let command = &args[1];

    if command == "decode" {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        eprintln!("Logs from your program will appear here!");

        // Uncomment this block to pass the first stage
        let encoded_value = &args[2];
        let (value, _) = decode_bencoded_value(encoded_value);
        println!("{}", value.to_string());
    } else {
        eprintln!("unknown command: {}", args[1])
    }
}
