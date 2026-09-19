/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::parse;
use proptest::prelude::*;
use serde_json::json;
use std::error::Error;

#[test]
fn malformed_json_and_extensions_are_rejected() {
    for input in [
        b"".as_slice(),
        b"\xff",
        b"{} {}",
        b"/* comment */ {}",
        b"{\"a\":1,}",
        b"{'a':1}",
        b"{a:1}",
        b"{\"a\":1 \"b\":2}",
        b"{\"a\":+1}",
        b"{\"a\":0x10}",
        b"{\"a\":01}",
        b"{\"a\":1.}",
        b"{\"a\":1e}",
    ] {
        assert!(parse(input).is_err(), "Accepted invalid input: {input:?}");
    }
}

#[test]
fn decoded_duplicate_keys_are_rejected_at_every_depth() {
    for input in [
        r#"{"a":1,"a":2}"#,
        r#"{"a":1,"\u0061":2}"#,
        r#"{"outer":[{"nested":{"a":1,"\u0061":2}}]}"#,
    ] {
        assert!(parse(input.as_bytes()).is_err(), "{input}");
    }
}

#[test]
fn number_values_preserve_precision_and_exponents() -> Result<(), Box<dyn Error>> {
    for literal in [
        "18446744073709551616000000000000000001",
        "-18446744073709551616000000000000000001",
        "0.123456789012345678901234567890123456789",
        "1e+4000",
        "1e-4000",
    ] {
        let input = format!("{{\"value\":{literal}}}");
        let value = parse(input.as_bytes())?;
        let number = value["value"].as_number().ok_or("Expected a JSON number")?;
        assert_eq!(number.to_string(), literal);
    }
    Ok(())
}

#[test]
fn object_keys_and_values_keep_their_json_meaning() -> Result<(), Box<dyn Error>> {
    let input = br#"{"$serde_json::private::Number":"business value","\u0061":[null,true,false,{"a":1}],"text":"escaped\ntext"}"#;
    assert_eq!(
        parse(input)?,
        json!({"$serde_json::private::Number":"business value", "a":[null,true,false,{"a":1}], "text":"escaped\ntext"})
    );
    Ok(())
}

proptest! {
    #[test]
    fn decimal_numbers_keep_every_digit(
        sign in prop::bool::ANY,
        integer in "[1-9][0-9]{0,100}",
        fraction in "[0-9]{1,100}",
        exponent in -10000i32..10000,
    ) {
        let sign = if sign { "-" } else { "" };
        let literal = format!("{sign}{integer}.{fraction}e{exponent:+}");
        let value = parse(literal.as_bytes())?;
        prop_assert_eq!(value.as_number().map(ToString::to_string), Some(literal));
    }

    #[test]
    fn escaped_keys_collide_with_their_decoded_name(name in "[a-z][a-z0-9]{0,31}") {
        let escaped = format!("\\u{:04x}{}", name.as_bytes()[0], &name[1..]);
        let input = format!(r#"{{"nested":[{{"{name}":1,"{escaped}":2}}]}}"#);
        prop_assert!(parse(input.as_bytes()).is_err());
    }
}
