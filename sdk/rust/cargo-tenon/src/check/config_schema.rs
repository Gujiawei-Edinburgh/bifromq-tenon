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

//! Checks the self-contained Draft 2020-12 Plugin configuration profile.
//!
//! Only schema positions are traversed: annotation data such as examples and
//! constants may legitimately contain names that are forbidden as keywords.
//! Retrieval features are disabled, and external references are rejected before
//! compilation. No instance configuration is loaded or changed here.

use super::CheckError;
use serde_json::Value;

const DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";

pub(super) fn validate(bytes: &[u8]) -> Result<(), CheckError> {
    let schema = super::json::parse(bytes).map_err(|error| {
        CheckError::caused_by(
            "config_schema.invalid_json",
            "config.schema.json is invalid JSON",
            error,
        )
    })?;
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err(profile_error("/type"));
    }
    if schema.get("$schema").and_then(Value::as_str) != Some(DRAFT) {
        return Err(profile_error("/$schema"));
    }
    jsonschema::draft202012::meta::validate(&schema).map_err(|error| {
        CheckError::caused_by(
            "config_schema.invalid",
            "config.schema.json violates Draft 2020-12",
            error.to_owned(),
        )
    })?;
    profile(&schema, "")?;
    jsonschema::draft202012::new(&schema).map_err(|error| {
        CheckError::caused_by(
            "config_schema.invalid",
            "config.schema.json cannot be compiled",
            error,
        )
    })?;
    Ok(())
}

fn profile(schema: &Value, path: &str) -> Result<(), CheckError> {
    let Some(entries) = schema.as_object() else {
        return Ok(());
    };
    for (keyword, value) in entries {
        let child = format!("{path}/{}", keyword.replace('~', "~0").replace('/', "~1"));
        match keyword.as_str() {
            "$schema" if value.as_str() == Some(DRAFT) => {}
            "$ref" | "$dynamicRef"
                if value.as_str().is_some_and(|reference| {
                    reference.is_empty() || reference.starts_with('#')
                }) => {}
            "$defs" | "properties" | "patternProperties" | "dependentSchemas" => {
                if let Some(schemas) = value.as_object() {
                    for (name, schema) in schemas {
                        profile(
                            schema,
                            &format!("{child}/{}", name.replace('~', "~0").replace('/', "~1")),
                        )?;
                    }
                }
            }
            "prefixItems" | "allOf" | "anyOf" | "oneOf" => {
                if let Some(schemas) = value.as_array() {
                    for (index, schema) in schemas.iter().enumerate() {
                        profile(schema, &format!("{child}/{index}"))?;
                    }
                }
            }
            "additionalProperties"
            | "unevaluatedProperties"
            | "propertyNames"
            | "contains"
            | "items"
            | "unevaluatedItems"
            | "not"
            | "if"
            | "then"
            | "else"
            | "contentSchema" => profile(value, &child)?,
            "$id" | "$anchor" | "$dynamicAnchor" | "$comment" | "type" | "const" | "enum"
            | "multipleOf" | "maximum" | "exclusiveMaximum" | "minimum" | "exclusiveMinimum"
            | "maxLength" | "minLength" | "pattern" | "maxItems" | "minItems" | "uniqueItems"
            | "maxContains" | "minContains" | "maxProperties" | "minProperties" | "required"
            | "dependentRequired" | "title" | "description" | "deprecated" | "readOnly"
            | "writeOnly" | "examples" | "format" | "contentEncoding" | "contentMediaType" => {}
            _ => return Err(profile_error(&child)),
        }
    }
    Ok(())
}

fn profile_error(path: &str) -> CheckError {
    CheckError::new(
        "config_schema.profile_invalid",
        format!("config.schema.json is outside the Tenon profile at {path}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{DRAFT, validate};
    use proptest::prelude::*;
    use serde_json::json;

    proptest! {
        #[test]
        fn an_external_reference_is_rejected_before_compilation(name in "[a-z]{1,32}") {
            let schema = json!({"$schema": DRAFT, "type": "object", "properties": {"value": {"$ref": format!("https://example.invalid/{name}")}}});
            let error = validate(schema.to_string().as_bytes()).err().ok_or_else(|| TestCaseError::fail("external reference passed"))?;
            prop_assert_eq!(error.code(), "config_schema.profile_invalid");
        }
    }
}
