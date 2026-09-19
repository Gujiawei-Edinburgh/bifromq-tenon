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

//! Validates the public query before any process is collected.

use crate::metrics::catalog;
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ToSchema)]
#[schema(rename_all = "lowercase")]
pub(super) enum Format {
    #[default]
    Json,
    Prometheus,
}

pub(super) struct MetricsQuery {
    pub(super) format: Format,
    pub(super) include: Vec<String>,
}

impl MetricsQuery {
    pub(super) fn parse(pairs: Vec<(String, String)>) -> Result<Self, ()> {
        let mut format = None;
        let mut include = None;
        for (name, value) in pairs {
            match name.as_str() {
                "format" if format.is_none() => {
                    format = Some(match value.as_str() {
                        "json" => Format::Json,
                        "prometheus" => Format::Prometheus,
                        _ => return Err(()),
                    });
                }
                "include" if include.is_none() => {
                    let mut names = Vec::new();
                    for name in value.split(',').map(str::trim) {
                        if !catalog::definitions()
                            .iter()
                            .any(|metric| metric.name == name)
                        {
                            return Err(());
                        }
                        if !names.iter().any(|included| included == name) {
                            names.push(name.to_owned());
                        }
                    }
                    include = Some(names);
                }
                _ => return Err(()),
            }
        }
        Ok(Self {
            format: format.unwrap_or_default(),
            include: include.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_filters_are_rejected_before_collection() {
        for pairs in [
            vec![("format", "")],
            vec![("format", "otlp")],
            vec![("format", "JSON")],
            vec![("include", "")],
            vec![("include", "tenon.process.cpu,")],
            vec![("include", "tenon_process_cpu")],
            vec![("include", "tenon.*")],
            vec![("include", "unknown")],
            vec![("other", "value")],
            vec![("format", "json"), ("format", "json")],
            vec![
                ("include", "tenon.process.cpu"),
                ("include", "tenon.process.memory"),
            ],
        ] {
            assert!(
                MetricsQuery::parse(
                    pairs
                        .into_iter()
                        .map(|(key, value)| (key.to_owned(), value.to_owned()))
                        .collect()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn defaults_and_exact_names_are_format_independent() -> Result<(), ()> {
        let query = MetricsQuery::parse(Vec::new())?;
        assert_eq!(query.format, Format::Json);
        assert!(query.include.is_empty());
        let query = MetricsQuery::parse(vec![
            ("format".into(), "prometheus".into()),
            (
                "include".into(),
                " tenon.process.cpu,tenon.process.memory, tenon.process.cpu ".into(),
            ),
        ])?;
        assert_eq!(query.format, Format::Prometheus);
        assert_eq!(query.include, ["tenon.process.cpu", "tenon.process.memory"]);
        Ok(())
    }
}
