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

mod payload;

use payload::SinkRecordPayload;
use std::future::ready;
use std::io::{self, Write};
use tenon_plugin_sdk::{Error, FlowChannel, SinkProgram, TenonSink};
use time::{OffsetDateTime, macros::format_description};

struct StdoutSink;

impl TenonSink<SinkRecordPayload> for StdoutSink {
    fn start(&mut self) {}

    fn write(
        &self,
        _channel: FlowChannel,
        records: Box<[SinkRecordPayload]>,
    ) -> impl Future<Output = Result<(), Error>> {
        ready(write_batch(&mut io::stdout().lock(), &records))
    }

    fn close(&mut self) {}
}

fn write_batch(output: &mut impl Write, records: &[SinkRecordPayload]) -> Result<(), Error> {
    for record in records {
        let timestamp = OffsetDateTime::now_utc().format(format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
        ))?;
        // Escape backslashes first so literal escape sequences remain distinguishable.
        let message = record
            .message
            .replace('\\', "\\\\")
            .replace('\r', "\\r")
            .replace('\n', "\\n");
        writeln!(output, "{timestamp} {message}")?;
    }
    output.flush()?;
    Ok(())
}

fn main() {
    SinkProgram::run(|_config| Ok(StdoutSink)).await_shutdown();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_remain_ordered_single_lines_with_utc_millisecond_timestamps() -> Result<(), Error> {
        let messages = ["", "中文 🦀", "first\r\nsecond", r"literal \n", "last"];
        let records = messages.map(|message| SinkRecordPayload {
            message: message.into(),
        });
        let mut output = Vec::new();
        write_batch(&mut output, &records)?;
        let text = String::from_utf8(output)?;
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), records.len());
        for (line, expected) in
            lines
                .iter()
                .zip(["", "中文 🦀", r"first\r\nsecond", r"literal \\n", "last"])
        {
            assert_eq!(&line[24..25], " ");
            assert_eq!(&line[25..], expected);
            assert_eq!(line.as_bytes()[23], b'Z');
            assert_eq!(line.as_bytes()[19], b'.');
            assert!(line[20..23].bytes().all(|byte| byte.is_ascii_digit()));
        }
        Ok(())
    }

    #[test]
    fn write_and_flush_failures_are_not_reported_as_success() {
        struct FailingOutput {
            fail_write: bool,
        }
        impl Write for FailingOutput {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.fail_write {
                    Err(io::ErrorKind::BrokenPipe.into())
                } else {
                    Ok(bytes.len())
                }
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
        }
        let records = [SinkRecordPayload {
            message: "probe".into(),
        }];
        for fail_write in [true, false] {
            let error = write_batch(&mut FailingOutput { fail_write }, &records).unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::BrokenPipe
            );
        }
    }
}
