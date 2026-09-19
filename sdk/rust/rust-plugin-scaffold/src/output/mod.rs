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

//! Owns the output file and serializes durable writes from different Queues.

use crate::payload::SinkRecordPayload;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use tenon_plugin_sdk::Error;

pub(super) struct Output(Mutex<File>);

impl Output {
    pub(super) fn open(path: &Path) -> Result<Self, Error> {
        Ok(Self(Mutex::new(
            OpenOptions::new().create(true).append(true).open(path)?,
        )))
    }

    pub(super) fn write(&self, records: &[SinkRecordPayload]) -> Result<(), Error> {
        let mut file = self.0.lock().expect("the SDK aborts the process on panic");
        for record in records {
            writeln!(file, "{}", record.message)?;
        }
        file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_append_in_order_and_survive_reopen() -> Result<(), Error> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("messages.txt");
        for messages in [["first", "second"], ["third", "fourth"]] {
            let output = Output::open(&path)?;
            let records = messages.map(|message| SinkRecordPayload {
                message: message.into(),
            });
            output.write(&records)?;
        }
        assert_eq!(
            std::fs::read_to_string(path)?,
            "first\nsecond\nthird\nfourth\n"
        );
        Ok(())
    }

    #[test]
    fn opening_a_directory_fails() -> Result<(), Error> {
        let directory = tempfile::tempdir()?;
        assert!(Output::open(directory.path()).is_err());
        Ok(())
    }
}
