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

//! Real business callbacks used by the repository's child-process tests.

#![expect(
    clippy::panic,
    clippy::expect_used,
    reason = "These subprocess scenarios inject fatal callbacks and require successful Source admission"
)]

mod business_panic;

use business_panic::panic_in_callback;
use std::thread;
use tenon_plugin_sdk::{AckCode, PayloadSender, SourceProgram, TenonSource, Value};

struct Producer {
    mode: String,
    sender: PayloadSender<Vec<u8>>,
    channel_count: usize,
    config: Value,
}

impl TenonSource for Producer {
    fn start(&mut self) {
        println!("source-start");
        if self.mode == "panic-start" {
            panic_in_callback();
        }
        if self.mode == "panic-thread" {
            thread::spawn(|| panic_in_callback())
                .join()
                .expect("Business thread panicked");
        }
        if matches!(
            self.mode.as_str(),
            "wait-for-queue-failure" | "failed-start-after-queue-failure"
        ) {
            use std::future::Future;
            use std::pin::Pin;
            use std::task::{Context, Poll, Waker};
            let waker = Waker::noop();
            loop {
                let mut completion = self.sender.send(0, &vec![]).expect("Source send failed");
                if matches!(
                    Pin::new(&mut completion).poll(&mut Context::from_waker(waker)),
                    Poll::Ready(Ok(AckCode::Error) | Err(_))
                ) {
                    break;
                }
                thread::yield_now();
            }
            println!("source-closed");
            if self.mode == "failed-start-after-queue-failure" {
                panic!("Business start failed");
            }
            return;
        }
        if self.mode == "blocked-start" {
            loop {
                thread::park()
            }
        }
        if self.config["send"].as_bool().unwrap_or(true) {
            let count = self.config["sends"].as_u64().unwrap_or(1);
            let channels = if self.mode == "all-channels" {
                self.channel_count
            } else {
                1
            };
            for sequence in 0..count {
                let result = self
                    .sender
                    .send(sequence as usize % channels, &vec![0; 8])
                    .expect("Source send failed");
                thread::spawn(move || println!("source-result {:?}", result.wait()));
            }
        }
        if self.mode == "failed-start" {
            panic!("Business start failed");
        }
    }

    fn quiesce(&mut self) {
        println!("source-quiesce");
        if self.mode == "panic-quiesce" {
            panic_in_callback();
        }
        if self.mode == "blocked-quiesce" {
            loop {
                thread::park()
            }
        }
        if self.mode == "failed-quiesce" {
            panic!("Business quiesce failed");
        }
    }

    fn close(&mut self) {
        println!("source-close-enter");
        if self.mode == "panic-close" {
            panic_in_callback();
        }
        if self.mode == "blocked-close" {
            loop {
                thread::park()
            }
        }
        println!("source-close");
    }
}

fn main() {
    let program = SourceProgram::run(|config: Value, parallelism, sender| {
        println!("create channels {parallelism}");
        let mode = config["mode"].as_str().unwrap_or("normal").to_owned();
        if mode == "panic-factory" {
            panic_in_callback();
        }
        if mode == "failed-factory" {
            return Err("Business factory failed".into());
        }
        Ok(Producer {
            mode,
            sender,
            channel_count: parallelism,
            config,
        })
    });
    if program.config()["mode"] == "drop-program" {
        drop(program);
        return;
    }
    program.await_shutdown();
}
