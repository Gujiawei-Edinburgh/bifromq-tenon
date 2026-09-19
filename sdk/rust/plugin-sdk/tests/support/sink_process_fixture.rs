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

//! Business behavior driven through the public Sink API by real-process tests.

#![expect(
    clippy::panic,
    clippy::expect_used,
    reason = "These subprocess scenarios deliberately inject fatal lifecycle and worker failures"
)]

mod business_panic;

use business_panic::panic_in_callback;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tenon_plugin_sdk::{Error, FlowChannel, SinkProgram, TenonSink};

struct Business {
    mode: String,
    first_batch: AtomicBool,
}

impl TenonSink<String> for Business {
    fn start(&mut self) {
        println!("start");
        match self.mode.as_str() {
            "failed-start" | "failed-start-and-close" => panic!("Business start failed"),
            "panic-start" => panic_in_callback(),
            "panic-thread" => {
                std::thread::spawn(panic_in_callback)
                    .join()
                    .expect("Business thread panicked");
            }
            "blocked-start" => block(),
            _ => {}
        }
    }

    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[String]>,
    ) -> impl Future<Output = Result<(), Error>> {
        println!(
            "write {} {} {:?}",
            channel.flow_id, channel.channel_id, records
        );
        match self.mode.as_str() {
            "panic-write" => panic_in_callback(),
            "blocked-write" => block(),
            _ => {}
        }
        Observation {
            business: self,
            polled: false,
            held: self.mode == "hold-first-input" && !self.first_batch.swap(true, Ordering::SeqCst),
            records: records.into(),
        }
    }

    fn close(&mut self) {
        println!("close");
        match self.mode.as_str() {
            "failed-close" | "failed-start-and-close" | "failed-write-and-close" => {
                panic!("Business close failed")
            }
            "panic-close" => panic_in_callback(),
            "blocked-close" => block(),
            _ => {}
        }
    }
}

struct Observation<'a> {
    business: &'a Business,
    polled: bool,
    held: bool,
    // This business-owned batch proves an observer need not be Send.
    records: std::rc::Rc<[String]>,
}

impl Future for Observation<'_> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.records.is_empty(), "Sink batches must be nonempty");
        if self.business.mode == "panic-poll" {
            panic_in_callback();
        }
        if self.business.mode == "pending" {
            return Poll::Pending;
        }
        if self.held {
            return Poll::Pending;
        }
        if !self.polled && matches!(self.business.mode.as_str(), "self-wake" | "async-failure") {
            self.polled = true;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        if matches!(
            self.business.mode.as_str(),
            "failed-write" | "failed-write-and-close" | "async-failure"
        ) {
            return Poll::Ready(Err("Business write failed".into()));
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for Observation<'_> {
    fn drop(&mut self) {
        println!("observer-dropped");
    }
}

fn block() -> ! {
    loop {
        std::thread::park();
    }
}

fn main() {
    SinkProgram::run(|config| {
        println!("factory");
        let mode = config["mode"].as_str().unwrap_or("normal").to_owned();
        if mode == "panic-factory" {
            panic_in_callback();
        }
        if mode == "failed-factory" {
            return Err("Business factory failed".into());
        }
        Ok(Business {
            mode,
            first_batch: AtomicBool::new(false),
        })
    })
    .await_shutdown();
}
