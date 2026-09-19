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

//! Weak observation registrations shared by instrument owners. Collection takes
//! one bounded snapshot without waiting for registration, then releases the lock
//! before reading objects or invoking OpenTelemetry. No metric is defined here.

use std::sync::{Arc, Mutex, TryLockError, Weak};

#[derive(Debug)]
pub(crate) struct Observations<T>(Mutex<Vec<Weak<T>>>);

impl<T> Observations<T> {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    #[allow(clippy::expect_used, reason = "registration never invokes user code")]
    pub(crate) fn register(&self, observation: &Arc<T>) {
        let mut entries = self.0.lock().expect("observation registry poisoned");
        if entries.len() == entries.capacity() {
            entries.retain(|entry| entry.strong_count() != 0);
            let capacity = entries.capacity();
            // Reserve enough runway to amortize scanning when most entries are live.
            // Retired VMs must not accumulate until the next export interval.
            if entries.len() > capacity / 2 {
                entries.reserve(capacity);
            }
        }
        entries.push(Arc::downgrade(observation));
    }

    /// A busy registration is unknown for this collection, not an empty set.
    #[allow(
        clippy::panic,
        reason = "poisoning proves an internal registry invariant failed"
    )]
    pub(crate) fn try_snapshot(&self) -> Option<Vec<Arc<T>>> {
        let mut entries = match self.0.try_lock() {
            Ok(entries) => entries,
            Err(TryLockError::WouldBlock) => return None,
            Err(TryLockError::Poisoned(_)) => panic!("observation registry poisoned"),
        };
        let mut snapshot = Vec::with_capacity(entries.len());
        entries.retain(|entry| {
            let Some(observation) = entry.upgrade() else {
                return false;
            };
            snapshot.push(observation);
            true
        });
        Some(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn repeated_retirement_without_collection_stays_bounded_and_preserves_live_entries()
    -> io::Result<()> {
        let observations = Observations::new();
        let live: Vec<_> = (0..8).map(Arc::new).collect();
        for value in &live {
            observations.register(value);
        }
        for value in 8..100_000 {
            observations.register(&Arc::new(value));
        }
        assert!(
            observations
                .0
                .lock()
                .map_err(|_| io::Error::other("registry poisoned"))?
                .capacity()
                <= 32
        );
        let snapshot = observations
            .try_snapshot()
            .ok_or_else(|| io::Error::other("uncontended registry skipped snapshot"))?;
        assert_eq!(snapshot.len(), live.len());
        assert!(
            live.iter()
                .all(|value| snapshot.iter().any(|observed| Arc::ptr_eq(value, observed)))
        );
        drop(snapshot);
        drop(live);
        assert!(
            observations
                .try_snapshot()
                .ok_or_else(|| io::Error::other("uncontended registry skipped snapshot"))?
                .is_empty()
        );
        Ok(())
    }
}
