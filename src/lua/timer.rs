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

//! Single-slot timer state shared by one Lua VM and its event-loop owner.
//!
//! Lua calls only mutate this logical slot. They never run `main` or create an
//! asynchronous task. The owning Flow Channel reads the one schedule after
//! a successful top-level load or `main` call and uses its remaining duration
//! for the next Submission Queue wait. Replacement and cancellation mutate
//! that same fact before the owner chooses its next event, so no update queue,
//! stale callback, or generation token exists.

use super::{
    ExecutionBudget, LuaApiFailure, LuaVmFatalFault, create_catchable_api_wrapper_factory,
    finish_api_call, protect_name,
};
use mlua::{Function, Lua, Table, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

const DELAY_ERROR: &str = "setTimeout delay must be a non-negative integer";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerSchedule {
    pub(crate) scheduled_at: Instant,
    pub(crate) delay: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerSlotInactive;

#[derive(Clone, Debug, Default)]
pub(super) struct TimerSlot {
    schedule: Rc<Cell<Option<TimerSchedule>>>,
}

impl TimerSlot {
    pub(super) fn schedule(&self) -> Option<TimerSchedule> {
        self.schedule.get()
    }

    pub(super) fn begin(&self) -> Result<(), TimerSlotInactive> {
        if self.schedule.take().is_none() {
            return Err(TimerSlotInactive);
        }
        Ok(())
    }

    pub(super) fn has_timeout(&self) -> bool {
        self.schedule.get().is_some()
    }

    fn set_timeout(&self, delay: Duration) {
        self.schedule.set(Some(TimerSchedule {
            scheduled_at: Instant::now(),
            delay,
        }));
    }

    fn clear(&self) {
        self.schedule.set(None);
    }
}

pub(super) fn install(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    slot: TimerSlot,
) -> mlua::Result<()> {
    let api_wrapper_factory = create_catchable_api_wrapper_factory(lua)?;

    let set_slot = slot.clone();
    let set_fault = Rc::clone(&fatal_fault);
    let set_budget = Rc::clone(&execution_budget);
    let native_set = lua.create_function(move |lua, value: Value| {
        let result = match value {
            Value::Integer(delay) if delay >= 0 => u64::try_from(delay)
                .map(Duration::from_millis)
                .map_err(|_| LuaApiFailure::Api(DELAY_ERROR))
                .map(|delay| {
                    set_slot.set_timeout(delay);
                    Value::Nil
                }),
            _ => Err(LuaApiFailure::Api(DELAY_ERROR)),
        };
        finish_api_call(lua, result, &set_fault, &set_budget)
    })?;
    environment_values.raw_set(
        "setTimeout",
        api_wrapper_factory.call::<Function>(native_set)?,
    )?;
    protect_name(&protected_names, "setTimeout");

    let clear_slot = slot.clone();
    environment_values.raw_set(
        "clearTimerTask",
        lua.create_function(move |_, ()| {
            clear_slot.clear();
            Ok(())
        })?,
    )?;
    protect_name(&protected_names, "clearTimerTask");

    environment_values.raw_set(
        "hasTimeout",
        lua.create_function(move |_, ()| Ok(slot.has_timeout()))?,
    )?;
    protect_name(&protected_names, "hasTimeout");
    Ok(())
}
