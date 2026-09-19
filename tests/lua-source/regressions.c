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

#include <stdio.h>

#include "lauxlib.h"
#include "lua.h"
#include "lualib.h"

static int run_check(lua_State *state, const char *name, const char *source) {
  if (luaL_dostring(state, source) != LUA_OK) {
    const char *message = lua_tostring(state, -1);
    fprintf(stderr, "%s raised an unexpected Lua error: %s\n", name,
            message == NULL ? "unknown error" : message);
    lua_pop(state, 1);
    return 0;
  }

  if (!lua_isboolean(state, -1) || !lua_toboolean(state, -1)) {
    fprintf(stderr, "%s returned a failing result\n", name);
    lua_pop(state, 1);
    return 0;
  }

  lua_pop(state, 1);
  return 1;
}

int main(void) {
  static const char invalid_utf8_check[] =
      "local invalid = string.char("
      "0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80) "
      "local length, position = utf8.len(invalid) "
      "return length == nil and position == 1";
  static const char gmatch_reset_check[] =
      "local count = 400 "
      "local iterator = string.gmatch("
      "string.rep('a', count), string.rep('a?', count)) "
      "local first_ok = pcall(iterator) "
      "local second_ok = pcall(iterator) "
      "local third_ok = pcall(iterator) "
      "return not first_ok and not second_ok and not third_ok";
  static const char official_language_check[] =
      "local formal = load('global value; value = 2 ^ 3; "
      "local closed <close> = nil; return value') "
      "local legacy = load('local global = 1') "
      "return formal ~= nil and formal() == 8 and legacy == nil";

  lua_State *state = luaL_newstate();
  if (state == NULL) {
    fputs("Failed to create the Lua regression state\n", stderr);
    return 1;
  }
  luaL_openlibs(state);

  const int invalid_utf8_passed =
      run_check(state, "invalid UTF-8 regression", invalid_utf8_check);
  const int gmatch_reset_passed =
      run_check(state, "gmatch reset regression", gmatch_reset_check);
  const int official_language_passed = run_check(
      state, "official Lua language profile", official_language_check);

  lua_close(state);
  return invalid_utf8_passed && gmatch_reset_passed && official_language_passed
             ? 0
             : 1;
}
