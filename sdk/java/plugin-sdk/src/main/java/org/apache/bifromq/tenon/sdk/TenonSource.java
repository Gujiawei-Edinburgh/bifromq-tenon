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

package org.apache.bifromq.tenon.sdk;

/** Owns one SDK-started Source lifecycle and its business resources. */
public interface TenonSource {
  /**
   * Starts upstream clients, subscriptions, and reconnect work, then returns promptly.
   *
   * <p>Temporary upstream unavailability belongs in asynchronous reconnect logic rather than an
   * indefinitely blocked startup call. If this callback fails its contract, the process terminates
   * immediately and no later lifecycle callback is promised.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void start();

  /**
   * Stops new production without waiting for in-flight send results or closing shared resources.
   *
   * <p>Threads waiting for a send result remain alive until final {@link #close()}. The SDK waits
   * for entered sends to finish submission and keeps delivering their asynchronous results.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void quiesce();

  /**
   * Signals every client, thread, and helper process owned by this Source to stop.
   *
   * <p>The SDK ends this session's pumps before calling this method. The callback must return
   * without waiting for business threads, network acknowledgements, or other external progress.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void close();
}
