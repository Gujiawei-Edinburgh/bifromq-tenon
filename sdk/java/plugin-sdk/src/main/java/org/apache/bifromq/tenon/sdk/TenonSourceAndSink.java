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

import com.google.protobuf.MessageLite;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Owns the shared business resources of one source-and-sink Plugin Program.
 *
 * @param <T> the generated top-level SinkRecordPayload type
 */
public interface TenonSourceAndSink<T extends MessageLite> {
  /**
   * Starts shared resources used by Sink processing. The SDK calls this during Program startup.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void start();

  /**
   * Stops new Source production while keeping shared resources available to Sink processing.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void quiesce();

  /**
   * Starts handling one non-empty, ordered, immutable Sink batch.
   *
   * <p>Ordering, overlap, completion, and replay semantics are identical to {@link
   * TenonSink#write(FlowChannel, List)}.
   *
   * @param channel the Flow and Channel that produced every record in this batch
   * @param records generated Sink payloads whose ownership has moved to this owner
   * @return a non-null result that completes normally for whole-batch success or exceptionally
   */
  CompletionStage<Void> write(FlowChannel channel, List<T> records);

  /**
   * Closes the shared business resources once during final Shutdown.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void close();
}
