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
 * Implements one generated Payload Contract and its downstream delivery behavior.
 *
 * @param <P> the generated top-level SinkRecordPayload type
 */
public interface TenonSink<P extends MessageLite> {
  /**
   * Starts local resources before the first payload is submitted.
   *
   * <p>Create downstream clients and validate external requirements here. This callback must return
   * promptly; if the downstream is unavailable, the implementation owns asynchronous reconnect
   * work.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void start();

  /**
   * Starts handling one non-empty, ordered, immutable batch without waiting for previous results.
   *
   * <p>Method bodies are entered in Queue order and never overlap, not even across FlowChannels:
   * one Egress loop serves every FlowChannel of this Instance in turn, so a method body that blocks
   * or waits for another Channel's progress stops the whole Instance's Egress. A later call on the
   * same Channel may begin after an earlier call returns its stage and before that stage completes;
   * batches from different Channels may be in flight at once and may complete out of order. Batch
   * size is not fixed and a one-record batch is valid. Complete the returned stage normally only
   * after every record reaches this Plugin's documented downstream success boundary; partial
   * success still fails the whole batch and may be replayed.
   *
   * @param channel the Flow and Channel that produced every record in this batch
   * @param records generated payloads whose ownership has permanently moved to this Sink
   * @return a non-null result that completes normally for whole-batch success or exceptionally
   */
  CompletionStage<Void> write(FlowChannel channel, List<P> records);

  /**
   * Signals synchronous planned shutdown.
   *
   * <p>Signal every client, thread, and helper process owned by this Sink to stop and return
   * promptly. Crash and forced process termination do not guarantee that this method runs.
   *
   * <p>This callback must return promptly and must not throw. A violation terminates the Plugin
   * process.
   */
  void close();
}
