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
import java.util.concurrent.CompletionStage;

/** Sends generated Source payloads through one Pipeline-owned Source session. */
public interface PayloadSender<P extends MessageLite> {
  /**
   * Attempts to send one payload through an ordered Pipeline channel.
   *
   * <p>The channel id is validated synchronously before payload encoding or admission. The returned
   * stage must not be discarded: its {@link AckCode} tells the Plugin whether to acknowledge,
   * retry, slow, or reject the corresponding upstream input. Non-async callbacks registered in send
   * order on the same Queue run in completion order on the SDK completion thread; a slow callback
   * intentionally delays later completions. Use an async callback and an explicit executor only
   * when the Plugin deliberately chooses different scheduling.
   *
   * @param channelId the zero-based ordered channel id, less than the factory-provided Flow
   *     parallelism
   * @param payload the non-null generated SourceRecordPayload
   * @return the asynchronous terminal result; normal SDK shutdown completes an admitted send
   *     exceptionally with {@link SourceSessionClosedException}
   * @throws IllegalArgumentException when {@code channelId} is outside the current channel range
   */
  CompletionStage<AckCode> send(int channelId, P payload);
}
