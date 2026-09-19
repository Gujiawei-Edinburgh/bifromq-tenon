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
import tools.jackson.databind.JsonNode;

/** Creates the one business Source that the SDK starts and owns for a Plugin Program. */
public interface TenonSourceFactory<P extends MessageLite> {
  /**
   * Creates the one Source lifecycle; the SDK starts it after this method returns.
   *
   * <p>The config has already passed this Plugin's JSON Schema. Every call in the same process
   * receives the same complete config, Flow parallelism, and concurrent sender. {@code parallelism}
   * is the number of independent ordered channels in the bound Flow, not the number of Source
   * instances. Validate only cross-field rules and external resource requirements that the Schema
   * cannot express.
   *
   * @param config the validated process configuration
   * @param parallelism the positive number of independent ordered channels in the bound Flow
   * @param sender the thread-safe sender shared by every Source in this process
   * @return the one business Source owned by the SDK
   * @throws Exception when business construction or validation fails
   */
  TenonSource create(JsonNode config, int parallelism, PayloadSender<P> sender) throws Exception;
}
