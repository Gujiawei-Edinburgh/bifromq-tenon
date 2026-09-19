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

/** Creates the single Sink business instance owned by one Plugin Program process. */
public interface TenonSinkFactory<P extends MessageLite> {
  /**
   * Creates the Sink from its validated process configuration before startup.
   *
   * <p>The SDK invokes this method exactly once per process. Parse config into business state here,
   * but start downstream clients and helper resources in {@link TenonSink#start()}.
   *
   * @param config the validated process configuration
   * @return the Sink business instance that the SDK starts
   * @throws Exception when business construction fails
   */
  TenonSink<P> create(JsonNode config) throws Exception;
}
