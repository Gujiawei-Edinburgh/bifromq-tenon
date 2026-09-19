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

import java.nio.file.Path;

/**
 * Identifies the Flow and its zero-based Channel that produced one Sink batch.
 *
 * @param flowId the exact Flow id authored in the Tenon Document
 * @param channelId the Channel index within that Flow's parallelism
 * @param channelBellPath the Bell Region the Channel that wrote this input waits in; releasing a
 *     batch rings the slot whose ordinal that Channel published, so the wake reaches the exact loop
 *     that produced the batch, while this input's own waiting loop publishes into the Sink's
 *     own-loop Region instead
 */
public record FlowChannel(String flowId, int channelId, Path channelBellPath) {}
