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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/** Derives the private Queue path from the sole startup identity. */
final class EgressQueueLayout {
  private EgressQueueLayout() {}

  static Path queue(Path workingDirectory, FlowChannel channel) {
    try {
      var flowDirectory =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(
                  MessageDigest.getInstance("SHA-256")
                      .digest(channel.flowId().getBytes(StandardCharsets.UTF_8)));
      return workingDirectory
          .resolve("sink")
          .resolve(flowDirectory)
          .resolve("egress-" + channel.channelId() + ".queue");
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("Every supported Java runtime provides SHA-256", impossible);
    }
  }
}
