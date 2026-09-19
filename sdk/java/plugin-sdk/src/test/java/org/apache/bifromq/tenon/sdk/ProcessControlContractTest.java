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

import static org.apache.bifromq.tenon.sdk.PluginProgramTestVectors.bytes;
import static org.apache.bifromq.tenon.sdk.PluginProgramTestVectors.load;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Attach;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PipelineToPlugin;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PluginToPipeline;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.QuiesceSource;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Ready;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Shutdown;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.SourceQuiesced;
import org.junit.jupiter.api.Test;

final class ProcessControlContractTest {
  @Test
  void generatedMessagesMatchEveryLanguageNeutralByteVector() throws Exception {
    var vectors = load("process_control_test_vectors.json");
    for (var vector : vectors.required("valid")) {
      var expected = bytes(vector.required("encoded"));
      var actual =
          encode(
              vector.required("direction").stringValue(),
              vector.required("messageKind").stringValue());
      assertArrayEquals(expected, actual, vector.required("name").stringValue());
    }
  }

  @Test
  void generatedParsersRejectEveryMalformedByteVector() throws Exception {
    for (var vector : load("process_control_test_vectors.json").required("malformed")) {
      var encoded = bytes(vector.required("encoded"));
      if (vector.required("direction").stringValue().equals("pluginToPipeline")) {
        assertThrows(
            InvalidProtocolBufferException.class,
            () -> PluginToPipeline.parseFrom(encoded),
            vector.required("name").stringValue());
      } else {
        assertThrows(
            InvalidProtocolBufferException.class,
            () -> PipelineToPlugin.parseFrom(encoded),
            vector.required("name").stringValue());
      }
    }
  }

  private static byte[] encode(String direction, String kind) {
    if (direction.equals("pluginToPipeline")) {
      var message = PluginToPipeline.newBuilder();
      switch (kind) {
        case "attach" ->
            message.setAttach(Attach.newBuilder().setLaunchId(ByteString.copyFrom(sequence(16))));
        case "ready" -> message.setReady(Ready.getDefaultInstance());
        case "sourceQuiesced" -> message.setSourceQuiesced(SourceQuiesced.getDefaultInstance());
        default -> throw new IllegalArgumentException("Unknown Plugin message kind");
      }
      return message.build().toByteArray();
    }
    var message = PipelineToPlugin.newBuilder();
    switch (kind) {
      case "quiesceSource" -> message.setQuiesceSource(QuiesceSource.getDefaultInstance());
      case "shutdown" -> message.setShutdown(Shutdown.getDefaultInstance());
      default -> throw new IllegalArgumentException("Unknown Pipeline message kind");
    }
    return message.build().toByteArray();
  }

  private static byte[] sequence(int length) {
    var bytes = new byte[length];
    for (var index = 0; index < length; index++) {
      bytes[index] = (byte) index;
    }
    return bytes;
  }
}
