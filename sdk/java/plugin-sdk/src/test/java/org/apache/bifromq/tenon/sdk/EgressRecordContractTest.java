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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.file.Path;
import java.util.List;
import org.apache.bifromq.tenon.contracts.sink.EgressRecordOuterClass.EgressRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

final class EgressRecordContractTest {
  private static final Path TEST_VECTORS =
      Path.of(
          System.getProperty("tenon.contracts.sink.directory"), "egress_record_test_vectors.json");

  @Test
  void sharedGoldenVectorsFixPayloadOnlyDecodingAndNonemptyEncoding() throws Exception {
    var vectors = new ObjectMapper().readValue(TEST_VECTORS.toFile(), TestVectors.class);

    for (var vector : vectors.valid()) {
      var encoded = bytes(vector.encoded());
      var payload = bytes(vector.payload());
      var decoded = EgressRecord.parseFrom(encoded);
      var rebuilt = EgressRecord.newBuilder().setPayload(ByteString.copyFrom(payload)).build();

      assertAll(
          vector.name(),
          () -> assertEquals(rebuilt, decoded),
          () -> assertArrayEquals(payload, decoded.getPayload().toByteArray()));
      if (payload.length == 0) {
        // Protobuf omits defaults; the Queue producer must emit the explicit empty field instead.
        assertEquals(0, rebuilt.getSerializedSize(), vector.name());
      } else {
        assertArrayEquals(encoded, rebuilt.toByteArray(), vector.name());
      }
    }

    for (var vector : vectors.malformed()) {
      assertThrows(
          InvalidProtocolBufferException.class,
          () -> EgressRecord.parseFrom(bytes(vector.encoded())),
          vector.name());
    }
  }

  private static byte[] bytes(int[] values) {
    var bytes = new byte[values.length];
    for (var index = 0; index < values.length; index++) {
      bytes[index] = (byte) values[index];
    }
    return bytes;
  }

  private record TestVectors(List<ValidVector> valid, List<MalformedVector> malformed) {}

  private record ValidVector(String name, int[] payload, int[] encoded) {}

  private record MalformedVector(String name, int[] encoded) {}
}
