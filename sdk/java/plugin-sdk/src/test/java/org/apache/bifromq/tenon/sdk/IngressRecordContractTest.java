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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.List;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletion;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletionStatus;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

final class IngressRecordContractTest {
  private static final Path TEST_VECTORS =
      Path.of(
          System.getProperty("tenon.contracts.source.directory"),
          "ingress_record_test_vectors.json");

  @Test
  void sharedVectorsFixIngressRecordAndCompletionBytes() throws Exception {
    var vectors = vectors();
    assertEquals(1, vectors.formatVersion());

    for (var vector : vectors.valid()) {
      var message =
          IngressRecord.newBuilder()
              .setRecordId(vector.recordId().longValue())
              .setPayload(ByteString.copyFrom(bytes(vector.payload())))
              .build();
      assertArrayEquals(bytes(vector.encoded()), message.toByteArray(), vector.name());
      assertEquals(message, IngressRecord.parseFrom(bytes(vector.encoded())), vector.name());
    }

    for (var vector : vectors.completionValid()) {
      var message =
          IngressCompletion.newBuilder()
              .setRecordId(vector.recordId().longValue())
              .setStatus(status(vector.status()))
              .build();
      assertArrayEquals(bytes(vector.encoded()), message.toByteArray(), vector.name());
      assertEquals(message, IngressCompletion.parseFrom(bytes(vector.encoded())), vector.name());
    }
  }

  @Test
  void maximumCompletionSizeAndMalformedInputAreFixed() {
    var vectors = vectors();
    var maximum =
        vectors.completionValid().stream()
            .mapToInt(vector -> vector.encoded().length)
            .max()
            .orElseThrow();
    assertEquals(IngressQueueLayout.COMPLETION_MAX_PAYLOAD_SIZE, maximum);

    for (var vector : vectors.malformed()) {
      assertThrows(
          InvalidProtocolBufferException.class,
          () -> IngressRecord.parseFrom(bytes(vector.encoded())),
          vector.name());
    }
  }

  private static IngressCompletionStatus status(String value) {
    return IngressCompletionStatus.valueOf("INGRESS_COMPLETION_STATUS_" + value);
  }

  private static TestVectors vectors() {
    return new ObjectMapper().readValue(TEST_VECTORS.toFile(), TestVectors.class);
  }

  private static byte[] bytes(int[] values) {
    var bytes = new byte[values.length];
    for (var index = 0; index < values.length; index++) {
      bytes[index] = (byte) values[index];
    }
    return bytes;
  }

  private record TestVectors(
      int formatVersion,
      List<IngressRecordVector> valid,
      List<IngressCompletionVector> completionValid,
      List<MalformedVector> malformed) {}

  private record IngressRecordVector(
      String name, BigInteger recordId, int[] payload, int[] encoded) {}

  private record IngressCompletionVector(
      String name, BigInteger recordId, String status, int[] encoded) {}

  private record MalformedVector(String name, int[] encoded) {}
}
