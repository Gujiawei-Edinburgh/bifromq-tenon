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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.apache.bifromq.tenon.sdk.sink.testpayload.SinkRecordPayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import tools.jackson.databind.ObjectMapper;

final class SinkRecordDecoderContractTest {
  private static final Path TEST_VECTORS =
      Path.of(
          System.getProperty("tenon.contracts.sink.directory"), "payload_decode_test_vectors.json");
  private static final Path PAYLOAD_DESCRIPTOR =
      Path.of(System.getProperty("tenon.test.payload.descriptor"));

  @Test
  void sameBuildGeneratesPayloadClassAndDescriptorFromOneProto() throws Exception {
    var descriptorBytes = Files.readAllBytes(PAYLOAD_DESCRIPTOR);
    var descriptorSet = FileDescriptorSet.parseFrom(descriptorBytes);
    var payloadFile =
        descriptorSet.getFileList().stream()
            .filter(file -> file.getName().equals("sink_record_payload.proto"))
            .findFirst()
            .orElseThrow();

    assertEquals("tenon.sink.test.v1", payloadFile.getPackage());
    assertEquals(
        List.of("SinkRecordPayload"),
        payloadFile.getMessageTypeList().stream().map(message -> message.getName()).toList());
    assertTrue(payloadFile.hasSourceCodeInfo());
    assertFalse(payloadFile.getSourceCodeInfo().getLocationList().isEmpty());
    assertEquals(
        "tenon.sink.test.v1.SinkRecordPayload", SinkRecordPayload.getDescriptor().getFullName());
    var describedRoot = payloadFile.getMessageType(0);
    var generatedRoot = SinkRecordPayload.getDescriptor();
    assertEquals(describedRoot.getFieldCount(), generatedRoot.getFields().size());
    for (var field : describedRoot.getFieldList()) {
      var generatedField = generatedRoot.findFieldByNumber(field.getNumber());
      // Generated Java omits inferred json_name entries; compare the resolved name separately.
      assertEquals(field.getJsonName(), generatedField.getJsonName());
      assertEquals(
          field.toBuilder().clearJsonName().build(),
          generatedField.toProto().toBuilder().clearJsonName().build());
    }
  }

  @Test
  void sharedVectorsDecodePayloadAndRejectMalformedRecordsOrPayloads() throws Exception {
    var vectors = vectors();
    var matching = vectors.matching();
    var generatedPayload =
        SinkRecordPayload.newBuilder()
            .setDestination(matching.destination())
            .setBody(ByteString.copyFrom(bytes(matching.body())))
            .build();

    var decoded =
        SinkRecordDecoder.decode(
            ByteString.copyFrom(bytes(matching.encoded())), SinkRecordPayload.parser());

    assertEquals(1, vectors.formatVersion());
    assertArrayEquals(bytes(matching.payload()), generatedPayload.toByteArray(), matching.name());
    assertEquals(matching.destination(), decoded.getDestination(), matching.name());
    assertArrayEquals(bytes(matching.body()), decoded.getBody().toByteArray(), matching.name());
    for (var vector : vectors.malformed()) {
      assertErrorCode(
          vector.expectedErrorCode(),
          () ->
              SinkRecordDecoder.decode(
                  ByteString.copyFrom(bytes(vector.encoded())), SinkRecordPayload.parser()),
          vector.name());
    }
  }

  @Test
  void oneCopyDetachesPayloadFromQueueAndAllLaterDecodingAliasesOwnedBytes() throws Exception {
    var vectors = vectors();
    var matching = vectors.matching();
    var capacity = IpcQueueFormat.DataCapacity.fromFileLength(4288);
    var maxPayloadSize = capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH;
    var liveQueueBytes =
        IpcQueueFormat.encodeRecordFrame(capacity, maxPayloadSize, bytes(matching.encoded()));
    var readerOwnedBytes = liveQueueBytes.clone();
    var recordFrame =
        assertInstanceOf(
            IpcQueueFormat.RecordFrame.class,
            IpcQueueFormat.decodeOwnedFrame(capacity, maxPayloadSize, readerOwnedBytes));

    assertSame(recordFrame.recordBytes(), recordFrame.recordBytes());
    var decoded = SinkRecordDecoder.decode(recordFrame.recordBytes(), SinkRecordPayload.parser());
    var bodyOffset = findSequence(readerOwnedBytes, bytes(matching.body()));

    liveQueueBytes[bodyOffset] = 42;
    assertEquals(0, decoded.getBody().byteAt(0));

    readerOwnedBytes[bodyOffset] = 42;
    assertEquals(42, decoded.getBody().byteAt(0));
  }

  private static TestVectors vectors() {
    return new ObjectMapper().readValue(TEST_VECTORS.toFile(), TestVectors.class);
  }

  private static int findSequence(byte[] haystack, byte[] needle) {
    for (var start = haystack.length - needle.length; start >= 0; start--) {
      var matches = true;
      for (var index = 0; index < needle.length; index++) {
        if (haystack[start + index] != needle[index]) {
          matches = false;
          break;
        }
      }
      if (matches) {
        return start;
      }
    }
    throw new AssertionError("Byte sequence not found");
  }

  private static void assertErrorCode(String expected, Executable operation, String name) {
    var error = assertThrows(SinkRecordDecodeException.class, operation, name);
    assertEquals(expected, error.code(), name);
  }

  private static byte[] bytes(int[] values) {
    var bytes = new byte[values.length];
    for (var index = 0; index < values.length; index++) {
      bytes[index] = (byte) values[index];
    }
    return bytes;
  }

  private record TestVectors(
      int formatVersion, MatchingVector matching, List<MalformedVector> malformed) {}

  private record MatchingVector(
      String name, String destination, int[] body, int[] payload, int[] encoded) {}

  private record MalformedVector(String name, int[] encoded, String expectedErrorCode) {}
}
