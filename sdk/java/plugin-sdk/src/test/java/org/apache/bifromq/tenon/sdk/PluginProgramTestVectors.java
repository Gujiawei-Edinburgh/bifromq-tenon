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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

final class PluginProgramTestVectors {
  private static final Path DIRECTORY =
      Path.of(System.getProperty("tenon.contracts.plugin.directory"));
  private static final JsonMapper JSON =
      JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();

  private PluginProgramTestVectors() {}

  static JsonNode load(String name) throws IOException {
    return JSON.readTree(Files.readString(DIRECTORY.resolve(name)));
  }

  static String[] arguments(JsonNode vector) {
    var values = vector.required("arguments");
    var result = new String[values.size()];
    for (var index = 0; index < values.size(); index++) {
      result[index] = values.get(index).stringValue();
    }
    return result;
  }

  static byte[] input(JsonNode vector) {
    return vector.required("stdin").stringValue().getBytes(StandardCharsets.UTF_8);
  }

  static byte[] bytes(JsonNode values) {
    var result = new byte[values.size()];
    for (var index = 0; index < values.size(); index++) {
      result[index] = (byte) values.get(index).intValue();
    }
    return result;
  }

  static JsonNode lifecycle(String scenario) throws IOException {
    for (var vector : load("process_protocol_test_vectors.json").required("lifecycle")) {
      if (vector.required("scenario").stringValue().equals(scenario)) return vector;
    }
    throw new IllegalArgumentException("Missing lifecycle scenario: " + scenario);
  }

  static void assertBusinessEvents(JsonNode vector, Path events) throws IOException {
    var observed =
        Files.readAllLines(events).stream()
            .map(
                event ->
                    switch (event) {
                      case "owner-start", "sink-start" -> "owner.start";
                      case "source-start" -> "source.start";
                      case "source-quiesce" -> "source.quiesce";
                      case "source-admission-closed" -> "source.admission-closed";
                      case "source-close" -> "source.close";
                      case "owner-close", "sink-close" -> "owner.close";
                      default -> event;
                    })
            .toList();
    var previous = -1;
    for (var expected : vector.required("businessEvents")) {
      var event = expected.stringValue();
      org.junit.jupiter.api.Assertions.assertEquals(
          1, observed.stream().filter(event::equals).count(), observed.toString());
      var position = observed.indexOf(event);
      org.junit.jupiter.api.Assertions.assertTrue(position > previous, observed.toString());
      previous = position;
    }
  }
}
