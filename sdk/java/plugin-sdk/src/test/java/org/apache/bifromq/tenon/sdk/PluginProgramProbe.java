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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import com.google.protobuf.StringValue;
import java.math.BigDecimal;
import java.util.function.Supplier;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Runs a real Source-only or Sink-only Program in a child JVM. */
public final class PluginProgramProbe {
  static final String INTERFACE_PROPERTY = "tenon.test.plugin.interface";

  private PluginProgramProbe() {}

  public static void main(String[] arguments) throws Exception {
    if (Boolean.getBoolean("tenon.test.uncaught-handler")) {
      Thread.setDefaultUncaughtExceptionHandler(
          (thread, failure) -> System.err.println("Existing uncaught handler: " + failure));
    }
    try {
      switch (System.getProperty(INTERFACE_PROPERTY)) {
        case "source" -> {
          var program = SourceProgram.<StringValue>run(arguments);
          verifyConfigSnapshots(program::config);
          var config = program.config();
          if (config.has("workerCount")) {
            ((ObjectNode) config).put("workerCount", 99);
            var secondSnapshot = program.config();
            assertNotSame(config, secondSnapshot);
            assertEquals(2, secondSnapshot.required("workerCount").intValue());
          }
          program.awaitShutdown();
        }
        case "sink" -> SinkProgram.run(arguments, StringValue.parser()).awaitShutdown();
        case "source-and-sink" -> {
          var program =
              SourceAndSinkProgram.<StringValue, StringValue>run(arguments, StringValue.parser());
          verifyConfigSnapshots(program::config);
          program.awaitShutdown();
        }
        default -> throw new IllegalArgumentException("Unknown Plugin probe interface");
      }
    } catch (Error fatal) {
      System.err.println("Error escaped the SDK callback boundary");
      throw fatal;
    }
  }

  private static void verifyConfigSnapshots(Supplier<JsonNode> configuration) {
    var snapshot = configuration.get();
    if (!snapshot.has("snapshotProbe")) {
      return;
    }
    var expected = new BigDecimal("123456789012345678901234567890.1234567890123456789");
    var nested = (ObjectNode) snapshot.required("snapshotProbe");
    assertEquals(0, expected.compareTo(nested.required("number").decimalValue()));
    nested.put("number", 0);
    var next = configuration.get();
    assertNotSame(snapshot, next);
    assertEquals(
        0, expected.compareTo(next.required("snapshotProbe").required("number").decimalValue()));
  }
}
