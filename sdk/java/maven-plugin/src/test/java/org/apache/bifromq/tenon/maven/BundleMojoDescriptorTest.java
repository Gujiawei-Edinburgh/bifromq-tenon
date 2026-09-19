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

package org.apache.bifromq.tenon.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class BundleMojoDescriptorTest {
  @Test
  void bundleGoalDoesNotExposeToolchainPaths() throws IOException {
    try (var descriptor = getClass().getResourceAsStream("/META-INF/maven/plugin.xml")) {
      assertNotNull(descriptor, "Maven Plugin descriptor is missing");
      var content = new String(descriptor.readAllBytes(), StandardCharsets.UTF_8);

      assertTrue(content.contains("<name>additionalJdkModules</name>"));
      assertTrue(content.contains("<name>platforms</name>"));
      assertFalse(content.contains("<name>jmodsDirectory</name>"));
      assertEquals(1, content.split("<goal>", -1).length - 1);
      assertTrue(content.contains("<goal>bundle</goal>"));
      assertFalse(content.contains("${tenon.kind}"));
      assertTrue(content.contains("${tenon.interface}"));
      assertTrue(content.contains("<alias>interface</alias>"));
    }
  }

  @Test
  void pluginInterfaceAcceptsExactlyTheThreeDeclaredInterfaces() {
    assertEquals(PluginInterface.SOURCE, PluginInterface.parse("source"));
    assertEquals(PluginInterface.SINK, PluginInterface.parse("sink"));
    assertEquals(PluginInterface.SOURCE_AND_SINK, PluginInterface.parse("source-and-sink"));
    assertThrows(IllegalArgumentException.class, () -> PluginInterface.parse(null));
    assertThrows(IllegalArgumentException.class, () -> PluginInterface.parse("both"));
  }

  @Test
  void platformSelectionDefaultsAndRejectsInvalidLists() {
    assertEquals(1, BundleMojo.selectPlatforms(List.of()).size());
    assertEquals(
        List.of(TargetPlatform.LINUX_AMD64, TargetPlatform.MACOS_ARM64),
        BundleMojo.selectPlatforms(List.of("linux-amd64", "macos-arm64")));
    assertThrows(
        IllegalArgumentException.class,
        () -> BundleMojo.selectPlatforms(List.of("linux-amd64", "linux-amd64")));
    assertThrows(
        IllegalArgumentException.class, () -> BundleMojo.selectPlatforms(List.of("windows-amd64")));
  }
}
