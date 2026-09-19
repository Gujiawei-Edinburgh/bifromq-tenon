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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class TargetPlatformTest {
  @Test
  void formalOperatingSystemAndArchitectureNamesMapToStableClassifiers() {
    assertEquals(TargetPlatform.LINUX_AMD64, TargetPlatform.parse("Linux", "x86_64"));
    assertEquals(TargetPlatform.LINUX_ARM64, TargetPlatform.parse("linux", "aarch64"));
    assertEquals(TargetPlatform.MACOS_AMD64, TargetPlatform.parse("Mac OS X", "amd64"));
    assertEquals(TargetPlatform.MACOS_ARM64, TargetPlatform.parse("macOS", "arm64"));
  }

  @Test
  void manifestTargetsUseCanonicalGoNames() {
    assertEquals("linux", TargetPlatform.LINUX_AMD64.os());
    assertEquals("amd64", TargetPlatform.LINUX_AMD64.architecture());
    assertEquals("linux", TargetPlatform.LINUX_ARM64.os());
    assertEquals("arm64", TargetPlatform.LINUX_ARM64.architecture());
    assertEquals("darwin", TargetPlatform.MACOS_AMD64.os());
    assertEquals("amd64", TargetPlatform.MACOS_AMD64.architecture());
    assertEquals("darwin", TargetPlatform.MACOS_ARM64.os());
    assertEquals("arm64", TargetPlatform.MACOS_ARM64.architecture());
  }

  @Test
  void classifiersRoundTripExactly() {
    for (var platform : TargetPlatform.values()) {
      assertEquals(platform, TargetPlatform.parseClassifier(platform.classifier()));
    }
    assertThrows(
        IllegalArgumentException.class, () -> TargetPlatform.parseClassifier("linux-x86_64"));
  }

  @Test
  void unsupportedPlatformFailsBeforePackaging() {
    assertThrows(IllegalStateException.class, () -> TargetPlatform.parse("Windows", "amd64"));
    assertThrows(IllegalStateException.class, () -> TargetPlatform.parse("Linux", "riscv64"));
  }
}
