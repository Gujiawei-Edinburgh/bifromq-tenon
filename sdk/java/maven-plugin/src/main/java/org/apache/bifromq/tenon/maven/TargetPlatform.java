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

import java.util.Locale;

enum TargetPlatform {
  LINUX_AMD64("linux-amd64"),
  LINUX_ARM64("linux-arm64"),
  MACOS_AMD64("macos-amd64"),
  MACOS_ARM64("macos-arm64");

  private final String classifier;

  TargetPlatform(String classifier) {
    this.classifier = classifier;
  }

  String classifier() {
    return classifier;
  }

  String os() {
    return switch (this) {
      case LINUX_AMD64, LINUX_ARM64 -> "linux";
      case MACOS_AMD64, MACOS_ARM64 -> "darwin";
    };
  }

  String architecture() {
    return switch (this) {
      case LINUX_AMD64, MACOS_AMD64 -> "amd64";
      case LINUX_ARM64, MACOS_ARM64 -> "arm64";
    };
  }

  static TargetPlatform current() {
    return parse(System.getProperty("os.name", ""), System.getProperty("os.arch", ""));
  }

  static TargetPlatform parseClassifier(String classifier) {
    for (var platform : values()) {
      if (platform.classifier.equals(classifier)) {
        return platform;
      }
    }
    throw new IllegalArgumentException(
        "Unsupported Tenon Java Plugin target platform: " + classifier);
  }

  static TargetPlatform parse(String osName, String architecture) {
    var os = osName.toLowerCase(Locale.ROOT);
    var arch = architecture.toLowerCase(Locale.ROOT);
    if (os.equals("linux")) {
      return switch (arch) {
        case "amd64", "x86_64" -> LINUX_AMD64;
        case "aarch64", "arm64" -> LINUX_ARM64;
        default -> throw unsupported(osName, architecture);
      };
    }
    if (os.equals("mac os x") || os.equals("macos")) {
      return switch (arch) {
        case "amd64", "x86_64" -> MACOS_AMD64;
        case "aarch64", "arm64" -> MACOS_ARM64;
        default -> throw unsupported(osName, architecture);
      };
    }
    throw unsupported(osName, architecture);
  }

  private static IllegalStateException unsupported(String osName, String architecture) {
    return new IllegalStateException(
        "Unsupported Tenon Java Plugin build platform: " + osName + "/" + architecture);
  }
}
