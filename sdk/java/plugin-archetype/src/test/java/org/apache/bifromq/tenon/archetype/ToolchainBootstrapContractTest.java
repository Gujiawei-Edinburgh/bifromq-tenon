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

package org.apache.bifromq.tenon.archetype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ToolchainBootstrapContractTest {
  private static final String VERSION = "25.0.3+9";
  private static final String VENDOR = "Eclipse Adoptium";

  @TempDir Path directory;

  private Path bootstrap;
  private Path javaEightBin;

  @BeforeEach
  void prepareScripts() throws Exception {
    bootstrap = directory.resolve("tenon-toolchain.sh");
    try (InputStream input =
        getClass().getResourceAsStream("/archetype-resources/.mvn/tenon-toolchain.sh")) {
      if (input == null) {
        throw new IllegalStateException("Toolchain bootstrap resource is missing");
      }
      Files.copy(input, bootstrap);
    }
    javaEightBin = directory.resolve("java-eight/bin");
    Files.createDirectories(javaEightBin);
    writeExecutable(
        javaEightBin.resolve("java"),
        "#!/bin/sh\n"
            + "echo '    java.home = /fake/java-eight' >&2\n"
            + "echo '    java.runtime.version = 1.8.0_999' >&2\n"
            + "echo '    java.vendor = Example Vendor' >&2\n");
    writeExecutable(javaEightBin.resolve("javac"), "#!/bin/sh\nexit 0\n");
  }

  @Test
  void javaEightIsIgnoredAndFixedTemurinIsCachedForOfflineReuse() throws Exception {
    var toolchain = createFakeTemurinToolchain();
    var jdkChecksum = sha256(toolchain.jdkArchive());
    var jmodsChecksum = sha256(toolchain.jmodsArchive());
    var manifest = writeManifest(toolchain, jdkChecksum, jmodsChecksum);
    var cache = directory.resolve("cache");

    var first = runBootstrap(manifest, cache);

    assertEquals(0, first.exitCode(), first.output());
    assertTrue(first.output().contains("Tenon is provisioning Eclipse Temurin " + VERSION));
    var cachedHome = cache.resolve("tenon/toolchains/temurin-" + VERSION).resolve(platform());
    assertTrue(Files.isSymbolicLink(cachedHome));
    assertTrue(Files.isExecutable(cachedHome.resolve("bin/java")));
    assertTrue(Files.isRegularFile(cachedHome.resolve("jmods/java.base.jmod")));
    assertEquals(
        jdkChecksum, Files.readString(cachedHome.resolve(".tenon-jdk-archive-sha256")).trim());
    assertEquals(
        jmodsChecksum, Files.readString(cachedHome.resolve(".tenon-jmods-archive-sha256")).trim());

    Files.delete(toolchain.jdkArchive());
    Files.delete(toolchain.jmodsArchive());
    var second = runBootstrap(manifest, cache);

    assertEquals(0, second.exitCode(), second.output());
    assertFalse(second.output().contains("Tenon is provisioning"));
    assertTrue(second.output().contains("JAVA_HOME=" + cachedHome));
  }

  @Test
  void matchingInstalledTemurinIsUsedWithoutDownload() throws Exception {
    var toolchain = createFakeTemurinToolchain();
    var manifest =
        writeManifest(toolchain, sha256(toolchain.jdkArchive()), sha256(toolchain.jmodsArchive()));
    Files.delete(toolchain.jdkArchive());
    Files.delete(toolchain.jmodsArchive());
    var cache = directory.resolve("unused-cache");

    var result = runBootstrap(manifest, cache, toolchain.jdkHome());

    assertEquals(0, result.exitCode(), result.output());
    assertFalse(result.output().contains("Tenon is provisioning"));
    assertTrue(result.output().contains("JAVA_HOME=" + toolchain.jdkHome()));
    assertFalse(Files.exists(cache));
  }

  @Test
  void checksumMismatchCannotPublishCachedToolchain() throws Exception {
    var toolchain = createFakeTemurinToolchain();
    var manifest = writeManifest(toolchain, sha256(toolchain.jdkArchive()), "0".repeat(64));
    var cache = directory.resolve("checksum-cache");

    var result = runBootstrap(manifest, cache);

    assertNotEquals(0, result.exitCode());
    assertTrue(result.output().contains("checksum mismatch"), result.output());
    assertFalse(
        Files.exists(cache.resolve("tenon/toolchains/temurin-" + VERSION).resolve(platform())));
  }

  @Test
  void concurrentFirstUsePublishesOneCompleteCachedToolchain() throws Exception {
    var toolchain = createFakeTemurinToolchain();
    var manifest =
        writeManifest(toolchain, sha256(toolchain.jdkArchive()), sha256(toolchain.jmodsArchive()));
    var cache = directory.resolve("concurrent-cache");

    var first = bootstrapProcess(manifest, cache).start();
    var second = bootstrapProcess(manifest, cache).start();
    var firstOutput = new String(first.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    var secondOutput = new String(second.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

    assertEquals(0, first.waitFor(), firstOutput);
    assertEquals(0, second.waitFor(), secondOutput);
    var cachedHome = cache.resolve("tenon/toolchains/temurin-" + VERSION).resolve(platform());
    assertTrue(Files.isSymbolicLink(cachedHome));
    assertTrue(Files.isExecutable(cachedHome.resolve("bin/java")));
    assertTrue(Files.isRegularFile(cachedHome.resolve("jmods/java.base.jmod")));
    try (var cacheEntries = Files.list(cachedHome.getParent())) {
      assertEquals(
          1,
          cacheEntries
              .filter(
                  entry ->
                      entry.getFileName().toString().startsWith("." + platform() + ".install."))
              .count());
    }
  }

  @Test
  void unsupportedPlatformIsRejectedBeforeDownload() throws Exception {
    var process =
        new ProcessBuilder(
                "/bin/sh",
                "-c",
                ". \"$1\"; if _tenon_platform_from Plan9 x86_64; then exit 1; fi",
                "toolchain-platform-test",
                bootstrap.toString())
            .redirectErrorStream(true)
            .start();

    var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

    assertEquals(0, process.waitFor(), output);
  }

  private FakeToolchain createFakeTemurinToolchain() throws Exception {
    var jdkArchiveRoot = directory.resolve("jdk-archive-root");
    var jdk = jdkArchiveRoot.resolve("fake-temurin");
    Files.createDirectories(jdk.resolve("bin"));
    writeExecutable(
        jdk.resolve("bin/java"),
        "#!/bin/sh\n"
            + "echo '    java.home = /fake/temurin' >&2\n"
            + "echo '    java.runtime.version = "
            + VERSION
            + "-LTS' >&2\n"
            + "echo '    java.vendor = "
            + VENDOR
            + "' >&2\n"
            + "echo '    java.vendor.version = Temurin-"
            + VERSION
            + "' >&2\n");
    writeExecutable(jdk.resolve("bin/javac"), "#!/bin/sh\nexit 0\n");
    writeExecutable(jdk.resolve("bin/jmod"), "#!/bin/sh\necho 'java.base@25.0.3'\n");
    Files.writeString(jdk.resolve("NOTICE"), "fake Temurin notice");
    var jdkArchive = createArchive(jdkArchiveRoot, jdk, "fake-temurin.tar.gz");

    var jmodsArchiveRoot = directory.resolve("jmods-archive-root");
    var jmods = jmodsArchiveRoot.resolve("fake-temurin-jmods");
    Files.createDirectories(jmods);
    Files.writeString(jmods.resolve("java.base.jmod"), "fake jmod");
    var jmodsArchive = createArchive(jmodsArchiveRoot, jmods, "fake-temurin-jmods.tar.gz");
    Files.createDirectories(jdk.resolve("jmods"));
    Files.copy(jmods.resolve("java.base.jmod"), jdk.resolve("jmods/java.base.jmod"));
    return new FakeToolchain(jdk, jdkArchive, jmodsArchive);
  }

  private Path createArchive(Path archiveRoot, Path content, String name) throws Exception {
    var archive = directory.resolve(name);
    var tar =
        new ProcessBuilder(
                "tar",
                "-czf",
                archive.toString(),
                "-C",
                archiveRoot.toString(),
                content.getFileName().toString())
            .redirectErrorStream(true)
            .start();
    var output = new String(tar.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(0, tar.waitFor(), output);
    return archive;
  }

  private Path writeManifest(FakeToolchain toolchain, String jdkChecksum, String jmodsChecksum)
      throws IOException {
    var manifest = directory.resolve("toolchain-" + jmodsChecksum.substring(0, 8) + ".properties");
    Files.writeString(
        manifest,
        "version="
            + VERSION
            + "\n"
            + "vendor="
            + VENDOR
            + "\n"
            + platform()
            + ".jdk.url="
            + toolchain.jdkArchive().toUri()
            + "\n"
            + platform()
            + ".jdk.sha256="
            + jdkChecksum
            + "\n"
            + platform()
            + ".jmods.url="
            + toolchain.jmodsArchive().toUri()
            + "\n"
            + platform()
            + ".jmods.sha256="
            + jmodsChecksum
            + "\n");
    return manifest;
  }

  private BootstrapResult runBootstrap(Path manifest, Path cache) throws Exception {
    var process = bootstrapProcess(manifest, cache).start();
    var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new BootstrapResult(process.waitFor(), output);
  }

  private BootstrapResult runBootstrap(Path manifest, Path cache, Path javaHome) throws Exception {
    var command =
        "set -eu; "
            + ". \"$1\"; "
            + "JAVA_HOME=\"$2\"; export JAVA_HOME; "
            + "PATH=\"$JAVA_HOME/bin:/usr/bin:/bin\"; export PATH; "
            + "MAVEN_USER_HOME=\"$3\"; export MAVEN_USER_HOME; "
            + "tenon_bootstrap_java \"$4\"; "
            + "printf 'JAVA_HOME=%s\\n' \"$JAVA_HOME\"";
    var process =
        new ProcessBuilder(
                "/bin/sh",
                "-c",
                command,
                "toolchain-installed-test",
                bootstrap.toString(),
                javaHome.toString(),
                cache.toString(),
                manifest.toString())
            .redirectErrorStream(true)
            .start();
    var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new BootstrapResult(process.waitFor(), output);
  }

  private ProcessBuilder bootstrapProcess(Path manifest, Path cache) {
    var command =
        "set -eu; "
            + ". \"$1\"; "
            + "unset JAVA_HOME; "
            + "PATH=\"$2:/usr/bin:/bin\"; export PATH; "
            + "MAVEN_USER_HOME=\"$3\"; export MAVEN_USER_HOME; "
            + "tenon_bootstrap_java \"$4\"; "
            + "printf 'JAVA_HOME=%s\\n' \"$JAVA_HOME\"";
    return new ProcessBuilder(
            "/bin/sh",
            "-c",
            command,
            "toolchain-bootstrap-test",
            bootstrap.toString(),
            javaEightBin.toString(),
            cache.toString(),
            manifest.toString())
        .redirectErrorStream(true);
  }

  private static void writeExecutable(Path path, String content) throws IOException {
    Files.writeString(path, content);
    if (!path.toFile().setExecutable(true, false)) {
      throw new IOException("Failed to make test fixture executable: " + path);
    }
  }

  private static String platform() {
    var operatingSystem = System.getProperty("os.name").toLowerCase(Locale.ROOT);
    var architecture = System.getProperty("os.arch").toLowerCase(Locale.ROOT);
    var os = operatingSystem.contains("linux") ? "linux" : "macos";
    var arch = architecture.equals("amd64") || architecture.equals("x86_64") ? "amd64" : "arm64";
    return os + "-" + arch;
  }

  private static String sha256(Path path) throws Exception {
    return HexFormat.of()
        .formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
  }

  private record FakeToolchain(Path jdkHome, Path jdkArchive, Path jmodsArchive) {}

  private record BootstrapResult(int exitCode, String output) {}
}
