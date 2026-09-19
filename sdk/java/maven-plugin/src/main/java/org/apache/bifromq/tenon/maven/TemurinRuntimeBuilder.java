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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.spi.ToolProvider;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

final class TemurinRuntimeBuilder {
  private static final Pattern MODULE_NAME =
      Pattern.compile("[A-Za-z][A-Za-z0-9]*(?:\\.[A-Za-z][A-Za-z0-9]*)+");
  private static final List<String> COMPATIBILITY_MODULES =
      List.of(
          "java.se",
          "jdk.charsets",
          "jdk.crypto.cryptoki",
          "jdk.crypto.ec",
          "jdk.httpserver",
          "jdk.jfr",
          "jdk.localedata",
          "jdk.management",
          "jdk.management.agent",
          "jdk.management.jfr",
          "jdk.naming.dns",
          "jdk.naming.rmi",
          "jdk.net",
          "jdk.nio.mapmode",
          "jdk.sctp",
          "jdk.security.auth",
          "jdk.security.jgss",
          "jdk.unsupported",
          "jdk.unsupported.desktop",
          "jdk.xml.dom",
          "jdk.zipfs");
  private static final String EXPECTED_VERSION = loadExpectedVersion();

  TemurinRuntime build(Path outputDirectory, List<String> additionalModules) throws IOException {
    return build(
        TargetPlatform.current(),
        outputDirectory,
        additionalModules,
        Path.of(System.getProperty("user.home"), ".m2", "repository"));
  }

  TemurinRuntime build(
      TargetPlatform platform,
      Path outputDirectory,
      List<String> additionalModules,
      Path mavenLocalRepository)
      throws IOException {
    var material = toolchain(platform, mavenLocalRepository);
    var jmodsDirectory = material.jmodsDirectory();
    validateJmods(jmodsDirectory);
    var modules = new TreeSet<>(COMPATIBILITY_MODULES);
    for (var module : additionalModules) {
      if (!MODULE_NAME.matcher(module).matches()) {
        throw new IllegalArgumentException("Invalid JDK module name: " + module);
      }
      modules.add(module);
    }

    recreateAbsentDirectory(outputDirectory);
    var moduleList = String.join(",", modules);
    runTool(
        "jlink",
        List.of(
            "--module-path",
            jmodsDirectory.toString(),
            "--add-modules",
            moduleList,
            "--limit-modules",
            moduleList,
            "--bind-services",
            "--strip-debug",
            "--no-header-files",
            "--no-man-pages",
            "--compress=zip-9",
            "--output",
            outputDirectory.toString()));

    var temurinNotice = material.notice();
    requireRegularFile(temurinNotice, "Temurin NOTICE");
    Files.copy(temurinNotice, outputDirectory.resolve("NOTICE"));
    requireRegularFile(outputDirectory.resolve("release"), "Linked runtime release file");
    if (platform == TargetPlatform.current()) {
      validateBuildJdk();
      verifyRuntimeStarts(outputDirectory.resolve("bin/java"));
    } else {
      verifyRuntimeLayout(outputDirectory, platform);
    }
    return new TemurinRuntime(outputDirectory, platform, EXPECTED_VERSION);
  }

  private ToolchainMaterial toolchain(TargetPlatform platform, Path mavenLocalRepository)
      throws IOException {
    if (platform == TargetPlatform.current()) {
      validateBuildJdk();
      var javaHome = Path.of(System.getProperty("java.home"));
      return new ToolchainMaterial(javaHome.resolve("jmods"), javaHome.resolve("NOTICE"));
    }
    var properties = toolchainProperties();
    var cacheRoot =
        mavenLocalRepository.resolve(
            "org/apache/bifromq/tenon/temurin-toolchains/" + EXPECTED_VERSION);
    var cache = cacheRoot.resolve(platform.classifier());
    var notice = cache.resolve("NOTICE");
    var jmods = cache.resolve("jmods");
    var jdkChecksum = properties.getProperty(platform.classifier() + ".jdk.sha256", "");
    var jmodsChecksum = properties.getProperty(platform.classifier() + ".jmods.sha256", "");
    if (cachedMaterialMatches(cache, jmods, notice, jdkChecksum, jmodsChecksum)) {
      return new ToolchainMaterial(jmods, notice);
    }
    Files.createDirectories(cacheRoot);
    var temporary =
        Files.createTempDirectory(cacheRoot, "." + platform.classifier() + ".download-");
    try {
      var jdkArchive = temporary.resolve("jdk.tar.gz");
      var jmodsArchive = temporary.resolve("jmods.tar.gz");
      downloadAndVerify(
          properties.getProperty(platform.classifier() + ".jdk.url", ""), jdkChecksum, jdkArchive);
      downloadAndVerify(
          properties.getProperty(platform.classifier() + ".jmods.url", ""),
          jmodsChecksum,
          jmodsArchive);
      var jmodsExtracted = temporary.resolve("jmods");
      extractArchive(jmodsArchive, jmodsExtracted);
      var downloadedNotice = extractNotice(jdkArchive, temporary.resolve("NOTICE"));
      var downloadedJmods = findFile(jmodsExtracted, "java.base.jmod").getParent();
      Files.createDirectories(temporary.resolve("material/jmods"));
      copyTree(downloadedJmods, temporary.resolve("material/jmods"));
      Files.copy(downloadedNotice, temporary.resolve("material/NOTICE"));
      Files.writeString(temporary.resolve("material/.jdk-archive-sha256"), jdkChecksum);
      Files.writeString(temporary.resolve("material/.jmods-archive-sha256"), jmodsChecksum);
      try {
        Files.move(temporary.resolve("material"), cache, StandardCopyOption.ATOMIC_MOVE);
      } catch (FileAlreadyExistsException race) {
        if (!cachedMaterialMatches(
            cache, cache.resolve("jmods"), cache.resolve("NOTICE"), jdkChecksum, jmodsChecksum)) {
          throw new IOException(
              "Concurrent Temurin toolchain cache publication produced invalid material", race);
        }
      }
      return new ToolchainMaterial(cache.resolve("jmods"), cache.resolve("NOTICE"));
    } finally {
      if (temporary != null) {
        deleteTree(temporary);
      }
    }
  }

  private static Properties toolchainProperties() throws IOException {
    var properties = new Properties();
    try (var input =
        TemurinRuntimeBuilder.class.getResourceAsStream(
            "/org/apache/bifromq/tenon/maven/tenon-toolchain.properties")) {
      if (input == null) {
        throw new IOException("Temurin toolchain profile is missing");
      }
      properties.load(input);
    }
    return properties;
  }

  private static boolean cachedMaterialMatches(
      Path cache, Path jmods, Path notice, String jdkChecksum, String jmodsChecksum)
      throws IOException {
    return Files.isDirectory(jmods)
        && Files.isRegularFile(jmods.resolve("java.base.jmod"))
        && Files.isRegularFile(notice)
        && Files.isRegularFile(cache.resolve(".jdk-archive-sha256"))
        && Files.isRegularFile(cache.resolve(".jmods-archive-sha256"))
        && Files.readString(cache.resolve(".jdk-archive-sha256")).trim().equals(jdkChecksum)
        && Files.readString(cache.resolve(".jmods-archive-sha256")).trim().equals(jmodsChecksum);
  }

  private static void downloadAndVerify(String url, String expectedSha256, Path destination)
      throws IOException {
    if (url.isBlank() || !expectedSha256.matches("[0-9a-f]{64}")) {
      throw new IOException("Temurin target toolchain metadata is missing");
    }
    var client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    IOException failure = null;
    for (var attempt = 1; attempt <= 3; attempt++) {
      Files.deleteIfExists(destination);
      try {
        var request =
            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMinutes(10)).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofFile(destination));
        if (response.statusCode() != 200) {
          throw new IOException(
              "Temurin target toolchain download failed with HTTP " + response.statusCode());
        }
        if (!sha256(destination).equals(expectedSha256)) {
          throw new IOException("Temurin target toolchain checksum mismatch");
        }
        return;
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while downloading Temurin target toolchain", error);
      } catch (IOException error) {
        failure = error;
        if (attempt < 3) {
          try {
            Thread.sleep(attempt * 1000L);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException(
                "Interrupted while retrying Temurin target toolchain download", interrupted);
          }
        }
      }
    }
    throw failure;
  }

  private static void extractArchive(Path archive, Path output) throws IOException {
    Files.createDirectories(output);
    try (var input =
        new TarArchiveInputStream(new GzipCompressorInputStream(Files.newInputStream(archive)))) {
      TarArchiveEntry entry;
      while ((entry = input.getNextEntry()) != null) {
        if (entry.isSymbolicLink() || entry.isLink()) {
          throw new IOException("Temurin archive contains a link: " + entry.getName());
        }
        var target = output.resolve(entry.getName()).normalize();
        if (!target.startsWith(output)) {
          throw new IOException("Temurin archive contains an unsafe path");
        }
        if (entry.isDirectory()) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  private static Path extractNotice(Path archive, Path destination) throws IOException {
    try (var input =
        new TarArchiveInputStream(new GzipCompressorInputStream(Files.newInputStream(archive)))) {
      TarArchiveEntry entry;
      while ((entry = input.getNextEntry()) != null) {
        var name = entry.getName();
        if (!entry.isDirectory() && (name.equals("NOTICE") || name.endsWith("/NOTICE"))) {
          Files.copy(input, destination, StandardCopyOption.REPLACE_EXISTING);
          return destination;
        }
      }
    }
    throw new IOException("Temurin archive is missing NOTICE");
  }

  private static Path findFile(Path root, String fileName) throws IOException {
    try (var paths = Files.walk(root)) {
      return paths
          .filter(path -> path.getFileName().toString().equals(fileName))
          .findFirst()
          .orElseThrow(() -> new IOException("Temurin archive is missing " + fileName));
    }
  }

  private static void copyTree(Path source, Path destination) throws IOException {
    try (var paths = Files.walk(source)) {
      for (var path : paths.toList()) {
        var target = destination.resolve(source.relativize(path).toString());
        if (Files.isDirectory(path)) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  private static void deleteTree(Path directory) throws IOException {
    if (directory == null || !Files.exists(directory)) {
      return;
    }
    try (var paths = Files.walk(directory)) {
      for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  private static void validateBuildJdk() throws IOException {
    var vendor = System.getProperty("java.vendor", "");
    var vendorVersion = System.getProperty("java.vendor.version", "");
    var runtimeVersion = System.getProperty("java.runtime.version", "");
    if (!vendor.equals("Eclipse Adoptium")
        || !vendorVersion.equals("Temurin-" + EXPECTED_VERSION)
        || !(runtimeVersion.equals(EXPECTED_VERSION)
            || runtimeVersion.startsWith(EXPECTED_VERSION + "-"))) {
      throw new IOException(
          "Tenon Plugin bundles require Eclipse Temurin "
              + EXPECTED_VERSION
              + "; current runtime is "
              + vendor
              + " "
              + runtimeVersion);
    }
  }

  private static void validateJmods(Path jmodsDirectory) throws IOException {
    if (!Files.isDirectory(jmodsDirectory)) {
      throw new IOException(
          "Matching Temurin JMODs directory is missing: "
              + jmodsDirectory
              + ". Build this project through its Maven Wrapper");
    }
    var javaBase = jmodsDirectory.resolve("java.base.jmod");
    requireRegularFile(javaBase, "Temurin java.base JMOD");
    var description = runTool("jmod", List.of("describe", javaBase.toString()));
    var expectedModuleVersion = EXPECTED_VERSION.substring(0, EXPECTED_VERSION.indexOf('+'));
    var firstLine = description.lines().findFirst().orElse("");
    if (!firstLine.equals("java.base@" + expectedModuleVersion)) {
      throw new IOException(
          "Temurin JMOD version does not match the required runtime: " + firstLine);
    }
  }

  private static void verifyRuntimeLayout(Path runtime, TargetPlatform platform)
      throws IOException {
    requireRegularFile(
        runtime.resolve("bin/java"), "Linked " + platform.classifier() + " Java launcher");
    requireRegularFile(
        runtime.resolve("release"), "Linked " + platform.classifier() + " runtime release file");
  }

  private static String runTool(String name, List<String> arguments) throws IOException {
    var tool =
        ToolProvider.findFirst(name)
            .orElseThrow(() -> new IOException("Current JDK does not provide " + name));
    var standardOutput = new StringWriter();
    var standardError = new StringWriter();
    var status =
        tool.run(
            new PrintWriter(standardOutput),
            new PrintWriter(standardError),
            arguments.toArray(String[]::new));
    if (status != 0) {
      throw new IOException(
          name + " failed with status " + status + ": " + standardError.toString().trim());
    }
    return standardOutput.toString();
  }

  private static void verifyRuntimeStarts(Path executable) throws IOException {
    requireRegularFile(executable, "Linked runtime Java launcher");
    var process =
        new ProcessBuilder(executable.toString(), "--version").redirectErrorStream(true).start();
    try {
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IOException("Linked runtime Java launcher did not finish --version");
      }
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
      throw new IOException("Interrupted while checking linked runtime", error);
    }
    if (process.exitValue() != 0) {
      throw new IOException(
          "Linked runtime Java launcher failed: "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private static void requireRegularFile(Path path, String role) throws IOException {
    if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) {
      throw new IOException(role + " is not a regular file: " + path);
    }
  }

  private static void recreateAbsentDirectory(Path directory) throws IOException {
    if (Files.exists(directory)) {
      try (var paths = Files.walk(directory)) {
        for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.delete(path);
        }
      }
    }
    var parent = directory.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
  }

  private static String loadExpectedVersion() {
    var properties = new Properties();
    try (var input =
        TemurinRuntimeBuilder.class.getResourceAsStream(
            "/org/apache/bifromq/tenon/maven/tenon-toolchain.properties")) {
      if (input == null) {
        throw new IllegalStateException("Temurin toolchain profile is missing");
      }
      properties.load(input);
    } catch (IOException error) {
      throw new IllegalStateException("Temurin toolchain profile cannot be read", error);
    }
    var version = properties.getProperty("version", "").trim();
    if (version.isEmpty()) {
      throw new IllegalStateException("Temurin runtime version is missing");
    }
    return version;
  }

  private static String sha256(Path path) throws IOException {
    try {
      var digest = MessageDigest.getInstance("SHA-256");
      try (var input = Files.newInputStream(path)) {
        var buffer = new byte[64 * 1024];
        for (var read = input.read(buffer); read >= 0; read = input.read(buffer)) {
          if (read > 0) {
            digest.update(buffer, 0, read);
          }
        }
      }
      return java.util.HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JDK does not provide SHA-256", impossible);
    }
  }

  private record ToolchainMaterial(Path jmodsDirectory, Path notice) {}
}
