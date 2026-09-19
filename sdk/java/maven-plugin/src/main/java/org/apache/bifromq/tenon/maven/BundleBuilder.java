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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import tools.jackson.databind.ObjectMapper;

final class BundleBuilder {
  private static final ObjectMapper JSON = new ObjectMapper();

  Path build(BundleRequest request) throws IOException {
    BundleContractValidator.validateIdentity(
        request.programName(), request.exactVersion(), request.mainClass());
    var configSchema = readRegularFile(request.configSchema(), "Config Schema");
    BundleContractValidator.validateConfigSchema(configSchema);
    var payloadDescriptor =
        BundleContractValidator.validatePayloadDescriptor(
            request.pluginInterface(),
            readRegularFile(request.payloadDescriptor(), "Payload descriptor"));
    requireRegularFile(request.programJar(), "Project JAR");
    if (!Files.isDirectory(request.runtime().directory())) {
      throw new IOException("Temurin runtime is not a directory: " + request.runtime().directory());
    }
    for (var dependency : request.runtimeDependencies()) {
      requireRegularFile(dependency.jar(), "Runtime dependency " + dependency.coordinate());
    }

    Files.createDirectories(request.outputDirectory());
    var staging =
        request
            .outputDirectory()
            .resolve("tenon-bundle-staging-" + request.runtime().platform().classifier());
    recreateDirectory(staging);
    copy(staging, "program.jar", request.programJar());
    write(staging, "config.schema.json", configSchema);
    write(
        staging,
        "payload.descriptor.pb",
        payloadDescriptor.toBuilder()
            .clearFile()
            .addAllFile(
                payloadDescriptor.getFileList().stream()
                    .sorted(Comparator.comparing(FileDescriptorProto::getName))
                    .toList())
            .build()
            .toByteArray());
    copyProjectLegalFile(staging, "LICENSE", request.license());
    copyProjectLegalFile(staging, "NOTICE", request.notice());

    var dependencyPaths = new TreeMap<String, String>();
    var classpath = new LinkedHashSet<String>();
    classpath.add("program.jar");
    var runtimeDependencies = new ArrayList<>(request.runtimeDependencies());
    runtimeDependencies.sort(Comparator.comparing(BundledDependency::coordinate));
    for (var dependency : runtimeDependencies) {
      var digest = sha256(dependency.jar());
      var relative = "lib/" + digest + ".jar";
      if (classpath.add(relative)) {
        copy(staging, relative, dependency.jar());
      }
      dependencyPaths.put(dependency.coordinate(), relative);
    }
    copyTree(staging, "runtime", request.runtime().directory());
    write(
        staging,
        "DEPENDENCIES",
        dependencies(request.runtime(), dependencyPaths).getBytes(StandardCharsets.UTF_8));
    write(staging, "README", readme(request.runtime()).getBytes(StandardCharsets.UTF_8));

    var command =
        List.of(
            "runtime/bin/java",
            "--enable-native-access=ALL-UNNAMED",
            "-cp",
            String.join(":", classpath),
            request.mainClass());
    var manifest =
        new ProgramManifest(
            request.programName(),
            request.exactVersion(),
            request.displayName(),
            request.description(),
            request.pluginInterface().manifestValue(),
            List.of(
                new Platform(
                    request.runtime().platform().os(),
                    request.runtime().platform().architecture())),
            command);
    var manifestBytes = JSON.writeValueAsBytes(manifest);
    BundleContractValidator.validateManifest(manifestBytes);
    var withNewline = new byte[manifestBytes.length + 1];
    System.arraycopy(manifestBytes, 0, withNewline, 0, manifestBytes.length);
    withNewline[manifestBytes.length] = '\n';
    Files.write(staging.resolve("manifest.json"), withNewline);

    var archive =
        request
            .outputDirectory()
            .resolve(
                request.finalName()
                    + "-tenon-plugin-"
                    + request.runtime().platform().classifier()
                    + ".tar.gz");
    var temporary = archive.resolveSibling(archive.getFileName() + ".tmp");
    Files.deleteIfExists(temporary);
    writeArchive(staging, temporary);
    moveIntoPlace(temporary, archive);
    return archive;
  }

  private static void copy(Path staging, String relative, Path source) throws IOException {
    var target = staging.resolve(relative);
    Files.createDirectories(target.getParent());
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
  }

  private static void write(Path staging, String relative, byte[] bytes) throws IOException {
    var target = staging.resolve(relative);
    Files.createDirectories(target.getParent());
    Files.write(target, bytes);
  }

  private static byte[] readRegularFile(Path path, String role) throws IOException {
    requireRegularFile(path, role);
    return Files.readAllBytes(path);
  }

  private static void copyTree(Path staging, String relativeRoot, Path sourceRoot)
      throws IOException {
    var realRoot = sourceRoot.toRealPath();
    try (var tree = Files.walk(sourceRoot)) {
      for (var source : tree.sorted().toList()) {
        if (source.equals(sourceRoot) || Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS)) {
          continue;
        }
        var realSource = source.toRealPath();
        if (!realSource.startsWith(realRoot) || !Files.isRegularFile(realSource)) {
          throw new IOException("Runtime contains an invalid link or file: " + source);
        }
        var relative =
            relativeRoot
                + "/"
                + sourceRoot
                    .relativize(source)
                    .toString()
                    .replace(source.getFileSystem().getSeparator(), "/");
        copy(staging, relative, realSource);
      }
    }
  }

  private static void requireRegularFile(Path path, String role) throws IOException {
    if (Files.isSymbolicLink(path) || !Files.isRegularFile(path)) {
      throw new IOException(role + " is not a regular file: " + path);
    }
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
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JDK does not provide SHA-256", impossible);
    }
  }

  private static void copyProjectLegalFile(Path staging, String name, Path source)
      throws IOException {
    if (Files.notExists(source)) {
      return;
    }
    requireRegularFile(source, "Project " + name);
    copy(staging, name, source);
  }

  private static String dependencies(TemurinRuntime runtime, Map<String, String> dependencyPaths) {
    var text = new StringBuilder();
    text.append("Bundled Maven runtime dependencies\n\n");
    for (var dependency : dependencyPaths.entrySet()) {
      text.append(dependency.getKey()).append(" -> ").append(dependency.getValue()).append('\n');
    }
    text.append("\nBundled Java runtime\n\n");
    text.append("Eclipse Temurin ").append(runtime.version()).append('\n');
    text.append("Target platform: ").append(runtime.platform().classifier()).append('\n');
    text.append("Runtime notices: runtime/NOTICE and runtime/legal/\n");
    return text.toString();
  }

  private static String readme(TemurinRuntime runtime) {
    return """
        Tenon Plugin Program binary package

        This package is self-contained for %s and starts with runtime/bin/java.
        It does not require JAVA_HOME or a system Java installation.

        Project LICENSE and NOTICE are included only when supplied by the plugin author.
        Bundled dependency inventory: DEPENDENCIES
        Eclipse Temurin %s notice and licenses: runtime/NOTICE and runtime/legal/

        The plugin author chooses the project license and must account for every bundled
        dependency.
        """
        .formatted(runtime.platform().classifier(), runtime.version());
  }

  private static void recreateDirectory(Path directory) throws IOException {
    if (Files.exists(directory)) {
      try (var paths = Files.walk(directory)) {
        for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.delete(path);
        }
      }
    }
    Files.createDirectories(directory);
  }

  private static void writeArchive(Path staging, Path archive) throws IOException {
    var gzipParameters = new GzipParameters();
    gzipParameters.setModificationInstant(Instant.EPOCH);
    gzipParameters.setOS(GzipParameters.OS.UNIX);
    try (var output = new BufferedOutputStream(Files.newOutputStream(archive));
        var gzip = new GzipCompressorOutputStream(output, gzipParameters);
        var tar = new TarArchiveOutputStream(gzip, StandardCharsets.UTF_8.name())) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      var paths = new ArrayList<Path>();
      try (var tree = Files.walk(staging)) {
        paths.addAll(tree.filter(path -> !path.equals(staging)).toList());
      }
      paths.sort(Comparator.comparing(path -> staging.relativize(path).toString()));
      for (var path : paths) {
        var relative =
            staging.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/");
        var directory = Files.isDirectory(path);
        var entry = new TarArchiveEntry(directory ? relative + "/" : relative);
        entry.setMode(directory ? 0700 : 0500);
        entry.setModTime(0);
        entry.setUserId(0);
        entry.setGroupId(0);
        entry.setUserName("");
        entry.setGroupName("");
        entry.setSize(directory ? 0 : Files.size(path));
        tar.putArchiveEntry(entry);
        if (!directory) {
          Files.copy(path, tar);
        }
        tar.closeArchiveEntry();
      }
      tar.finish();
    }
  }

  private static void moveIntoPlace(Path source, Path target) throws IOException {
    try {
      Files.move(
          source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException ignored) {
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private record Platform(String os, String architecture) {}

  private record ProgramManifest(
      String programName,
      String exactVersion,
      String displayName,
      String description,
      @JsonProperty("interface") String pluginInterface,
      List<Platform> platforms,
      List<String> command) {}
}
