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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.google.protobuf.UnknownFieldSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

final class BundleBuilderTest {
  static final byte[] CONFIG_SCHEMA =
      ("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
              + "\"type\":\"object\",\"additionalProperties\":false}")
          .getBytes(StandardCharsets.UTF_8);
  private static final ObjectMapper JSON = new ObjectMapper();

  @TempDir Path directory;

  @Test
  void bundleContainsOneVerifiedProgramTreeAndExactManifest() throws Exception {
    var request = request("1.2.3");

    var archive = new BundleBuilder().build(request);
    var entries = readArchive(archive);
    var manifest = JSON.readTree(entries.get("manifest.json"));

    assertEquals(
        List.of(
            "runtime/bin/java",
            "--enable-native-access=ALL-UNNAMED",
            "-cp",
            "program.jar:lib/" + sha256("dependency".getBytes(StandardCharsets.UTF_8)) + ".jar",
            "com.example.source.Main"),
        JSON.treeToValue(manifest.get("command"), List.class));
    assertEquals("source", manifest.get("interface").stringValue());
    assertEquals("com.example.source", manifest.get("programName").stringValue());
    assertEquals("1.2.3", manifest.get("exactVersion").stringValue());
    assertArrayEquals("program".getBytes(), entries.get("program.jar"));
    assertArrayEquals(CONFIG_SCHEMA, entries.get("config.schema.json"));
    assertArrayEquals(sourceDescriptor(), entries.get("payload.descriptor.pb"));
    assertArrayEquals("project license".getBytes(), entries.get("LICENSE"));
    assertArrayEquals("project notice".getBytes(), entries.get("NOTICE"));
    assertArrayEquals("runtime license".getBytes(), entries.get("runtime/legal/java.base/LICENSE"));
    assertArrayEquals(
        "runtime license".getBytes(), entries.get("runtime/legal/java.logging/LICENSE"));
    assertTrue(
        new String(entries.get("DEPENDENCIES")).contains("com.example:dependency:jar:1.0.0"));
    assertTrue(new String(entries.get("README")).contains("linux-amd64"));

    assertTrue(entries.keySet().stream().anyMatch(path -> path.startsWith("lib/")));
  }

  @Test
  void projectLegalFilesAreOptionalAndNeverInvented() throws Exception {
    var request = request("1.2.3");
    Files.delete(request.license());
    Files.delete(request.notice());

    var entries = readArchive(new BundleBuilder().build(request));

    assertTrue(!entries.containsKey("LICENSE"));
    assertTrue(!entries.containsKey("NOTICE"));
    assertArrayEquals("runtime notice".getBytes(), entries.get("runtime/NOTICE"));
    assertArrayEquals("runtime license".getBytes(), entries.get("runtime/legal/java.base/LICENSE"));
  }

  @Test
  void anExistingInvalidProjectLegalFileIsRejected() throws Exception {
    var request = request("1.2.3");
    Files.delete(request.license());
    Files.createDirectory(request.license());
    assertThrows(IOException.class, () -> new BundleBuilder().build(request));
  }

  @Test
  void sameInputsProduceIdenticalArchiveBytes() throws Exception {
    var request = request("1.2.3-rc.1");

    var first = Files.readAllBytes(new BundleBuilder().build(request));
    var second = Files.readAllBytes(new BundleBuilder().build(request));

    assertArrayEquals(first, second);
  }

  @Test
  void invalidIdentityAndContractFailBeforeAnArchiveIsPublished() throws Exception {
    var invalidVersion = request("1.2.3-SNAPSHOT");
    assertThrows(IllegalArgumentException.class, () -> new BundleBuilder().build(invalidVersion));

    var externalSchema = directory.resolve("external.schema.json");
    Files.writeString(
        externalSchema,
        "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
            + "\"type\":\"object\",\"$ref\":\"https://example.com/schema\"}");
    var invalidSchema =
        new BundleRequest(
            invalidVersion.pluginInterface(),
            invalidVersion.programName(),
            "1.2.3",
            "Example Plugin",
            "Read and write example records.",
            invalidVersion.mainClass(),
            invalidVersion.programJar(),
            invalidVersion.runtimeDependencies(),
            invalidVersion.runtime(),
            invalidVersion.license(),
            invalidVersion.notice(),
            externalSchema,
            invalidVersion.payloadDescriptor(),
            invalidVersion.outputDirectory(),
            invalidVersion.finalName());

    assertThrows(IOException.class, () -> new BundleBuilder().build(invalidSchema));
    assertTrue(Files.notExists(directory.resolve("example-1.2.3-tenon-plugin-linux-amd64.tar.gz")));
  }

  @Test
  void configSchemaRejectsDuplicateKeysAndTrailingJson() {
    var duplicate =
        ("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
                + "\"type\":\"object\",\"type\":\"array\"}")
            .getBytes(StandardCharsets.UTF_8);
    var trailing =
        (new String(CONFIG_SCHEMA, StandardCharsets.UTF_8) + "{}").getBytes(StandardCharsets.UTF_8);

    assertThrows(IOException.class, () -> BundleContractValidator.validateConfigSchema(duplicate));
    assertThrows(IOException.class, () -> BundleContractValidator.validateConfigSchema(trailing));
  }

  @Test
  void payloadDescriptorRejectsCustomOptionDataBelowFileLevel() throws Exception {
    var unknownField = UnknownFieldSet.Field.newBuilder().addVarint(1).build();
    var options =
        MessageOptions.newBuilder()
            .setUnknownFields(UnknownFieldSet.newBuilder().addField(50_001, unknownField).build())
            .build();
    var descriptor = FileDescriptorSet.parseFrom(sourceDescriptor());
    var root = descriptor.getFile(0).getMessageType(0).toBuilder().setOptions(options).build();
    var file = descriptor.getFile(0).toBuilder().setMessageType(0, root).build();
    var withCustomOption = descriptor.toBuilder().setFile(0, file).build().toByteArray();

    assertThrows(
        IOException.class,
        () ->
            BundleContractValidator.validatePayloadDescriptor(
                PluginInterface.SOURCE, withCustomOption));
  }

  private BundleRequest request(String version) throws Exception {
    return request(directory, PluginInterface.SOURCE, version);
  }

  static BundleRequest request(Path directory, PluginInterface pluginInterface, String version)
      throws Exception {
    Files.createDirectories(directory);
    var program = directory.resolve("program.jar");
    var dependency = directory.resolve("dependency.jar");
    var schema = directory.resolve("config.schema.json");
    var descriptor = directory.resolve("payload.descriptor.pb");
    var license = directory.resolve("LICENSE");
    var notice = directory.resolve("NOTICE");
    var runtime = directory.resolve("runtime");
    Files.write(program, "program".getBytes());
    Files.write(dependency, "dependency".getBytes());
    Files.write(schema, CONFIG_SCHEMA);
    Files.write(descriptor, sourceDescriptor());
    Files.write(license, "project license".getBytes());
    Files.write(notice, "project notice".getBytes());
    Files.createDirectories(runtime.resolve("bin"));
    Files.createDirectories(runtime.resolve("legal/java.base"));
    Files.createDirectories(runtime.resolve("legal/java.logging"));
    Files.write(runtime.resolve("bin/java"), "runtime java".getBytes());
    Files.write(runtime.resolve("release"), "runtime release".getBytes());
    Files.write(runtime.resolve("NOTICE"), "runtime notice".getBytes());
    Files.write(runtime.resolve("legal/java.base/LICENSE"), "runtime license".getBytes());
    Files.createSymbolicLink(
        runtime.resolve("legal/java.logging/LICENSE"), Path.of("../java.base/LICENSE"));
    return new BundleRequest(
        pluginInterface,
        "com.example.source",
        version,
        "Example Plugin",
        "Read and write example records.",
        "com.example.source.Main",
        program,
        List.of(new BundledDependency("com.example:dependency:jar:1.0.0", dependency)),
        new TemurinRuntime(runtime, TargetPlatform.LINUX_AMD64, "25.0.3+9"),
        license,
        notice,
        schema,
        descriptor,
        directory,
        "example-" + version);
  }

  private static byte[] sourceDescriptor() {
    var root =
        DescriptorProto.newBuilder()
            .setName("SourceRecordPayload")
            .addField(
                FieldDescriptorProto.newBuilder()
                    .setName("value")
                    .setNumber(1)
                    .setLabel(Label.LABEL_OPTIONAL)
                    .setType(Type.TYPE_STRING))
            .build();
    var sourceInfo =
        SourceCodeInfo.newBuilder()
            .addLocation(SourceCodeInfo.Location.newBuilder().addPath(4).addPath(0))
            .build();
    var file =
        FileDescriptorProto.newBuilder()
            .setName("source_record_payload.proto")
            .setPackage("com.example.source")
            .setSyntax("proto3")
            .setSourceCodeInfo(sourceInfo)
            .addMessageType(root)
            .build();
    return FileDescriptorSet.newBuilder().addFile(file).build().toByteArray();
  }

  static Map<String, byte[]> readArchive(Path archive) throws Exception {
    Map<String, byte[]> entries = new HashMap<>();
    try (var input = Files.newInputStream(archive);
        var gzip = new GzipCompressorInputStream(input);
        var tar = new TarArchiveInputStream(gzip)) {
      for (var entry = tar.getNextEntry(); entry != null; entry = tar.getNextEntry()) {
        if (!entry.isDirectory()) {
          var bytes = new ByteArrayOutputStream();
          tar.transferTo(bytes);
          entries.put(entry.getName(), bytes.toByteArray());
        }
      }
    }
    return entries;
  }

  private static String sha256(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }
}
