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
import com.google.protobuf.DescriptorProtos.SourceCodeInfo;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.ObjectMapper;

final class ProgramBundleBuilderTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @TempDir Path directory;

  @Test
  void sharedManifestVectorsMatchTheCanonicalSchema() throws Exception {
    try (var schemaInput =
            getClass().getResourceAsStream("/contracts/plugin/manifest.schema.json");
        var vectorsInput =
            getClass().getResourceAsStream("/contracts/plugin/manifest.test-vectors.json")) {
      var schema =
          SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
              .getSchema(JSON.readTree(schemaInput));
      var vectors = JSON.readTree(vectorsInput);
      for (var category : List.of("valid", "invalid")) {
        for (var vector : vectors.get(category)) {
          assertEquals(
              category.equals("valid"),
              schema.validate(vector.get("manifest")).isEmpty(),
              vector.get("name").stringValue());
          var bytes = JSON.writeValueAsBytes(vector.get("manifest"));
          if (category.equals("valid")) {
            BundleContractValidator.validateManifest(bytes);
          } else {
            org.junit.jupiter.api.Assertions.assertThrows(
                IOException.class,
                () -> BundleContractValidator.validateManifest(bytes),
                vector.get("name").stringValue());
          }
        }
      }
    }
  }

  @Test
  void allInterfacesProduceCurrentBundles() throws Exception {
    var output =
        Path.of(System.getProperty("tenon.test.bundleOutputDirectory", directory.toString()));
    try (var schemaInput =
        getClass().getResourceAsStream("/contracts/plugin/manifest.schema.json")) {
      var manifestSchema =
          SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
              .getSchema(JSON.readTree(schemaInput));
      for (var pluginInterface : PluginInterface.values()) {
        var request =
            BundleBuilderTest.request(
                output.resolve(pluginInterface.manifestValue()),
                pluginInterface,
                "1.0." + pluginInterface.ordinal());
        var descriptor = descriptor(pluginInterface).toByteArray();
        Files.write(request.payloadDescriptor(), descriptor);

        var archive = new BundleBuilder().build(request);
        var first = Files.readAllBytes(archive);
        var entries = BundleBuilderTest.readArchive(archive);
        var manifest = JSON.readTree(entries.get("manifest.json"));

        assertTrue(manifestSchema.validate(manifest).isEmpty(), manifest.toString());
        assertEquals(7, manifest.size());
        assertEquals(1, manifest.get("platforms").size());
        assertEquals(
            request.runtime().platform().os(),
            manifest.get("platforms").get(0).get("os").stringValue());
        assertEquals(
            request.runtime().platform().architecture(),
            manifest.get("platforms").get(0).get("architecture").stringValue());
        assertEquals(pluginInterface.manifestValue(), manifest.get("interface").stringValue());
        assertEquals(request.programName(), manifest.get("programName").stringValue());
        assertEquals(request.exactVersion(), manifest.get("exactVersion").stringValue());
        assertEquals(request.displayName(), manifest.get("displayName").stringValue());
        assertEquals(request.description(), manifest.get("description").stringValue());
        assertEquals("runtime/bin/java", manifest.get("command").get(0).stringValue());
        assertEquals(request.mainClass(), manifest.get("command").get(4).stringValue());
        assertArrayEquals(BundleBuilderTest.CONFIG_SCHEMA, entries.get("config.schema.json"));
        assertArrayEquals(descriptor, entries.get("payload.descriptor.pb"));
        assertArrayEquals(Files.readAllBytes(request.programJar()), entries.get("program.jar"));
        assertArrayEquals(
            entries.get("runtime/legal/java.base/LICENSE"),
            entries.get("runtime/legal/java.logging/LICENSE"));
        assertArrayEquals(first, Files.readAllBytes(new BundleBuilder().build(request)));
      }
    }
  }

  @Test
  void descriptorFileOrderDoesNotChangeTheBundleOrItsDependencies() throws Exception {
    var request = BundleBuilderTest.request(directory, PluginInterface.SOURCE_AND_SINK, "1.0.0");
    var original = descriptor(PluginInterface.SOURCE_AND_SINK);
    var shared = original.getFile(0).toBuilder().setName("z-shared.proto").build();
    var source = original.getFile(2).toBuilder().setDependency(0, "z-shared.proto").build();
    var sink = original.getFile(1).toBuilder().setDependency(0, "z-shared.proto").build();
    var input =
        original.toBuilder().clearFile().addFile(shared).addFile(source).addFile(sink).build();
    Files.write(request.payloadDescriptor(), input.toByteArray());

    var archive = new BundleBuilder().build(request);
    var first = Files.readAllBytes(archive);
    var outputBytes = BundleBuilderTest.readArchive(archive).get("payload.descriptor.pb");
    var output = FileDescriptorSet.parseFrom(outputBytes);
    assertEquals(List.of(sink, source, shared), output.getFileList());
    BundleContractValidator.validatePayloadDescriptor(PluginInterface.SOURCE_AND_SINK, outputBytes);

    var reversed = input.toBuilder().clearFile().addAllFile(input.getFileList().reversed()).build();
    Files.write(request.payloadDescriptor(), reversed.toByteArray());
    assertArrayEquals(first, Files.readAllBytes(new BundleBuilder().build(request)));
    assertArrayEquals(reversed.toByteArray(), Files.readAllBytes(request.payloadDescriptor()));
  }

  @ParameterizedTest
  @EnumSource(PluginInterface.class)
  void descriptorMustMatchExactlyTheDeclaredInterfaces(PluginInterface pluginInterface) {
    for (var other : PluginInterface.values()) {
      if (other != pluginInterface) {
        assertThrows(
            IOException.class,
            () ->
                BundleContractValidator.validatePayloadDescriptor(
                    pluginInterface, descriptor(other).toByteArray()),
            pluginInterface + " must reject " + other);
      }
    }
    var noRoots = FileDescriptorSet.newBuilder().addFile(file("empty.proto", "empty")).build();
    assertThrows(
        IOException.class,
        () ->
            BundleContractValidator.validatePayloadDescriptor(
                pluginInterface, noRoots.toByteArray()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"SourceRecordPayload", "SinkRecordPayload"})
  void duplicateStandardRootsAcrossPackagesAreRejected(String root) {
    var descriptor =
        descriptor(PluginInterface.SOURCE_AND_SINK).toBuilder()
            .addFile(
                file("duplicate.proto", "duplicate")
                    .addMessageType(DescriptorProto.newBuilder().setName(root)))
            .build();
    assertThrows(
        IOException.class,
        () ->
            BundleContractValidator.validatePayloadDescriptor(
                PluginInterface.SOURCE_AND_SINK, descriptor.toByteArray()));
  }

  @Test
  void nestedMessagesDoNotDeclareAnInterface() {
    var descriptor =
        FileDescriptorSet.newBuilder()
            .addFile(
                file("nested.proto", "nested")
                    .addMessageType(
                        DescriptorProto.newBuilder()
                            .setName("Container")
                            .addNestedType(
                                DescriptorProto.newBuilder().setName("SourceRecordPayload"))))
            .build();
    assertThrows(
        IOException.class,
        () ->
            BundleContractValidator.validatePayloadDescriptor(
                PluginInterface.SOURCE, descriptor.toByteArray()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", ",\"type\":\"array\"", ",\"type\":[\"object\",\"null\"]"})
  void targetSchemaRequiresAnExplicitObjectRootBeforePublishing(String type) throws Exception {
    var schema =
        ("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\"" + type + "}")
            .getBytes(StandardCharsets.UTF_8);
    var request = BundleBuilderTest.request(directory, PluginInterface.SOURCE, "1.0.0");
    Files.write(request.configSchema(), schema);

    assertThrows(IOException.class, () -> new BundleBuilder().build(request));
    assertTrue(Files.notExists(directory.resolve("tenon-bundle-staging")));
  }

  @Test
  void targetDescriptorStillRejectsInvalidSharedMaterials() {
    var valid = descriptor(PluginInterface.SOURCE);
    var root = valid.getFile(1);
    for (var invalid :
        List.of(
            valid.toBuilder().setFile(1, root.toBuilder().setSyntax("proto2")).build(),
            valid.toBuilder().setFile(1, root.toBuilder().clearSourceCodeInfo()).build(),
            valid.toBuilder().setFile(1, root.toBuilder().addDependency("absent.proto")).build(),
            valid.toBuilder().addFile(root).build(),
            valid.toBuilder()
                .setFile(
                    1,
                    root.toBuilder()
                        .setMessageType(
                            0,
                            root.getMessageType(0).toBuilder()
                                .setField(
                                    0,
                                    root.getMessageType(0).getField(0).toBuilder()
                                        .setTypeName(".absent.Message"))))
                .build())) {
      assertThrows(
          IOException.class,
          () ->
              BundleContractValidator.validatePayloadDescriptor(
                  PluginInterface.SOURCE, invalid.toByteArray()));
    }
  }

  static FileDescriptorSet descriptor(PluginInterface pluginInterface) {
    var descriptor =
        FileDescriptorSet.newBuilder()
            .addFile(
                file("shared.proto", "shared")
                    .addMessageType(DescriptorProto.newBuilder().setName("Shared")));
    for (var direction : List.of(PluginInterface.SINK, PluginInterface.SOURCE)) {
      if (pluginInterface == direction || pluginInterface == PluginInterface.SOURCE_AND_SINK) {
        var root =
            direction == PluginInterface.SOURCE ? "SourceRecordPayload" : "SinkRecordPayload";
        descriptor.addFile(
            file(direction.manifestValue() + ".proto", direction.manifestValue())
                .addDependency("shared.proto")
                .addMessageType(
                    DescriptorProto.newBuilder()
                        .setName(root)
                        .addField(
                            FieldDescriptorProto.newBuilder()
                                .setName("value")
                                .setNumber(1)
                                .setLabel(Label.LABEL_OPTIONAL)
                                .setType(Type.TYPE_MESSAGE)
                                .setTypeName(".shared.Shared"))));
      }
    }
    return descriptor.build();
  }

  private static FileDescriptorProto.Builder file(String name, String packageName) {
    return FileDescriptorProto.newBuilder()
        .setName(name)
        .setPackage(packageName)
        .setSyntax("proto3")
        .setSourceCodeInfo(
            SourceCodeInfo.newBuilder()
                .addLocation(SourceCodeInfo.Location.newBuilder().addPath(4).addPath(0)));
  }
}
