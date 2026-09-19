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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

final class BundleContractValidator {
  private static final String DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema";
  private static final Pattern PROGRAM_NAME =
      Pattern.compile(
          "^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?){2,}$");
  private static final Pattern EXACT_VERSION =
      Pattern.compile(
          "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9a-z-]*[a-z-][0-9a-z-]*)(?:\\.(?:0|[1-9][0-9]*|[0-9a-z-]*[a-z-][0-9a-z-]*))*)?$");
  private static final Pattern MAIN_CLASS =
      Pattern.compile("^[A-Za-z_$][A-Za-z0-9_$]*(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)+$");
  private static final Set<String> SCHEMA_KEYWORDS =
      Set.of(
          "$schema",
          "$id",
          "$ref",
          "$anchor",
          "$dynamicRef",
          "$dynamicAnchor",
          "$comment",
          "$defs",
          "prefixItems",
          "items",
          "contains",
          "additionalProperties",
          "properties",
          "patternProperties",
          "dependentSchemas",
          "propertyNames",
          "if",
          "then",
          "else",
          "allOf",
          "anyOf",
          "oneOf",
          "not",
          "unevaluatedItems",
          "unevaluatedProperties",
          "type",
          "const",
          "enum",
          "multipleOf",
          "maximum",
          "exclusiveMaximum",
          "minimum",
          "exclusiveMinimum",
          "maxLength",
          "minLength",
          "pattern",
          "maxItems",
          "minItems",
          "uniqueItems",
          "maxContains",
          "minContains",
          "maxProperties",
          "minProperties",
          "required",
          "dependentRequired",
          "title",
          "description",
          "deprecated",
          "readOnly",
          "writeOnly",
          "examples",
          "format",
          "contentEncoding",
          "contentMediaType",
          "contentSchema");
  private static final Set<String> OPTION_MESSAGES =
      Set.of(
          "google.protobuf.FileOptions",
          "google.protobuf.MessageOptions",
          "google.protobuf.FieldOptions",
          "google.protobuf.OneofOptions",
          "google.protobuf.EnumOptions",
          "google.protobuf.EnumValueOptions",
          "google.protobuf.ServiceOptions",
          "google.protobuf.MethodOptions",
          "google.protobuf.ExtensionRangeOptions");
  private static final ObjectMapper JSON =
      JsonMapper.builder(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
          .build();

  private BundleContractValidator() {}

  static void validateIdentity(String programName, String exactVersion, String mainClass) {
    if (!PROGRAM_NAME.matcher(programName).matches()) {
      throw new IllegalArgumentException("Program name is not a reverse-domain identifier");
    }
    if (!EXACT_VERSION.matcher(exactVersion).matches()) {
      throw new IllegalArgumentException(
          "Project version must be strict SemVer without build metadata");
    }
    if (!MAIN_CLASS.matcher(mainClass).matches()) {
      throw new IllegalArgumentException("Main class must be a fully qualified Java class name");
    }
  }

  static void validateManifest(byte[] bytes) throws IOException {
    try (var input =
        BundleContractValidator.class.getResourceAsStream(
            "/contracts/plugin/manifest.schema.json")) {
      var schema =
          SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
              .getSchema(JSON.readTree(input));
      var errors = schema.validate(JSON.readTree(bytes));
      if (!errors.isEmpty()) {
        throw new IOException("Plugin manifest violates its Schema: " + errors.getFirst());
      }
    }
  }

  static void validateConfigSchema(byte[] bytes) throws IOException {
    JsonNode schema;
    try (var parser = JSON.createParser(bytes)) {
      schema = JSON.readTree(parser);
      if (schema == null || parser.nextToken() != null) {
        throw new IOException("Config Schema must contain exactly one JSON value");
      }
    } catch (Exception error) {
      if (error instanceof IOException ioError) {
        throw ioError;
      }
      throw new IOException("Config Schema is not valid JSON", error);
    }
    if (!schema.isObject()
        || !DRAFT_2020_12.equals(schema.path("$schema").stringValueOpt().orElse(null))) {
      throw new IOException("Config Schema must declare Draft 2020-12");
    }
    if (!"object".equals(schema.path("type").stringValueOpt().orElse(null))) {
      throw new IOException("Config Schema root must explicitly declare type object");
    }
    validateSchemaProfile(schema, "");
    var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
    var metaSchema =
        registry.getSchema(SchemaLocation.of(SpecificationVersion.DRAFT_2020_12.getDialectId()));
    var errors = metaSchema.validate(schema);
    if (!errors.isEmpty()) {
      throw new IOException("Config Schema violates Draft 2020-12: " + errors.getFirst());
    }
    try {
      registry.getSchema(schema);
    } catch (RuntimeException error) {
      throw new IOException("Config Schema cannot be compiled", error);
    }
  }

  static FileDescriptorSet validatePayloadDescriptor(PluginInterface pluginInterface, byte[] bytes)
      throws IOException {
    FileDescriptorSet descriptorSet;
    try {
      descriptorSet = FileDescriptorSet.parseFrom(bytes);
    } catch (InvalidProtocolBufferException error) {
      throw new IOException("Payload descriptor is malformed", error);
    }
    if (descriptorSet.getFileCount() == 0) {
      throw new IOException("Payload descriptor contains no files");
    }
    var files = new HashMap<String, FileDescriptorProto>();
    for (var file : descriptorSet.getFileList()) {
      if (file.getName().isEmpty() || files.putIfAbsent(file.getName(), file) != null) {
        throw new IOException("Payload descriptor file names must be present and unique");
      }
    }
    for (var file : descriptorSet.getFileList()) {
      if (!file.getSyntax().equals("proto3") || file.hasEdition()) {
        throw new IOException("Payload descriptor files must use proto3 without Editions");
      }
      if (!file.hasSourceCodeInfo() || file.getSourceCodeInfo().getLocationCount() == 0) {
        throw new IOException("Payload descriptor must include source information");
      }
      for (var dependency : file.getDependencyList()) {
        if (!files.containsKey(dependency)) {
          throw new IOException("Payload descriptor is not self-contained");
        }
      }
      rejectCustomOptions(file);
      for (var message : file.getMessageTypeList()) {
        validatePortableFieldNames(file.getName(), file.getPackage(), message);
      }
    }
    validateProgramRoots(pluginInterface, files);
    buildDescriptorGraph(files);
    return descriptorSet;
  }

  private static void validateProgramRoots(
      PluginInterface pluginInterface, Map<String, FileDescriptorProto> files) throws IOException {
    var sourceRoots = 0;
    var sinkRoots = 0;
    for (var file : files.values()) {
      for (var message : file.getMessageTypeList()) {
        switch (message.getName()) {
          case "SourceRecordPayload" -> sourceRoots++;
          case "SinkRecordPayload" -> sinkRoots++;
          default -> {}
        }
      }
    }
    var expectedSourceRoots = pluginInterface == PluginInterface.SINK ? 0 : 1;
    var expectedSinkRoots = pluginInterface == PluginInterface.SOURCE ? 0 : 1;
    if (sourceRoots != expectedSourceRoots || sinkRoots != expectedSinkRoots) {
      throw new IOException(
          "Payload descriptor for "
              + pluginInterface.manifestValue()
              + " must contain exactly "
              + expectedSourceRoots
              + " top-level SourceRecordPayload and "
              + expectedSinkRoots
              + " top-level SinkRecordPayload");
    }
  }

  private static void validateSchemaProfile(JsonNode schema, String path) throws IOException {
    if (!schema.isObject()) {
      return;
    }
    var fields = schema.properties().iterator();
    while (fields.hasNext()) {
      var field = fields.next();
      var keyword = field.getKey();
      var value = field.getValue();
      var keywordPath = path + "/" + keyword.replace("~", "~0").replace("/", "~1");
      if (keyword.equals("default")
          || keyword.equals("$vocabulary")
          || !SCHEMA_KEYWORDS.contains(keyword)) {
        throw new IOException("Config Schema keyword is outside the Tenon profile: " + keywordPath);
      }
      var reference = value.stringValueOpt().orElse(null);
      if ((keyword.equals("$ref") || keyword.equals("$dynamicRef"))
          && reference != null
          && !reference.isEmpty()
          && !reference.startsWith("#")) {
        throw new IOException("Config Schema reference must be internal: " + keywordPath);
      }
      visitChildSchemas(keyword, value, keywordPath);
    }
  }

  private static void visitChildSchemas(String keyword, JsonNode value, String path)
      throws IOException {
    if (Set.of("$defs", "properties", "patternProperties", "dependentSchemas").contains(keyword)) {
      if (value.isObject()) {
        var children = value.properties().iterator();
        while (children.hasNext()) {
          var child = children.next();
          validateSchemaProfile(child.getValue(), path + "/" + child.getKey());
        }
      }
      return;
    }
    if (Set.of("prefixItems", "allOf", "anyOf", "oneOf").contains(keyword)) {
      if (value.isArray()) {
        for (var index = 0; index < value.size(); index++) {
          validateSchemaProfile(value.get(index), path + "/" + index);
        }
      }
      return;
    }
    if (Set.of(
            "additionalProperties",
            "unevaluatedProperties",
            "propertyNames",
            "contains",
            "items",
            "unevaluatedItems",
            "not",
            "if",
            "then",
            "else",
            "contentSchema")
        .contains(keyword)) {
      validateSchemaProfile(value, path);
    }
  }

  private static void rejectCustomOptions(FileDescriptorProto file) throws IOException {
    if (containsCustomOptionData(file)
        || file.getExtensionList().stream().anyMatch(BundleContractValidator::extendsOption)
        || file.getMessageTypeList().stream().anyMatch(BundleContractValidator::definesOption)) {
      throw new IOException("Custom Protobuf options are unsupported: " + file.getName());
    }
  }

  private static boolean definesOption(DescriptorProto message) {
    return message.getExtensionList().stream().anyMatch(BundleContractValidator::extendsOption)
        || message.getNestedTypeList().stream().anyMatch(BundleContractValidator::definesOption);
  }

  private static boolean extendsOption(FieldDescriptorProto extension) {
    var extendee = extension.getExtendee();
    if (extendee.startsWith(".")) {
      extendee = extendee.substring(1);
    }
    return OPTION_MESSAGES.contains(extendee);
  }

  private static boolean hasCustomData(Message options) {
    return !options.getUnknownFields().asMap().isEmpty();
  }

  private static boolean containsCustomOptionData(FileDescriptorProto file) {
    return hasCustomData(file.getOptions())
        || file.getMessageTypeList().stream()
            .anyMatch(BundleContractValidator::containsCustomOptionData)
        || file.getEnumTypeList().stream()
            .anyMatch(BundleContractValidator::containsCustomOptionData)
        || file.getExtensionList().stream().anyMatch(field -> hasCustomData(field.getOptions()))
        || file.getServiceList().stream()
            .anyMatch(
                service ->
                    hasCustomData(service.getOptions())
                        || service.getMethodList().stream()
                            .anyMatch(method -> hasCustomData(method.getOptions())));
  }

  private static boolean containsCustomOptionData(DescriptorProto message) {
    return hasCustomData(message.getOptions())
        || message.getFieldList().stream().anyMatch(field -> hasCustomData(field.getOptions()))
        || message.getExtensionList().stream().anyMatch(field -> hasCustomData(field.getOptions()))
        || message.getOneofDeclList().stream().anyMatch(oneof -> hasCustomData(oneof.getOptions()))
        || message.getExtensionRangeList().stream()
            .anyMatch(range -> hasCustomData(range.getOptions()))
        || message.getEnumTypeList().stream()
            .anyMatch(BundleContractValidator::containsCustomOptionData)
        || message.getNestedTypeList().stream()
            .anyMatch(BundleContractValidator::containsCustomOptionData);
  }

  private static boolean containsCustomOptionData(EnumDescriptorProto enumeration) {
    return hasCustomData(enumeration.getOptions())
        || enumeration.getValueList().stream().anyMatch(value -> hasCustomData(value.getOptions()));
  }

  private static void validatePortableFieldNames(
      String fileName, String parent, DescriptorProto message) throws IOException {
    var name = parent.isEmpty() ? message.getName() : parent + "." + message.getName();
    Map<String, String> names = new HashMap<>();
    for (var field : message.getFieldList()) {
      var normalized = field.getName().replace("_", "").toLowerCase(Locale.ROOT);
      var prior = names.putIfAbsent(normalized, field.getName());
      if (prior != null) {
        throw new IOException(
            "Payload field names collide after normalization: "
                + fileName
                + ":"
                + name
                + "."
                + prior
                + " and "
                + field.getName());
      }
    }
    for (var nested : message.getNestedTypeList()) {
      validatePortableFieldNames(fileName, name, nested);
    }
  }

  private static void buildDescriptorGraph(Map<String, FileDescriptorProto> files)
      throws IOException {
    Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
    Set<String> building = new HashSet<>();
    for (var name : files.keySet()) {
      buildDescriptor(name, files, built, building);
    }
  }

  private static Descriptors.FileDescriptor buildDescriptor(
      String name,
      Map<String, FileDescriptorProto> files,
      Map<String, Descriptors.FileDescriptor> built,
      Set<String> building)
      throws IOException {
    var existing = built.get(name);
    if (existing != null) {
      return existing;
    }
    if (!building.add(name)) {
      throw new IOException("Payload descriptor dependency graph contains a cycle");
    }
    var file = files.get(name);
    var dependencies = new Descriptors.FileDescriptor[file.getDependencyCount()];
    for (var index = 0; index < dependencies.length; index++) {
      dependencies[index] = buildDescriptor(file.getDependency(index), files, built, building);
    }
    try {
      var descriptor = Descriptors.FileDescriptor.buildFrom(file, dependencies);
      built.put(name, descriptor);
      return descriptor;
    } catch (Descriptors.DescriptorValidationException error) {
      throw new IOException("Payload descriptor graph is invalid", error);
    } finally {
      building.remove(name);
    }
  }
}
