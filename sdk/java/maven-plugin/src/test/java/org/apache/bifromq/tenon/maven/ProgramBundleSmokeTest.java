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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

final class ProgramBundleSmokeTest {
  @TempDir Path directory;

  @Test
  void extractedTargetBundleRunsWithoutSystemJava() throws Exception {
    var inputs = BundleBuilderTest.request(directory, PluginInterface.SOURCE_AND_SINK, "1.0.0");
    Files.write(
        inputs.payloadDescriptor(),
        ProgramBundleBuilderTest.descriptor(PluginInterface.SOURCE_AND_SINK).toByteArray());
    var classPath = BundleSmokeMain.class.getName().replace('.', '/') + ".class";
    try (var classBytes = getClass().getResourceAsStream("/" + classPath);
        var jar = new JarOutputStream(Files.newOutputStream(inputs.programJar()))) {
      assertNotNull(classBytes, "Bundle smoke main class is missing");
      jar.putNextEntry(new JarEntry(classPath));
      classBytes.transferTo(jar);
      jar.closeEntry();
    }
    var runtime = new TemurinRuntimeBuilder().build(directory.resolve("temurin"), List.of());
    var request =
        new BundleRequest(
            inputs.pluginInterface(),
            inputs.programName(),
            inputs.exactVersion(),
            "Example Plugin",
            "Read and write example records.",
            BundleSmokeMain.class.getName(),
            inputs.programJar(),
            List.of(),
            runtime,
            inputs.license(),
            inputs.notice(),
            inputs.configSchema(),
            inputs.payloadDescriptor(),
            inputs.outputDirectory(),
            inputs.finalName());
    var archive = new BundleBuilder().build(request);
    var extracted = Files.createDirectory(directory.resolve("extracted"));
    run(new ProcessBuilder("tar", "-xzf", archive.toString(), "-C", extracted.toString()));
    var manifest =
        new ObjectMapper().readTree(Files.readAllBytes(extracted.resolve("manifest.json")));
    var command = new ArrayList<String>();
    for (var argument : manifest.get("command")) {
      command.add(argument.stringValue());
    }
    command.set(0, extracted.resolve(command.getFirst()).toString());
    var launch = new ProcessBuilder(command).directory(extracted.toFile());
    launch.environment().remove("JAVA_HOME");
    launch.environment().put("PATH", directory.resolve("no-system-java").toString());

    assertEquals("bundled runtime started\n", run(launch));
  }

  private static String run(ProcessBuilder command) throws Exception {
    var process = command.redirectErrorStream(true).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Bundle smoke process did not exit");
      var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(0, process.exitValue(), output);
      return output;
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly().waitFor();
      }
    }
  }
}
