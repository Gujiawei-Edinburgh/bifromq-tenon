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

import java.nio.file.Path;
import java.util.List;

record BundleRequest(
    PluginInterface pluginInterface,
    String programName,
    String exactVersion,
    String displayName,
    String description,
    String mainClass,
    Path programJar,
    List<BundledDependency> runtimeDependencies,
    TemurinRuntime runtime,
    Path license,
    Path notice,
    Path configSchema,
    Path payloadDescriptor,
    Path outputDirectory,
    String finalName) {
  BundleRequest {
    runtimeDependencies = List.copyOf(runtimeDependencies);
  }
}
