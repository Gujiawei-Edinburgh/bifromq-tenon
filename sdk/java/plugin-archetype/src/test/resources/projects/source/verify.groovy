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

def verifyGeneratedPlugin = evaluate(new File(basedir, "../../verify-generated-plugin.groovy"))

verifyGeneratedPlugin(
        basedir: basedir,
        artifactId: "generated-plugin-source",
        pluginInterface: "source",
        programName: "com.example.plugin.source",
        packageName: "com.example.plugin.source",
        factoryName: "com.example.plugin.source.SourcePluginFactory",
        servicePath: "src/main/resources/META-INF/services/org.apache.bifromq.tenon.sdk.TenonSourceFactory",
        schemaRequired: ["message", "queueIndex"],
        presentPaths: [
                "src/main/java/com/example/plugin/source/Main.java",
                "src/main/java/com/example/plugin/source/SourcePluginFactory.java",
                "src/test/java/com/example/plugin/source/SourcePluginFactoryTest.java",
                "src/main/proto/source_record_payload.proto"
        ],
        absentPaths: [
                "src/main/java/com/example/plugin/source/SinkPluginFactory.java",
                "src/main/java/com/example/plugin/source/SourceAndSinkPluginFactory.java",
                "src/test/java/com/example/plugin/source/SinkPluginFactoryTest.java",
                "src/test/java/com/example/plugin/source/SourceAndSinkPluginFactoryTest.java",
                "src/main/proto/sink_record_payload.proto"
        ])

return true
