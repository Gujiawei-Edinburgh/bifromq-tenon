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
        artifactId: "generated-plugin-sink",
        pluginInterface: "sink",
        programName: "com.example.plugin.sink",
        packageName: "com.example.plugin.sink",
        factoryName: "com.example.plugin.sink.SinkPluginFactory",
        servicePath: "src/main/resources/META-INF/services/org.apache.bifromq.tenon.sdk.TenonSinkFactory",
        schemaRequired: ["outputFile"],
        presentPaths: [
                "src/main/java/com/example/plugin/sink/Main.java",
                "src/main/java/com/example/plugin/sink/SinkPluginFactory.java",
                "src/test/java/com/example/plugin/sink/SinkPluginFactoryTest.java",
                "src/main/proto/sink_record_payload.proto"
        ],
        absentPaths: [
                "src/main/java/com/example/plugin/sink/SourcePluginFactory.java",
                "src/main/java/com/example/plugin/sink/SourceAndSinkPluginFactory.java",
                "src/test/java/com/example/plugin/sink/SourcePluginFactoryTest.java",
                "src/test/java/com/example/plugin/sink/SourceAndSinkPluginFactoryTest.java",
                "src/main/proto/source_record_payload.proto"
        ])

return true
