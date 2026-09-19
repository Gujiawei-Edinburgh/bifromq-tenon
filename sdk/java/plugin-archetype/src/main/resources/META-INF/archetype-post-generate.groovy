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

def projectDirectory = new File(request.outputDirectory, request.artifactId)
def wrapper = new File(projectDirectory, "mvnw")
def windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("windows")

if (!windows && !wrapper.setExecutable(true, false)) {
    throw new IllegalStateException("Failed to make the generated Maven Wrapper executable")
}

def pluginInterface = request.properties.getProperty("interface")
if (!(pluginInterface in ["source", "sink", "source-and-sink"])) {
    throw new IllegalArgumentException("Plugin interface must be source, sink, or source-and-sink")
}

def packagePath = request.properties.getProperty("package").replace(".", File.separator)
def mainJava = new File(projectDirectory, "src/main/java/${packagePath}")
def testJava = new File(projectDirectory, "src/test/java/${packagePath}")
def proto = new File(projectDirectory, "src/main/proto")
def services = new File(projectDirectory, "src/main/resources/META-INF/services")
def tenon = new File(projectDirectory, "src/main/tenon")

def keepOnly = { File directory, List<String> candidates, String selected ->
    candidates.each { name ->
        def file = new File(directory, name)
        if (name != selected && file.exists() && !file.delete()) {
            throw new IllegalStateException("Failed to remove unselected Plugin template ${file}")
        }
    }
}
def selectAndRename = { File directory, List<String> candidates, String selected, String target ->
    keepOnly(directory, candidates, selected)
    def source = new File(directory, selected)
    def destination = new File(directory, target)
    if (!source.renameTo(destination)) {
        throw new IllegalStateException("Failed to select Plugin template ${source}")
    }
}

def sourceEnabled = pluginInterface != "sink"
def sinkEnabled = pluginInterface != "source"
selectAndRename(
        mainJava,
        ["SourceMain.java", "SinkMain.java", "SourceAndSinkMain.java"],
        pluginInterface == "source" ? "SourceMain.java" :
                pluginInterface == "sink" ? "SinkMain.java" : "SourceAndSinkMain.java",
        "Main.java")
keepOnly(
        mainJava,
        ["SourcePluginFactory.java", "SinkPluginFactory.java", "SourceAndSinkPluginFactory.java"],
        pluginInterface == "source" ? "SourcePluginFactory.java" :
                pluginInterface == "sink" ? "SinkPluginFactory.java" : "SourceAndSinkPluginFactory.java")
keepOnly(
        testJava,
        ["SourcePluginFactoryTest.java", "SinkPluginFactoryTest.java", "SourceAndSinkPluginFactoryTest.java"],
        pluginInterface == "source" ? "SourcePluginFactoryTest.java" :
                pluginInterface == "sink" ? "SinkPluginFactoryTest.java" : "SourceAndSinkPluginFactoryTest.java")
if (!(sourceEnabled && sinkEnabled)) {
    def unselectedProto = new File(
            proto, sourceEnabled ? "sink_record_payload.proto" : "source_record_payload.proto")
    if (unselectedProto.exists() && !unselectedProto.delete()) {
        throw new IllegalStateException("Failed to remove unselected Plugin template ${unselectedProto}")
    }
}
keepOnly(
        services,
        [
                "org.apache.bifromq.tenon.sdk.TenonSourceFactory",
                "org.apache.bifromq.tenon.sdk.TenonSinkFactory",
                "org.apache.bifromq.tenon.sdk.TenonSourceAndSinkFactory"
        ],
        pluginInterface == "source" ?
                "org.apache.bifromq.tenon.sdk.TenonSourceFactory" :
                pluginInterface == "sink" ?
                        "org.apache.bifromq.tenon.sdk.TenonSinkFactory" :
                        "org.apache.bifromq.tenon.sdk.TenonSourceAndSinkFactory")
selectAndRename(
        tenon,
        ["source.config.schema.json", "sink.config.schema.json", "source-and-sink.config.schema.json"],
        "${pluginInterface}.config.schema.json",
        "config.schema.json")
