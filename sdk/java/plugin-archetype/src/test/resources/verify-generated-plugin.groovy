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

import groovy.json.JsonSlurper
import groovy.xml.XmlSlurper
import java.nio.file.Files

return { Map settings ->
    def project = new File(settings.basedir, "project/${settings.artifactId}")
    def os = System.getProperty("os.name").toLowerCase(Locale.ROOT)
    def architecture = System.getProperty("os.arch").toLowerCase(Locale.ROOT)
    def osClassifier = os == "linux" ? "linux" : os == "mac os x" || os == "macos" ? "macos" : null
    def architectureClassifier = architecture == "amd64" || architecture == "x86_64" ? "amd64" :
            architecture == "aarch64" || architecture == "arm64" ? "arm64" : null
    assert osClassifier != null
    assert architectureClassifier != null
    def platform = "${osClassifier}-${architectureClassifier}"
    def bundle = new File(
            project,
            "target/${settings.artifactId}-1.0.0-tenon-plugin-${platform}.tar.gz")
    def wrapper = new File(project, "mvnw")
    def pomFile = new File(project, "pom.xml")
    def schema = new File(project, "src/main/tenon/config.schema.json")

    assert !new File(project, "LICENSE").exists()
    assert !new File(project, "NOTICE").exists()
    assert bundle.isFile()
    assert wrapper.isFile()
    assert wrapper.canExecute()
    settings.presentPaths.each { relative -> assert new File(project, relative).isFile() }
    settings.absentPaths.each { relative -> assert !new File(project, relative).exists() }

    def pom = new XmlSlurper(false, false).parse(pomFile)
    def directTenonDependencies = pom.dependencies.dependency.findAll {
        it.groupId.text() == "org.apache.bifromq.tenon"
    }.collect { it.artifactId.text() }
    assert directTenonDependencies == ["tenon-plugin-sdk"]
    assert pomFile.text.contains("<interface>${settings.pluginInterface}</interface>")
    assert !pomFile.text.contains("<kind>")

    def service = new File(project, settings.servicePath)
    assert service.text.trim() == settings.factoryName
    def schemaJson = new JsonSlurper().parse(schema)
    assert schemaJson.required == settings.schemaRequired
    assert !schema.text.contains('"default"')
    def readme = new File(project, "README.md")
    assert readme.text.contains(settings.factoryName.split('\\.').last() + ".java")
    assert !readme.text.find(/[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]/)
    Files.walk(new File(project, "src").toPath()).withCloseable { paths ->
        paths.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".java") }
                .forEach { source ->
                    assert !source.toFile().text.contains("org.apache.bifromq.tenon.sdk.source")
                    assert !source.toFile().text.contains("org.apache.bifromq.tenon.sdk.sink")
                    assert !source.toFile().text.contains("org.apache.bifromq.tenon.sdk.program")
                }
    }

    def wrapperCheck = new ProcessBuilder(wrapper.absolutePath, "--version")
            .directory(project)
            .redirectErrorStream(true)
            .start()
    def wrapperOutput = wrapperCheck.inputStream.text
    assert wrapperCheck.waitFor() == 0 : wrapperOutput
    assert wrapperOutput.contains("Java version: 25.0.3")

    def extracted = new File(project, "target/verified-tenon-plugin")
    extracted.deleteDir()
    extracted.mkdirs()
    def extraction = new ProcessBuilder("tar", "-xzf", bundle.absolutePath, "-C", extracted.absolutePath)
            .redirectErrorStream(true)
            .start()
    def extractionOutput = extraction.inputStream.text
    assert extraction.waitFor() == 0 : extractionOutput
    assert !new File(extracted, "LICENSE").exists()
    assert !new File(extracted, "NOTICE").exists()
    assert new File(extracted, "runtime/NOTICE").isFile()
    assert new File(extracted, "runtime/legal").isDirectory()
    assert new File(extracted, "runtime/bin/java").isFile()
    assert new File(extracted, "config.schema.json").isFile()
    assert new File(extracted, "payload.descriptor.pb").isFile()
    assert new File(extracted, "program.jar").isFile()
    def manifest = new JsonSlurper().parse(new File(extracted, "manifest.json"))
    assert manifest.keySet() == ["programName", "exactVersion", "displayName", "description", "interface", "platforms", "command"] as Set
    assert manifest.programName == settings.programName
    assert manifest.exactVersion == "1.0.0"
    assert manifest.interface == settings.pluginInterface
    assert manifest.platforms.size() == 1
    assert manifest.platforms.first().keySet() == ["os", "architecture"] as Set
    assert manifest.command.first() == "runtime/bin/java"
    assert manifest.command.last() == "${settings.packageName}.Main"
    Files.walk(extracted.toPath()).withCloseable { paths ->
        assert paths.noneMatch { Files.isSymbolicLink(it) }
    }
    def runtimeCheck = new ProcessBuilder(new File(extracted, "runtime/bin/java").absolutePath, "--version")
            .redirectErrorStream(true)
            .start()
    def runtimeOutput = runtimeCheck.inputStream.text
    assert runtimeCheck.waitFor() == 0 : runtimeOutput
    assert runtimeOutput.contains("25.0.3")
}
