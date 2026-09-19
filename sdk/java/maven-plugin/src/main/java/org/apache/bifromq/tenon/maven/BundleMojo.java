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

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;

/** Builds one validated, self-contained Tenon Plugin Program bundle. */
@Mojo(
    name = "bundle",
    defaultPhase = LifecyclePhase.PACKAGE,
    requiresDependencyResolution = ResolutionScope.RUNTIME,
    threadSafe = true)
public final class BundleMojo extends AbstractMojo {
  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Inject private MavenProjectHelper projectHelper;

  @Parameter(alias = "interface", property = "tenon.interface", required = true)
  private String pluginInterface;

  @Parameter(property = "tenon.programName", required = true)
  private String programName;

  @Parameter(property = "tenon.displayName", required = true)
  private String displayName;

  @Parameter(property = "tenon.description", required = true)
  private String description;

  @Parameter(property = "tenon.mainClass", required = true)
  private String mainClass;

  @Parameter(
      property = "tenon.configSchema",
      defaultValue = "${project.basedir}/src/main/tenon/config.schema.json",
      required = true)
  private File configSchema;

  @Parameter(
      property = "tenon.payloadDescriptor",
      defaultValue = "${project.build.directory}/tenon-contract/payload.descriptor.pb",
      required = true)
  private File payloadDescriptor;

  @Parameter(
      property = "tenon.outputDirectory",
      defaultValue = "${project.build.directory}",
      required = true)
  private File outputDirectory;

  @Parameter(property = "tenon.license", defaultValue = "${project.basedir}/LICENSE")
  private File license;

  @Parameter(property = "tenon.notice", defaultValue = "${project.basedir}/NOTICE")
  private File notice;

  @Parameter private List<String> additionalJdkModules = List.of();

  @Parameter(property = "tenon.platforms")
  private List<String> platforms = List.of();

  @Parameter(defaultValue = "${settings.localRepository}", readonly = true, required = true)
  private File localRepository;

  @Override
  public void execute() throws MojoExecutionException {
    try {
      var selectedInterface = PluginInterface.parse(pluginInterface);
      var programJar = project.getArtifact().getFile();
      if (programJar == null) {
        throw new IllegalStateException("Project JAR has not been built");
      }
      var runtimeDependencies =
          project.getArtifacts().stream()
              .filter(BundleMojo::isRuntimeJar)
              .filter(artifact -> artifact.getFile() != null)
              .map(
                  artifact ->
                      new BundledDependency(coordinate(artifact), artifact.getFile().toPath()))
              .sorted(Comparator.comparing(BundledDependency::coordinate))
              .toList();
      var targetPlatforms = targetPlatforms();
      for (var targetPlatform : targetPlatforms) {
        var runtime =
            new TemurinRuntimeBuilder()
                .build(
                    targetPlatform,
                    outputDirectory
                        .toPath()
                        .resolve("tenon-temurin-runtime-" + targetPlatform.classifier()),
                    additionalJdkModules == null ? List.of() : additionalJdkModules,
                    localRepository.toPath());
        var request =
            new BundleRequest(
                selectedInterface,
                programName,
                project.getVersion(),
                displayName,
                description,
                mainClass,
                programJar.toPath(),
                runtimeDependencies,
                runtime,
                license.toPath(),
                notice.toPath(),
                configSchema.toPath(),
                payloadDescriptor.toPath(),
                outputDirectory.toPath(),
                project.getBuild().getFinalName());
        var archive = new BundleBuilder().build(request);
        projectHelper.attachArtifact(
            project, "tar.gz", "tenon-plugin-" + targetPlatform.classifier(), archive.toFile());
        getLog().info("Built Tenon Plugin bundle: " + archive);
      }
    } catch (Exception error) {
      throw new MojoExecutionException("Tenon Plugin bundle failed: " + error.getMessage(), error);
    }
  }

  private List<TargetPlatform> targetPlatforms() {
    return selectPlatforms(platforms);
  }

  static List<TargetPlatform> selectPlatforms(List<String> platforms) {
    if (platforms == null || platforms.isEmpty()) {
      return List.of(TargetPlatform.current());
    }
    var selected = new java.util.LinkedHashSet<TargetPlatform>();
    for (var classifier : platforms) {
      selected.add(TargetPlatform.parseClassifier(classifier));
    }
    if (selected.size() != platforms.size()) {
      throw new IllegalArgumentException("Duplicate Tenon Java Plugin target platform");
    }
    return List.copyOf(selected);
  }

  private static boolean isRuntimeJar(Artifact artifact) {
    return artifact.getType().equals("jar")
        && Set.of(Artifact.SCOPE_COMPILE, Artifact.SCOPE_RUNTIME).contains(artifact.getScope());
  }

  private static String coordinate(Artifact artifact) {
    var classifier = artifact.getClassifier();
    if (classifier == null || classifier.isBlank()) {
      return String.join(
          ":",
          artifact.getGroupId(),
          artifact.getArtifactId(),
          artifact.getType(),
          artifact.getVersion());
    }
    return String.join(
        ":",
        artifact.getGroupId(),
        artifact.getArtifactId(),
        artifact.getType(),
        classifier,
        artifact.getVersion());
  }
}
