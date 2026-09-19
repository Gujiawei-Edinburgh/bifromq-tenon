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

package org.apache.bifromq.tenon.sdk;

import com.google.protobuf.MessageLite;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import tools.jackson.databind.JsonNode;

/** Owns the process, control, Queue, and business lifecycle of one Source-only Program. */
public final class SourceProgram<P extends MessageLite> {
  private final SourceProgramOwner<P> sourceOwner;
  private final PluginProgramRuntime runtime;

  private SourceProgram(SourceProgramOwner<P> sourceOwner, PluginProgramRuntime runtime) {
    this.sourceOwner = Objects.requireNonNull(sourceOwner, "sourceOwner");
    this.runtime = Objects.requireNonNull(runtime, "runtime");
  }

  /**
   * Attaches control, discovers one factory, and opens every Source Queue without publishing Ready.
   *
   * @param arguments the exact reserved arguments received by the Plugin entry point
   * @return the Program whose Source lifecycle is owned by the SDK
   */
  public static <P extends MessageLite> SourceProgram<P> run(String[] arguments) {
    try {
      return run0(arguments);
    } catch (Throwable error) {
      return PluginProgramRuntime.terminateProcess(error);
    }
  }

  private static <P extends MessageLite> SourceProgram<P> run0(String[] arguments)
      throws Exception {
    var startup = PluginProgramRuntime.startSource(arguments);
    var runtime = startup.runtime();
    var factory = SourceProgram.<P>loadFactory();
    var session =
        SourceSession.<P>open(
            runtime.workingDirectory().resolve("source"), startup.channelBellPath());
    var sender = session.sender();
    TenonSource source =
        Objects.requireNonNull(
            factory.create(runtime.config(), session.parallelism(), sender),
            "TenonSourceFactory returned null");
    var sourceOwner = new SourceProgramOwner<>(session, source);
    session
        .failure()
        .whenComplete(
            (ignored, error) -> {
              if (error != null) PluginProgramRuntime.terminateProcess(error);
            });
    sourceOwner.start();
    return new SourceProgram<>(sourceOwner, runtime);
  }

  /** Returns an independent copy of the validated process configuration. */
  public JsonNode config() {
    return runtime.config().deepCopy();
  }

  /** Publishes Ready, performs Source quiesce, and exits only after final Shutdown cleanup. */
  public void awaitShutdown() {
    try {
      runtime.publishReady();
      runtime.awaitSourceQuiesce(sourceOwner.failure());
      sourceOwner.quiesce();
      runtime.publishSourceQuiesced();
      runtime.awaitShutdown(sourceOwner.failure());
      sourceOwner.shutdown();
      runtime.completeShutdown();
    } catch (Throwable error) {
      PluginProgramRuntime.terminateProcess(error);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static <P extends MessageLite> TenonSourceFactory<P> loadFactory() {
    List<TenonSourceFactory<?>> factories = new ArrayList<>();
    for (var provider : ServiceLoader.load(TenonSourceFactory.class).stream().toList()) {
      factories.add(provider.get());
    }
    return (TenonSourceFactory<P>) requireSingleFactory(factories);
  }

  static TenonSourceFactory<?> requireSingleFactory(
      List<? extends TenonSourceFactory<?>> factories) {
    if (factories.size() != 1) {
      throw new IllegalStateException("Exactly one TenonSourceFactory must be registered");
    }
    return factories.getFirst();
  }
}
