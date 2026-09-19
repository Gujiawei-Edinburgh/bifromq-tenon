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
import com.google.protobuf.Parser;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import tools.jackson.databind.JsonNode;

/** Runs one Java Sink implementation through the unified Plugin lifecycle and Sink capability. */
public final class SinkProgram {
  private final PluginProgramRuntime runtime;
  private final SinkProgramOwner<?> owner;
  private final TenonSink<?> sink;

  private SinkProgram(PluginProgramRuntime runtime, SinkProgramOwner<?> owner, TenonSink<?> sink) {
    this.runtime = runtime;
    this.owner = owner;
    this.sink = sink;
  }

  /**
   * Discovers one factory and owns the fixed process, Queue, and shutdown protocol until exit.
   *
   * @param arguments the exact arguments received by the Plugin entry point
   * @param payloadParser the parser generated for this Plugin's SinkRecordPayload
   */
  public static <P extends MessageLite> SinkProgram run(
      String[] arguments, Parser<P> payloadParser) {
    try {
      return run0(arguments, payloadParser);
    } catch (Throwable error) {
      return PluginProgramRuntime.terminateProcess(error);
    }
  }

  private static <P extends MessageLite> SinkProgram run0(
      String[] arguments, Parser<P> payloadParser) throws Exception {
    Objects.requireNonNull(payloadParser, "payloadParser");
    var startup = PluginProgramRuntime.startSink(arguments);
    var runtime = startup.runtime();
    var factory = SinkProgram.<P>loadFactory();
    TenonSink<P> sink =
        Objects.requireNonNull(factory.create(runtime.config()), "TenonSinkFactory returned null");
    var owner =
        SinkProgramOwner.open(sink, runtime.workingDirectory(), startup.channels(), payloadParser);
    owner
        .failure()
        .whenComplete(
            (ignored, error) -> {
              if (error != null) PluginProgramRuntime.terminateProcess(error);
            });
    sink.start();
    owner.startPaused();
    return new SinkProgram(runtime, owner, sink);
  }

  /** Returns the validated process configuration. */
  public JsonNode config() {
    return runtime.config().deepCopy();
  }

  /** Publishes Ready, processes Sink input, and completes planned shutdown. */
  public void awaitShutdown() {
    try {
      runtime.publishReady();
      owner.activate();
      runtime.awaitShutdown(owner.failure());
      owner.shutdown();
      sink.close();
      runtime.completeShutdown();
    } catch (Throwable error) {
      PluginProgramRuntime.terminateProcess(error);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static <P extends MessageLite> TenonSinkFactory<P> loadFactory() {
    List<TenonSinkFactory<?>> factories = new ArrayList<>();
    for (var provider : ServiceLoader.load(TenonSinkFactory.class).stream().toList()) {
      factories.add(provider.get());
    }
    return (TenonSinkFactory<P>) requireSingleFactory(factories);
  }

  static TenonSinkFactory<?> requireSingleFactory(List<? extends TenonSinkFactory<?>> factories) {
    if (factories.size() != 1) {
      throw new IllegalStateException("Exactly one TenonSinkFactory must be registered");
    }
    return factories.getFirst();
  }
}
