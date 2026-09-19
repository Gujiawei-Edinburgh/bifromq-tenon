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

package com.example.plugin.source;

import com.example.plugin.source.payload.SourceRecordPayload;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.bifromq.tenon.sdk.TenonSource;
import org.apache.bifromq.tenon.sdk.TenonSourceFactory;
import tools.jackson.databind.JsonNode;

/** Sends one record beyond the configured admission capacity. */
public final class SourcePluginFactory implements TenonSourceFactory<SourceRecordPayload> {
  @Override
  public TenonSource create(
      JsonNode config, int parallelism, PayloadSender<SourceRecordPayload> sender) {
    return new CapacitySource(config, parallelism, sender);
  }

  private record SourceConfig(String message, int queueIndex, Path resultFile) {
    private static SourceConfig parse(JsonNode config, int parallelism) {
      var queueIndex = config.required("queueIndex").intValue();
      if (queueIndex >= parallelism) {
        throw new IllegalArgumentException("queueIndex must be less than Flow parallelism");
      }
      return new SourceConfig(
          config.required("message").stringValue(),
          queueIndex,
          Path.of(config.required("resultFile").stringValue()));
    }
  }

  private static final class CapacitySource implements TenonSource {
    private final SourceConfig config;
    private final PayloadSender<SourceRecordPayload> sender;

    private CapacitySource(
        JsonNode config, int parallelism, PayloadSender<SourceRecordPayload> sender) {
      this.config = SourceConfig.parse(config, parallelism);
      this.sender = sender;
    }

    @Override
    public void start() {
      var payload = SourceRecordPayload.newBuilder().setMessage(config.message()).build();
      sender.send(config.queueIndex(), payload).thenAccept(this::recordAcknowledgement);
      sender.send(config.queueIndex(), payload).thenAccept(this::recordAcknowledgement);
    }

    @Override
    public void quiesce() {
      // Both one-shot sends have entered admission before startup returns.
    }

    @Override
    public void close() {}

    private void recordAcknowledgement(AckCode acknowledgement) {
      var result =
          switch (acknowledgement) {
            case OK -> "ok";
            case RETRY -> "retry";
            case BACKPRESSURE -> "backpressure";
            case ERROR -> "error";
          };
      try {
        Files.writeString(
            config.resultFile(),
            result + '\n',
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND);
      } catch (IOException error) {
        throw new UncheckedIOException("Failed to record the generated Source result", error);
      }
      if (acknowledgement == AckCode.OK) {
        System.err.println("Generated Source payload acknowledged");
      }
    }
  }
}
