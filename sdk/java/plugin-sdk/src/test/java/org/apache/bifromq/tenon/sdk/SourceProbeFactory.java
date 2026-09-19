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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.StringValue;
import java.io.IOException;
import java.nio.file.Path;
import tools.jackson.databind.JsonNode;

/** Provides the real Source used by the child-process lifecycle test. */
public final class SourceProbeFactory implements TenonSourceFactory<StringValue> {
  public SourceProbeFactory() {
    if (Boolean.getBoolean("tenon.test.provider.error")) {
      throw new AssertionError("Fatal provider construction failure");
    }
  }

  @Override
  public TenonSource create(JsonNode config, int parallelism, PayloadSender<StringValue> sender) {
    if (config.has("workerCount")) {
      assertEquals(2, parallelism);
      assertEquals(2, config.required("workerCount").intValue());
      ProbeEvents.append(ProbeEvents.path(config), "source-context-verified");
    } else {
      assertEquals(1, parallelism);
    }
    var failurePoint =
        config.has("failurePoint")
            ? FailurePoint.valueOf(config.required("failurePoint").stringValue())
            : FailurePoint.NONE;
    if (failurePoint == FailurePoint.ERROR_FACTORY) {
      ProbeEvents.append(ProbeEvents.path(config), "source-factory-error");
      throw new AssertionError("Fatal Source factory failure");
    }
    return new SourceProbe(ProbeEvents.path(config), sender, parallelism, failurePoint);
  }

  static final class SourceProbe implements TenonSource {
    private final Path events;
    private final PayloadSender<StringValue> sender;
    private final int parallelism;
    private final FailurePoint failurePoint;

    SourceProbe(
        Path events,
        PayloadSender<StringValue> sender,
        int parallelism,
        FailurePoint failurePoint) {
      this.events = events;
      this.sender = sender;
      this.parallelism = parallelism;
      this.failurePoint = failurePoint;
    }

    @Override
    public void start() {
      ProbeEvents.append(events, "source-start");
      if (failurePoint == FailurePoint.ERROR_THREAD
          || failurePoint == FailurePoint.EXCEPTION_THREAD) {
        try {
          Thread.ofPlatform()
              .name("source-probe-failure")
              .start(
                  () -> {
                    if (failurePoint == FailurePoint.ERROR_THREAD) {
                      throw new AssertionError("Fatal Source business thread failure");
                    }
                    throw new IllegalStateException("Ordinary business thread failure");
                  })
              .join();
        } catch (InterruptedException error) {
          Thread.currentThread().interrupt();
          sneakyThrow(error);
        }
      }
      for (var channel = 0; channel < parallelism; channel++) {
        var channelId = channel;
        if (parallelism > 1) {
          Thread.ofPlatform().name("source-probe-send-" + channelId).start(() -> submit(channelId));
        } else {
          submit(channelId);
        }
      }
      failAt(FailurePoint.START);
    }

    private void submit(int channel) {
      sender
          .send(channel, StringValue.of("telemetry"))
          .whenComplete(
              (ack, error) ->
                  ProbeEvents.append(
                      events, error == null ? "source-ack-" + ack : "source-result-failed"));
    }

    @Override
    public void quiesce() {
      // This one-shot producer already submitted its only send during start.
      ProbeEvents.append(events, "source-quiesce");
      var rejected =
          org.junit.jupiter.api.Assertions.assertThrows(
              java.util.concurrent.CompletionException.class,
              () ->
                  sender
                      .send(0, StringValue.of("after-admission-close"))
                      .toCompletableFuture()
                      .join());
      org.junit.jupiter.api.Assertions.assertInstanceOf(
          SourceSessionClosedException.class, rejected.getCause());
      ProbeEvents.append(events, "source-admission-closed");
      failAt(FailurePoint.QUIESCE);
    }

    @Override
    public void close() {
      ProbeEvents.append(events, "source-close");
      failAt(FailurePoint.CLOSE);
    }

    private void failAt(FailurePoint operation) {
      if (failurePoint.name().equals("ERROR_" + operation.name())) {
        throw new AssertionError("Fatal Source callback failure during " + operation);
      }
      if (failurePoint == FailurePoint.START_AND_CLOSE) {
        if (operation == FailurePoint.START) {
          sneakyThrow(
              new IOException(
                  "Expected Source startup failure",
                  new IllegalStateException("Expected Source root cause")));
        }
        if (operation == FailurePoint.CLOSE) {
          sneakyThrow(new IOException("Expected Source cleanup failure"));
        }
      }
      if (failurePoint == operation) {
        sneakyThrow(new IOException("Source probe failed during " + operation));
      }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable error) throws E {
      throw (E) error;
    }
  }

  enum FailurePoint {
    NONE,
    START,
    QUIESCE,
    CLOSE,
    START_AND_CLOSE,
    EXCEPTION_THREAD,
    ERROR_FACTORY,
    ERROR_START,
    ERROR_THREAD,
    ERROR_QUIESCE,
    ERROR_CLOSE
  }
}
