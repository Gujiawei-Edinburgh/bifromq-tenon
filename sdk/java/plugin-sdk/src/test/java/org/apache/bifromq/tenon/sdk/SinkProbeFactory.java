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

import com.google.protobuf.StringValue;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import tools.jackson.databind.JsonNode;

/** Provides the real Sink used by the child-process lifecycle test. */
public final class SinkProbeFactory implements TenonSinkFactory<StringValue> {
  public SinkProbeFactory() {
    if (Boolean.getBoolean("tenon.test.provider.error")) {
      throw new AssertionError("Fatal provider construction failure");
    }
  }

  @Override
  public TenonSink<StringValue> create(JsonNode config) {
    var blockedMethod = config.get("blockedMethod");
    var errorPoint =
        config.has("errorPoint")
            ? ErrorPoint.valueOf(config.required("errorPoint").stringValue())
            : ErrorPoint.NONE;
    if (errorPoint == ErrorPoint.FACTORY) {
      ProbeEvents.append(ProbeEvents.path(config), "sink-factory-error");
      throw new AssertionError("Fatal Sink factory failure");
    }
    return new SinkProbe(
        ProbeEvents.path(config),
        blockedMethod == null
            ? BlockedMethod.NONE
            : BlockedMethod.valueOf(blockedMethod.stringValue()),
        config.has("failWrite"),
        config.has("nativeCompletion"),
        config.has("holdPayload") ? config.required("holdPayload").stringValue() : null,
        errorPoint,
        Thread.currentThread());
  }

  enum ErrorPoint {
    NONE,
    FACTORY,
    START,
    WRITE,
    WRITE_AFTER_STOP,
    ASYNC_WRITE_AFTER_STOP,
    CLOSE
  }

  enum BlockedMethod {
    NONE,
    START,
    CLOSE
  }

  private static final class SinkProbe implements TenonSink<StringValue> {
    private final Path events;
    private final BlockedMethod blockedMethod;
    private final boolean failWrite;
    private final boolean nativeCompletion;
    private final String holdPayload;
    private final ErrorPoint errorPoint;
    private final Thread lifecycleThread;

    private SinkProbe(
        Path events,
        BlockedMethod blockedMethod,
        boolean failWrite,
        boolean nativeCompletion,
        String holdPayload,
        ErrorPoint errorPoint,
        Thread lifecycleThread) {
      this.events = events;
      this.blockedMethod = blockedMethod;
      this.failWrite = failWrite;
      this.nativeCompletion = nativeCompletion;
      this.holdPayload = holdPayload;
      this.errorPoint = errorPoint;
      this.lifecycleThread = lifecycleThread;
    }

    @Override
    public void start() {
      ProbeEvents.append(events, "sink-start");
      failAt(ErrorPoint.START);
      try {
        blockIfSelected(BlockedMethod.START);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        sneakyThrow(error);
      }
    }

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<StringValue> records) {
      ProbeEvents.append(events, "sink-write-" + records.getFirst().getValue());
      if (errorPoint == ErrorPoint.WRITE_AFTER_STOP
          || errorPoint == ErrorPoint.ASYNC_WRITE_AFTER_STOP) {
        var threads = java.lang.management.ManagementFactory.getThreadMXBean();
        var deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (true) {
          var waitingOn = threads.getThreadInfo(lifecycleThread.threadId()).getLockInfo();
          if (waitingOn != null
              && waitingOn.getIdentityHashCode()
                  == System.identityHashCode(Thread.currentThread())) {
            break;
          }
          if (System.nanoTime() >= deadline) {
            throw new AssertionError("Lifecycle did not join the entered Sink write");
          }
          Thread.yield();
        }
        ProbeEvents.append(events, "sink-stop-observed");
        if (errorPoint == ErrorPoint.ASYNC_WRITE_AFTER_STOP) {
          var result = CompletableFuture.runAsync(() -> failAt(ErrorPoint.ASYNC_WRITE_AFTER_STOP));
          result.handle((ignored, failure) -> null).join();
          return result;
        }
        failAt(ErrorPoint.WRITE_AFTER_STOP);
      }
      failAt(ErrorPoint.WRITE);
      if (records.getFirst().getValue().equals(holdPayload)) {
        // The batch stays in flight without blocking the loop, so a test can keep one Queue's batch
        // unreleased while another Queue fails.
        return new CompletableFuture<>();
      }
      if (nativeCompletion) {
        var completion = new CompletableFuture<Void>();
        Thread.ofPlatform()
            .name("native-completion")
            .start(
                () -> {
                  try {
                    PluginLifecycleIntegrationTest.awaitEvent(events, "native-wait-entered");
                    if (failWrite) completion.completeExceptionally(writeFailure());
                    else completion.complete(null);
                  } catch (Exception error) {
                    throw new AssertionError(error);
                  }
                });
        return completion;
      }
      if (failWrite) return CompletableFuture.failedFuture(writeFailure());
      return CompletableFuture.completedFuture(null);
    }

    private static IOException writeFailure() {
      var failure =
          new IOException(
              "Expected Sink write failure", new IllegalStateException("Expected Sink root cause"));
      failure.addSuppressed(new IOException("Expected Sink suppressed failure"));
      return failure;
    }

    @Override
    public void close() {
      ProbeEvents.append(events, "sink-close");
      failAt(ErrorPoint.CLOSE);
      try {
        blockIfSelected(BlockedMethod.CLOSE);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        sneakyThrow(error);
      }
    }

    private void blockIfSelected(BlockedMethod method) throws InterruptedException {
      if (blockedMethod == method) {
        new CountDownLatch(1).await();
      }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable error) throws E {
      throw (E) error;
    }

    private void failAt(ErrorPoint operation) {
      if (errorPoint == operation) {
        throw new AssertionError("Fatal Sink callback failure during " + operation);
      }
    }
  }
}
