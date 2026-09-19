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

import static org.apache.bifromq.tenon.sdk.PluginLifecycleIntegrationTest.awaitEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.StringValue;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PipelineToPlugin;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.QuiesceSource;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Shutdown;
import org.apache.bifromq.tenon.contracts.sink.EgressRecordOuterClass.EgressRecord;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

final class NativeWaitFailureTest {
  /** The event one Source probe's parent records once the quiesce boundary has been observed. */
  private static final String CLOSE_PHASE_EVENT = "quiesce-observed";

  @TempDir Path directory;

  @Test
  void sourceWakeFailureExitsBeforeBusinessClose() throws Exception {
    assertTrue(runProbe("source-wake").contains("Injected native wake failure"));
  }

  @Test
  void sourceWaitFailureDuringShutdownFailsBeforeBusinessClose() throws Exception {
    assertTrue(runProbe("source-wait").contains("Injected native wait failure"));
  }

  @Test
  void sinkBusinessFailureRetainsItsCauseWhenCompletionWakeFails() throws Exception {
    assertTrue(runProbe("sink-wake").contains("Expected Sink write failure"));
  }

  @Test
  void sinkStopWakeFailureExitsWithoutJoiningOrBusinessClose() throws Exception {
    assertTrue(runProbe("sink-stop-wake").contains("Injected native wake failure"));
  }

  @Test
  void sinkCompletionWakeFailureExitsBeforeBusinessClose() throws Exception {
    assertTrue(runProbe("sink-completion-wake").contains("Injected native wake failure"));
  }

  @Test
  void sinkWaitFailureDuringShutdownExitsWithoutBusinessClose() throws Exception {
    assertTrue(runProbe("sink-wait").contains("Injected native wait failure"));
  }

  private String runProbe(String scenario) throws Exception {
    var source = scenario.startsWith("source-");
    var events = directory.resolve("events");
    var output = directory.resolve("process-output");
    var channels =
        scenario.equals("sink-wait")
            ? List.of(
                SideFixtures.flowChannel(directory, "main", 0),
                SideFixtures.flowChannel(directory, "main", 1))
            : List.of(SideFixtures.flowChannel(directory, "main", 0));
    SideFixtures.SourceBells sourceBells = null;
    SideFixtures.SinkBells sinkBells = null;
    if (source) {
      var queues = Files.createDirectory(directory.resolve("source"));
      IpcQueue.create(
          queues.resolve("submission-0.queue"),
          IngressQueueLayout.submissionCapacity(1, 1024),
          1024);
      IpcQueue.create(
          queues.resolve("completion-0.queue"),
          IngressQueueLayout.completionCapacity(1),
          IngressQueueLayout.COMPLETION_MAX_PAYLOAD_SIZE);
      sourceBells = SideFixtures.SourceBells.create(queues, 1);
    } else {
      sinkBells = SideFixtures.SinkBells.create(directory, channels);
      for (var channel : channels) {
        var path = EgressQueueLayout.queue(directory, channel);
        Files.createDirectories(path.getParent());
        IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(4096), 1024);
      }
    }
    try (var server =
        PluginLifecycleIntegrationTest.LifecycleServer.start(directory.resolve("native.sock"))) {
      var command =
          new java.util.ArrayList<>(
              List.of(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "--enable-native-access=ALL-UNNAMED",
                  "-Dtenon.test.native.events=" + events,
                  "-cp",
                  System.getProperty("java.class.path"),
                  Probe.class.getName(),
                  scenario,
                  "--sdk-config",
                  SideFixtures.sdkConfig(
                      directory,
                      server.socket(),
                      new byte[16],
                      source ? sourceBells.channelsPath() : null,
                      source ? List.of() : channels)));
      var process =
          new ProcessBuilder(command)
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      try {
        var values = new java.util.HashMap<String, Object>(Map.of("eventsFile", events.toString()));
        if (scenario.equals("sink-wake")) values.put("failWrite", true);
        if (scenario.equals("sink-wake") || scenario.equals("sink-completion-wake"))
          values.put("nativeCompletion", true);
        var json = new JsonMapper();
        var input = json.writeValueAsString(values) + "\n";
        process.getOutputStream().write(input.getBytes(StandardCharsets.UTF_8));
        process.getOutputStream().flush();
        assertTrue(server.awaitMessage().hasAttach());
        assertTrue(server.awaitMessage().hasReady());
        if (scenario.equals("sink-wake") || scenario.equals("sink-completion-wake")) {
          try (var writer =
              sinkBells.write(
                  EgressQueueLayout.queue(directory, channels.getFirst()), channels.getFirst())) {
            assertTrue(
                writer.tryWrite(
                        EgressRecord.newBuilder()
                            .setPayload(StringValue.of("held").toByteString())
                            .build()
                            .toByteArray())
                    instanceof IpcQueue.Committed);
          }
        }
        if (source) {
          server.send(
              PipelineToPlugin.newBuilder()
                  .setQuiesceSource(QuiesceSource.getDefaultInstance())
                  .build());
          assertTrue(server.awaitMessage().hasSourceQuiesced());
          // The Source rings its own Submission doorbell to reach this boundary, so the probe
          // injects only the wakes of the close phase that begins here.
          ProbeEvents.append(events, CLOSE_PHASE_EVENT);
        }
        if (source || scenario.equals("sink-stop-wake") || scenario.equals("sink-wait")) {
          awaitEvent(events, "native-wait-entered");
          server.send(
              PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        }
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Failure probe did not exit");
        var text = Files.readString(output);
        assertEquals(1, process.exitValue(), text);
        var recorded = Files.readAllLines(events);
        if (scenario.equals("sink-wake"))
          assertTrue(recorded.contains("native-wake-failed"), recorded.toString());
        assertFalse(recorded.contains("source-close"), recorded.toString());
        assertFalse(recorded.contains("sink-close"), recorded.toString());
        return text;
      } finally {
        process.destroyForcibly();
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Failure probe was not reaped");
      }
    }
  }

  /** Injects native failures in a real Program, leaving process exit to the SDK. */
  public static final class Probe {
    public static void main(String[] arguments) throws Exception {
      var scenario = arguments[0];
      var wake = new CountDownLatch(1);
      var events = Path.of(System.getProperty("tenon.test.native.events"));
      interceptNativeCalls(
          operation -> {
            var thread = Thread.currentThread().getName();
            if (operation == NativeOperation.WAIT
                && (thread.equals("tenon-source-completion")
                    || thread.equals("tenon-sink-egress"))) {
              if (!(scenario.equals("sink-wake") || scenario.equals("sink-completion-wake"))
                  || Files.readString(events).contains("sink-write-held")) {
                ProbeEvents.append(events, "native-wait-entered");
              }
              if (scenario.endsWith("-wait")) {
                assertTrue(wake.await(5, TimeUnit.SECONDS), "Shutdown did not wake the Queue");
                if (closePhase(scenario, events)) {
                  throw new IOException("Injected native wait failure");
                }
              }
            }
            if (operation == NativeOperation.WAKE) {
              if (scenario.endsWith("-wait") && thread.equals("main")) {
                if (closePhase(scenario, events)) {
                  wake.countDown();
                }
              } else if (closePhase(scenario, events)
                  && ((scenario.equals("source-wake") || scenario.equals("sink-stop-wake"))
                          && thread.equals("main")
                      || (scenario.equals("sink-wake") || scenario.equals("sink-completion-wake"))
                          && thread.equals("native-completion"))) {
                ProbeEvents.append(events, "native-wake-failed");
                throw new IOException("Injected native wake failure");
              }
            }
          });
      var reserved = Arrays.copyOfRange(arguments, 1, arguments.length);
      if (scenario.startsWith("source-")) {
        SourceProgram.<StringValue>run(reserved).awaitShutdown();
      } else {
        SinkProgram.run(reserved, StringValue.parser()).awaitShutdown();
      }
    }
  }

  @FunctionalInterface
  private interface NativeCall {
    void before(NativeOperation operation) throws Exception;
  }

  private enum NativeOperation {
    WAIT,
    WAKE
  }

  /**
   * Reports whether a probe may inject the failure its scenario describes.
   *
   * <p>A Source reaches its quiesce boundary by ringing the doorbell of its own Submission loop, so
   * a wake that happens before this probe's parent observed that boundary belongs to normal
   * progress rather than to the close phase the scenario fails. Every other scenario has no such
   * boundary and injects from its first wake.
   */
  private static boolean closePhase(String scenario, Path events) throws IOException {
    if (!scenario.startsWith("source-")) {
      return true;
    }
    return Files.exists(events) && Files.readString(events).contains(CLOSE_PHASE_EVENT);
  }

  /** Replaces only the native-call handles, before any worker or Queue is created. */
  private static void interceptNativeCalls(NativeCall call) throws ReflectiveOperationException {
    var waiter = Class.forName("org.apache.bifromq.tenon.sdk.ipc.PlatformWaiter");
    var backendField = waiter.getDeclaredField("BACKEND");
    backendField.setAccessible(true);
    var backend = backendField.get(null);
    for (var field : backend.getClass().getDeclaredFields()) {
      if (field.getType() != MethodHandle.class) {
        continue;
      }
      field.setAccessible(true);
      var original = (MethodHandle) field.get(backend);
      var intercepted =
          MethodHandles.lookup()
              .findStatic(
                  NativeWaitFailureTest.class,
                  "invokeNative",
                  MethodType.methodType(
                      Object.class,
                      MethodHandle.class,
                      String.class,
                      NativeCall.class,
                      Object[].class))
              .bindTo(original)
              .bindTo(field.getName())
              .bindTo(call)
              .asCollector(Object[].class, original.type().parameterCount())
              .asType(original.type());
      field.set(backend, intercepted);
    }
  }

  private static Object invokeNative(
      MethodHandle original, String name, NativeCall call, Object[] arguments) throws Throwable {
    var operation =
        name.equals("syscall")
            ? ((int) arguments[3] == 1 ? NativeOperation.WAKE : NativeOperation.WAIT)
            : (name.equals("wake") ? NativeOperation.WAKE : NativeOperation.WAIT);
    call.before(operation);
    return original.invokeWithArguments(arguments);
  }
}
