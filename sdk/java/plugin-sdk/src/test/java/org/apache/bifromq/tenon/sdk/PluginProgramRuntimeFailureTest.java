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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PluginProgramRuntimeFailureTest {
  @TempDir Path directory;

  @Test
  void concurrentFailureCannotHaltBeforeTheFirstDiagnosticIsFlushed() throws Exception {
    var output = directory.resolve("error");
    var process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                Probe.class.getName())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(process.waitFor(5, TimeUnit.SECONDS));
      assertEquals(1, process.exitValue());
      var diagnostic = Files.readString(output);
      assertTrue(diagnostic.contains("First diagnostic started"), diagnostic);
      assertTrue(diagnostic.contains("First diagnostic complete"), diagnostic);
      assertFalse(diagnostic.contains("Second failure"), diagnostic);
    } finally {
      process.destroyForcibly();
      assertTrue(process.waitFor(5, TimeUnit.SECONDS));
    }
  }

  public static final class Probe {
    public static void main(String[] args) {
      var printing = new CountDownLatch(1);
      var second =
          new Thread(
              () -> {
                try {
                  printing.await();
                } catch (InterruptedException error) {
                  throw new AssertionError(error);
                }
                PluginProgramRuntime.terminateProcess(new IllegalStateException("Second failure"));
              });
      second.start();
      PluginProgramRuntime.terminateProcess(
          new IllegalStateException("First failure") {
            @Override
            public void printStackTrace(PrintStream output) {
              output.println("First diagnostic started");
              printing.countDown();
              var threads = ManagementFactory.getThreadMXBean();
              var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
              while (true) {
                var waiting = threads.getThreadInfo(second.threadId());
                if (waiting != null
                    && waiting.getLockOwnerId() == Thread.currentThread().threadId()) break;
                if (System.nanoTime() >= deadline)
                  throw new AssertionError("Second reporter did not wait");
                Thread.yield();
              }
              super.printStackTrace(output);
              output.println("First diagnostic complete");
            }
          });
    }
  }
}
