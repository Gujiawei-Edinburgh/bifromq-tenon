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

package com.example.plugin.sink;

import com.example.plugin.sink.payload.SinkRecordPayload;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.TenonSink;
import org.apache.bifromq.tenon.sdk.TenonSinkFactory;
import tools.jackson.databind.JsonNode;

/** Exercises replay after one accepted batch fails after producing an external side effect. */
public final class SinkPluginFactory implements TenonSinkFactory<SinkRecordPayload> {
  @Override
  public TenonSink<SinkRecordPayload> create(JsonNode config) throws IOException {
    var outputFile = Path.of(config.required("outputFile").stringValue());
    return new ReplaySink(outputFile);
  }

  private static final class ReplaySink implements TenonSink<SinkRecordPayload> {
    private final Path outputFile;
    private final Path closeLog;
    private final Path failureMarker;
    private final CountDownLatch stopHelper = new CountDownLatch(1);
    private final FileChannel output;

    private ReplaySink(Path outputFile) throws IOException {
      this.outputFile = outputFile;
      this.closeLog = Path.of(outputFile + ".closes");
      this.failureMarker = Path.of(outputFile + ".failed-once");
      appendAndForce(Path.of(outputFile + ".starts"), "start\n");
      output =
          FileChannel.open(
              outputFile,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND);
    }

    @Override
    public void start() {
      Thread.ofPlatform().daemon(false).name("replay-sink-helper").start(this::waitForClose);
    }

    @Override
    public synchronized CompletionStage<Void> write(
        FlowChannel channel, List<SinkRecordPayload> records) {
      System.out.println("Generated Sink received a batch");
      var failThisProcess = Files.notExists(failureMarker);
      try {
        var text = new StringBuilder();
        records.forEach(record -> text.append(record.getMessage()).append('\n'));
        writeFully(output, StandardCharsets.UTF_8.encode(text.toString()));
        output.force(true);
        if (failThisProcess) {
          Files.createFile(failureMarker);
          // The test observes Pipeline Ready before releasing this runtime failure.
          while (Files.notExists(Path.of(outputFile + ".fail-release"))) {
            Thread.sleep(5);
          }
          return CompletableFuture.failedFuture(
              new IOException("Expected first-process delivery failure"));
        }
        return CompletableFuture.completedFuture(null);
      } catch (IOException error) {
        return CompletableFuture.failedFuture(error);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(
            new IOException("Interrupted while waiting for the Sink failure gate", error));
      }
    }

    @Override
    public void close() {
      stopHelper.countDown();
      try {
        output.close();
        appendAndForce(closeLog, "close\n");
      } catch (IOException error) {
        throw new UncheckedIOException(error);
      }
    }

    private void waitForClose() {
      try {
        stopHelper.await();
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
      }
    }

    private static void appendAndForce(Path path, String text) throws IOException {
      try (var channel =
          FileChannel.open(
              path,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND)) {
        writeFully(channel, StandardCharsets.UTF_8.encode(text));
        channel.force(true);
      }
    }

    private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
      while (bytes.hasRemaining()) {
        channel.write(bytes);
      }
    }
  }
}
