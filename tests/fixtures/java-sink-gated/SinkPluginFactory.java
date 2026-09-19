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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.TenonSink;
import org.apache.bifromq.tenon.sdk.TenonSinkFactory;
import tools.jackson.databind.JsonNode;

/** Holds one generated Sink batch until the black-box test opens its delivery gate. */
public final class SinkPluginFactory implements TenonSinkFactory<SinkRecordPayload> {
  @Override
  public TenonSink<SinkRecordPayload> create(JsonNode config) throws IOException {
    return new GatedSink(Path.of(config.required("outputFile").stringValue()));
  }

  private static final class GatedSink implements TenonSink<SinkRecordPayload> {
    private final Path outputFile;
    private final FileChannel output;
    private final ExecutorService deliveries = Executors.newVirtualThreadPerTaskExecutor();

    private GatedSink(Path outputFile) throws IOException {
      this.outputFile = outputFile;
      output =
          FileChannel.open(
              outputFile,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND);
    }

    @Override
    public void start() {}

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<SinkRecordPayload> records) {
      try {
        appendAndForce(Path.of(outputFile + ".write-started"), "write\n");
      } catch (IOException error) {
        return CompletableFuture.failedFuture(error);
      }

      var completion = new CompletableFuture<Void>();
      deliveries.submit(() -> deliverAfterRelease(records, completion));
      return completion;
    }

    @Override
    public void close() {
      deliveries.shutdownNow();
      try {
        output.close();
      } catch (IOException error) {
        throw new UncheckedIOException(error);
      }
    }

    private void deliverAfterRelease(
        List<SinkRecordPayload> records, CompletableFuture<Void> completion) {
      try {
        while (Files.notExists(Path.of(outputFile + ".write-release"))) {
          Thread.sleep(5);
        }
        var text = new StringBuilder();
        records.forEach(record -> text.append(record.getMessage()).append('\n'));
        synchronized (output) {
          writeFully(output, StandardCharsets.UTF_8.encode(text.toString()));
          output.force(true);
        }
        completion.complete(null);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        completion.completeExceptionally(
            new IOException("Interrupted while waiting for the Sink delivery gate", error));
      } catch (IOException error) {
        completion.completeExceptionally(error);
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
