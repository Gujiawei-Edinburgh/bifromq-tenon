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

package ${package};

import ${package}.payload.SinkRecordPayload;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.TenonSink;
import org.apache.bifromq.tenon.sdk.TenonSinkFactory;
import tools.jackson.databind.JsonNode;

/** Replace this example with the Sink business implementation. */
public final class SinkPluginFactory implements TenonSinkFactory<SinkRecordPayload> {
  @Override
  public TenonSink<SinkRecordPayload> create(JsonNode config) throws IOException {
    return new PluginSink(SinkConfig.parse(config));
  }

  private static final class PluginSink implements TenonSink<SinkRecordPayload> {
    private final FileChannel output;

    private PluginSink(SinkConfig config) throws IOException {
      output = openOutput(config.outputFile());
    }

    @Override
    public void start() {}

    @Override
    public synchronized CompletionStage<Void> write(
        FlowChannel channel, List<SinkRecordPayload> records) {
      try {
        writeMessages(output, records);
        return CompletableFuture.completedFuture(null);
      } catch (IOException error) {
        return CompletableFuture.failedFuture(error);
      }
    }

    @Override
    public void close() {
      try {
        output.close();
      } catch (IOException error) {
        throw new UncheckedIOException(error);
      }
    }
  }

  private record SinkConfig(Path outputFile) {
    private static SinkConfig parse(JsonNode config) throws IOException {
      var outputFile = Path.of(config.required("outputFile").stringValue());
      if (!outputFile.isAbsolute()) {
        throw new IOException("outputFile must be an absolute path");
      }
      return new SinkConfig(outputFile);
    }
  }

  private static FileChannel openOutput(Path outputFile) throws IOException {
    return FileChannel.open(
        outputFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
  }

  private static void writeMessages(FileChannel output, List<SinkRecordPayload> records)
      throws IOException {
    var text = new StringBuilder();
    records.forEach(record -> text.append(record.getMessage()).append('\n'));
    ByteBuffer bytes = StandardCharsets.UTF_8.encode(text.toString());
    while (bytes.hasRemaining()) {
      output.write(bytes);
    }
    output.force(true);
  }
}
