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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletion;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletionStatus;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressRecord;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Run manually with {@code ./mvnw -q -pl plugin-sdk -am -Dtenon.performance=true
 * -Dtest=SourcePerformanceProbeTest -Dsurefire.failIfNoSpecifiedTests=false test}.
 */
@EnabledIfSystemProperty(named = "tenon.performance", matches = "true")
final class SourcePerformanceProbeTest {
  @Test
  void measuresSourceSendCriticalPaths() throws Exception {
    backpressureLatencyMicros();
    independentEncoderLatencyMicros();
    roundTripThroughput(10_000, 256, 64);

    System.out.printf("backpressure_us=%.3f%n", backpressureLatencyMicros());
    System.out.printf("independent_encoder_us=%.3f%n", independentEncoderLatencyMicros());
    System.out.printf(
        "round_trip_records_per_second=%.0f%n", roundTripThroughput(100_000, 256, 64));
  }

  private static double backpressureLatencyMicros() throws Exception {
    var pair = createPair(1, 1024);
    var runtime = SourceSession.open(pair.directory(), pair.bells().channelsPath());
    var encodingStarted = new CountDownLatch(1);
    var releaseEncoding = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var admitted =
          executor.submit(
              () ->
                  runtime.send(
                      0,
                      maxRecordBytes -> {
                        encodingStarted.countDown();
                        releaseEncoding.await();
                        return ByteString.copyFromUtf8("admitted");
                      }));
      assertTrue(encodingStarted.await(1, TimeUnit.SECONDS));

      var started = System.nanoTime();
      var result = runtime.send(0, maxRecordBytes -> ByteString.copyFromUtf8("rejected"));
      var elapsed = System.nanoTime() - started;

      assertEquals(AckCode.BACKPRESSURE, result.toCompletableFuture().getNow(null));
      releaseEncoding.countDown();
      admitted.get();
      return elapsed / 1_000.0;
    } finally {
      releaseEncoding.countDown();
      runtime.close();
      deleteTree(pair.root());
    }
  }

  private static double independentEncoderLatencyMicros() throws Exception {
    var pair = createPair(2, 1024);
    var runtime = SourceSession.open(pair.directory(), pair.bells().channelsPath());
    var encodingStarted = new CountDownLatch(1);
    var releaseEncoding = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var slow =
          executor.submit(
              () ->
                  runtime.send(
                      0,
                      maxRecordBytes -> {
                        encodingStarted.countDown();
                        releaseEncoding.await();
                        return ByteString.copyFromUtf8("slow");
                      }));
      assertTrue(encodingStarted.await(1, TimeUnit.SECONDS));

      var started = System.nanoTime();
      runtime.send(0, maxRecordBytes -> ByteString.copyFromUtf8("fast"));
      var elapsed = System.nanoTime() - started;

      releaseEncoding.countDown();
      slow.get();
      return elapsed / 1_000.0;
    } finally {
      releaseEncoding.countDown();
      runtime.close();
      deleteTree(pair.root());
    }
  }

  private static double roundTripThroughput(int count, int window, int payloadSize)
      throws Exception {
    var pair = createPair(window * 2L, 1024);
    var directory = pair.directory();
    var runtime = SourceSession.open(directory, pair.bells().channelsPath());
    var payload = new byte[payloadSize];
    try (var submission = pair.bells().readSubmission(0);
        var completion = pair.bells().writeCompletion(0);
        var executor = Executors.newSingleThreadExecutor()) {
      var pipeline =
          executor.submit(
              () -> {
                for (var index = 0; index < count; index++) {
                  var record = submission.tryRead();
                  while (record.isEmpty()) {
                    submission.waitReadable();
                    record = submission.tryRead();
                  }
                  var ingress = IngressRecord.parseFrom(record.orElseThrow());
                  submission.release(1);
                  var reply =
                      IngressCompletion.newBuilder()
                          .setRecordId(ingress.getRecordId())
                          .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                          .build()
                          .toByteArray();
                  while (!(completion.tryWrite(reply) instanceof IpcQueue.Committed)) {
                    completion.waitWritable(reply.length);
                  }
                }
                return null;
              });

      var pending = new ArrayDeque<CompletableFuture<AckCode>>(window);
      var started = System.nanoTime();
      for (var index = 0; index < count; index++) {
        if (pending.size() == window) {
          pending.removeFirst().join();
        }
        pending.addLast(
            runtime.send(0, maxRecordBytes -> ByteString.copyFrom(payload)).toCompletableFuture());
      }
      while (!pending.isEmpty()) {
        pending.removeFirst().join();
      }
      pipeline.get(30, TimeUnit.SECONDS);
      var elapsed = System.nanoTime() - started;
      return count / (elapsed / 1_000_000_000.0);
    } finally {
      runtime.close();
      deleteTree(pair.root());
    }
  }

  /**
   * One Source instance directory, the Bell Regions a Core runtime creates beside it, and the root
   * a fixture deletes afterwards.
   */
  private record Pair(Path root, Path directory, SideFixtures.SourceBells bells) {}

  private static Pair createPair(long pending, long maximumRecordSize) throws Exception {
    var root = Files.createTempDirectory("tenon-source-performance-");
    var directory = Files.createDirectory(root.resolve("source"));
    IpcQueue.create(
        directory.resolve("submission-0.queue"),
        IngressQueueLayout.submissionCapacity(pending, maximumRecordSize),
        maximumRecordSize);
    IpcQueue.create(
        directory.resolve("completion-0.queue"),
        IngressQueueLayout.completionCapacity(pending),
        IngressQueueLayout.COMPLETION_MAX_PAYLOAD_SIZE);
    return new Pair(root, directory, SideFixtures.SourceBells.create(directory, 1));
  }

  private static void deleteTree(Path directory) throws Exception {
    try (var paths = Files.walk(directory)) {
      for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(path);
      }
    }
  }
}
