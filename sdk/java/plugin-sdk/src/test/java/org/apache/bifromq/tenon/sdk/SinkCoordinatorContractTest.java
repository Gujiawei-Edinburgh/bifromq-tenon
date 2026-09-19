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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.tenon.contracts.sink.EgressRecordOuterClass.EgressRecord;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.apache.bifromq.tenon.sdk.sink.testpayload.SinkRecordPayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SinkCoordinatorContractTest {
  @TempDir Path directory;

  private SideFixtures.SinkBells bells;

  @Test
  void availableRecordsEnterOneOrderedImmutableBatchAndReleaseOnSynchronousSuccess()
      throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.IMMEDIATE_SUCCESS);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      var first = write(writer, "first");
      var second = write(writer, "second");
      var third = write(writer, "third");

      coordinator.startPaused();
      coordinator.activate();

      waitUntil(() -> sink.batches().size() == 1);
      var batch = sink.batches().getFirst();
      assertEquals(List.of("first", "second", "third"), messages(batch));
      assertThrows(UnsupportedOperationException.class, () -> batch.add(payload("extra")));
      waitUntil(() -> writer.isReleased(third));
      assertTrue(writer.isReleased(first));
      assertTrue(writer.isReleased(second));

      coordinator.requestStop();
      assertDoesNotThrow(coordinator::awaitTermination);
    }
  }

  @Test
  void emptyPayloadIsDeliveredAndReleasedOnlyAfterBusinessSuccess() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      var receipt =
          assertInstanceOf(IpcQueue.Committed.class, writer.tryWrite(new byte[] {10, 0})).receipt();
      coordinator.startPaused();
      coordinator.activate();
      try {
        waitUntil(() -> sink.batches().size() == 1);
        assertEquals(List.of(SinkRecordPayload.getDefaultInstance()), sink.batches().getFirst());
        assertFalse(writer.isReleased(receipt));

        sink.results().getFirst().complete(null);
        waitUntil(() -> writer.isReleased(receipt));
      } finally {
        coordinator.requestStop();
        assertDoesNotThrow(coordinator::awaitTermination);
      }
    }
  }

  @Test
  void outOfOrderSuccessCannotReleaseAcrossAnEarlierIncompleteBatch() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      var first = write(writer, "first");
      waitUntil(() -> sink.batches().size() == 1);
      var second = write(writer, "second");
      waitUntil(() -> sink.batches().size() == 2);

      sink.results().get(1).complete(null);
      waitUntil(() -> sink.completedCallbacks() == 1);
      assertFalse(writer.isReleased(first));
      assertFalse(writer.isReleased(second));

      sink.results().getFirst().complete(null);
      waitUntil(() -> writer.isReleased(second));
      assertTrue(writer.isReleased(first));

      coordinator.requestStop();
      assertDoesNotThrow(coordinator::awaitTermination);
    }
  }

  @Test
  void failedGapAndLaterSuccessRemainUnreleasedForTheNextProcess() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    IpcQueue.WriteReceipt first;
    IpcQueue.WriteReceipt second;
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      first = write(writer, "first");
      waitUntil(() -> sink.batches().size() == 1);
      second = write(writer, "second");
      waitUntil(() -> sink.batches().size() == 2);

      sink.results().get(1).complete(null);
      var expectedFailure = new Exception("Expected delivery failure");
      sink.results().getFirst().completeExceptionally(expectedFailure);

      var failure = assertThrows(Exception.class, coordinator::awaitTermination);
      assertSame(expectedFailure, failure);
      assertFalse(writer.isReleased(first));
      assertFalse(writer.isReleased(second));
    }

    try (var replay = bells().read(queue, channel())) {
      assertEquals("first", decode(replay.tryRead().orElseThrow()).getDestination());
      assertEquals("second", decode(replay.tryRead().orElseThrow()).getDestination());
    }
  }

  @Test
  void publishedFailurePreventsAnyLaterBatchAdmission() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      var failed = write(writer, "failed");
      waitUntil(() -> sink.batches().size() == 1);

      sink.results().getFirst().completeExceptionally(new Exception("Expected delivery failure"));
      var later = write(writer, "later");

      assertThrows(Exception.class, coordinator::awaitTermination);
      assertEquals(1, sink.batches().size());
      assertFalse(writer.isReleased(failed));
      assertFalse(writer.isReleased(later));
    }
  }

  @Test
  void plannedStopDoesNotWaitForOrReleaseAnOutstandingAsyncResult() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      var receipt = write(writer, "pending");
      waitUntil(() -> sink.batches().size() == 1);

      coordinator.requestStop();

      assertDoesNotThrow(coordinator::awaitTermination);
      assertFalse(writer.isReleased(receipt));
      assertFalse(sink.results().getFirst().isDone());
    }
  }

  @Test
  void plannedStopWaitsForTheWriteMethodBodyToReturn() throws Exception {
    var queue = createQueue();
    var sink = new BlockingSink();
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      write(writer, "blocking");
      assertTrue(sink.entered.await(1, TimeUnit.SECONDS));

      coordinator.requestStop();
      var awaitStarted = new CountDownLatch(1);
      var awaitReturned = new CountDownLatch(1);
      var outcome = new AtomicReference<Throwable>();
      var waiter =
          Thread.ofPlatform()
              .name("tenon-sink-test-await")
              .start(
                  () -> {
                    awaitStarted.countDown();
                    try {
                      coordinator.awaitTermination();
                    } catch (Throwable error) {
                      outcome.set(error);
                    } finally {
                      awaitReturned.countDown();
                    }
                    if (Thread.currentThread().isInterrupted()) {
                      Thread.currentThread().interrupt();
                    }
                  });
      try {
        assertTrue(awaitStarted.await(1, TimeUnit.SECONDS));
        assertFalse(awaitReturned.await(100, TimeUnit.MILLISECONDS));
      } finally {
        sink.release.countDown();
      }
      assertTrue(awaitReturned.await(1, TimeUnit.SECONDS));
      waiter.join();
      assertTrue(outcome.get() == null, () -> "Unexpected coordinator failure: " + outcome.get());
    }
  }

  @Test
  void resultCompletedAfterPlannedStopIsOutsideTheCoordinatorLifetime() throws Exception {
    var queue = createQueue();
    var sink = new BlockingSink();
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      var receipt = write(writer, "stopping");
      assertTrue(sink.entered.await(1, TimeUnit.SECONDS));

      coordinator.requestStop();
      sink.result.completeExceptionally(new Exception("Expected late failure"));
      sink.release.countDown();

      assertDoesNotThrow(coordinator::awaitTermination);
      assertFalse(writer.isReleased(receipt));
    }
  }

  @Test
  void decodeFailureDuringPlannedStopRemainsAFailure() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    var parser = new BlockingFailureParser(1);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink, parser)) {
      var receipt = write(writer, "stopping-during-decode");
      coordinator.startPaused();
      coordinator.activate();
      assertTrue(parser.entered.await(1, TimeUnit.SECONDS));

      coordinator.requestStop();
      parser.release.countDown();

      assertThrows(Exception.class, coordinator::awaitTermination);
      assertFalse(writer.isReleased(receipt));
      assertTrue(sink.batches().isEmpty());
    } finally {
      parser.release.countDown();
    }
  }

  @Test
  void firstAsyncFailureWinsOverALaterDecodeFailure() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.CONTROLLED);
    var parser = new BlockingFailureParser(2);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink, parser)) {
      coordinator.startPaused();
      coordinator.activate();
      var first = write(writer, "first");
      waitUntil(() -> sink.batches().size() == 1);
      var second = write(writer, "decode-failure");
      assertTrue(parser.entered.await(1, TimeUnit.SECONDS));

      var expectedFailure = new Exception("Expected delivery failure");
      sink.results().getFirst().completeExceptionally(expectedFailure);
      parser.release.countDown();

      assertSame(expectedFailure, assertThrows(Exception.class, coordinator::awaitTermination));
      assertFalse(writer.isReleased(first));
      assertFalse(writer.isReleased(second));
      assertEquals(1, sink.batches().size());
    } finally {
      parser.release.countDown();
    }
  }

  @Test
  void successObservedBeforePlannedStopStillReleasesItsContinuousPrefix() throws Exception {
    var queue = createQueue();
    var sink = new StopBoundarySink();
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      coordinator.startPaused();
      coordinator.activate();
      var first = write(writer, "completed");
      assertTrue(sink.firstEntered.await(1, TimeUnit.SECONDS));
      var second = write(writer, "in-progress");
      assertTrue(sink.secondEntered.await(1, TimeUnit.SECONDS));

      sink.firstResult.complete(null);
      coordinator.requestStop();
      sink.releaseSecond.countDown();

      assertDoesNotThrow(coordinator::awaitTermination);
      assertTrue(writer.isReleased(first));
      assertFalse(writer.isReleased(second));
    }
  }

  @Test
  void malformedPayloadFailsWithoutReleasingTheRecord() throws Exception {
    var queue = createQueue();
    var sink = new ControlledSink(CompletionMode.IMMEDIATE_SUCCESS);
    try (var writer = bells().write(queue, channel());
        var coordinator = coordinator(queue, sink)) {
      var encoded =
          EgressRecord.newBuilder()
              .setPayload(ByteString.copyFrom(new byte[] {0}))
              .build()
              .toByteArray();
      var receipt = assertInstanceOf(IpcQueue.Committed.class, writer.tryWrite(encoded)).receipt();
      coordinator.startPaused();
      coordinator.activate();

      var failure = assertThrows(Exception.class, coordinator::awaitTermination);

      var decodeFailure = assertInstanceOf(SinkRecordDecodeException.class, failure);
      assertEquals("sink.payload_invalid", decodeFailure.code());
      assertFalse(writer.isReleased(receipt));
      assertTrue(sink.batches().isEmpty());
    }
  }

  private Path createQueue() throws Exception {
    var queue = directory.resolve("egress.queue");
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    return queue;
  }

  /**
   * Returns the Regions this Sink input's peers own, creating them once per Queue.
   *
   * <p>One Egress Queue carries one Channel, so the Sink's own-loop Region holds one slot and the
   * Flow's Region one doorbell, exactly as the Pipeline lays them out.
   */
  private SideFixtures.SinkBells bells() throws IOException {
    if (bells == null) {
      bells = SideFixtures.SinkBells.create(directory, List.of(channel()));
    }
    return bells;
  }

  private FlowChannel channel() {
    return SideFixtures.flowChannel(directory, "test", 0);
  }

  private SinkCoordinator<SinkRecordPayload> coordinator(
      Path queue, TenonSink<SinkRecordPayload> sink) throws Exception {
    return coordinator(queue, sink, SinkRecordPayload.parser());
  }

  private SinkCoordinator<SinkRecordPayload> coordinator(
      Path queue, TenonSink<SinkRecordPayload> sink, Parser<SinkRecordPayload> parser)
      throws Exception {
    return new SinkCoordinator<>(
        bells().sinkLoop(),
        List.of(
            new SinkCoordinator.Input<>(
                bells().read(queue, channel()), records -> sink.write(channel(), records))),
        parser,
        failure -> {});
  }

  private static IpcQueue.WriteReceipt write(IpcQueue.Writer writer, String message)
      throws Exception {
    var encoded =
        EgressRecord.newBuilder().setPayload(payload(message).toByteString()).build().toByteArray();
    return assertInstanceOf(IpcQueue.Committed.class, writer.tryWrite(encoded)).receipt();
  }

  private static SinkRecordPayload decode(ByteString bytes) throws Exception {
    return SinkRecordDecoder.decode(bytes, SinkRecordPayload.parser());
  }

  private static SinkRecordPayload payload(String message) {
    return SinkRecordPayload.newBuilder().setDestination(message).setBody(ByteString.EMPTY).build();
  }

  private static List<String> messages(List<SinkRecordPayload> records) {
    return records.stream().map(SinkRecordPayload::getDestination).toList();
  }

  private static void waitUntil(CheckedCondition condition) throws Exception {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!condition.test()) {
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Timed out waiting for the Sink coordinator");
      }
      Thread.sleep(1);
    }
  }

  private enum CompletionMode {
    IMMEDIATE_SUCCESS,
    CONTROLLED
  }

  private static final class ControlledSink implements TenonSink<SinkRecordPayload> {
    private final CompletionMode mode;
    private final List<List<SinkRecordPayload>> batches = new ArrayList<>();
    private final List<CompletableFuture<Void>> results = new ArrayList<>();
    private int completedCallbacks;

    private ControlledSink(CompletionMode mode) {
      this.mode = mode;
    }

    @Override
    public void start() {}

    @Override
    public synchronized CompletionStage<Void> write(
        FlowChannel channel, List<SinkRecordPayload> records) {
      batches.add(records);
      var result = new CompletableFuture<Void>();
      result.whenComplete((ignored, failure) -> incrementCompletedCallbacks());
      results.add(result);
      if (mode == CompletionMode.IMMEDIATE_SUCCESS) {
        result.complete(null);
      }
      return result;
    }

    @Override
    public void close() {}

    private synchronized List<List<SinkRecordPayload>> batches() {
      return List.copyOf(batches);
    }

    private synchronized List<CompletableFuture<Void>> results() {
      return List.copyOf(results);
    }

    private synchronized int completedCallbacks() {
      return completedCallbacks;
    }

    private synchronized void incrementCompletedCallbacks() {
      completedCallbacks++;
    }
  }

  private static final class BlockingSink implements TenonSink<SinkRecordPayload> {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final CompletableFuture<Void> result = new CompletableFuture<>();

    @Override
    public void start() {}

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<SinkRecordPayload> records) {
      entered.countDown();
      try {
        release.await();
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(error);
      }
      return result;
    }

    @Override
    public void close() {}
  }

  private static final class StopBoundarySink implements TenonSink<SinkRecordPayload> {
    private final CountDownLatch firstEntered = new CountDownLatch(1);
    private final CountDownLatch secondEntered = new CountDownLatch(1);
    private final CountDownLatch releaseSecond = new CountDownLatch(1);
    private final CompletableFuture<Void> firstResult = new CompletableFuture<>();
    private int writeCount;

    @Override
    public void start() {}

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<SinkRecordPayload> records) {
      if (writeCount++ == 0) {
        firstEntered.countDown();
        return firstResult;
      }
      secondEntered.countDown();
      try {
        releaseSecond.await();
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(error);
      }
      return new CompletableFuture<>();
    }

    @Override
    public void close() {}
  }

  private static final class BlockingFailureParser extends AbstractParser<SinkRecordPayload> {
    private final Parser<SinkRecordPayload> delegate = SinkRecordPayload.parser();
    private final int failingCall;
    private final AtomicInteger calls = new AtomicInteger();
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    private BlockingFailureParser(int failingCall) {
      this.failingCall = failingCall;
    }

    @Override
    public SinkRecordPayload parsePartialFrom(
        CodedInputStream input, ExtensionRegistryLite extensionRegistry)
        throws InvalidProtocolBufferException {
      if (calls.incrementAndGet() != failingCall) {
        return delegate.parsePartialFrom(input, extensionRegistry);
      }
      entered.countDown();
      try {
        release.await();
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        var failure = new InvalidProtocolBufferException("Sink payload parser was interrupted");
        failure.initCause(error);
        throw failure;
      }
      throw new InvalidProtocolBufferException("Expected Sink payload decode failure");
    }
  }

  @FunctionalInterface
  private interface CheckedCondition {
    boolean test() throws Exception;
  }
}
