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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.StringValue;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletion;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletionStatus;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressRecord;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.QueueFixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SourceRuntimeContractTest {
  @TempDir Path directory;
  private SideFixtures.SourceBells bells;

  @Test
  void sourceSessionRoundTripsOnePayloadThroughRealQueues() throws Exception {
    createPair(2, 1024);
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0);
        SourceSession<StringValue> session =
            SourceSession.open(directory, bells().channelsPath())) {
      var payload = StringValue.of("hello");
      var result = session.sender().send(0, payload);

      var ingress = readSubmission(submission);
      assertArrayEquals(payload.toByteArray(), ingress.getPayload().toByteArray());
      complete(completion, ingress.getRecordId());

      assertEquals(AckCode.OK, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void senderRoutesPayloadToTheSelectedQueue() throws Exception {
    createPair(0, 1, 1024);
    createPair(1, 1, 1024);
    try (var firstSubmission = bells().readSubmission(0);
        var secondSubmission = bells().readSubmission(1);
        var secondCompletion = bells().writeCompletion(1);
        SourceSession<StringValue> session =
            SourceSession.open(directory, bells().channelsPath())) {
      var payload = StringValue.of("second");
      var result = session.sender().send(1, payload);

      assertTrue(firstSubmission.tryRead().isEmpty());
      var ingress = readSubmission(secondSubmission);
      assertArrayEquals(payload.toByteArray(), ingress.getPayload().toByteArray());
      complete(secondCompletion, ingress.getRecordId());
      assertEquals(AckCode.OK, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void oneBusinessSourceCanSendAcrossEveryFlowChannel() throws Exception {
    createPair(0, 1, 1024);
    createPair(1, 1, 1024);
    var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
    var results = new ArrayList<CompletionStage<AckCode>>();
    var creations = new AtomicInteger();
    var closes = new AtomicInteger();
    var program =
        new SourceProgramOwner<StringValue>(
            session,
            new TenonSource() {
              @Override
              public void start() {
                for (var channel = 0; channel < session.parallelism(); channel++) {
                  results.add(session.sender().send(channel, StringValue.of("channel-" + channel)));
                }
              }

              @Override
              public void quiesce() {}

              @Override
              public void close() {
                closes.incrementAndGet();
              }
            });
    creations.incrementAndGet();
    try {
      program.start();
      for (var channel = 0; channel < session.parallelism(); channel++) {
        try (var submission = bells().readSubmission(channel);
            var completion = bells().writeCompletion(channel)) {
          var record = readSubmission(submission);
          assertEquals("channel-" + channel, StringValue.parseFrom(record.getPayload()).getValue());
          complete(completion, record.getRecordId());
          assertEquals(
              AckCode.OK, results.get(channel).toCompletableFuture().get(1, TimeUnit.SECONDS));
        }
      }
      program.quiesce();
    } finally {
      program.shutdown();
    }
    assertEquals(1, creations.get());
    assertEquals(1, closes.get());
  }

  @Test
  void exhaustedAdmissionReturnsBackpressureWithoutTouchingQueue() throws Exception {
    createPair(1, 1024);
    try (SourceSession<StringValue> session =
        SourceSession.open(directory, bells().channelsPath())) {
      var first = session.sender().send(0, StringValue.of("first"));
      var second = session.sender().send(0, StringValue.of("second"));

      assertEquals(AckCode.BACKPRESSURE, second.toCompletableFuture().getNow(null));
      first.toCompletableFuture().cancel(false);
    }
  }

  @Test
  void exhaustedAdmissionDoesNotWaitForAnEncoder() throws Exception {
    createPair(1, 1024);
    var runtime = SourceSession.open(directory, bells().channelsPath());
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

      try {
        var rejected =
            executor.submit(
                () ->
                    runtime.send(
                        0,
                        maxRecordBytes -> {
                          throw new AssertionError("Backpressured send encoded its payload");
                        }));
        assertEquals(
            AckCode.BACKPRESSURE,
            rejected.get(1, TimeUnit.SECONDS).toCompletableFuture().getNow(null));
      } finally {
        releaseEncoding.countDown();
      }
      admitted.get(1, TimeUnit.SECONDS);
    } finally {
      releaseEncoding.countDown();
      runtime.close();
    }
  }

  @Test
  void admittedEncoderDoesNotDelayAnotherEncoderOnTheSameChannel() throws Exception {
    createPair(2, 1024);
    var runtime = SourceSession.open(directory, bells().channelsPath());
    var slowEncodingStarted = new CountDownLatch(1);
    var releaseSlowEncoding = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var slow =
          executor.submit(
              () ->
                  runtime.send(
                      0,
                      maxRecordBytes -> {
                        slowEncodingStarted.countDown();
                        releaseSlowEncoding.await();
                        return ByteString.copyFromUtf8("slow");
                      }));
      assertTrue(slowEncodingStarted.await(1, TimeUnit.SECONDS));

      try {
        var fast =
            executor.submit(
                () -> runtime.send(0, maxRecordBytes -> ByteString.copyFromUtf8("fast")));
        assertFalse(fast.get(1, TimeUnit.SECONDS).toCompletableFuture().isDone());
      } finally {
        releaseSlowEncoding.countDown();
      }
      slow.get(1, TimeUnit.SECONDS);
    } finally {
      releaseSlowEncoding.countDown();
      runtime.close();
    }
  }

  @Test
  void stoppedSessionRejectsNewSendsAndCloseFailsPendingSends() throws Exception {
    createPair(1, 1024);
    var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
    var admitted = session.sender().send(0, StringValue.of("pending"));

    session.stopAccepting();
    var rejected = session.sender().send(0, StringValue.of("late"));
    session.close();

    assertInstanceOf(
        SourceSessionClosedException.class,
        assertThrows(
                java.util.concurrent.CompletionException.class,
                () -> rejected.toCompletableFuture().join())
            .getCause());
    assertTrue(admitted.toCompletableFuture().isCompletedExceptionally());
  }

  @Test
  void concurrentCallersReceiveCompletionsFromOneOrderedChannel() throws Exception {
    var count = 32;
    createPair(count, 1024);
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0);
        SourceSession<StringValue> session = SourceSession.open(directory, bells().channelsPath());
        var callers = Executors.newVirtualThreadPerTaskExecutor()) {
      var ready = new CountDownLatch(count);
      var start = new CountDownLatch(1);
      var calls = new ArrayList<Future<CompletionStage<AckCode>>>();
      for (var index = 0; index < count; index++) {
        var value = index;
        calls.add(
            callers.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  return session.sender().send(0, StringValue.of(Integer.toString(value)));
                }));
      }
      assertTrue(ready.await(1, TimeUnit.SECONDS));
      start.countDown();

      long previousRecordId = 0;
      for (var index = 0; index < count; index++) {
        var ingress = readSubmission(submission);
        assertTrue(Long.compareUnsigned(ingress.getRecordId(), previousRecordId) > 0);
        previousRecordId = ingress.getRecordId();
        complete(completion, ingress.getRecordId());
      }

      for (var call : calls) {
        assertEquals(AckCode.OK, call.get().toCompletableFuture().get(1, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void oversizedPayloadIsRejectedBeforeEncodingAndReturnsItsPermit() throws Exception {
    createPair(1, 1024);
    var payload =
        (MessageLite)
            Proxy.newProxyInstance(
                MessageLite.class.getClassLoader(),
                new Class<?>[] {MessageLite.class},
                (proxy, method, arguments) -> {
                  if (method.getName().equals("getSerializedSize")) {
                    return 1025;
                  }
                  throw new AssertionError(
                      "Oversized payload must not be encoded: " + method.getName());
                });
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0);
        SourceSession<MessageLite> session =
            SourceSession.open(directory, bells().channelsPath())) {
      assertEquals(
          AckCode.ERROR, session.sender().send(0, payload).toCompletableFuture().getNow(null));
      assertTrue(submission.tryRead().isEmpty());
      var next = session.sender().send(0, StringValue.of("accepted"));
      var ingress = readSubmission(submission);
      complete(completion, ingress.getRecordId());
      assertEquals(AckCode.OK, next.toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void oversizedPayloadReturnsErrorWithoutWritingSubmission() throws Exception {
    createPair(1, 16);
    try (var submission = bells().readSubmission(0);
        SourceSession<StringValue> session =
            SourceSession.open(directory, bells().channelsPath())) {
      var result = session.sender().send(0, StringValue.of("x".repeat(100)));

      assertEquals(AckCode.ERROR, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
      assertTrue(submission.tryRead().isEmpty());
    }
  }

  @Test
  void recordAtTheExactMaximumSizeIsAccepted() throws Exception {
    createPair(1, 16);
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0);
        var runtime = SourceSession.open(directory, bells().channelsPath())) {
      var result = runtime.send(0, maxRecordBytes -> ByteString.copyFrom(new byte[12]));

      var ingress = readSubmission(submission);
      assertEquals(16, ingress.getSerializedSize());
      complete(completion, ingress.getRecordId());
      assertEquals(AckCode.OK, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void recordIdGrowthRejectsOnlyTheOversizedRecordAndReturnsItsPermit() throws Exception {
    createPair(1, 16);
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0);
        var runtime = SourceSession.open(directory, bells().channelsPath())) {
      for (var index = 0; index < 6; index++) {
        var result = runtime.send(0, maxRecordBytes -> ByteString.copyFrom(new byte[12]));
        var ingress = readSubmission(submission);
        assertEquals(1 + index * 24, ingress.getRecordId());
        assertEquals(16, ingress.getSerializedSize());
        complete(completion, ingress.getRecordId());
        assertEquals(AckCode.OK, result.toCompletableFuture().get(1, TimeUnit.SECONDS));
      }
      var oversized = runtime.send(0, maxRecordBytes -> ByteString.copyFrom(new byte[12]));
      assertEquals(AckCode.ERROR, oversized.toCompletableFuture().get(1, TimeUnit.SECONDS));
      assertTrue(submission.tryRead().isEmpty());
      var accepted = runtime.send(0, maxRecordBytes -> ByteString.copyFrom(new byte[11]));
      var ingress = readSubmission(submission);
      assertEquals(145, ingress.getRecordId());
      assertEquals(16, ingress.getSerializedSize());
      complete(completion, ingress.getRecordId());
      assertEquals(AckCode.OK, accepted.toCompletableFuture().get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void invalidQueueIndexFailsAtTheCallSite() throws Exception {
    createPair(1, 1024);
    try (var submission = bells().readSubmission(0);
        SourceSession<StringValue> session =
            SourceSession.open(directory, bells().channelsPath())) {
      assertThrows(
          IllegalArgumentException.class,
          () -> session.sender().send(1, StringValue.of("invalid")));
      assertThrows(
          IllegalArgumentException.class,
          () -> session.sender().send(-1, StringValue.of("invalid")));
      assertThrows(IllegalArgumentException.class, () -> session.sender().send(-1, null));
      assertTrue(submission.tryRead().isEmpty());
    }
  }

  @Test
  void admittedSendAndConcurrentCloseHaveOneCompletionOwner() throws Exception {
    createPair(1, 1024);
    var runtime = SourceSession.open(directory, bells().channelsPath());
    var encodingStarted = new CountDownLatch(1);
    var releaseEncoding = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var send =
          executor.submit(
              () ->
                  runtime.send(
                      0,
                      maxRecordBytes -> {
                        encodingStarted.countDown();
                        releaseEncoding.await();
                        return StringValue.of("racing").toByteString();
                      }));
      assertTrue(encodingStarted.await(1, TimeUnit.SECONDS));

      var closeFailure = new AtomicReference<Throwable>();
      var closeStarted = new CountDownLatch(1);
      var closeFinished = new CountDownLatch(1);
      var close =
          Thread.ofPlatform()
              .start(
                  () -> {
                    closeStarted.countDown();
                    try {
                      runtime.close();
                    } catch (Throwable error) {
                      closeFailure.set(error);
                    } finally {
                      closeFinished.countDown();
                    }
                  });
      assertTrue(closeStarted.await(1, TimeUnit.SECONDS));
      assertFalse(closeFinished.await(100, TimeUnit.MILLISECONDS));

      releaseEncoding.countDown();
      var result = send.get(1, TimeUnit.SECONDS).toCompletableFuture();
      close.join(TimeUnit.SECONDS.toMillis(1));

      assertFalse(close.isAlive());
      assertNull(closeFailure.get());
      assertTrue(result.isCompletedExceptionally());
    }
  }

  @Test
  void synchronousCallbacksFollowCompletionOrderOnOneChannel() throws Exception {
    createPair(2, 1024);
    var releaseCallback = new CountDownLatch(1);
    var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0)) {
      var first = session.sender().send(0, StringValue.of("first"));
      var second = session.sender().send(0, StringValue.of("second"));
      var firstRecord = readSubmission(submission);
      var secondRecord = readSubmission(submission);
      var firstCallbackStarted = new CountDownLatch(1);
      var secondCallbackFinished = new CountDownLatch(1);
      var callbackOrder = new ArrayList<String>();
      first.thenAccept(
          ignored -> {
            callbackOrder.add("first-started");
            firstCallbackStarted.countDown();
            awaitUninterruptibly(releaseCallback);
            callbackOrder.add("first-finished");
          });
      second.thenAccept(
          ignored -> {
            callbackOrder.add("second");
            secondCallbackFinished.countDown();
          });

      complete(completion, firstRecord.getRecordId());
      assertTrue(firstCallbackStarted.await(1, TimeUnit.SECONDS));
      complete(completion, secondRecord.getRecordId());
      assertFalse(secondCallbackFinished.await(100, TimeUnit.MILLISECONDS));
      assertFalse(second.toCompletableFuture().isDone());

      releaseCallback.countDown();
      assertEquals(AckCode.OK, second.toCompletableFuture().get(1, TimeUnit.SECONDS));
      assertTrue(secondCallbackFinished.await(1, TimeUnit.SECONDS));
      assertEquals(List.of("first-started", "first-finished", "second"), callbackOrder);
    } finally {
      releaseCallback.countDown();
      session.close();
    }
  }

  @Test
  void explicitAsyncCallbackDoesNotBlockCompletionPump() throws Exception {
    createPair(2, 1024);
    var releaseCallback = new CountDownLatch(1);
    var callbackExecutor = Executors.newSingleThreadExecutor();
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0);
        SourceSession<StringValue> session =
            SourceSession.open(directory, bells().channelsPath())) {
      var first = session.sender().send(0, StringValue.of("first"));
      var second = session.sender().send(0, StringValue.of("second"));
      var firstRecord = readSubmission(submission);
      var secondRecord = readSubmission(submission);
      var callbackStarted = new CountDownLatch(1);
      first.thenAcceptAsync(
          ignored -> {
            callbackStarted.countDown();
            awaitUninterruptibly(releaseCallback);
          },
          callbackExecutor);

      complete(completion, firstRecord.getRecordId());
      assertTrue(callbackStarted.await(1, TimeUnit.SECONDS));
      complete(completion, secondRecord.getRecordId());

      assertEquals(AckCode.OK, second.toCompletableFuture().get(1, TimeUnit.SECONDS));
    } finally {
      releaseCallback.countDown();
      callbackExecutor.shutdownNow();
    }
  }

  @Test
  void synchronousExceptionalCallbackDelaysSessionClose() throws Exception {
    createPair(1, 1024);
    var releaseCallback = new CountDownLatch(1);
    var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
    var result = session.sender().send(0, StringValue.of("pending"));
    var callbackStarted = new CountDownLatch(1);
    result.whenComplete(
        (ignored, failure) -> {
          callbackStarted.countDown();
          awaitUninterruptibly(releaseCallback);
        });

    var closeFailure = new AtomicReference<Throwable>();
    var close =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    session.close();
                  } catch (Throwable error) {
                    closeFailure.set(error);
                  }
                });
    try {
      assertTrue(callbackStarted.await(1, TimeUnit.SECONDS));
      close.join(100);

      assertTrue(close.isAlive());
      releaseCallback.countDown();
      close.join(TimeUnit.SECONDS.toMillis(1));
      assertFalse(close.isAlive());
      assertNull(closeFailure.get());
    } finally {
      releaseCallback.countDown();
      close.join(TimeUnit.SECONDS.toMillis(1));
      assertFalse(close.isAlive());
    }
  }

  @Test
  void fatalCompletionFailureUnblocksSessionOwnerAndPreventsSuccessfulClose() throws Exception {
    createPair(1, 1024);
    var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
    try (var completion = bells().writeCompletion(0)) {
      assertTrue(completion.tryWrite(new byte[] {0}) instanceof IpcQueue.Committed);
      var execution =
          assertThrows(
              ExecutionException.class,
              () -> session.failure().toCompletableFuture().get(1, TimeUnit.SECONDS));
      assertTrue(execution.getCause() instanceof IOException);
      var closeFailure =
          assertThrows(java.util.concurrent.CompletionException.class, session::close);
      assertSame(execution.getCause(), closeFailure.getCause());
    }
  }

  @Test
  void restartedSessionCannotMatchACompletionFromThePreviousSession() throws Exception {
    createPair(1, 1024);
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0)) {
      var previousSession = SourceSession.<StringValue>open(directory, bells().channelsPath());
      var previousSend = previousSession.sender().send(0, StringValue.of("previous"));
      var previousRecordId = readSubmission(submission).getRecordId();
      previousSession.close();
      assertThrows(
          ExecutionException.class,
          () -> previousSend.toCompletableFuture().get(1, TimeUnit.SECONDS));

      try (SourceSession<StringValue> currentSession =
          SourceSession.open(directory, bells().channelsPath())) {
        var currentSend = currentSession.sender().send(0, StringValue.of("current"));
        var currentRecordId = readSubmission(submission).getRecordId();
        assertTrue(Long.compareUnsigned(currentRecordId, previousRecordId) > 0);

        complete(completion, previousRecordId);
        assertFalse(currentSend.toCompletableFuture().isDone());
        complete(completion, currentRecordId);
        assertEquals(AckCode.OK, currentSend.toCompletableFuture().get(1, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void restartedSessionWaitsForPreviousSessionSubmissionSpace() throws Exception {
    createPair(1, 1024);
    var submissionPath = directory.resolve("submission-0.queue");
    try (var submission = bells().readSubmission(0);
        var completion = bells().writeCompletion(0)) {
      long previousRecordId = 0;
      for (var index = 0; index < 2; index++) {
        var previousSession = SourceSession.<StringValue>open(directory, bells().channelsPath());
        var previousSend = previousSession.sender().send(0, StringValue.of("x".repeat(1014)));
        previousRecordId = readUnreleasedSubmission(submission).getRecordId();
        previousSession.close();
        assertTrue(previousSend.toCompletableFuture().isCompletedExceptionally());
      }

      try (SourceSession<StringValue> currentSession =
          SourceSession.open(directory, bells().channelsPath())) {
        var currentSend = currentSession.sender().send(0, StringValue.of("current"));
        waitForSubmissionWait(submissionPath, currentSend);

        submission.release(2);
        var current = readSubmission(submission);
        assertTrue(Long.compareUnsigned(current.getRecordId(), previousRecordId) > 0);
        complete(completion, current.getRecordId());
        assertEquals(AckCode.OK, currentSend.toCompletableFuture().get(1, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void sharedFailureAcrossChannelsStopsShutdownAtTheFirstFailure() throws Exception {
    createPair(0, 1, 1024);
    createPair(1, 1, 1024);
    var failure = new IOException("Shared Sink failed while Source was quiescing");
    var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
    session.fail(failure);
    assertSame(failure, assertThrows(IOException.class, session::quiesce));
    var closeFailure = assertThrows(java.util.concurrent.CompletionException.class, session::close);
    assertSame(failure, closeFailure.getCause());
    assertEquals(0, failure.getSuppressed().length);
  }

  @Test
  void sharedFailureBeforeQuiesceRejectsTheBoundaryWithoutCompletingBusinessResults()
      throws Exception {
    createPair(1, 1024);
    var failure = new IOException("Shared Sink failed before Source quiesce");
    try (var submission = bells().readSubmission(0)) {
      var session = SourceSession.<StringValue>open(directory, bells().channelsPath());
      var pending = session.sender().send(0, StringValue.of("pending"));
      readUnreleasedSubmission(submission);
      session.fail(failure);
      assertSame(failure, assertThrows(IOException.class, session::quiesce));
      var closeFailure =
          assertThrows(java.util.concurrent.CompletionException.class, session::close);
      assertSame(failure, closeFailure.getCause());
      assertFalse(pending.toCompletableFuture().isDone());
    }
  }

  @Test
  void queueIndexOutsideIntegerRangeIsReportedAsInputFailure() throws Exception {
    Files.createFile(directory.resolve("submission-999999999999999999999.queue"));

    var error =
        assertThrows(
            IOException.class, () -> SourceSession.open(directory, bells().channelsPath()));

    assertTrue(error.getMessage().contains("outside the supported range"));
  }

  /**
   * Proves the Source runtime thread count is this SDK's own decision rather than a function of the
   * Channel count: three Channel pairs share the one Submission loop and the one Completion loop,
   * and both loops end with the session.
   */
  @Test
  void oneSubmissionLoopAndOneCompletionLoopServeEveryChannel() throws Exception {
    createPair(0, 1, 1024);
    createPair(1, 1, 1024);
    createPair(2, 1, 1024);
    var submissions = loopThreads("tenon-source-submission");
    var completions = loopThreads("tenon-source-completion");

    try (var session = SourceSession.<StringValue>open(directory, bells().channelsPath())) {
      assertEquals(3, session.parallelism());
      assertEquals(submissions + 1, loopThreads("tenon-source-submission"));
      assertEquals(completions + 1, loopThreads("tenon-source-completion"));
    }

    assertEquals(submissions, loopThreads("tenon-source-submission"));
    assertEquals(completions, loopThreads("tenon-source-completion"));
  }

  @Test
  void pluginSdkContainsNoExecutableMainMethod() throws Exception {
    var classesRoot =
        Path.of(SourceProgram.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    var packageRoot = classesRoot.resolve("org/apache/bifromq/tenon/sdk");

    try (var classes = Files.walk(packageRoot)) {
      for (var classFile : classes.filter(path -> path.toString().endsWith(".class")).toList()) {
        var relative = classesRoot.relativize(classFile).toString();
        var className = relative.substring(0, relative.length() - 6).replace('/', '.');
        var type = Class.forName(className, false, SourceProgram.class.getClassLoader());
        for (var method : type.getDeclaredMethods()) {
          var executableMain =
              method.getName().equals("main")
                  && Modifier.isPublic(method.getModifiers())
                  && Modifier.isStatic(method.getModifiers())
                  && method.getReturnType() == void.class
                  && Arrays.equals(method.getParameterTypes(), new Class<?>[] {String[].class});
          assertFalse(
              executableMain, () -> "SDK contains executable entry point: " + type.getName());
        }
      }
    }
  }

  private static IngressRecord readSubmission(IpcQueue.Reader submission) throws Exception {
    var ingress = readUnreleasedSubmission(submission);
    submission.release(1);
    return ingress;
  }

  private static IngressRecord readUnreleasedSubmission(IpcQueue.Reader submission)
      throws Exception {
    var bytes = submission.tryRead();
    if (bytes.isEmpty()) {
      submission.waitReadable();
      bytes = submission.tryRead();
    }
    return IngressRecord.parseFrom(bytes.orElseThrow());
  }

  private static void complete(IpcQueue.Writer completion, long recordId) throws Exception {
    var reply =
        IngressCompletion.newBuilder()
            .setRecordId(recordId)
            .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
            .build()
            .toByteArray();
    assertTrue(completion.tryWrite(reply) instanceof IpcQueue.Committed);
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    var interrupted = false;
    while (latch.getCount() != 0) {
      try {
        latch.await();
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /** Counts the live threads that carry one exact loop name. */
  private static long loopThreads(String name) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.isAlive() && thread.getName().equals(name))
        .count();
  }

  /**
   * Waits until the Source's single Submission loop has parked on the doorbell every Submission
   * Queue shares.
   *
   * <p>The Queue header publishes the ordinal of the slot that loop parks in, and the Source's own
   * Region holds the armed word, so this waits on the same two addresses its peer rings.
   */
  private void waitForSubmissionWait(Path submissionPath, CompletionStage<AckCode> send)
      throws Exception {
    var loops = bells().loopsPath();
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (true) {
      if (QueueFixtures.writerIsArmed(submissionPath, loops)) {
        return;
      }
      if (send.toCompletableFuture().isDone()) {
        throw new AssertionError("Source send completed before waiting for Submission space");
      }
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Source submission loop did not wait for Queue space");
      }
      Thread.onSpinWait();
    }
  }

  private void createPair(long pending, long maximumRecordSize) throws Exception {
    createPair(0, pending, maximumRecordSize);
  }

  /**
   * Creates the Bell Regions a Core runtime creates before it launches this test's Source.
   *
   * <p>The Source's own Region holds exactly the two slots its Submission and Completion loops park
   * in, whatever the Channel count is, and the Flow's Region holds one slot per Channel, so the
   * fixture's Channel-side endpoints park in exactly the addresses the real Channel loop does for
   * the Queue pairs this test created.
   */
  private SideFixtures.SourceBells bells() throws IOException {
    if (bells == null) {
      bells = SideFixtures.SourceBells.create(directory, channelCount());
    }
    return bells;
  }

  private int channelCount() throws IOException {
    try (var files = Files.list(directory)) {
      return Math.toIntExact(
          files.filter(path -> path.getFileName().toString().startsWith("submission-")).count());
    }
  }

  private void createPair(int index, long pending, long maximumRecordSize) throws Exception {
    IpcQueue.create(
        directory.resolve("submission-" + index + ".queue"),
        IngressQueueLayout.submissionCapacity(pending, maximumRecordSize),
        maximumRecordSize);
    IpcQueue.create(
        directory.resolve("completion-" + index + ".queue"),
        IngressQueueLayout.completionCapacity(pending),
        IngressQueueLayout.COMPLETION_MAX_PAYLOAD_SIZE);
  }
}
