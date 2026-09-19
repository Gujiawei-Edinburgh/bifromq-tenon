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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.MessageLite;
import com.google.protobuf.StringValue;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletion;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletionStatus;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressRecord;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SourceQuiesceContractTest {
  @TempDir Path directory;

  @Test
  void quiesceStopsAdmissionBeforeCallingBusiness() throws Exception {
    var events = new ArrayList<String>();
    var sourceDirectory = Files.createDirectory(directory.resolve("source"));
    IpcQueue.create(
        sourceDirectory.resolve("submission-0.queue"),
        IngressQueueLayout.submissionCapacity(1, 1024),
        1024);
    IpcQueue.create(
        sourceDirectory.resolve("completion-0.queue"),
        IngressQueueLayout.completionCapacity(1),
        IngressQueueLayout.COMPLETION_MAX_PAYLOAD_SIZE);
    var bells = SideFixtures.SourceBells.create(sourceDirectory, 1);
    var session = SourceSession.<MessageLite>open(sourceDirectory, bells.channelsPath());
    var owner =
        new SourceProgramOwner<MessageLite>(
            session,
            new TenonSource() {
              @Override
              public void start() {}

              @Override
              public void quiesce() {
                events.add("business-quiesce");
                var result = session.sender().send(0, com.google.protobuf.StringValue.of("late"));
                assertInstanceOf(
                    SourceSessionClosedException.class,
                    assertThrows(
                            java.util.concurrent.CompletionException.class,
                            () -> result.toCompletableFuture().join())
                        .getCause());
              }

              @Override
              public void close() {
                events.add("close");
              }
            });
    owner.start();

    owner.quiesce();
    owner.shutdown();
    assertEquals(List.of("business-quiesce", "close"), events);
  }

  @Test
  void sessionCloseWaitsForAnAlreadyDeliveredCompletionCallback() throws Exception {
    var bells = createQueues();
    var callbackThread = new CompletableFuture<Thread>();
    var releaseCallback = new CountDownLatch(1);
    var closeThread = new CompletableFuture<Thread>();
    var session = SourceSession.<StringValue>open(directory, bells.channelsPath());
    try (var submission = bells.readSubmission(0);
        var completion = bells.writeCompletion(0);
        var executor = Executors.newSingleThreadExecutor()) {
      var closeRequested = new CountDownLatch(1);
      var closing =
          executor.submit(
              () -> {
                closeRequested.await();
                closeThread.complete(Thread.currentThread());
                session.close();
                return null;
              });
      var callback =
          session
              .sender()
              .send(0, StringValue.of("payload"))
              .thenApply(
                  ack -> {
                    callbackThread.complete(Thread.currentThread());
                    try {
                      assertTrue(releaseCallback.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException error) {
                      throw new AssertionError("Completion callback was interrupted", error);
                    }
                    return ack;
                  });
      try {
        session.quiesce();
        var record = IngressRecord.parseFrom(submission.tryRead().orElseThrow());
        submission.release(1);
        assertTrue(
            completion.tryWrite(
                    IngressCompletion.newBuilder()
                        .setRecordId(record.getRecordId())
                        .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                        .build()
                        .toByteArray())
                instanceof IpcQueue.Committed);
        var completionWorker = callbackThread.get(5, TimeUnit.SECONDS);
        closeRequested.countDown();
        var closingWorker = closeThread.get(5, TimeUnit.SECONDS);
        var threads = ManagementFactory.getThreadMXBean();
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
          assertFalse(closing.isDone(), "Session close returned before its Completion callback");
          var waitingOn = threads.getThreadInfo(closingWorker.threadId()).getLockInfo();
          // Platform Thread.join waits on the target Thread's monitor in the pinned JDK.
          if (waitingOn != null
              && waitingOn.getIdentityHashCode() == System.identityHashCode(completionWorker)) {
            break;
          }
          assertTrue(
              System.nanoTime() < deadline, "Session close did not join its Completion loop");
          Thread.yield();
        }
        assertFalse(closing.isDone());
        releaseCallback.countDown();
        closing.get(5, TimeUnit.SECONDS);
        assertEquals(AckCode.OK, callback.toCompletableFuture().get(5, TimeUnit.SECONDS));
      } finally {
        releaseCallback.countDown();
        closeRequested.countDown();
        closing.get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void quiesceWaitsForEnteredEncodingAndItsSubmissionCommit() throws Exception {
    var bells = createQueues();
    var encoderEntered = new CountDownLatch(1);
    var releaseEncoder = new CountDownLatch(1);
    var quiesceStarted = new CountDownLatch(1);
    var payload = StringValue.of("payload");
    var blockingPayload =
        (MessageLite)
            Proxy.newProxyInstance(
                MessageLite.class.getClassLoader(),
                new Class<?>[] {MessageLite.class},
                (proxy, method, arguments) ->
                    switch (method.getName()) {
                      case "getSerializedSize" -> payload.getSerializedSize();
                      case "toByteString" -> {
                        encoderEntered.countDown();
                        releaseEncoder.await();
                        yield payload.toByteString();
                      }
                      default ->
                          throw new AssertionError(
                              "Unexpected payload operation: " + method.getName());
                    });
    try (var session = SourceSession.<MessageLite>open(directory, bells.channelsPath());
        var submission = bells.readSubmission(0);
        var completion = bells.writeCompletion(0);
        var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var sending = executor.submit(() -> session.sender().send(0, blockingPayload));
      try {
        assertTrue(encoderEntered.await(1, TimeUnit.SECONDS));
        var quiesced =
            executor.submit(
                () -> {
                  quiesceStarted.countDown();
                  session.quiesce();
                  return null;
                });
        assertTrue(quiesceStarted.await(1, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> quiesced.get(100, TimeUnit.MILLISECONDS));
        releaseEncoder.countDown();

        var ack = sending.get(1, TimeUnit.SECONDS);
        quiesced.get(1, TimeUnit.SECONDS);
        var record = IngressRecord.parseFrom(submission.tryRead().orElseThrow());
        assertEquals("payload", StringValue.parseFrom(record.getPayload()).getValue());
        submission.release(1);
        assertTrue(
            completion.tryWrite(
                    IngressCompletion.newBuilder()
                        .setRecordId(record.getRecordId())
                        .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                        .build()
                        .toByteArray())
                instanceof IpcQueue.Committed);
        assertEquals(AckCode.OK, ack.toCompletableFuture().get(1, TimeUnit.SECONDS));
        assertInstanceOf(
            SourceSessionClosedException.class,
            assertThrows(
                    java.util.concurrent.CompletionException.class,
                    () ->
                        session
                            .sender()
                            .send(0, StringValue.of("late"))
                            .toCompletableFuture()
                            .join())
                .getCause());
      } finally {
        releaseEncoder.countDown();
      }
    }
  }

  private SideFixtures.SourceBells createQueues() throws Exception {
    IpcQueue.create(
        directory.resolve("submission-0.queue"),
        IngressQueueLayout.submissionCapacity(2, 1024),
        1024);
    IpcQueue.create(
        directory.resolve("completion-0.queue"),
        IngressQueueLayout.completionCapacity(2),
        IngressQueueLayout.COMPLETION_MAX_PAYLOAD_SIZE);
    return SideFixtures.SourceBells.create(directory, 1);
  }
}
