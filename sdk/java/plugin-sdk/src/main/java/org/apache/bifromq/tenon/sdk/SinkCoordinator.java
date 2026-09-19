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

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.bifromq.tenon.sdk.ipc.BellInterrupter;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;
import org.apache.bifromq.tenon.sdk.ipc.LoopBell;

/**
 * Owns the only Egress loop of one Sink Instance: it serves every Egress Queue of that Instance in
 * turn instead of giving each Queue its own thread.
 *
 * <p>One loop means one doorbell, so every Queue this Instance reads publishes the same slot
 * ordinal and the parked condition covers every one of them. Batching, the local read position and
 * the continuous-success prefix stay per Queue: one Queue can be mid-batch while another is idle.
 */
final class SinkCoordinator<P extends MessageLite> implements AutoCloseable {
  private final Object admissionLock = new Object();
  private final LoopBell bell;
  private final BellInterrupter waitInterrupter;
  private final Consumer<Throwable> localFailureHandler;
  private final Parser<P> payloadParser;
  private final List<Egress<P>> egress;
  private final AtomicReference<Throwable> terminalFailure = new AtomicReference<>();
  private final ConcurrentLinkedQueue<BatchCompletion> completions = new ConcurrentLinkedQueue<>();
  private final CountDownLatch activation = new CountDownLatch(1);
  private final Thread thread;
  private volatile boolean stopping;

  /** One Egress Queue the Instance reads and the business write that consumes its batches. */
  record Input<P extends MessageLite>(
      IpcQueue.Reader reader, Function<List<P>, CompletionStage<Void>> writer) {}

  SinkCoordinator(
      LoopBell bell,
      List<Input<P>> inputs,
      Parser<P> payloadParser,
      Consumer<Throwable> localFailureHandler) {
    this.bell = bell;
    this.waitInterrupter = bell.interrupter();
    this.localFailureHandler = localFailureHandler;
    this.payloadParser = payloadParser;
    this.egress =
        inputs.stream().map(input -> new Egress<>(input.reader(), input.writer())).toList();
    this.thread = Thread.ofPlatform().name("tenon-sink-egress").unstarted(this::run);
  }

  void startPaused() {
    thread.start();
  }

  void activate() {
    activation.countDown();
  }

  void requestStop() throws IOException {
    synchronized (admissionLock) {
      if (stopping || terminalFailure.get() != null) {
        return;
      }
      stopping = true;
    }
    activation.countDown();
    try {
      waitInterrupter.interrupt();
    } catch (IOException error) {
      abortOnWakeFailure(error);
      throw error;
    }
  }

  void awaitTermination() throws Exception {
    thread.join();
    var failure = terminalFailure.get();
    if (failure != null) {
      if (failure instanceof Exception exception) {
        throw exception;
      }
      if (failure instanceof Error error) {
        throw error;
      }
      throw new RuntimeException(failure);
    }
  }

  @Override
  public void close() {
    for (var queue : egress) {
      queue.reader.close();
    }
  }

  private void run() {
    try {
      activation.await();
      coordinate();
    } catch (Throwable error) {
      localFailureHandler.accept(recordTerminalFailure(error));
    }
  }

  private void coordinate() throws Throwable {
    while (true) {
      processCompletions();
      var failure = terminalFailure.get();
      if (failure != null) {
        throw failure;
      }
      if (stopping) {
        return;
      }

      var progressed = false;
      for (var queue : egress) {
        progressed |= advance(queue);
        if (stopping || terminalFailure.get() != null) {
          break;
        }
      }
      if (progressed || stopping || terminalFailure.get() != null) {
        continue;
      }

      // Every Queue is drained, so the loop parks on the one doorbell all of their peers ring, and
      // it rechecks every Queue after every wake.
      // Queues are served in a fixed order and fairness is not promised: a Queue that always has
      // data is drained first in every round.
      bell.until(this::anyReadable);
    }
  }

  /** Serves one Egress Queue once, and reports whether it moved a record or a batch. */
  private boolean advance(Egress<P> queue) throws Throwable {
    var progressed = false;
    while (true) {
      Optional<ByteString> record;
      try {
        record = queue.reader.tryReadCommittedSnapshot();
      } catch (Throwable error) {
        recordReadFailureUnlessSuperseded(error);
        return progressed;
      }
      if (record.isPresent()) {
        P decoded;
        try {
          decoded = SinkRecordDecoder.decode(record.orElseThrow(), payloadParser);
        } catch (Throwable error) {
          recordReadFailureUnlessSuperseded(error);
          return progressed;
        }
        synchronized (admissionLock) {
          if (stopping || terminalFailure.get() != null) {
            return progressed;
          }
        }
        queue.records.add(decoded);
        progressed = true;
        continue;
      }
      if (!queue.records.isEmpty()) {
        switch (submit(queue, Collections.unmodifiableList(queue.records))) {
          case SUBMITTED -> queue.records = new ArrayList<>();
          case STOPPING, FAILURE_PENDING -> {
            return progressed;
          }
        }
        progressed = true;
        continue;
      }
      return progressed;
    }
  }

  /** Reports whether any Queue this loop subscribed to has a committed record waiting. */
  private boolean anyReadable() throws IOException {
    for (var queue : egress) {
      if (queue.reader.readable()) {
        return true;
      }
    }
    return false;
  }

  private SubmissionOutcome submit(Egress<P> queue, List<P> records) {
    PendingBatch batch;
    synchronized (admissionLock) {
      if (stopping) {
        return SubmissionOutcome.STOPPING;
      }
      if (terminalFailure.get() != null) {
        return SubmissionOutcome.FAILURE_PENDING;
      }
      batch = new PendingBatch(records.size());
      // Registering the batch is the admission point. Shutdown and earlier failures cannot
      // overtake it, while callbacks never wait for the user-owned write method body.
      queue.pending.addLast(batch);
    }
    var result = Objects.requireNonNull(queue.writer.apply(records), "Sink write returned null");
    result.whenComplete((ignored, failure) -> publishCompletion(batch, failure));
    return SubmissionOutcome.SUBMITTED;
  }

  private void publishCompletion(PendingBatch batch, Throwable failure) {
    synchronized (admissionLock) {
      if (terminalFailure.get() != null || stopping) {
        return;
      }
      completions.add(new BatchCompletion(batch, failure));
      if (failure != null) {
        terminalFailure.set(failure);
      }
    }
    try {
      waitInterrupter.interrupt();
    } catch (Throwable error) {
      abortOnWakeFailure(error);
    }
  }

  private void abortOnWakeFailure(Throwable error) {
    // A failed native wake is propagated to the Program boundary; the process is the recovery
    // boundary when this coordinator cannot be proven to terminate.
    localFailureHandler.accept(recordTerminalFailure(error));
  }

  private Throwable recordTerminalFailure(Throwable failure) {
    synchronized (admissionLock) {
      var primary = terminalFailure.get();
      if (primary == null) {
        terminalFailure.set(failure);
        return failure;
      }
      return primary;
    }
  }

  private void recordReadFailureUnlessSuperseded(Throwable failure) {
    synchronized (admissionLock) {
      if (terminalFailure.get() == null) {
        terminalFailure.set(failure);
      }
    }
  }

  private void processCompletions() throws IOException, IpcQueueFormatException {
    for (var completion = completions.poll(); completion != null; completion = completions.poll()) {
      if (completion.batch().state != BatchState.PENDING) {
        throw new IllegalStateException("Sink batch completed more than once");
      }
      if (completion.failure() == null) {
        completion.batch().state = BatchState.SUCCEEDED;
      } else {
        completion.batch().state = BatchState.FAILED;
      }
    }

    for (var queue : egress) {
      var releaseCount = 0;
      while (!queue.pending.isEmpty() && queue.pending.getFirst().state == BatchState.SUCCEEDED) {
        releaseCount = Math.addExact(releaseCount, queue.pending.removeFirst().recordCount);
      }
      if (releaseCount != 0) {
        queue.reader.release(releaseCount);
      }
    }
  }

  /** One Egress Queue as the Instance's single loop sees it. */
  private static final class Egress<P extends MessageLite> {
    private final IpcQueue.Reader reader;
    private final Function<List<P>, CompletionStage<Void>> writer;
    private final ArrayDeque<PendingBatch> pending = new ArrayDeque<>();
    private List<P> records = new ArrayList<>();

    private Egress(IpcQueue.Reader reader, Function<List<P>, CompletionStage<Void>> writer) {
      this.reader = reader;
      this.writer = writer;
    }
  }

  private enum BatchState {
    PENDING,
    SUCCEEDED,
    FAILED
  }

  private enum SubmissionOutcome {
    SUBMITTED,
    STOPPING,
    FAILURE_PENDING
  }

  private static final class PendingBatch {
    private final int recordCount;
    private BatchState state = BatchState.PENDING;

    private PendingBatch(int recordCount) {
      this.recordCount = recordCount;
    }
  }

  private record BatchCompletion(PendingBatch batch, Throwable failure) {}
}
