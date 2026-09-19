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
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bifromq.tenon.sdk.ipc.BellRegion;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;
import org.apache.bifromq.tenon.sdk.ipc.LoopBell;

/**
 * Owns admission and both Queue endpoints of one Source Channel.
 *
 * <p>A Channel holds state and endpoints only. The Source's single Submission loop writes every
 * Channel's Submission Queue and its single Completion loop reads every Channel's Completion Queue,
 * so the Channel count never changes the thread count while each Channel keeps its own admission
 * and enqueue order.
 */
final class SourceChannel implements AutoCloseable {
  private final IpcQueue.Writer submission;
  private final IpcQueue.Reader completion;
  private final LoopBell submissionBell;
  private final long pendingLimit;
  private final Semaphore permits;
  private final LinkedBlockingQueue<Submission> preflight;
  private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
  private final ReentrantLock lifecycleLock = new ReentrantLock();
  private final Condition encodersIdle = lifecycleLock.newCondition();
  private volatile boolean accepting = true;
  private volatile boolean stopping;
  private int activeEncoders;
  private final CompletableFuture<Void> failure;

  private SourceChannel(
      IpcQueue.Writer submission,
      IpcQueue.Reader completion,
      LoopBell submissionBell,
      long pendingLimit,
      CompletableFuture<Void> failure) {
    this.submission = submission;
    this.completion = completion;
    this.submissionBell = submissionBell;
    this.pendingLimit = pendingLimit;
    this.failure = failure;
    permits = new Semaphore(Math.toIntExact(pendingLimit));
    preflight = new LinkedBlockingQueue<>(Math.addExact(Math.toIntExact(pendingLimit), 1));
  }

  static SourceChannel open(
      Path directory,
      int index,
      CompletableFuture<Void> failure,
      LoopBell submissionBell,
      LoopBell completionBell,
      BellRegion channelsRegion)
      throws IOException, IpcQueueFormatException {
    var submission =
        IpcQueue.openWriter(
            directory.resolve("submission-" + index + ".queue"), submissionBell, channelsRegion);
    try {
      var completion =
          IpcQueue.openReader(
              directory.resolve("completion-" + index + ".queue"), completionBell, channelsRegion);
      var pendingLimit =
          IngressQueueLayout.validatePair(
              submission.capacity(), submission.maxPayloadSize(),
              completion.capacity(), completion.maxPayloadSize());
      return new SourceChannel(submission, completion, submissionBell, pendingLimit, failure);
    } catch (IOException | RuntimeException error) {
      submission.close();
      throw error;
    }
  }

  long pendingLimit() {
    return pendingLimit;
  }

  long maximumRecordSize() {
    return submission.maxPayloadSize();
  }

  CompletionStage<AckCode> send(SourcePayloadEncoder encoder) {
    var admission = beginEncoding();
    if (admission == EncodingAdmission.CLOSED) {
      return CompletableFuture.failedStage(new SourceSessionClosedException());
    }
    if (admission == EncodingAdmission.FULL) {
      return CompletableFuture.completedStage(AckCode.BACKPRESSURE);
    }

    ByteString payload;
    try {
      payload = encoder.encode(submission.maxPayloadSize());
    } catch (Exception error) {
      finishEncoding();
      permits.release();
      return CompletableFuture.completedStage(AckCode.ERROR);
    }

    var result = new CompletableFuture<AckCode>();
    var accepted = preflight.offer(new Pending(payload, result));
    finishEncoding();
    if (!accepted) {
      permits.release();
      var error = new IllegalStateException("Source preflight Queue exceeded its admission limit");
      result.completeExceptionally(error);
      failAll(error);
      return result;
    }
    // The one Submission loop parks with nothing left to write, so the admitting thread rings the
    // doorbell it waits on. A needless ring is a spurious wake.
    ringSubmissionLoop();
    return result;
  }

  /**
   * Pops the next queued item in this Channel's enqueue order.
   *
   * <p>The Submission loop only pops while it holds no record of this Channel, so an admitted
   * request is never overtaken by a later one.
   */
  Submission pollQueued() {
    return preflight.poll();
  }

  /** Reports whether this Channel has an admitted request the Submission loop has not taken. */
  boolean queued() {
    return !preflight.isEmpty();
  }

  /**
   * Reports whether this Channel left the Submission loop: it stopped, or it stopped accepting with
   * nothing admitted left to hand over.
   */
  boolean handedOver() {
    lifecycleLock.lock();
    try {
      return !accepting && activeEncoders == 0 && preflight.isEmpty();
    } finally {
      lifecycleLock.unlock();
    }
  }

  boolean writable(int recordLength) throws IpcQueueFormatException {
    return submission.writable(recordLength);
  }

  long nextRecordId() throws IpcQueueFormatException {
    return submission.nextWriteSequence();
  }

  /**
   * Registers the visible result before the frame can be committed, so a Completion that returns
   * before the loop finishes looking it up is kept.
   */
  void register(long recordId, Pending request) {
    pending.put(recordId, request);
  }

  boolean commit(byte[] record) throws IOException, IpcQueueFormatException {
    return submission.tryWrite(record) instanceof IpcQueue.Committed;
  }

  /** Completes one admitted request normally and returns its admission. */
  void resolve(Pending request, AckCode ack) {
    permits.release();
    request.result().complete(ack);
  }

  /** Reports whether one Completion Queue record is already waiting. */
  boolean readable() throws IpcQueueFormatException {
    return completion.readable();
  }

  Optional<ByteString> readCompletion() throws IpcQueueFormatException {
    return completion.tryRead();
  }

  void releaseCompletion() throws IOException, IpcQueueFormatException {
    completion.release(1);
  }

  /** Completes the request one Completion record names, if this Channel is still waiting for it. */
  void resolveCompletion(long recordId, AckCode ack) {
    var request = pending.remove(recordId);
    if (request != null) {
      permits.release();
      request.result().complete(ack);
    }
  }

  void stopAdmission() {
    lifecycleLock.lock();
    try {
      accepting = false;
    } finally {
      lifecycleLock.unlock();
    }
  }

  /** Waits until every admitted send is committed or failed and no later commit is possible. */
  void awaitSubmissionBoundary() throws Exception {
    awaitEncoders();
    try {
      var barrier = new SubmissionBarrier();
      lifecycleLock.lock();
      try {
        if (stopping) {
          var failure = pendingFailure();
          if (failure instanceof Error fatal) throw fatal;
          if (failure instanceof Exception exception) throw exception;
          throw new RuntimeException(failure);
        }
        if (!preflight.offer(barrier)) {
          throw new IllegalStateException(
              "Source quiesce barrier exceeded reserved Queue capacity");
        }
      } finally {
        lifecycleLock.unlock();
      }
      // The barrier reaches the loop only through the doorbell that loop parks on.
      ringSubmissionLoop();
      CompletableFuture.anyOf(failure, barrier.reached()).join();
    } catch (CompletionException error) {
      var failure = PluginProgramRuntime.unwrapFailure(error);
      if (failure instanceof Error fatal) {
        throw fatal;
      }
      if (failure instanceof Exception exception) {
        throw exception;
      }
      throw new RuntimeException(failure);
    }
  }

  /** Ends this Channel's participation in the Source session. */
  void beginStop() {
    lifecycleLock.lock();
    try {
      accepting = false;
      stopping = true;
    } finally {
      lifecycleLock.unlock();
    }
  }

  boolean stopped() {
    return stopping;
  }

  void failAll(Throwable error) {
    beginStop();
    failure.completeExceptionally(error);
  }

  /**
   * Resolves everything this Channel still holds after both loops ended.
   *
   * <p>A registered request whose frame was never committed must not count as delivered, and a
   * request the loop still held is registered, so failing the registered set covers both.
   */
  void failOutstanding(Throwable failure) {
    for (var submission = preflight.poll(); submission != null; submission = preflight.poll()) {
      if (submission instanceof Pending request) {
        permits.release();
        request.result().completeExceptionally(failure);
      } else {
        ((SubmissionBarrier) submission).reached().completeExceptionally(failure);
      }
    }
    for (var entry : pending.entrySet()) {
      if (pending.remove(entry.getKey(), entry.getValue())) {
        permits.release();
        entry.getValue().result().completeExceptionally(failure);
      }
    }
  }

  @Override
  public void close() {
    submission.close();
    completion.close();
  }

  private void ringSubmissionLoop() {
    try {
      submissionBell.ring();
    } catch (IOException error) {
      // A failed native wake cannot be told apart from a lost one, so the loop it would have woken
      // is treated as failed rather than left parked.
      failAll(error);
    }
  }

  Throwable pendingFailure() {
    return failure.isCompletedExceptionally()
        ? failure.exceptionNow()
        : new SourceSessionClosedException();
  }

  private EncodingAdmission beginEncoding() {
    if (!accepting) {
      return EncodingAdmission.CLOSED;
    }
    if (!permits.tryAcquire()) {
      return accepting ? EncodingAdmission.FULL : EncodingAdmission.CLOSED;
    }
    lifecycleLock.lock();
    try {
      if (!accepting) {
        permits.release();
        return EncodingAdmission.CLOSED;
      }
      activeEncoders++;
      return EncodingAdmission.ACCEPTED;
    } finally {
      lifecycleLock.unlock();
    }
  }

  private void finishEncoding() {
    lifecycleLock.lock();
    try {
      activeEncoders--;
      if (activeEncoders == 0) {
        encodersIdle.signalAll();
      }
    } finally {
      lifecycleLock.unlock();
    }
  }

  void awaitEncoders() throws IOException {
    lifecycleLock.lock();
    try {
      while (activeEncoders != 0) {
        encodersIdle.await();
      }
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for Source encoders", error);
    } finally {
      lifecycleLock.unlock();
    }
  }

  private enum EncodingAdmission {
    ACCEPTED,
    CLOSED,
    FULL
  }

  sealed interface Submission permits Pending, SubmissionBarrier {}

  record Pending(ByteString payload, CompletableFuture<AckCode> result) implements Submission {}

  record SubmissionBarrier(CompletableFuture<Void> reached) implements Submission {
    private SubmissionBarrier() {
      this(new CompletableFuture<>());
    }
  }
}
