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

import com.google.protobuf.MessageLite;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletion;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletionStatus;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressRecord;
import org.apache.bifromq.tenon.sdk.ipc.BellRegion;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;
import org.apache.bifromq.tenon.sdk.ipc.LoopBell;

/**
 * Internal owner of all Source Queue endpoints, admission, loops, and pending completions.
 *
 * <p>A Source opens one Queue pair per Channel but exactly two waiting loops: one writes every
 * Submission Queue and one reads every Completion Queue. The thread count is this SDK's local
 * decision and never follows the Channel count, so both loops publish one doorbell ordinal of their
 * own Region and every Queue endpoint points at the loop that serves it.
 */
final class SourceSession<P extends MessageLite> implements AutoCloseable {
  /** The Submission loop's doorbell: every Submission Queue publishes this ordinal. */
  private static final int SUBMISSION_LOOP_SLOT = 0;

  /** The Completion loop's doorbell: every Completion Queue publishes this ordinal. */
  private static final int COMPLETION_LOOP_SLOT = 1;

  private static final Pattern SUBMISSION = Pattern.compile("submission-(\\d+)\\.queue");
  private static final Pattern COMPLETION = Pattern.compile("completion-(\\d+)\\.queue");

  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final List<SourceChannel> channels;
  private final CompletableFuture<Void> failure;
  private final List<BellRegion> regions;
  private final LoopBell submissionBell;
  private final LoopBell completionBell;
  private final Thread submissionLoop;
  private final Thread completionLoop;

  private SourceSession(
      List<SourceChannel> channels,
      CompletableFuture<Void> failure,
      List<BellRegion> regions,
      LoopBell submissionBell,
      LoopBell completionBell) {
    this.channels = List.copyOf(channels);
    this.failure = failure;
    this.regions = regions;
    this.submissionBell = submissionBell;
    this.completionBell = completionBell;
    submissionLoop =
        Thread.ofPlatform()
            .daemon(true)
            .name("tenon-source-submission")
            .unstarted(this::pumpSubmissions);
    completionLoop =
        Thread.ofPlatform()
            .daemon(true)
            .name("tenon-source-completion")
            .unstarted(this::pumpCompletions);
  }

  /**
   * Opens and validates every continuous Source Queue pair in the existing directory, together with
   * the doorbells both loops park on.
   *
   * <p>{@code channelBellPath} is the Region of the Flow this Source feeds. Both loops ring it and
   * publish their own ordinals in {@code loops.bells} instead, so releasing a Submission or
   * committing a Completion wakes the exact Channel that is waiting for it. This side's own Region
   * holds one slot per loop, not per Channel: every Submission endpoint publishes the Submission
   * loop's ordinal and every Completion endpoint publishes the Completion loop's, whatever the
   * Channel count is.
   */
  static <P extends MessageLite> SourceSession<P> open(Path directory, Path channelBellPath)
      throws IOException {
    Objects.requireNonNull(directory, "workingDirectory");
    Objects.requireNonNull(channelBellPath, "channelBellPath");
    var regions = new ArrayList<BellRegion>(2);
    try {
      var indexes = discoverIndexes(directory);
      var loops = openRegion(directory.resolve(BellRegion.LOOPS_BELL_FILE_NAME), regions);
      var channelsRegion = openRegion(channelBellPath, regions);
      var submissionBell = loops.loopBell(SUBMISSION_LOOP_SLOT);
      var completionBell = loops.loopBell(COMPLETION_LOOP_SLOT);
      var channels = new ArrayList<SourceChannel>(indexes.size());
      var failure = new CompletableFuture<Void>();
      for (var index : indexes) {
        channels.add(
            SourceChannel.open(
                directory, index, failure, submissionBell, completionBell, channelsRegion));
      }
      assertConsistentLayout(channels);
      var session =
          new SourceSession<P>(
              channels, failure, List.copyOf(regions), submissionBell, completionBell);
      session.submissionLoop.start();
      session.completionLoop.start();
      return session;
    } catch (IOException | RuntimeException error) {
      for (var region : regions.reversed()) {
        region.close();
      }
      if (error instanceof IpcQueueFormatException) {
        throw new IOException("Source Queue layout is invalid", error);
      }
      throw error;
    }
  }

  private static BellRegion openRegion(Path path, List<BellRegion> regions) throws IOException {
    var region = BellRegion.open(path);
    regions.add(region);
    return region;
  }

  int parallelism() {
    return channels.size();
  }

  /** Returns the thread-safe sender bound to this session. */
  PayloadSender<P> sender() {
    return this::send;
  }

  /** Completes with the first fatal Queue or pump failure. */
  CompletionStage<Void> failure() {
    return failure;
  }

  /** Ends admission and wakes boundary waiters without running business cleanup here. */
  void fail(Throwable error) {
    stopAccepting();
    failure.completeExceptionally(error);
  }

  /** Reserves admission, synchronously encodes, and enqueues one send attempt. */
  CompletionStage<AckCode> send(int channelId, SourcePayloadEncoder encoder) {
    if (channelId < 0 || channelId >= channels.size()) {
      throw new IllegalArgumentException("Source channel id is out of range");
    }
    if (!accepting.get()) {
      return CompletableFuture.failedFuture(new SourceSessionClosedException());
    }
    return channels.get(channelId).send(encoder);
  }

  /** Stops new sends before the owning framework closes business resources. */
  void stopAccepting() {
    if (accepting.compareAndSet(true, false)) {
      channels.forEach(SourceChannel::stopAdmission);
    }
  }

  /** Establishes the final Submission commit boundary for every Source channel. */
  void quiesce() throws Exception {
    stopAccepting();
    for (var channel : channels) {
      channel.awaitSubmissionBoundary();
    }
  }

  @Override
  public void close() throws IOException {
    stopAccepting();
    for (var channel : channels) {
      channel.beginStop();
    }
    interruptLoops();
    for (var channel : channels) {
      channel.awaitEncoders();
    }
    joinLoops();
    if (failure.isCompletedExceptionally()) {
      throw new CompletionException(failure.exceptionNow());
    }
    for (var channel : channels) {
      channel.failOutstanding(channel.pendingFailure());
    }
    for (var channel : channels) {
      channel.close();
    }
    for (var region : regions) {
      region.close();
    }
  }

  /**
   * Writes every Channel's Submission Queue from the one loop that owns them.
   *
   * <p>Each Channel keeps its own enqueue order, and at most one admitted record per Channel waits
   * here for space, so a full Queue blocks nothing but its own Channel. Channels are served in a
   * fixed order and a Channel that always has space is drained first in every round, so fairness
   * across Channels is not promised.
   *
   * <p>Only stopping every Channel ends this loop. A quiesced Channel is handed over through the
   * Queue rather than by leaving the loop, so the one loop that owns every Channel is still there
   * to consume a later Channel's boundary.
   */
  private void pumpSubmissions() {
    // The one encoded record per Channel that is waiting for Queue space.
    var held = new byte[channels.size()][];
    try {
      while (!allStopped()) {
        var progressed = false;
        for (var index = 0; index < channels.size(); index++) {
          switch (plan(index, held)) {
            // Stopping drops what this loop still holds: close resolves the registered requests,
            // and a frame the Pipeline never received must not count as delivered.
            case DONE -> held[index] = null;
            // A plan that promised a write the Queue would not take leaves this Channel waiting
            // for its peer to free space, which the next round sees as WAIT.
            case WAIT -> {}
            case WRITE -> progressed |= write(index, held);
          }
        }
        if (!progressed) {
          // Park on the one doorbell every Submission Queue's peer rings, and re-read every
          // Channel after every wake.
          submissionBell.until(() -> submissionReady(held));
        }
      }
    } catch (IOException error) {
      fail(error);
    }
  }

  /** Reads every Channel's Completion Queue from the one loop that owns them. */
  private void pumpCompletions() {
    try {
      while (!allStopped()) {
        var progressed = false;
        for (var channel : channels) {
          while (true) {
            var record = channel.readCompletion();
            if (record.isEmpty()) {
              break;
            }
            var message = IngressCompletion.parseFrom(record.orElseThrow());
            // Even if stopping arrives after the read, an already-read result keeps its status;
            // close resolves the requests that are left.
            channel.releaseCompletion();
            channel.resolveCompletion(message.getRecordId(), toAck(message.getStatus()));
            progressed = true;
          }
        }
        if (progressed) {
          continue;
        }
        // Park on the one doorbell every Completion Queue's peer rings, and re-read every Channel
        // after every wake.
        completionBell.until(this::completionReady);
      }
    } catch (Exception error) {
      fail(error);
    }
  }

  /** What the Submission loop does with one Channel in one round. */
  private enum Plan {
    /** Write the record this loop holds, or pick up the Channel's next admitted one. */
    WRITE,
    /** The Queue is full, or this Channel has not admitted a record yet. */
    WAIT,
    /** Nothing is owed: the Channel is stopped, or it stopped with nothing left to hand over. */
    DONE
  }

  /**
   * Returns what the Submission loop owes one Channel in this round.
   *
   * <p>The loop body and the doorbell condition ask the same plan, so a Channel that frees Queue
   * space, admits a record, or reaches hand-over can never leave the loop parked with work owed.
   */
  private Plan plan(int index, byte[][] held) throws IOException {
    var channel = channels.get(index);
    if (channel.stopped()) {
      return Plan.DONE;
    }
    if (held[index] == null && channel.handedOver()) {
      return Plan.DONE;
    }
    if (held[index] != null) {
      // A held record is the one this loop owns until the Queue takes it whole.
      return channel.writable(held[index].length) ? Plan.WRITE : Plan.WAIT;
    }
    return channel.queued() ? Plan.WRITE : Plan.WAIT;
  }

  /**
   * Writes one Channel's owed record, and reports whether this round wrote.
   *
   * <p>The record is registered before the frame can be committed, so a Completion that returns
   * before this loop looks it up is not lost.
   */
  private boolean write(int index, byte[][] held) throws IOException {
    var channel = channels.get(index);
    if (held[index] != null) {
      if (!channel.commit(held[index])) {
        return false;
      }
      held[index] = null;
      return true;
    }
    var queued = channel.pollQueued();
    if (queued == null) {
      return false;
    }
    if (queued instanceof SourceChannel.SubmissionBarrier barrier) {
      // Everything this Channel admitted is committed, so its quiesce boundary is reached.
      barrier.reached().complete(null);
      return true;
    }
    var request = (SourceChannel.Pending) queued;
    var recordId = channel.nextRecordId();
    var record =
        IngressRecord.newBuilder().setRecordId(recordId).setPayload(request.payload()).build();
    var encodedSize = record.getSerializedSize();
    if (encodedSize < 0 || encodedSize > channel.maximumRecordSize()) {
      channel.resolve(request, AckCode.ERROR);
      return true;
    }
    var encoded = record.toByteArray();
    channel.register(recordId, request);
    held[index] = encoded;
    if (!channel.commit(encoded)) {
      return false;
    }
    held[index] = null;
    return true;
  }

  /**
   * Reports whether the Submission loop has a Channel it can write to now.
   *
   * <p>A Channel that is stopped or handed over owes nothing, so it is not work. Leaving the
   * predicate free of that state is what keeps the loop alive across quiesce: the boundary of a
   * Channel that has not been served yet still arrives on this loop's doorbell.
   */
  private boolean submissionReady(byte[][] held) throws IOException {
    for (var index = 0; index < channels.size(); index++) {
      if (plan(index, held) == Plan.WRITE) {
        return true;
      }
    }
    return false;
  }

  /** Reports whether the Completion loop has a Channel with a result waiting. */
  private boolean completionReady() throws IOException {
    for (var channel : channels) {
      if (channel.readable()) {
        return true;
      }
    }
    return false;
  }

  private boolean allStopped() {
    for (var channel : channels) {
      if (!channel.stopped()) {
        return false;
      }
    }
    return true;
  }

  private static AckCode toAck(IngressCompletionStatus status) {
    return switch (status) {
      case INGRESS_COMPLETION_STATUS_OK -> AckCode.OK;
      case INGRESS_COMPLETION_STATUS_RETRY -> AckCode.RETRY;
      case INGRESS_COMPLETION_STATUS_BACKPRESSURE -> AckCode.BACKPRESSURE;
      case INGRESS_COMPLETION_STATUS_ERROR -> AckCode.ERROR;
      case INGRESS_COMPLETION_STATUS_UNSPECIFIED, UNRECOGNIZED ->
          throw new IllegalArgumentException("Invalid Ingress completion status");
    };
  }

  private void joinLoops() throws IOException {
    try {
      submissionLoop.join();
      completionLoop.join();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while stopping Source loops", error);
    }
  }

  /**
   * Wakes both loops through their own doorbells.
   *
   * <p>A loop only ever parks in its own native wait, so the local interrupt is the one wake path,
   * and a completion callback already running on a loop thread is not interrupted.
   */
  private void interruptLoops() {
    try {
      submissionBell.interrupter().interrupt();
      completionBell.interrupter().interrupt();
    } catch (Exception error) {
      // A failed native wake cannot prove that the loops will exit; keep their mappings alive.
      throw new CompletionException(error);
    }
  }

  private CompletionStage<AckCode> send(int channelId, P payload) {
    return send(
        channelId,
        maxRecordBytes -> {
          var size = Objects.requireNonNull(payload, "payload").getSerializedSize();
          if (size < 0 || size > maxRecordBytes) {
            throw new IllegalArgumentException("Source payload exceeds the record byte limit");
          }
          return payload.toByteString();
        });
  }

  private static List<Integer> discoverIndexes(Path directory) throws IOException {
    Set<Integer> submissions = new HashSet<>();
    Set<Integer> completions = new HashSet<>();
    try (var files = Files.list(directory)) {
      for (var path : files.toList()) {
        var name = path.getFileName().toString();
        if (name.equals(BellRegion.LOOPS_BELL_FILE_NAME)) {
          continue;
        }
        if (!addIndex(name, SUBMISSION, submissions) && !addIndex(name, COMPLETION, completions)) {
          throw new IOException("Source working directory contains an unexpected file");
        }
      }
    }
    if (submissions.isEmpty() || !submissions.equals(completions)) {
      throw new IOException("Source Queue files do not form complete pairs");
    }
    var indexes = submissions.stream().sorted().toList();
    for (var expected = 0; expected < indexes.size(); expected++) {
      if (indexes.get(expected) != expected) {
        throw new IOException("Source Queue indexes must be continuous from zero");
      }
    }
    return indexes;
  }

  private static boolean addIndex(String name, Pattern pattern, Set<Integer> target)
      throws IOException {
    var matcher = pattern.matcher(name);
    if (matcher.matches()) {
      try {
        target.add(Integer.parseInt(matcher.group(1)));
      } catch (NumberFormatException error) {
        throw new IOException("Source Queue index is outside the supported range", error);
      }
      return true;
    }
    return false;
  }

  private static void assertConsistentLayout(List<SourceChannel> channels) throws IOException {
    var first = channels.getFirst();
    for (var channel : channels) {
      if (channel.pendingLimit() != first.pendingLimit()
          || channel.maximumRecordSize() != first.maximumRecordSize()) {
        throw new IOException("Source Queue channels use inconsistent capacity limits");
      }
    }
  }
}
