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

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioDomainSocketChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.tenon.contracts.plugin.PluginLifecycleGrpc;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Attach;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PipelineToPlugin;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PluginToPipeline;

/** Owns the single UDS gRPC stream used by one Java Plugin Program. */
final class PluginControlConnection {
  private final ManagedChannel channel;
  private final EventLoopGroup eventLoop;
  private final StreamObserver<PluginToPipeline> outbound;
  private final AtomicReference<CompletableFuture<PipelineToPlugin>> nextCommand;
  private final AtomicBoolean finishing;

  private PluginControlConnection(
      ManagedChannel channel,
      EventLoopGroup eventLoop,
      StreamObserver<PluginToPipeline> outbound,
      AtomicReference<CompletableFuture<PipelineToPlugin>> nextCommand,
      AtomicBoolean finishing) {
    this.channel = channel;
    this.eventLoop = eventLoop;
    this.outbound = outbound;
    this.nextCommand = nextCommand;
    this.finishing = finishing;
  }

  static PluginControlConnection open(
      Path socket, byte[] launchId, Thread.UncaughtExceptionHandler failureHandler) {
    Objects.requireNonNull(socket, "socket");
    Objects.requireNonNull(launchId, "launchId");
    Objects.requireNonNull(failureHandler, "failureHandler");
    var nextCommand =
        new AtomicReference<CompletableFuture<PipelineToPlugin>>(new CompletableFuture<>());
    var finishing = new AtomicBoolean();
    var eventLoop = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    ManagedChannel channel = null;
    try {
      channel =
          NettyChannelBuilder.forAddress(UnixDomainSocketAddress.of(socket))
              .overrideAuthority("localhost")
              .channelType(NioDomainSocketChannel.class, UnixDomainSocketAddress.class)
              // Null removes gRPC's TCP-only default from the Unix-domain bootstrap.
              .withOption(ChannelOption.SO_KEEPALIVE, null)
              .eventLoopGroup(eventLoop)
              .usePlaintext()
              .build();
      var observer = new InboundObserver(nextCommand, finishing, failureHandler);
      var outbound = PluginLifecycleGrpc.newStub(channel).run(observer);
      outbound.onNext(
          PluginToPipeline.newBuilder()
              .setAttach(
                  Attach.newBuilder()
                      .setLaunchId(com.google.protobuf.ByteString.copyFrom(launchId)))
              .build());
      return new PluginControlConnection(channel, eventLoop, outbound, nextCommand, finishing);
    } catch (RuntimeException | Error error) {
      if (channel != null) {
        channel.shutdownNow();
      }
      eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
      throw error;
    }
  }

  PipelineToPlugin awaitCommand(CompletionStage<Void> localFailure) throws Exception {
    Objects.requireNonNull(localFailure, "localFailure");
    var slot = nextCommand.get();
    var outcome = new CompletableFuture<PipelineToPlugin>();
    slot.whenComplete(
        (command, failure) -> {
          if (failure == null) {
            outcome.complete(command);
          } else {
            outcome.completeExceptionally(failure);
          }
        });
    localFailure.whenComplete(
        (ignored, failure) ->
            outcome.completeExceptionally(
                failure == null
                    ? new IllegalStateException("Local Plugin failure completed normally")
                    : PluginProgramRuntime.unwrapFailure(failure)));
    try {
      var command = outcome.get();
      if (!nextCommand.compareAndSet(slot, new CompletableFuture<>())) {
        throw new IllegalStateException("Plugin command slot changed without being consumed");
      }
      return command;
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for a Plugin lifecycle command", error);
    } catch (ExecutionException error) {
      var failure = PluginProgramRuntime.unwrapFailure(error.getCause());
      if (failure instanceof Error fatal) throw fatal;
      if (failure instanceof Exception exception) throw exception;
      throw new RuntimeException(failure);
    }
  }

  void send(PluginToPipeline message) throws IOException {
    try {
      outbound.onNext(message);
    } catch (RuntimeException error) {
      throw new IOException("Failed to send a Plugin lifecycle message", error);
    }
  }

  void finish() throws IOException {
    if (!finishing.compareAndSet(false, true)) {
      throw new IllegalStateException("Plugin control stream is already finishing");
    }
    try {
      outbound.onCompleted();
    } catch (RuntimeException error) {
      throw new IOException("Failed to finish the Plugin control stream", error);
    }
    channel.shutdown();
    try {
      while (!channel.awaitTermination(1, TimeUnit.DAYS)) {
        // Pipeline owns the timeout and process termination boundary.
      }
      eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while closing the Plugin control stream", error);
    }
  }

  private static final class InboundObserver implements StreamObserver<PipelineToPlugin> {
    private final AtomicReference<CompletableFuture<PipelineToPlugin>> nextCommand;
    private final AtomicBoolean finishing;
    private final Thread.UncaughtExceptionHandler failureHandler;

    private InboundObserver(
        AtomicReference<CompletableFuture<PipelineToPlugin>> nextCommand,
        AtomicBoolean finishing,
        Thread.UncaughtExceptionHandler failureHandler) {
      this.nextCommand = nextCommand;
      this.finishing = finishing;
      this.failureHandler = failureHandler;
    }

    @Override
    public void onNext(PipelineToPlugin message) {
      if (!nextCommand.get().complete(message)) {
        fail(new IOException("Pipeline sent more than one pending lifecycle command"));
      }
    }

    @Override
    public void onError(Throwable failure) {
      if (!finishing.get()) {
        fail(new IOException("Plugin control stream failed", failure));
      }
    }

    @Override
    public void onCompleted() {
      if (!finishing.get()) {
        fail(new IOException("Plugin control stream ended before process shutdown"));
      }
    }

    private void fail(Throwable failure) {
      failureHandler.uncaughtException(Thread.currentThread(), failure);
    }
  }
}
