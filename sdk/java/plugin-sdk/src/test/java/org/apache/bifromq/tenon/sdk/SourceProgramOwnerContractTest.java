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

import com.google.protobuf.MessageLite;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SourceProgramOwnerContractTest {
  @TempDir Path directory;

  @Test
  void oneSourceStartsOnceAndClosesOnce() throws Exception {
    var events = new ArrayList<String>();
    var owner = owner(source(events, false));

    owner.start();
    owner.quiesce();
    owner.shutdown();

    assertEquals(List.of("start", "quiesce", "close"), events);
  }

  private SourceProgramOwner<MessageLite> owner(TenonSource source) throws Exception {
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
    return new SourceProgramOwner<>(
        SourceSession.open(sourceDirectory, bells.channelsPath()), source);
  }

  private static TenonSource source(List<String> events, boolean unused) {
    return new TenonSource() {
      @Override
      public void start() {
        events.add("start");
      }

      @Override
      public void quiesce() {
        events.add("quiesce");
      }

      @Override
      public void close() {
        events.add("close");
      }
    };
  }
}
