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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ${package}.payload.SourceRecordPayload;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

final class SourcePluginFactoryTest {
  @Test
  void startSendsConfiguredPayloadToConfiguredQueue() throws Exception {
    var sentQueue = new AtomicReference<Integer>();
    var sentPayload = new AtomicReference<SourceRecordPayload>();
    var result = new CompletableFuture<AckCode>();
    var config = new ObjectMapper().readTree("{\"message\":\"hello\",\"queueIndex\":1}");
    var source =
        new SourcePluginFactory()
            .create(
                config,
                2,
                (queue, payload) -> {
                  sentQueue.set(queue);
                  sentPayload.set(payload);
                  return result;
                });

    source.start();
    source.quiesce();

    assertFalse(result.isDone());
    assertEquals(1, sentQueue.get());
    assertEquals("hello", sentPayload.get().getMessage());
    result.complete(AckCode.OK);
    source.close();
  }

  @Test
  void createRejectsQueueOutsideTheCurrentPipeline() throws Exception {
    var config = new ObjectMapper().readTree("{\"message\":\"hello\",\"queueIndex\":1}");

    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SourcePluginFactory()
                    .create(
                        config,
                        1,
                        (queue, payload) -> CompletableFuture.completedFuture(AckCode.OK)));

    assertEquals("queueIndex must be less than Flow parallelism", error.getMessage());
  }
}
