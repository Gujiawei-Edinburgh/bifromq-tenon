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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

final class IngressQueueLayoutContractTest {
  @Test
  void sourceParametersProduceAndRecoverOneExactQueuePair() throws Exception {
    var submissionCapacity = IngressQueueLayout.submissionCapacity(3, 1016);
    var completionCapacity = IngressQueueLayout.completionCapacity(3);

    assertEquals(4096, submissionCapacity.bytes());
    assertEquals(4288, submissionCapacity.fileLength());
    assertEquals(96, completionCapacity.bytes());
    assertEquals(288, completionCapacity.fileLength());
    assertEquals(
        3, IngressQueueLayout.validatePair(submissionCapacity, 1016, completionCapacity, 13));
  }

  @Test
  void pairValidationRejectsEveryIndependentCapacityMismatch() throws Exception {
    var submissionCapacity = IngressQueueLayout.submissionCapacity(3, 1016);
    var completionCapacity = IngressQueueLayout.completionCapacity(3);

    assertErrorCode(
        "ipc.queue.capacity_invalid",
        () ->
            IngressQueueLayout.validatePair(
                IpcQueueFormat.DataCapacity.of(4088), 1016, completionCapacity, 13),
        "Submission capacity must encode an exact pending limit");
    assertErrorCode(
        "ipc.queue.capacity_invalid",
        () -> IngressQueueLayout.validatePair(submissionCapacity, 1016, completionCapacity, 12),
        "Completion payload size must be exact");
    assertErrorCode(
        "ipc.queue.capacity_invalid",
        () ->
            IngressQueueLayout.validatePair(
                submissionCapacity, 1016, IpcQueueFormat.DataCapacity.of(120), 13),
        "Completion capacity must encode the same pending limit");
    assertErrorCode(
        "ipc.queue.capacity_invalid",
        () -> IngressQueueLayout.submissionCapacity(Long.MAX_VALUE, 1016),
        "Capacity arithmetic must not overflow");
  }

  private static void assertErrorCode(String expected, Executable operation, String name) {
    var error = assertThrows(IpcQueueFormatException.class, operation, name);
    assertEquals(expected, error.code(), name);
  }
}
