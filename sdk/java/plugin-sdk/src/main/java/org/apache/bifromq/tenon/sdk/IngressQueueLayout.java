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

import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat.DataCapacity;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;

/** Applies the Source Submission and Ingress Completion layout policy to generic IPC Queues. */
final class IngressQueueLayout {
  static final int COMPLETION_MAX_PAYLOAD_SIZE = 13;

  private IngressQueueLayout() {}

  /** Returns Submission data capacity for one Source channel. */
  static DataCapacity submissionCapacity(long maxPendingRecords, long maxRecordSizeBytes)
      throws IpcQueueFormatException {
    return IpcQueueFormat.dataCapacityForRecordLimit(maxPendingRecords, maxRecordSizeBytes);
  }

  /** Returns Completion data capacity paired with one Source channel. */
  static DataCapacity completionCapacity(long maxPendingRecords) throws IpcQueueFormatException {
    return IpcQueueFormat.dataCapacityForRecordLimit(
        maxPendingRecords, COMPLETION_MAX_PAYLOAD_SIZE);
  }

  /** Validates the Queue pair and returns its shared positive pending limit. */
  static long validatePair(
      DataCapacity submissionCapacity,
      long submissionMaxPayloadSize,
      DataCapacity completionCapacity,
      long completionMaxPayloadSize)
      throws IpcQueueFormatException {
    var maxPendingRecords =
        IpcQueueFormat.recordLimitFromDataCapacity(submissionCapacity, submissionMaxPayloadSize);
    IpcQueueFormat.validateRecordLimitLayout(
        completionCapacity,
        completionMaxPayloadSize,
        maxPendingRecords,
        COMPLETION_MAX_PAYLOAD_SIZE);
    return maxPendingRecords;
  }
}
