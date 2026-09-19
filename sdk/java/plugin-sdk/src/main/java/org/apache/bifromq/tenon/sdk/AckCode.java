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

/** The terminal result of one Source send attempt. */
public enum AckCode {
  /** The current Tenon Document's completion boundary accepted this attempt. */
  OK,

  /** This attempt did not complete, but retrying the original upstream input may succeed. */
  RETRY,

  /**
   * The Source must slow down or retry later because the current channel has no admission space.
   */
  BACKPRESSURE,

  /**
   * The current payload or runtime state cannot process this input; immediate retry is unsuitable.
   */
  ERROR
}
