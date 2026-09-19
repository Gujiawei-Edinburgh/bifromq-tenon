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

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class PluginSdkApiContractTest {
  @Test
  void sourceAndSinkIsAnIndependentSharedOwner() {
    assertFalse(TenonSink.class.isAssignableFrom(TenonSourceAndSink.class));
    assertEquals(
        Set.of("start", "quiesce", "write", "close"),
        Arrays.stream(TenonSourceAndSink.class.getDeclaredMethods())
            .map(method -> method.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  void sourceAndSinkProgramOwnsItsEntireLifecycle() {
    assertEquals(
        Set.of("run", "config", "awaitShutdown"),
        Arrays.stream(SourceAndSinkProgram.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(method -> method.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  void sourceInterfacesOnlyCarryPayloadTypesTheyUse() {
    assertEquals(0, TenonSource.class.getTypeParameters().length);
    assertEquals(1, TenonSourceAndSink.class.getTypeParameters().length);
  }

  @Test
  void sourceQuiesceAndCloseAreDistinctRequiredOperations() {
    assertEquals(
        Set.of("start", "quiesce", "close"),
        Arrays.stream(TenonSource.class.getDeclaredMethods())
            .map(method -> method.getName())
            .collect(Collectors.toSet()));
    assertFalse(
        Arrays.stream(TenonSource.class.getDeclaredMethods())
            .anyMatch(method -> method.isDefault()));
  }

  @Test
  void programRuntimeRemainsAnSdkImplementationDetail() {
    assertFalse(Modifier.isPublic(PluginProgramRuntime.class.getModifiers()));
  }
}
