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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

final class SourceApiContractTest {
  @Test
  void publicSourceAndFactoryApisStayMinimal() throws Exception {
    var sourceMethods = publicDeclaredMethods(TenonSource.class);
    assertEquals(Set.of("start", "quiesce", "close"), publicMethodNames(sourceMethods));
    for (var methodName : List.of("start", "quiesce", "close")) {
      var method = TenonSource.class.getMethod(methodName);
      assertEquals(void.class, method.getReturnType());
      assertArrayEquals(new Class<?>[0], method.getExceptionTypes());
    }

    var factoryMethods = publicDeclaredMethods(TenonSourceFactory.class);
    assertEquals(1, factoryMethods.size());
    var create =
        TenonSourceFactory.class.getMethod(
            "create", JsonNode.class, int.class, PayloadSender.class);
    assertEquals(TenonSource.class, create.getReturnType());
    assertArrayEquals(new Class<?>[] {Exception.class}, create.getExceptionTypes());
  }

  @Test
  void programApiExposesOnlyConfigAndLifecycleAssembly() throws Exception {
    var methods = publicDeclaredMethods(SourceProgram.class);
    assertEquals(3, methods.size());
    assertEquals(Set.of("run", "config", "awaitShutdown"), publicMethodNames(methods));
    assertArrayEquals(
        new Class<?>[] {String[].class},
        SourceProgram.class.getDeclaredMethod("run", String[].class).getParameterTypes());
    assertEquals(JsonNode.class, SourceProgram.class.getDeclaredMethod("config").getReturnType());
    assertTrue(
        Arrays.stream(SourceProgram.class.getDeclaredConstructors())
            .noneMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
  }

  @Test
  void queueSessionStaysInsideTheSdk() {
    assertFalse(Modifier.isPublic(SourceSession.class.getModifiers()));
  }

  private static List<Method> publicDeclaredMethods(Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(method -> Modifier.isPublic(method.getModifiers()))
        .toList();
  }

  private static Set<String> publicMethodNames(List<Method> methods) {
    return methods.stream().map(Method::getName).collect(Collectors.toUnmodifiableSet());
  }
}
