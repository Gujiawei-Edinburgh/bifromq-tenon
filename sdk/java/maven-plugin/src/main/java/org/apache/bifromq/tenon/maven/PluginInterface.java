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

package org.apache.bifromq.tenon.maven;

enum PluginInterface {
  SOURCE,
  SINK,
  SOURCE_AND_SINK;

  static PluginInterface parse(String value) {
    for (var pluginInterface : values()) {
      if (pluginInterface.manifestValue().equals(value)) {
        return pluginInterface;
      }
    }
    throw new IllegalArgumentException("Plugin interface must be source, sink, or source-and-sink");
  }

  String manifestValue() {
    return switch (this) {
      case SOURCE -> "source";
      case SINK -> "sink";
      case SOURCE_AND_SINK -> "source-and-sink";
    };
  }
}
