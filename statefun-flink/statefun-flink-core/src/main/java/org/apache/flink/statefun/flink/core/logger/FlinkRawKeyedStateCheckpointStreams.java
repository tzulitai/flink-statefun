/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.statefun.flink.core.logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.util.ResourceGuard.Lease;

public final class FlinkRawKeyedStateCheckpointStreams implements KeyGroupCheckpointStreams {

  private final KeyedStateCheckpointOutputStream rawKeyedStateOutputStream;
  private final Iterator<Integer> keyGroups;

  public FlinkRawKeyedStateCheckpointStreams(
      KeyedStateCheckpointOutputStream rawKeyedStateOutputStream) {
    this.rawKeyedStateOutputStream = Objects.requireNonNull(rawKeyedStateOutputStream);
    this.keyGroups = rawKeyedStateOutputStream.getKeyGroupList().iterator();
  }

  @Override
  public Closeable acquireLease() {
    try {
      final Lease lease = rawKeyedStateOutputStream.acquireLease();
      return lease::close;
    } catch (IOException e) {
      throw new RuntimeException("Failed to acquire lease for raw keyed state stream.", e);
    }
  }

  @Override
  public boolean hasNext() {
    return keyGroups.hasNext();
  }

  @Override
  public KeyGroupCheckpointStream next() {
    final int nextKeyGroupId = keyGroups.next();
    try {
      rawKeyedStateOutputStream.startNewKeyGroup(nextKeyGroupId);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to start a new key group on raw keyed state stream for key group "
              + nextKeyGroupId,
          e);
    }
    return new KeyGroupCheckpointStream(rawKeyedStateOutputStream, nextKeyGroupId);
  }
}
