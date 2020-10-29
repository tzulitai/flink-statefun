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

import static org.apache.flink.util.Preconditions.checkState;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.statefun.flink.core.feedback.FeedbackConsumer;
import org.apache.flink.util.IOUtils;

public final class UnboundedFeedbackLogger<T> implements FeedbackLogger<T> {
  private final Supplier<KeyGroupStream<T>> supplier;
  private final ToIntFunction<T> keyGroupAssigner;
  private final Map<Integer, KeyGroupStream<T>> keyGroupStreams;

  @Nullable private KeyGroupCheckpointStreams keyGroupCheckpointStreams;
  private TypeSerializer<T> serializer;
  private Closeable snapshotLease;

  public UnboundedFeedbackLogger(
      Supplier<KeyGroupStream<T>> supplier,
      ToIntFunction<T> keyGroupAssigner,
      TypeSerializer<T> serializer) {
    this.supplier = Objects.requireNonNull(supplier);
    this.keyGroupAssigner = Objects.requireNonNull(keyGroupAssigner);
    this.serializer = Objects.requireNonNull(serializer);
    this.keyGroupStreams = new TreeMap<>();
  }

  @Override
  public void startLogging(KeyGroupCheckpointStreams keyGroupCheckpointStreams) {
    this.keyGroupCheckpointStreams = Objects.requireNonNull(keyGroupCheckpointStreams);
    this.snapshotLease = keyGroupCheckpointStreams.acquireLease();
  }

  @Override
  public void append(T message) {
    if (keyGroupCheckpointStreams == null) {
      //
      // we are not currently logging.
      //
      return;
    }
    KeyGroupStream<T> keyGroup = keyGroupStreamFor(message);
    keyGroup.append(message);
  }

  @Override
  public void commit() {
    try {
      flushToKeyedStateOutputStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      keyGroupStreams.clear();
      IOUtils.closeQuietly(snapshotLease);
      snapshotLease = null;
      keyGroupCheckpointStreams = null;
    }
  }

  private void flushToKeyedStateOutputStream() throws IOException {
    checkState(
        keyGroupCheckpointStreams != null, "Trying to flush envelopes not in a logging state");

    while (keyGroupCheckpointStreams.hasNext()) {
      final KeyGroupCheckpointStreams.KeyGroupCheckpointStream keyGroupStream =
          keyGroupCheckpointStreams.next();
      final int keyGroupId = keyGroupStream.keyGroupId();
      final DataOutputView outputView = new DataOutputViewStreamWrapper(keyGroupStream.get());

      Header.writeHeader(outputView);

      @Nullable KeyGroupStream<T> stream = keyGroupStreams.get(keyGroupId);
      if (stream == null) {
        KeyGroupStream.writeEmptyTo(outputView);
      } else {
        stream.writeTo(outputView);
      }
    }
  }

  public void replyLoggedEnvelops(InputStream rawKeyedStateInputs, FeedbackConsumer<T> consumer)
      throws Exception {
    DataInputView in =
        new DataInputViewStreamWrapper(Header.skipHeaderSilently(rawKeyedStateInputs));
    KeyGroupStream.readFrom(in, serializer, consumer);
  }

  @Nonnull
  private KeyGroupStream<T> keyGroupStreamFor(T target) {
    final int keyGroupId = keyGroupAssigner.applyAsInt(target);
    KeyGroupStream<T> keyGroup = keyGroupStreams.get(keyGroupId);
    if (keyGroup == null) {
      keyGroupStreams.put(keyGroupId, keyGroup = supplier.get());
    }
    return keyGroup;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(snapshotLease);
    snapshotLease = null;
    keyGroupCheckpointStreams = null;
    keyGroupStreams.clear();
  }

  @VisibleForTesting
  static final class Header {
    private static final int STATEFUN_VERSION = 0;
    private static final int STATEFUN_MAGIC = 710818519;
    private static final byte[] HEADER_BYTES = headerBytes();

    public static void writeHeader(DataOutputView target) throws IOException {
      target.write(HEADER_BYTES);
    }

    public static InputStream skipHeaderSilently(InputStream rawKeyedInput) throws IOException {
      byte[] header = new byte[HEADER_BYTES.length];
      PushbackInputStream input =
          new PushbackInputStream(new BufferedInputStream(rawKeyedInput), header.length);
      int bytesRead = input.read(header);
      if (bytesRead > 0 && !Arrays.equals(header, HEADER_BYTES)) {
        input.unread(header, 0, bytesRead);
      }
      return input;
    }

    private static byte[] headerBytes() {
      DataOutputSerializer out = new DataOutputSerializer(8);
      try {
        out.writeInt(STATEFUN_VERSION);
        out.writeInt(STATEFUN_MAGIC);
      } catch (IOException e) {
        throw new IllegalStateException("Unable to compute the header bytes");
      }
      return out.getCopyOfBuffer();
    }
  }
}
