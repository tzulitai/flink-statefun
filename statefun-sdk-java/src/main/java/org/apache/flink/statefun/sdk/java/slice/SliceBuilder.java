package org.apache.flink.statefun.sdk.java.slice;

import java.util.Arrays;
import java.util.Objects;

public final class SliceBuilder {
  private byte[] buf;
  private int position;

  private SliceBuilder(int initialSize) {
    this.buf = new byte[initialSize];
    this.position = 0;
  }

  public void put(byte[] buffer, int offset, int len) {
    Objects.requireNonNull(buffer);
    if (offset < 0 || offset > buffer.length) {
      throw new IllegalArgumentException("Offset out of range " + offset);
    }
    if (len < 0) {
      throw new IllegalArgumentException("Negative length " + len);
    }
    ensureCapacity(position + len);
    System.arraycopy(buffer, offset, buf, position, len);
    position += len;
  }

  public Slice build() {
    return Slices.copyOf(buf, 0, position);
  }

  public Slice asSlice() {
    return Slices.wrap(buf, 0, position);
  }

  public void reset() {
    position = 0;
  }

  private void ensureCapacity(int newLength) {
    if (newLength <= buf.length) {
      this.buf = Arrays.copyOf(buf, 2 * newLength);
    }
  }
}
