package org.apache.flink.statefun.sdk.java.slice;

import com.google.protobuf.ByteString;
import com.google.protobuf.MoreByteStrings;
import java.nio.ByteBuffer;

public final class Slices {
  private Slices() {}

  public static Slice wrap(ByteBuffer buffer) {
    return wrap(MoreByteStrings.wrap(buffer));
  }

  public static Slice wrap(byte[] bytes) {
    return wrap(MoreByteStrings.wrap(bytes, 0, bytes.length));
  }

  private static Slice wrap(ByteString bytes) {
    return new ByteStringSlice(bytes);
  }

  public static Slice wrap(byte[] bytes, int offset, int len) {
    return wrap(MoreByteStrings.wrap(bytes, offset, len));
  }

  public static Slice copyOf(byte[] bytes) {
    return wrap(ByteString.copyFrom(bytes));
  }

  public static Slice copyOf(byte[] bytes, int offset, int len) {
    return wrap(ByteString.copyFrom(bytes, offset, len));
  }
}
