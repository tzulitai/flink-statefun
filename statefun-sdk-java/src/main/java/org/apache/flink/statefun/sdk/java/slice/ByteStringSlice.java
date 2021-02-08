package org.apache.flink.statefun.sdk.java.slice;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class ByteStringSlice implements Slice {
  private final ByteString byteString;

  public ByteStringSlice(ByteString bytes) {
    this.byteString = Objects.requireNonNull(bytes);
  }

  public ByteString byteString() {
    return byteString;
  }

  @Override
  public ByteBuffer asReadOnlyByteBuffer() {
    return byteString.asReadOnlyByteBuffer();
  }

  @Override
  public int readableBytes() {
    return byteString.size();
  }

  @Override
  public void copyTo(byte[] target, int targetOffset) {
    byteString.copyTo(target, targetOffset);
  }

  @Override
  public void copyTo(OutputStream outputStream) throws IOException {
    byteString.writeTo(outputStream);
  }

  @Override
  public void copyTo(ByteBuffer buffer) {
    byteString.copyTo(buffer);
  }

  @Override
  public byte[] toByteArray() {
    return byteString.toByteArray();
  }
}
