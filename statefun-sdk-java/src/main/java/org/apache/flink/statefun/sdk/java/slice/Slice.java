package org.apache.flink.statefun.sdk.java.slice;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface Slice {

  int readableBytes();

  void copyTo(ByteBuffer target);

  void copyTo(byte[] target, int targetOffset);

  void copyTo(OutputStream outputStream) throws IOException;

  ByteBuffer asReadOnlyByteBuffer();

  byte[] toByteArray();
}
