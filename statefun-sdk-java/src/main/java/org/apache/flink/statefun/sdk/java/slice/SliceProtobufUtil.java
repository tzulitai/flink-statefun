package org.apache.flink.statefun.sdk.java.slice;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class SliceProtobufUtil {
  private SliceProtobufUtil() {}

  public static <T> T parseFrom(Parser<T> parser, Slice slice)
      throws InvalidProtocolBufferException {
    if (slice instanceof ByteStringSlice) {
      ByteString byteString = ((ByteStringSlice) slice).byteString();
      return parser.parseFrom(byteString);
    }
    return parser.parseFrom(slice.asReadOnlyByteBuffer());
  }

  public static Slice toSlice(Message message) {
    return Slices.wrap(message.toByteArray());
  }
}
