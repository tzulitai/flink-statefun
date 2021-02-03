package org.apache.flink.statefun.sdk.java.types;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.TypeName;

public final class SimpleType<T> implements Type<T> {

  @FunctionalInterface
  public interface Fn<I, O> {

    O apply(I input) throws Throwable;
  }

  public static <T> Type<T> simpleTypeFrom(
      TypeName typeName, Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
    return new SimpleType<>(typeName, serialize, deserialize, Collections.emptySet());
  }

  public static <T> Type<T> simpleImmutableTypeFrom(
      TypeName typeName, Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
    return new SimpleType<>(
        typeName, serialize, deserialize, EnumSet.of(TypeCharacteristics.IMMUTABLE_VALUES));
  }

  private final TypeName typeName;
  private final TypeSerializer<T> serializer;
  private final Set<TypeCharacteristics> typeCharacteristics;

  public SimpleType(
      TypeName typeName,
      Fn<T, byte[]> serialize,
      Fn<byte[], T> deserialize,
      Set<TypeCharacteristics> typeCharacteristics) {
    this.typeName = Objects.requireNonNull(typeName);
    this.serializer = new Serializer<>(serialize, deserialize);
    this.typeCharacteristics = Collections.unmodifiableSet(EnumSet.copyOf(typeCharacteristics));
  }

  @Override
  public TypeName typeName() {
    return typeName;
  }

  @Override
  public TypeSerializer<T> typeSerializer() {
    return serializer;
  }

  @Override
  public Set<TypeCharacteristics> typeCharacteristics() {
    return typeCharacteristics;
  }

  private static final class Serializer<T> implements TypeSerializer<T> {
    private final Fn<T, byte[]> serialize;
    private final Fn<byte[], T> deserialize;

    private Serializer(Fn<T, byte[]> serialize, Fn<byte[], T> deserialize) {
      this.serialize = Objects.requireNonNull(serialize);
      this.deserialize = Objects.requireNonNull(deserialize);
    }

    @Override
    public byte[] serialize(T value) {
      try {
        return serialize.apply(value);
      } catch (Throwable throwable) {
        throw new IllegalStateException(throwable);
      }
    }

    @Override
    public T deserialize(byte[] bytes) {
      try {
        return deserialize.apply(bytes);
      } catch (Throwable throwable) {
        throw new IllegalStateException(throwable);
      }
    }
  }
}
