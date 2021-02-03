package org.apache.flink.statefun.sdk.java.types;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.statefun.sdk.java.TypeName;

public interface Type<T> {

  TypeName typeName();

  TypeSerializer<T> typeSerializer();

  default Set<TypeCharacteristics> typeCharacteristics() {
    return Collections.emptySet();
  }
}
