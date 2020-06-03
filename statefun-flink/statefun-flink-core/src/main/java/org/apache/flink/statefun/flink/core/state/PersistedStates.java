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
package org.apache.flink.statefun.flink.core.state;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class PersistedStates {

  static List<?> findReflectively(@Nullable Object instance) {
    PersistedStates visitor = new PersistedStates();
    visitor.visit(instance);
    return visitor.getPersistedStates();
  }

  private final List<Object> persistedStates = new ArrayList<>();

  private void visit(@Nullable Object instance) {
    if (instance == null) {
      return;
    }
    for (Field field : findAnnotatedFields(instance.getClass(), Persisted.class)) {
      visitField(instance, field);
    }
  }

  private List<Object> getPersistedStates() {
    return persistedStates;
  }

  private void visitField(@Nonnull Object instance, @Nonnull Field field) {
    if (Modifier.isStatic(field.getModifiers())) {
      throw new IllegalArgumentException(
          "Static persisted states are not legal in: "
              + field.getType()
              + " on "
              + instance.getClass().getName());
    }
    Object persistedState = getPersistedStateReflectively(instance, field);
    try {
      visitPersistedStateObject(persistedState);
    } catch (NullPersistedStateObjectException e) {
      throw new NullPersistedStateObjectException(
          "The field "
              + field
              + " of a "
              + instance.getClass().getName()
              + " contains a NULL state object. Either the field was not initialized, or it contains an inner nested object that is NULL.");
    }
  }

  private void visitPersistedStateObject(Object stateObject) {
    if (stateObject == null) {
      throw new NullPersistedStateObjectException();
    }

    final Class<?> stateObjectType = stateObject.getClass();
    if (isPersistedState(stateObjectType)) {
      persistedStates.add(stateObject);
    } else if (isCollectionOfPersistedStates(stateObjectType)) {
      final Collection<?> casted = (Collection<?>) stateObject;
      casted.forEach(this::visitPersistedStateObject);
    } else if (isMapOfPersistedStates(stateObjectType)) {
      final Map<?, ?> casted = (Map<?, ?>) stateObject;
      casted.values().forEach(this::visitPersistedStateObject);
    } else {
      // treat as an object containing @Persisted fields
      final List<?> innerFields = findReflectively(stateObject);
      persistedStates.addAll(innerFields);
    }
  }

  private static boolean isPersistedState(Class<?> fieldType) {
    return fieldType == PersistedValue.class
        || fieldType == PersistedTable.class
        || fieldType == PersistedAppendingBuffer.class;
  }

  private static boolean isCollectionOfPersistedStates(Class<?> fieldType) {
    return Collection.class.isAssignableFrom(fieldType);
  }

  private static boolean isMapOfPersistedStates(Class<?> fieldType) {
    return Map.class.isAssignableFrom(fieldType);
  }

  private static Object getPersistedStateReflectively(Object instance, Field persistedField) {
    try {
      persistedField.setAccessible(true);
      return persistedField.get(instance);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Unable access field " + persistedField.getName() + " of " + instance.getClass());
    }
  }

  private static Iterable<Field> findAnnotatedFields(
      Class<?> javaClass, Class<? extends Annotation> annotation) {
    Stream<Field> fields =
        definedFields(javaClass).filter(field -> field.getAnnotation(annotation) != null);

    return fields::iterator;
  }

  private static Stream<Field> definedFields(Class<?> javaClass) {
    if (javaClass == null || javaClass == Object.class) {
      return Stream.empty();
    }
    Stream<Field> selfMethods = Arrays.stream(javaClass.getDeclaredFields());
    Stream<Field> superMethods = definedFields(javaClass.getSuperclass());
    return Stream.concat(selfMethods, superMethods);
  }

  private static class NullPersistedStateObjectException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private NullPersistedStateObjectException() {
      super();
    }

    private NullPersistedStateObjectException(String msg) {
      super(msg);
    }
  }
}
