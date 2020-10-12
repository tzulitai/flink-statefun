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

package org.apache.flink.statefun.flink.core.reqreply;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.flink.core.httpfn.StateSpec;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction;
import org.apache.flink.statefun.flink.core.polyglot.generated.FromFunction.MissingPersistedValue;
import org.apache.flink.statefun.flink.core.polyglot.generated.ToFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public final class PersistedRemoteFunctionValues {

  @Persisted private final PersistedStateRegistry stateRegistry = new PersistedStateRegistry();

  private final Map<String, PersistedValue<byte[]>> managedStates;

  public PersistedRemoteFunctionValues(List<StateSpec> stateSpecs) {
    Objects.requireNonNull(stateSpecs);
    this.managedStates = new HashMap<>(stateSpecs.size());
    stateSpecs.forEach(this::createAndRegisterValueState);
  }

  void attachStateValues(ToFunction.InvocationBatchRequest.Builder batchBuilder) {
    managedStates.forEach(
        (stateName, stateHandle) -> {
          ToFunction.PersistedValue.Builder valueBuilder =
              ToFunction.PersistedValue.newBuilder().setStateName(stateName);

          byte[] stateValue = stateHandle.get();
          if (stateValue != null) {
            valueBuilder.setStateValue(ByteString.copyFrom(stateValue));
          }
          batchBuilder.addState(valueBuilder);
        });
  }

  void updateStateValues(List<FromFunction.PersistedValueMutation> valueMutations) {
    for (FromFunction.PersistedValueMutation mutate : valueMutations) {
      final String stateName = mutate.getStateName();
      switch (mutate.getMutationType()) {
        case DELETE:
          getStateHandleOrThrow(stateName).clear();
          ;
          break;
        case MODIFY:
          getStateHandleOrThrow(stateName).set(mutate.getStateValue().toByteArray());
          break;
        case UNRECOGNIZED:
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + mutate.getMutationType());
      }
    }
  }

  void addNewStateHandles(List<MissingPersistedValue> missingPersistedValues) {
    missingPersistedValues.forEach(this::createAndRegisterValueStateIfAbsent);
  }

  private void createAndRegisterValueStateIfAbsent(MissingPersistedValue missingPersistedValue) {
    final String stateName = missingPersistedValue.getStateName();

    if (!managedStates.containsKey(stateName)) {
      final PersistedValue<byte[]> stateValue =
          PersistedValue.of(
              stateName,
              byte[].class,
              sdkTtlExpiration(
                  missingPersistedValue.getExpireAfterMillis(),
                  missingPersistedValue.getExpireMode()));
      stateRegistry.registerValue(stateValue);
      managedStates.put(stateName, stateValue);
    }
  }

  private static Expiration sdkTtlExpiration(
      long expirationTtlMillis, MissingPersistedValue.ExpireMode expireMode) {
    switch (expireMode) {
      case AFTER_INVOKE:
        return Expiration.expireAfterReadingOrWriting(Duration.ofMillis(expirationTtlMillis));
      case AFTER_WRITE:
        return Expiration.expireAfterWriting(Duration.ofMillis(expirationTtlMillis));
      case NONE:
        return Expiration.none();
      default:
        throw new IllegalStateException("Safe guard");
    }
  }

  private void createAndRegisterValueState(StateSpec stateSpec) {
    final String stateName = stateSpec.name();

    final PersistedValue<byte[]> stateValue =
        PersistedValue.of(stateName, byte[].class, stateSpec.ttlExpiration());
    stateRegistry.registerValue(stateValue);
    managedStates.put(stateName, stateValue);
  }

  private PersistedValue<byte[]> getStateHandleOrThrow(String stateName) {
    final PersistedValue<byte[]> handle = managedStates.get(stateName);
    if (handle == null) {
      throw new IllegalStateException(
          "Accessing a non-existing remote function state: " + stateName);
    }
    return handle;
  }
}
