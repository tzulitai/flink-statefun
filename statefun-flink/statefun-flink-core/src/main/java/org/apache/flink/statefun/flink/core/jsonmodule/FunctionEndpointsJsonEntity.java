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

package org.apache.flink.statefun.flink.core.jsonmodule;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.StreamSupport;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.flink.core.httpfn.DynamicHttpFunctionProvider;
import org.apache.flink.statefun.flink.core.httpfn.HttpFunctionEndpointsSpec;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.util.TimeUtils;

/** Created by tzulitai on 2020/10/19. */
public class FunctionEndpointsJsonEntity implements JsonEntity {

  private static final JsonPointer FUNCTION_ENDPOINTS_POINTER =
      JsonPointer.compile("/functionEndpoints");

  private static final class MetaPointers {
    private static final JsonPointer KIND = JsonPointer.compile("/functionEndpoint/meta/kind");
    private static final JsonPointer TYPE = JsonPointer.compile("/functionEndpoint/meta/type");
  }

  private static final class SpecPointers {
    private static final JsonPointer URL_PATH_TEMPLATE =
        JsonPointer.compile("/functionEndpoint/spec/urlPathTemplate");

    private static final JsonPointer TIMEOUT =
        JsonPointer.compile("/functionEndpoint/spec/timeout");
    private static final JsonPointer CONNECT_TIMEOUT =
        JsonPointer.compile("/functionEndpoint/spec/connectTimeout");
    private static final JsonPointer READ_TIMEOUT =
        JsonPointer.compile("/functionEndpoint/spec/readTimeout");
    private static final JsonPointer WRITE_TIMEOUT =
        JsonPointer.compile("/functionEndpoint/spec/writeTimeout");

    private static final JsonPointer MAX_NUM_BATCH_REQUESTS =
        JsonPointer.compile("/functionEndpoint/spec/maxNumBatchRequests");
  }

  @Override
  public void bind(
      StatefulFunctionModule.Binder binder, JsonNode moduleSpecNode, FormatVersion formatVersion) {
    if (formatVersion != FormatVersion.v3_0) {
      throw new IllegalArgumentException(
          "functionEndpoints is only supported with format version 3.0");
    }

    final Iterable<? extends JsonNode> functionEndpointsSpecNodes =
        functionEndpointSpecNodes(moduleSpecNode);

    for (Map.Entry<FunctionEndpointsSpec.Kind, List<FunctionEndpointsSpec>> entry :
        parseFunctionEndpointSpecs(functionEndpointsSpecNodes).entrySet()) {
      final Map<FunctionType, FunctionEndpointsSpec> specificTypeEndpointSpecs = new HashMap<>();
      final Map<String, FunctionEndpointsSpec> perNamespaceEndpointSpecs = new HashMap<>();

      entry
          .getValue()
          .forEach(
              spec -> {
                if (spec.targetType().isSpecificFunctionType()) {
                  specificTypeEndpointSpecs.put(spec.targetType().asSpecificFunctionType(), spec);
                } else {
                  perNamespaceEndpointSpecs.put(spec.targetType().owningNamespace(), spec);
                }
              });

      StatefulFunctionProvider provider =
          functionProvider(entry.getKey(), specificTypeEndpointSpecs, perNamespaceEndpointSpecs);
      specificTypeEndpointSpecs
          .keySet()
          .forEach(specificType -> binder.bindFunctionProvider(specificType, provider));
      perNamespaceEndpointSpecs
          .keySet()
          .forEach(namespace -> binder.bindFunctionProvider(namespace, provider));
    }
  }

  private static Map<FunctionEndpointsSpec.Kind, List<FunctionEndpointsSpec>>
      parseFunctionEndpointSpecs(Iterable<? extends JsonNode> functionEndpointsSpecNodes) {
    return StreamSupport.stream(functionEndpointsSpecNodes.spliterator(), false)
        .map(FunctionEndpointsJsonEntity::parseFunctionEndpointsSpec)
        .collect(groupingBy(FunctionEndpointsSpec::kind, toList()));
  }

  private static FunctionEndpointsSpec parseFunctionEndpointsSpec(
      JsonNode functionEndpointSpecNode) {
    FunctionEndpointsSpec.Kind kind = endpointKind(functionEndpointSpecNode);
    FunctionEndpointsSpec.TargetType targetType = targetType(functionEndpointSpecNode);

    switch (kind) {
      case HTTP:
        final HttpFunctionEndpointsSpec.Builder specBuilder =
            HttpFunctionEndpointsSpec.builder(
                targetType, urlPathTemplate(functionEndpointSpecNode));

        optionalMaxNumBatchRequests(functionEndpointSpecNode)
            .ifPresent(specBuilder::withMaxNumBatchRequests);
        optionalTimeoutDuration(functionEndpointSpecNode, SpecPointers.TIMEOUT)
            .ifPresent(specBuilder::withMaxRequestDuration);
        optionalTimeoutDuration(functionEndpointSpecNode, SpecPointers.CONNECT_TIMEOUT)
            .ifPresent(specBuilder::withConnectTimeoutDuration);
        optionalTimeoutDuration(functionEndpointSpecNode, SpecPointers.READ_TIMEOUT)
            .ifPresent(specBuilder::withReadTimeoutDuration);
        optionalTimeoutDuration(functionEndpointSpecNode, SpecPointers.WRITE_TIMEOUT)
            .ifPresent(specBuilder::withWriteTimeoutDuration);

        return specBuilder.build();
      case GRPC:
        throw new UnsupportedOperationException("GRPC endpoints are not supported yet.");
      default:
        throw new IllegalArgumentException("Unrecognized function endpoint kind " + kind);
    }
  }

  private static Iterable<? extends JsonNode> functionEndpointSpecNodes(
      JsonNode moduleSpecRootNode) {
    return Selectors.listAt(moduleSpecRootNode, FUNCTION_ENDPOINTS_POINTER);
  }

  private static FunctionEndpointsSpec.Kind endpointKind(JsonNode functionEndpointSpecNode) {
    String endpointKind = Selectors.textAt(functionEndpointSpecNode, MetaPointers.KIND);
    return FunctionEndpointsSpec.Kind.valueOf(endpointKind.toUpperCase(Locale.getDefault()));
  }

  private static FunctionEndpointsSpec.TargetType targetType(JsonNode functionEndpointSpecNode) {
    String pair = Selectors.textAt(functionEndpointSpecNode, MetaPointers.TYPE);
    return new FunctionEndpointsSpec.TargetType(NamespaceNamePair.from(pair));
  }

  private static FunctionEndpointsSpec.UrlPathTemplate urlPathTemplate(
      JsonNode functionEndpointSpecNode) {
    String template = Selectors.textAt(functionEndpointSpecNode, SpecPointers.URL_PATH_TEMPLATE);
    return new FunctionEndpointsSpec.UrlPathTemplate(template);
  }

  private static OptionalInt optionalMaxNumBatchRequests(JsonNode functionNode) {
    return Selectors.optionalIntegerAt(functionNode, SpecPointers.MAX_NUM_BATCH_REQUESTS);
  }

  private static Optional<Duration> optionalTimeoutDuration(
      JsonNode functionNode, JsonPointer timeoutPointer) {
    return Selectors.optionalTextAt(functionNode, timeoutPointer).map(TimeUtils::parseDuration);
  }

  private static StatefulFunctionProvider functionProvider(
      FunctionEndpointsSpec.Kind kind,
      Map<FunctionType, FunctionEndpointsSpec> specificTypeEndpointSpecs,
      Map<String, FunctionEndpointsSpec> perNamespaceEndpointSpecs) {
    switch (kind) {
      case HTTP:
        return new DynamicHttpFunctionProvider(
            castValues(specificTypeEndpointSpecs), castValues(perNamespaceEndpointSpecs));
      case GRPC:
        throw new UnsupportedOperationException("GRPC endpoints are not supported yet.");
      default:
        throw new IllegalStateException("Unexpected kind: " + kind);
    }
  }

  @SuppressWarnings("unchecked")
  private static <K, NV extends FunctionEndpointsSpec> Map<K, NV> castValues(
      Map<K, FunctionEndpointsSpec> toCast) {
    return new HashMap(toCast);
  }
}
