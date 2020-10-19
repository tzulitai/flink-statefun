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

import java.net.URI;
import org.apache.flink.statefun.flink.common.json.NamespaceNamePair;
import org.apache.flink.statefun.sdk.FunctionType;

public interface FunctionEndpointsSpec {

  TargetType targetType();

  Kind kind();

  UrlPathTemplate urlPathTemplate();

  enum Kind {
    HTTP,
    GRPC
  }

  class TargetType {
    private static final String NAME_WILDCARD = "*";

    private final NamespaceNamePair namespaceNamePair;

    public TargetType(NamespaceNamePair namespaceNamePair) {
      this.namespaceNamePair = namespaceNamePair;
    }

    boolean isSpecificFunctionType() {
      return !namespaceNamePair.name().equals(NAME_WILDCARD);
    }

    public FunctionType asSpecificFunctionType() {
      if (!isSpecificFunctionType()) {
        throw new IllegalStateException("This is a non-specific function type.");
      }
      return new FunctionType(namespaceNamePair.namespace(), namespaceNamePair.name());
    }

    public String owningNamespace() {
      return namespaceNamePair.namespace();
    }
  }

  class UrlPathTemplate {
    private static final String FUNCTION_NAME_HOLDER = "{typeName}";

    private final String template;

    public UrlPathTemplate(String template) {
      this.template = template;
    }

    public URI apply(FunctionType functionType) {
      return URI.create(template.replace(FUNCTION_NAME_HOLDER, functionType.name()));
    }
  }
}
