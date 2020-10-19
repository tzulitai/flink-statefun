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

package org.apache.flink.statefun.flink.core.httpfn;

import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketBridge.configureUnixDomainSocket;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.statefun.flink.core.common.ManagingResources;
import org.apache.flink.statefun.flink.core.reqreply.PersistedRemoteFunctionValues;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

public class DynamicHttpFunctionProvider implements StatefulFunctionProvider, ManagingResources {
  private final Map<FunctionType, HttpFunctionEndpointsSpec> specificTypeEndpointSpecs;
  private final Map<String, HttpFunctionEndpointsSpec> perNamespaceEndpointSpecs;

  /** lazily initialized by {code buildHttpClient} */
  @Nullable private OkHttpClient sharedClient;

  private volatile boolean shutdown;

  public DynamicHttpFunctionProvider(
      Map<FunctionType, HttpFunctionEndpointsSpec> specificTypeEndpointSpecs,
      Map<String, HttpFunctionEndpointsSpec> perNamespaceEndpointSpecs) {
    this.specificTypeEndpointSpecs = specificTypeEndpointSpecs;
    this.perNamespaceEndpointSpecs = perNamespaceEndpointSpecs;
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    final HttpFunctionEndpointsSpec endpointsSpec = getEndpointsSpecOrThrow(functionType);
    return new RequestReplyFunction(
        new PersistedRemoteFunctionValues(Collections.emptyList()),
        endpointsSpec.maxNumBatchRequests(),
        buildHttpClient(endpointsSpec, functionType));
  }

  private HttpFunctionEndpointsSpec getEndpointsSpecOrThrow(FunctionType functionType) {
    HttpFunctionEndpointsSpec endpointsSpec = specificTypeEndpointSpecs.get(functionType);
    if (endpointsSpec != null) {
      return endpointsSpec;
    }
    endpointsSpec = perNamespaceEndpointSpecs.get(functionType.namespace());
    if (endpointsSpec != null) {
      return endpointsSpec;
    }

    throw new IllegalStateException("Unknown type: " + functionType);
  }

  private RequestReplyClient buildHttpClient(
      HttpFunctionEndpointsSpec spec, FunctionType functionType) {
    if (sharedClient == null) {
      sharedClient = OkHttpUtils.newClient();
    }
    OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();
    clientBuilder.callTimeout(spec.maxRequestDuration());
    clientBuilder.connectTimeout(spec.connectTimeout());
    clientBuilder.readTimeout(spec.readTimeout());
    clientBuilder.writeTimeout(spec.writeTimeout());

    URI endpointUrl = spec.urlPathTemplate().apply(functionType);

    final HttpUrl url;
    if (spec.isUnixDomainSocket()) {
      UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(endpointUrl);

      url =
          new HttpUrl.Builder()
              .scheme("http")
              .host("unused")
              .addPathSegment(endpoint.pathSegment)
              .build();

      configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
    } else {
      url = HttpUrl.get(endpointUrl);
    }
    return new HttpRequestReplyClient(url, clientBuilder.build(), () -> shutdown);
  }

  @Override
  public void shutdown() {
    shutdown = true;
    OkHttpUtils.closeSilently(sharedClient);
  }

  public Map<String, HttpFunctionEndpointsSpec> perNamespaceEndpointSpecs() {
    return perNamespaceEndpointSpecs;
  }

  public Map<FunctionType, HttpFunctionEndpointsSpec> specificTypeEndpointSpecs() {
    return specificTypeEndpointSpecs;
  }
}
