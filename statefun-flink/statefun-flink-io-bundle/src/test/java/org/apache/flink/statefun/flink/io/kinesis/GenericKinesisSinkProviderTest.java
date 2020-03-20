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
package org.apache.flink.statefun.flink.io.kinesis;

import com.google.protobuf.Any;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.io.kinesis.polyglot.GenericKinesisSinkProvider;
import org.apache.flink.statefun.flink.io.spi.JsonEgressSpec;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.junit.Test;

import static org.apache.flink.statefun.flink.io.testutils.YamlUtils.loadAsJsonFromClassResource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class GenericKinesisSinkProviderTest {

    @Test
    public void exampleUsage() {
        JsonNode egressDefinition =
                loadAsJsonFromClassResource(
                        getClass().getClassLoader(), "generic-kinesis-egress.yaml");
        JsonEgressSpec<?> spec =
                new JsonEgressSpec<>(
                        PolyglotKinesisIOTypes.GENERIC_KINESIS_EGRESS_TYPE,
                        new EgressIdentifier<>("foo", "bar", Any.class),
                        egressDefinition);

        GenericKinesisSinkProvider provider = new GenericKinesisSinkProvider();
        SinkFunction<?> source = provider.forSpec(spec);

        assertThat(source, instanceOf(FlinkKinesisProducer.class));
    }
}
