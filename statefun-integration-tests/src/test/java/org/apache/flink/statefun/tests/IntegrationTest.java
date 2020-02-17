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

package org.apache.flink.statefun.tests;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.StringContains.containsString;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;

/** Created by tzulitai on 2020/2/8. */
public class IntegrationTest {

  private static final String CONFLUENT_PLATFORM_VERSION = "5.0.3";

  @Rule public Network network = Network.newNetwork();

  private static ImageFromDockerfile greeterImage =
      new ImageFromDockerfile("greeter-master")
          .withFileFromClasspath("Dockerfile", "Dockerfile")
          .withFileFromPath(
              ".",
              Paths.get(
                  "/Users/tzulitai/repos/apache/flink-statefun/statefun-examples/statefun-greeter-example/target/"));

  @Rule
  public KafkaContainer kafkaContainer =
      new KafkaContainer(CONFLUENT_PLATFORM_VERSION)
          .withNetwork(network)
          .withNetworkAliases("kafka-broker");

  @Rule
  public GenericContainer greeterMasterContainer =
      new GenericContainer(greeterImage)
          .dependsOn(kafkaContainer)
          .withNetwork(network)
          .withNetworkAliases("master")
          .withEnv("ROLE", "master")
          .withEnv("MASTER_HOST", "master");

  @Rule
  public GenericContainer greeterWorkerContainer =
      new GenericContainer(greeterImage)
          .dependsOn(kafkaContainer, greeterMasterContainer)
          .withNetwork(network)
          .withNetworkAliases("worker")
          .withEnv("ROLE", "worker")
          .withEnv("MASTER_HOST", "master");

  @Test
  public void greeterExampleSanityTest() throws Exception {
    Producer<String, String> producer = kafkaProducer(kafkaContainer.getBootstrapServers());
    producer.send(new ProducerRecord<>("names", "key", "Gordon"));
    producer.send(new ProducerRecord<>("names", "key", "Igal"));
    producer.send(new ProducerRecord<>("names", "key", "Gordon"));
    producer.send(new ProducerRecord<>("names", "key", "Joe"));
    producer.send(new ProducerRecord<>("names", "key", "Gordon"));
    producer.flush();

    Consumer<String, String> consumer = kafkaConsumer(kafkaContainer.getBootstrapServers());
    consumer.subscribe(Collections.singletonList("greetings"));

    final int expectedResponses = 5;
    Set<String> responses = new HashSet<>();
    while (responses.size() < expectedResponses) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        responses.add(record.value());
      }
    }

    assertThat(
        responses,
        allOf(
            hasItem(containsString("Hello Gordon !")),
            hasItem(containsString("Hello Igal !")),
            hasItem(containsString("Hello again Gordon !")),
            hasItem(containsString("Hello Joe !")),
            hasItem(containsString("Third time is a charm! Gordon!"))));
  }

  private static Producer<String, String> kafkaProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  private static Consumer<String, String> kafkaConsumer(String bootstrapServers) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", bootstrapServers);
    consumerProps.setProperty("group.id", "test");
    consumerProps.setProperty("auto.offset.reset", "earliest");
    consumerProps.setProperty(
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.setProperty(
        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    return new KafkaConsumer<>(consumerProps);
  }
}
