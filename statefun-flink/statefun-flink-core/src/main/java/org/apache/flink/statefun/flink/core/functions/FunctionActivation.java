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
package org.apache.flink.statefun.flink.core.functions;

import java.util.ArrayDeque;
import org.apache.flink.statefun.flink.core.message.Message;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.StatefulFunction;

/** An {@link StatefulFunction} instance with a mailbox. */
final class FunctionActivation {
  private final ArrayDeque<Message> mailbox;
  private Address self;
  private LiveFunction function;

  FunctionActivation() {
    this.mailbox = new ArrayDeque<>();
  }

  void setFunction(Address self, LiveFunction function) {
    this.self = self;
    this.function = function;
  }

  void add(Message message) {
    mailbox.addLast(message);
  }

  boolean hasPendingEnvelope() {
    return !mailbox.isEmpty();
  }

  void applyNextPendingEnvelope(ApplyingContext context) {
    Message message = mailbox.pollFirst();
    context.apply(function, message);
  }

  Address self() {
    return self;
  }
}
