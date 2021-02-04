package org.apache.flink.statefun.sdk.java;

import java.time.Duration;
import java.util.Optional;
import org.apache.flink.statefun.sdk.java.message.EgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;

public interface Context {

  Address self();

  Optional<Address> caller();

  void send(Message message);

  void reply(Message message);

  void sendAfter(Duration duration, Message message);

  void send(EgressMessage message);

  AddressScopedStorage storage();
}
