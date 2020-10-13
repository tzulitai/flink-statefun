################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from datetime import timedelta

from google.protobuf.any_pb2 import Any

from statefun.core import SdkAddress
from statefun.core import AnyStateHandle
from statefun.core import Expiration
from statefun.core import parse_typename

# generated function protocol
from statefun.request_reply_pb2 import FromFunction
from statefun.request_reply_pb2 import ToFunction


class InvocationContext:
    def __init__(self):
        self.batch = None
        self.context = None
        self.target_function = None

    def setup(self, target_addr, target_function, state, invocations_batch):
        self.batch = invocations_batch
        self.context = BatchContext(target_addr, state)
        self.target_function = target_function

    def complete(self):
        from_function = FromFunction()
        invocation_result = from_function.invocation_result
        context = self.context
        self.add_mutations(context, invocation_result)
        self.add_outgoing_messages(context, invocation_result)
        self.add_delayed_messages(context, invocation_result)
        self.add_egress(context, invocation_result)
        # reset the state for the next invocation
        self.batch = None
        self.context = None
        self.target_function = None
        # return the result
        return from_function.SerializeToString()

    @staticmethod
    def add_outgoing_messages(context, invocation_result):
        outgoing_messages = invocation_result.outgoing_messages
        for typename, id, message in context.messages:
            outgoing = outgoing_messages.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_mutations(context, invocation_result):
        for name, handle in context.states.items():
            if not handle.modified:
                continue
            mutation = invocation_result.state_mutations.add()

            mutation.state_name = name
            if handle.deleted:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('DELETE')
            else:
                mutation.mutation_type = FromFunction.PersistedValueMutation.MutationType.Value('MODIFY')
                mutation.state_value = handle.bytes()

    @staticmethod
    def add_delayed_messages(context, invocation_result):
        delayed_invocations = invocation_result.delayed_invocations
        for delay, typename, id, message in context.delayed_messages:
            outgoing = delayed_invocations.add()

            namespace, type = parse_typename(typename)
            outgoing.target.namespace = namespace
            outgoing.target.type = type
            outgoing.target.id = id
            outgoing.delay_in_ms = delay
            outgoing.argument.CopyFrom(message)

    @staticmethod
    def add_egress(context, invocation_result):
        outgoing_egresses = invocation_result.outgoing_egresses
        for typename, message in context.egresses:
            outgoing = outgoing_egresses.add()

            namespace, type = parse_typename(typename)
            outgoing.egress_namespace = namespace
            outgoing.egress_type = type
            outgoing.argument.CopyFrom(message)


class RequestReplyHandlerBase:
    def __init__(self, functions):
        self.functions = functions

    def get_function(self, target_addr):
        target_function = self.functions.for_type(
            target_addr.namespace,
            target_addr.type)
        if target_function is None:
            raise ValueError("Unable to find a function of type ", target_addr)
        return target_function

    @staticmethod
    def parse_request(request_bytes):
        to_function = ToFunction()
        to_function.ParseFromString(request_bytes)
        return to_function

    @staticmethod
    def provided_states(to_function: ToFunction):
        return {s.state_name: AnyStateHandle(s.state_value) for s in to_function.invocation.state}

    @staticmethod
    def resolve_persisted_values(provided_states, target_function):
        missing_persisted_values = []
        resolved_state_values = {}
        declared_state_values = target_function.state_values
        if declared_state_values:
            for state in declared_state_values:
                if state.state_name not in provided_states:
                    missing_persisted_values.append(state)
                else:
                    resolved_state_values[state.state_name] = provided_states[state.state_name]
        return missing_persisted_values, resolved_state_values

    @staticmethod
    def retry_request(missing_persisted_values):
        from_function = FromFunction()
        retry_request = from_function.retry_request
        missing_values = retry_request.missing_values
        for mpv in missing_persisted_values:
            missing_value = missing_values.add()
            missing_value.state_name = mpv.state_name
            ttl = mpv.state_ttl
            if not ttl:
                missing_value.expire_mode = FromFunction.MissingPersistedValue.ExpireMode.NONE
            else:
                if ttl.expire_mode is Expiration.Mode.AFTER_INVOKE:
                    missing_value.expire_mode = FromFunction.MissingPersistedValue.ExpireMode.AFTER_INVOKE
                elif ttl.expire_mode is Expiration.Mode.AFTER_WRITE:
                    missing_value.expire_mode = FromFunction.MissingPersistedValue.ExpireMode.AFTER_WRITE
                else:
                    raise ValueError("Safe guard; invalid expire mode: " + ttl.expire_mode)
                missing_value.expire_after_millis = ttl.expire_after_millis
        return from_function.SerializeToString()


class RequestReplyHandler(RequestReplyHandlerBase):

    def __call__(self, request_bytes):
        to_function = super().parse_request(request_bytes)
        target_function = super().get_function(to_function.invocation.target)
        provided_states = super().provided_states(to_function)
        missing_persisted_values, resolved_persisted_values = super().resolve_persisted_values(provided_states, target_function)
        if not missing_persisted_values:
            ic = InvocationContext()
            ic.setup(
                to_function.invocation.target,
                target_function,
                resolved_persisted_values,
                to_function.invocation.invocations)
            self.handle_invocation(ic)
            return ic.complete()
        else:
            return self.retry_request(missing_persisted_values)

    @staticmethod
    def handle_invocation(ic: InvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            unpacked = target_function.unpack_any(invocation.argument)
            if not unpacked:
                fun(context, invocation.argument)
            else:
                fun(context, unpacked)


class AsyncRequestReplyHandler(RequestReplyHandlerBase):

    async def __call__(self, request_bytes):
        to_function = super().parse_request(request_bytes)
        target_function = super().get_function(to_function.invocation.target)
        provided_states = super().provided_states(to_function)
        missing_persisted_values, resolved_persisted_values = super().resolve_persisted_values(provided_states, target_function)
        if not missing_persisted_values:
            ic = InvocationContext()
            ic.setup(
                to_function.invocation.target,
                target_function,
                resolved_persisted_values,
                to_function.invocation.invocations)
            await self.handle_invocation(ic)
            return ic.complete()
        else:
            return self.retry_request(missing_persisted_values)

    @staticmethod
    async def handle_invocation(ic: InvocationContext):
        batch = ic.batch
        context = ic.context
        target_function = ic.target_function
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            unpacked = target_function.unpack_any(invocation.argument)
            if not unpacked:
                await fun(context, invocation.argument)
            else:
                await fun(context, unpacked)


class BatchContext(object):
    def __init__(self, target, states):
        # populate the state store with the eager state values provided in the batch
        self.states = states
        # remember own address
        self.address = SdkAddress(target.namespace, target.type, target.id)
        # the caller address would be set for each individual invocation in the batch
        self.caller = None
        # outgoing messages
        self.messages = []
        self.delayed_messages = []
        self.egresses = []

    def prepare(self, invocation):
        """setup per invocation """
        if invocation.caller:
            caller = invocation.caller
            self.caller = SdkAddress(caller.namespace, caller.type, caller.id)
        else:
            self.caller = None

    # --------------------------------------------------------------------------------------
    # state access
    # --------------------------------------------------------------------------------------

    def state(self, name):
        if name not in self.states:
            raise KeyError('unknown state name [' + name + '], states needed to be explicitly provided when binding the function')
        return self.states[name]

    def __getitem__(self, name):
        return self.state(name).value

    def __delitem__(self, name):
        state = self.state(name)
        del state.value

    def __setitem__(self, name, value):
        state = self.state(name)
        state.value = value

    # --------------------------------------------------------------------------------------
    # messages
    # --------------------------------------------------------------------------------------

    def send(self, typename: str, id: str, message: Any):
        """
        Send a message to a function of type and id.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        out = (typename, id, message)
        self.messages.append(out)

    def pack_and_send(self, typename: str, id: str, message):
        """
        Send a Protobuf message to a function.

        This variant of send, would first pack this message
        into a google.protobuf.Any and then send it.

        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to pack into an Any and the send.
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send(typename, id, any)

    def reply(self, message: Any):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        """
        caller = self.caller
        if not caller:
            raise AssertionError(
                "Unable to reply without a caller. Was this message was sent directly from an ingress?")
        self.send(caller.typename(), caller.identity, message)

    def pack_and_reply(self, message):
        """
        Reply to the sender (assuming there is a sender)

        :param message: the message to reply to.
        """
        any = Any()
        any.Pack(message)
        self.reply(any)

    def send_after(self, delay: timedelta, typename: str, id: str, message: Any):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not delay:
            raise ValueError("missing delay")
        if not typename:
            raise ValueError("missing type name")
        if not id:
            raise ValueError("missing id")
        if not message:
            raise ValueError("missing message")
        duration_ms = int(delay.total_seconds() * 1000.0)
        out = (duration_ms, typename, id, message)
        self.delayed_messages.append(out)

    def pack_and_send_after(self, delay: timedelta, typename: str, id: str, message):
        """
        Send a message to a function of type and id.

        :param delay: the amount of time to wait before sending this message.
        :param typename: the target function type name, for example: "org.apache.flink.statefun/greeter"
        :param id: the id of the target function
        :param message: the message to send
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_after(delay, typename, id, any)

    def send_egress(self, typename, message: Any):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not typename:
            raise ValueError("missing type name")
        if not message:
            raise ValueError("missing message")
        self.egresses.append((typename, message))

    def pack_and_send_egress(self, typename, message):
        """
        Sends a message to an egress defined by @typename
        :param typename: an egress identifier of the form <namespace>/<name>
        :param message: the message to send.
        """
        if not message:
            raise ValueError("missing message")
        any = Any()
        any.Pack(message)
        self.send_egress(typename, any)
