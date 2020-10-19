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

from remote_module_verification_pb2 import Invoke, InvokeResult, InvokeCount

from statefun import StatefulFunctions
from statefun import PersistedValue
from statefun import RequestReplyHandler
from statefun import kafka_egress_record

import uuid

functions = StatefulFunctions()

@functions.bind("org.apache.flink.statefun.e2e.remote/forwarder")
def forward_to_egress(context, invoke_result: InvokeResult):
    """
    Simply forwards the results to the Kafka egress.
    """
    egress_message = kafka_egress_record(
        topic="invoke-results",
        key=invoke_result.id,
        value=invoke_result
    )
    context.pack_and_send_egress(
        "org.apache.flink.statefun.e2e.remote/invoke-results",
        egress_message
    )


handler = RequestReplyHandler(functions)

#
# Serve the endpoint
#

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)


@app.route('/service', methods=['POST'])
def handle():
    response_data = handler(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response


if __name__ == "__main__":
    app.run()
