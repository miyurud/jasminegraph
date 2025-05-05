/**
Copyright 2019 JasminGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "JasmineGraphBackendProtocol.h"

const string HANDSHAKE = "hske";
const string HANDSHAKE_OK = "hske-ok";
const string EXIT_BACKEND = "exit";
const string EXIT_ACK = "bye";
const string ACKNOWLEGE_MASTER = "ack-master";
const string WORKER_INFO_SEND = "worker-info";
const string HOST_OK = "hst-ok";
const string UPDATE_DONE = "done";
const string WORKER_DETAILS = "worker-details";
const string WORKER_DETAILS_ACK = "worker-details-ack";
const string PARTITION_ALGORITHM_DETAILS = "partition-algo-details";
const string PARTITION_ALGORITHM_DETAILS_ACK = "partition-algo-details-ack";
const string CONTENT_LENGTH_ACK = "content-length-ack";
