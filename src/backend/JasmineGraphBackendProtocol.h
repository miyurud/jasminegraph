/**
Copyright 2018 JasminGraph Team
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

#ifndef JASMINEGRAPH_JASMINEGRAPHBACKENDPROTOCOL_H
#define JASMINEGRAPH_JASMINEGRAPHBACKENDPROTOCOL_H

#include <iostream>

using namespace std;

extern const string HANDSHAKE;     // To notify the server the host name of this worker;
extern const string HANDSHAKE_OK;  // Response to say it is ready for handshaking.
extern const string OK;            // To check if the status is ok
extern const string RUOK;          // To check if the status is ok
extern const string IMOK;          // Response to check if the status is ok
extern const string EXIT_BACKEND;  // To exit from the query session
extern const string EXIT_ACK;
extern const string
    SEND;  // Ask the client to send some data. This is used during a comminication session with the client.
extern const string ERROR;
extern const string OUT_DEGREE_DISTRIBUTION_FOR_PARTITION;
extern const string IN_DEGREE_DISTRIBUTION_FOR_PARTITION;  // This commad gets the in degree distribution from the
                                                           // external world to this partition
extern const string WORLD_ONLY_AUTHFLOW_FOR_PARTITION;
extern const string LOCAL_TO_WORLD_AUTHFLOW_FOR_PARTITION;
extern const string WORLD_TO_LOCAL_FLOW_FROMIDS;  // This command gets all the vertices connected with the external
                                                  // graph and their corresponding fromIDs
extern const string DONE;
extern const string PARTITIONS_ON_HOST;  // The command to get the list of partitions on particular host
extern const string ACKNOWLEGE_MASTER;   // Command to acknowledge the master stating that the worker has started
extern const string WORKER_INFO_SEND;
extern const string HOST_OK;
extern const string UPDATE_DONE;
extern const string WORKER_DETAILS;
extern const string WORKER_DETAILS_ACK;
extern const string PARTITION_ALGORITHM_DETAILS;
extern const string PARTITION_ALGORITHM_DETAILS_ACK;
extern const string CONTENT_LENGTH_ACK;

class JasmineGraphBackendProtocol {};

const int BACKEND_COMMAND_LENGTH = 4;
const int BACKEND_DATA_LENGTH = 300;

#endif  // JASMINEGRAPH_JASMINEGRAPHBACKENDPROTOCOL_H
