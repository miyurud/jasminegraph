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

#ifndef JASMINEGRAPH_JASMINEGRAPHINSTANCEPROTOCOL_H
#define JASMINEGRAPH_JASMINEGRAPHINSTANCEPROTOCOL_H

#include <iostream>

using namespace std;

class JasmineGraphInstanceProtocol{
public:
    //Handshaking is the first task that JasmineGraph's main server does with an JasmineGraph Instance once it gets connected.
    //During the phase of Handshaking, JasmineGraph server informs its host name to the instance so that it can connect with the server later time.
    static const string HANDSHAKE;
    static const string HANDSHAKE_OK;
    static const string CLOSE;
    static const string CLOSE_ACK;
    static const string SHUTDOWN;
    static const string SHUTDOWN_ACK;
    static const string READY;
    static const string OK;
    static const string ERROR;
    static const string BATCH_UPLOAD;               // This is to upload a file as a batch
    static const string BATCH_UPLOAD_CENTRAL;       // This is to upload centralstore file as a batch
    static const string UPLOAD_RDF_ATTRIBUTES;       // This is to upload attribute list of partitions file as a batch
    static const string UPLOAD_RDF_ATTRIBUTES_CENTRAL; // This is to upload attribute list of centralstore file as a batch
    static const string BATCH_UPLOAD_CHK;           // This is to check whether the upload process has finished or not.
    static const string BATCH_UPLOAD_WAIT;
    static const string BATCH_UPLOAD_ACK;
    static const string SEND_FILE;
    static const string SEND_FILE_LEN;              // This is to indicate server to send the size of the file.
    static const string SEND_FILE_CONT;             // This is to indicate server to send the file contents.
    static const string SEND_FILE_COMPLETE;
    static const string SEND_FILE_NAME;
    static const string SEND_PARTITION_ID;          // This command is used by the Instance service session to ask for partition id.
    static const string FILE_RECV_CHK;
    static const string FILE_RECV_WAIT;
    static const string FILE_RECV_ERROR;
    static const string FILE_ACK;
    static const string STATUS;                     // This is sent to the client to check its status.
    static const string DELETE_GRAPH;               // This mesage deletes a particular graph from JasmineGraph
    static const string INITIATE_TRAIN;
    static const string INITIATE_PREDICT;
    static const string SEND_HOSTS;
    static const string INITIATE_MODEL_COLLECTION;

};



#endif //JASMINEGRAPH_JASMINEGRAPHINSTANCEPROTOCOL_H