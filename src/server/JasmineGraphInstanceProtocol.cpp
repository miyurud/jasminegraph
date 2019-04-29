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

#include "JasmineGraphInstanceProtocol.h"

const string JasmineGraphInstanceProtocol::HANDSHAKE = "hske";
const string JasmineGraphInstanceProtocol::HANDSHAKE_OK = "hske-ok";
const string JasmineGraphInstanceProtocol::CLOSE = "close";
const string JasmineGraphInstanceProtocol::CLOSE_ACK = "close-ok" ;
const string JasmineGraphInstanceProtocol::SHUTDOWN = "shtdn";
const string JasmineGraphInstanceProtocol::SHUTDOWN_ACK = "shtdn-ok";
const string JasmineGraphInstanceProtocol::READY = "ready";
const string JasmineGraphInstanceProtocol::OK = "ok";
const string JasmineGraphInstanceProtocol::ERROR = "error";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD = "upload-g";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL = "upload-g-c";
const string JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES = "upload-attrib";
const string JasmineGraphInstanceProtocol::UPLOAD_RDF_ATTRIBUTES_CENTRAL = "upload-attrib-c";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK = "upload-g-chk";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT = "upload-g-wait";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK = "upload-g-ack";
const string JasmineGraphInstanceProtocol::SEND_FILE = "file";
const string JasmineGraphInstanceProtocol::SEND_FILE_LEN = "file-len";
const string JasmineGraphInstanceProtocol::SEND_FILE_CONT = "file-cont";
const string JasmineGraphInstanceProtocol::SEND_FILE_COMPLETE = "file-complete";
const string JasmineGraphInstanceProtocol::SEND_FILE_NAME = "file-name";
const string JasmineGraphInstanceProtocol::SEND_PARTITION_ID = "partid";
const string JasmineGraphInstanceProtocol::FILE_RECV_CHK = "file-rcpt?";
const string JasmineGraphInstanceProtocol::FILE_RECV_WAIT = "file-wait";
const string JasmineGraphInstanceProtocol::FILE_RECV_ERROR = "file-error";
const string JasmineGraphInstanceProtocol::FILE_ACK = "file-ok";
const string JasmineGraphInstanceProtocol::STATUS = "status";
const string JasmineGraphInstanceProtocol::DELETE_GRAPH = "delete";
const string JasmineGraphInstanceProtocol::INITIATE_TRAIN = "initiate-train";
