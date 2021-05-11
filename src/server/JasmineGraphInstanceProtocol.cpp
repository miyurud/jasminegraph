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
const string JasmineGraphInstanceProtocol::HOST_OK = "hst-ok";
const string JasmineGraphInstanceProtocol::CLOSE = "close";
const string JasmineGraphInstanceProtocol::CLOSE_ACK = "close-ok" ;
const string JasmineGraphInstanceProtocol::SHUTDOWN = "shdn";
const string JasmineGraphInstanceProtocol::SHUTDOWN_ACK = "shdn-ok";
const string JasmineGraphInstanceProtocol::READY = "ready";
const string JasmineGraphInstanceProtocol::OK = "ok";
const string JasmineGraphInstanceProtocol::ERROR = "error";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD = "upload-g";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_CENTRAL = "upload-g-c";
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL = "upload-g-c-c";
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
const string JasmineGraphInstanceProtocol::SEND_PARTITION_ITERATION = "part-iter";
const string JasmineGraphInstanceProtocol::SEND_PARTITION_COUNT = "count";
const string JasmineGraphInstanceProtocol::FILE_RECV_CHK = "file-rcpt?";
const string JasmineGraphInstanceProtocol::FILE_RECV_WAIT = "file-wait";
const string JasmineGraphInstanceProtocol::FILE_RECV_ERROR = "file-error";
const string JasmineGraphInstanceProtocol::FILE_ACK = "file-ok";
const string JasmineGraphInstanceProtocol::STATUS = "status";
const string JasmineGraphInstanceProtocol::DELETE_GRAPH = "delete";
const string JasmineGraphInstanceProtocol::DELETE_GRAPH_FRAGMENT = "del-frag";
const string JasmineGraphInstanceProtocol::NPLACES = "nplac";
const string JasmineGraphInstanceProtocol::TRIANGLES = "tria";
const string JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR = "aggre-copy";
const string JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR = "composite-aggre-copy";
const string JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES = "aggregate";
const string JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES = "aggregate-composite";
const string JasmineGraphInstanceProtocol::PERFORMANCE_STATISTICS = "perf-stat";
const string JasmineGraphInstanceProtocol::INITIATE_TRAIN = "initiate-train";
const string JasmineGraphInstanceProtocol::INITIATE_PREDICT = "init-predict";
const string JasmineGraphInstanceProtocol::CREATE_BLOOM_FILTERS = "create-bloom-filters";
const string JasmineGraphInstanceProtocol::BUCKET_LOCAL_CLUSTERS = "bucket-local-clusters";
const string JasmineGraphInstanceProtocol::SEND_HOSTS = "send-hosts";
const string JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION = "initiate-model-collection";
const string JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION = "frag-res";
const string JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK = "frag-chk";
const string JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE = "frag-done";
const string JasmineGraphInstanceProtocol::ACKNOWLEDGE_MASTER = "ack-master";
const string JasmineGraphInstanceProtocol::WORKER_INFO_SEND = "worker-info";
const string JasmineGraphInstanceProtocol::UPDATE_DONE="done";
const string JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE="check-file-accessible";
const string JasmineGraphInstanceProtocol::SEND_FILE_TYPE="send-file-type";
const string JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE="file-type-centralstore-aggregate";
const string JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE="file-type-centralstore-composite";
