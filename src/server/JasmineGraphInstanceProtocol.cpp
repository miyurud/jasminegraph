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
const string JasmineGraphInstanceProtocol::BATCH_UPLOAD_COMPOSITE_CENTRAL = "upload-g-c-c";
const string JasmineGraphInstanceProtocol::CLOSE = "close";
const string JasmineGraphInstanceProtocol::SHUTDOWN = "shdn";
const string JasmineGraphInstanceProtocol::SHUTDOWN_ACK = "shdn-ok";
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
const string JasmineGraphInstanceProtocol::PAGE_RANK = "pgrn";
const string JasmineGraphInstanceProtocol::OUT_DEGREE_DISTRIBUTION = "odd";
const string JasmineGraphInstanceProtocol::WORKER_OUT_DEGREE_DISTRIBUTION = "odd-worker";
const string JasmineGraphInstanceProtocol::IN_DEGREE_DISTRIBUTION = "idd";
const string JasmineGraphInstanceProtocol::WORKER_IN_DEGREE_DISTRIBUTION = "idd-worker";
const string JasmineGraphInstanceProtocol::WORKER_PAGE_RANK_DISTRIBUTION = "pgrn-worker";
const string JasmineGraphInstanceProtocol::EGONET = "egont";
const string JasmineGraphInstanceProtocol::WORKER_EGO_NET = "egont-worker";
const string JasmineGraphInstanceProtocol::DP_CENTRALSTORE = "dp-central";
const string JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR = "aggre-copy";
const string JasmineGraphInstanceProtocol::SEND_COMPOSITE_CENTRALSTORE_TO_AGGREGATOR = "composite-aggre-copy";
const string JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES = "aggregate";
const string JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES = "aggregate-streaming-central";
const string JasmineGraphInstanceProtocol::AGGREGATE_COMPOSITE_CENTRALSTORE_TRIANGLES = "aggregate-composite";
const string JasmineGraphInstanceProtocol::START_STAT_COLLECTION = "begin-stat";
const string JasmineGraphInstanceProtocol::REQUEST_COLLECTED_STATS = "request-stat";
const string JasmineGraphInstanceProtocol::INITIATE_TRAIN = "initiate-train";
const string JasmineGraphInstanceProtocol::INITIATE_PREDICT = "init-predict";
const string JasmineGraphInstanceProtocol::SEND_HOSTS = "send-hosts";
const string JasmineGraphInstanceProtocol::INITIATE_MODEL_COLLECTION = "initiate-model-collection";
const string JasmineGraphInstanceProtocol::INITIATE_FRAGMENT_RESOLUTION = "frag-res";
const string JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_CHK = "frag-chk";
const string JasmineGraphInstanceProtocol::FRAGMENT_RESOLUTION_DONE = "frag-done";
const string JasmineGraphInstanceProtocol::ACKNOWLEDGE_MASTER = "ack-master";
const string JasmineGraphInstanceProtocol::WORKER_INFO_SEND = "worker-info";
const string JasmineGraphInstanceProtocol::UPDATE_DONE = "done";
const string JasmineGraphInstanceProtocol::CHECK_FILE_ACCESSIBLE = "check-file-accessible";
const string JasmineGraphInstanceProtocol::SEND_FILE_TYPE = "send-file-type";
const string JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_AGGREGATE = "file-type-centralstore-aggregate";
const string JasmineGraphInstanceProtocol::FILE_TYPE_CENTRALSTORE_COMPOSITE = "file-type-centralstore-composite";
const string JasmineGraphInstanceProtocol::FILE_TYPE_DATA = "file-type-data";
const string JasmineGraphInstanceProtocol::GRAPH_STREAM_START = "stream-start";
const string JasmineGraphInstanceProtocol::GRAPH_STREAM_START_ACK = "stream-start-ack";
const string JasmineGraphInstanceProtocol::SEND_PRIORITY = "send-priority";
const string JasmineGraphInstanceProtocol::PUSH_PARTITION = "push-partition";
const string JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK = "stream-c-length-ack";
const string JasmineGraphInstanceProtocol::GRAPH_STREAM_END_OF_EDGE = "\r\n";  // CRLF equivelent in HTTP
const string JasmineGraphInstanceProtocol::GRAPH_CSV_STREAM_START = "csv-stream-start";
const string JasmineGraphInstanceProtocol::GRAPH_CSV_STREAM_START_ACK = "stream-csv-start-ack";
const string JasmineGraphInstanceProtocol::GRAPH_CSV_STREAM_C_length_ACK = "stream-csv-c-length-ack";
const string JasmineGraphInstanceProtocol::GRAPH_CSV_STREAM_END_OF_EDGE = "\r\n";  // CRLF equivelent in HTTP
const string JasmineGraphInstanceProtocol::INITIATE_FILES = "initiate-file";
const string JasmineGraphInstanceProtocol::INITIATE_SERVER = "initiate-server";
const string JasmineGraphInstanceProtocol::INITIATE_ORG_SERVER = "initiate-org-server";
const string JasmineGraphInstanceProtocol::INITIATE_CLIENT = "initiate-client";
const string JasmineGraphInstanceProtocol::MERGE_FILES = "merge-files";
const string JasmineGraphInstanceProtocol::INITIATE_AGG = "initiate-agg";
const string JasmineGraphInstanceProtocol::INITIATE_FED_PREDICT = "initiate-fed-predict";
const string JasmineGraphInstanceProtocol::INITIATE_STREAMING_SERVER = "initiate-streaming-server";
const string JasmineGraphInstanceProtocol::INITIATE_STREAMING_CLIENT = "initiate-streaming-client";
const string JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN = "initiate-streaming-trian";
const string JasmineGraphInstanceProtocol::SEND_WORKER_LOCAL_FILE_CHUNK= "send-worker-local-file-chunk";
const string JasmineGraphInstanceProtocol::SEND_WORKER_CENTRAL_FILE_CHUNK= "send-worker-central-file-chunk";
const string JasmineGraphInstanceProtocol::END_OF_WORKER_FILE_CHUNKS_MSG= "end-of-worker-file-chunks-msg";
const string JasmineGraphInstanceProtocol::END_OF_WORKER_FILE_CHUNKS_MSG_ACK= "end-of-worker-file-chunks-msg-ack";
const string JasmineGraphInstanceProtocol::SEND_WORKER_FILE_CHUNK_CHK="send-worker-file-chunk-chk";
const string JasmineGraphInstanceProtocol::SEND_WORKER_FILE_CHUNK_ACK="send-worker-file-chunk-ack";
const string JasmineGraphInstanceProtocol::SEND_WORKER_FILE_CHUNK_WAIT="send-worker-file-chunk-wait";
