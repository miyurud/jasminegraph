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
const string JasmineGraphInstanceProtocol::INITIATE_STREAMING_KG_CONSTRUCTION = "initiate-streaming-kg-construction";
const string JasmineGraphInstanceProtocol::INITIATE_STREAMING_TUPLE_CONSTRUCTION = "initiate-streaming-tuple-extraction";
const string JasmineGraphInstanceProtocol:: SEMANTIC_BEAM_SEARCH = "initiate-semantic-beam-search";
 const string  JasmineGraphInstanceProtocol::CHUNK_STREAM_END = "chunk-stream-end";


const string JasmineGraphInstanceProtocol::QUERY_START = "query-start";
const string JasmineGraphInstanceProtocol::SUB_QUERY_START = "sub-query-start";
const string JasmineGraphInstanceProtocol::QUERY_START_ACK = "query-start-ack";
const string JasmineGraphInstanceProtocol::SUB_QUERY_START_ACK = "sub-query-start-ack";
const string JasmineGraphInstanceProtocol::QUERY_DATA_START = "query-data-start";
const string JasmineGraphInstanceProtocol::QUERY_DATA_ACK = "query-data-ack";
const string JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS = "graph-data-success";
const string JasmineGraphInstanceProtocol::HDFS_LOCAL_STREAM_START = "hdfs-local-stream-start";
const string JasmineGraphInstanceProtocol::HDFS_CENTRAL_STREAM_START = "hdfs-central-stream-start";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_END_WAIT = "hdfs-stream-end-wait";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_START_ACK = "hdfs-stream-start-ack";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_END_ACK = "hdfs-stream-end-ack";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_END_CHK = "hdfs-stream-end-chk";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_NAME_ACK = "hdfs-file-name-ack";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_NAME_LENGTH_ACK = "hdfs-file-name-length-ack";
const string JasmineGraphInstanceProtocol::HDFS_STREAM_FILE_SIZE_ACK = "hdfs-file-size-ack";
const string JasmineGraphInstanceProtocol::HDFS_FILE_CHUNK_END_CHK = "hdfs-file-chunk-end-chk";
const string JasmineGraphInstanceProtocol::HDFS_FILE_CHUNK_END_ACK = "hdfs-file-chunk-end-ack";
