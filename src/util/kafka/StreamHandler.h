/**
Copyright 2023 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#include <cppkafka/cppkafka.h>

#include <string>
#include <vector>

#include "../../nativestore/DataPublisher.h"
#include "../../partitioner/stream/Partitioner.h"
#include "../logger/Logger.h"
#include "KafkaCC.h"
#include "../../metadb/SQLiteDBInterface.h"

class StreamHandler {
 public:
    StreamHandler(KafkaConnector *kstream, int numberOfPartitions,
                  std::vector<DataPublisher *> &workerClients, SQLiteDBInterface* sqlite,
                  int graphId, bool isDirected, spt::Algorithms algo = spt::Algorithms::HASH, 
                  bool enableTemporal = false, bool enableOperationType = false);
    void listen_to_kafka_topic();
    cppkafka::Message pollMessage();
    bool isErrorInMessage(const cppkafka::Message &msg);
    bool isEndOfStream(const cppkafka::Message &msg);
    Partitioner graphPartitioner;
    int  graphId;
 private:
    KafkaConnector *kstream;
    Logger frontend_logger;
    std::string stream_topic_name;
    std::vector<DataPublisher *> &workerClients;
    bool temporalStreamingEnabled;
    bool operationTypeEnabled;
};
