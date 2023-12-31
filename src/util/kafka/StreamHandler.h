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
#include "KafkaCC.h"
#include "../logger/Logger.h"
#include <string>
#include <vector>
#include "../../partitioner/stream/Partitioner.h"
#include "../../nativestore/DataPublisher.h"

class StreamHandler {
public:
    StreamHandler(KafkaConnector *kstream, Partitioner &graphPartitioner, std::vector<DataPublisher *> &workerClients);
    void listen_to_kafka_topic();
    cppkafka::Message pollMessage();
    bool isErrorInMessage(const cppkafka::Message &msg);
    bool isEndOfStream(const cppkafka::Message &msg);

private:
    KafkaConnector *kstream;
    Logger frontend_logger;
    std::string stream_topic_name;
    Partitioner &graphPartitioner;
    std::vector<DataPublisher *> &workerClients;
};