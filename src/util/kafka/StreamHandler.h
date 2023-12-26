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