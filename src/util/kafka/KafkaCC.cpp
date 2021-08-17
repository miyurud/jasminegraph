#include "./KafkaCC.h"

#include <cppkafka/cppkafka.h>

#include <nlohmann/json.hpp>

#include "../../partitioner/stream/Partitioner.h"
#include "../Utils.h"
#include "../logger/Logger.h"

using json = nlohmann::json;

using namespace std;
using namespace cppkafka;
Logger kafka_client_logger;

void KafkaConnector::Subscribe(string topic) { consumer.subscribe({topic}); }

void *KafkaConnector::startStream(string topicName, std::vector<DataPublisher *> workerClients,
                                  std::map<std::string, std::atomic<bool>> *streamsState) {
    Utils utils;

    // After getting the topic name , need to close the connection and ask the user to send the data to given topic

    std::string kafkaHost = utils.getJasmineGraphProperty("org.jasminegraph.server.streaming.kafka.host");
    cppkafka::Configuration configs = {{"metadata.broker.list", kafkaHost}, {"group.id", "jasmine"}};
    cppkafka::Consumer consumer(configs);
    // KafkaConnector kstream(configs);
    std::string partitionCount = utils.getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    int numberOfPartitions = std::stoi(partitionCount);

    Partitioner graphPartitioner(numberOfPartitions, 0, spt::Algorithms::HASH);

    consumer.subscribe({topicName});
    kafka_client_logger.log("Start listening to " + topicName, "info");
    // std::atomic<bool> shouldStop(false);
    bool shouldStop = false;

    while (true) {
        cppkafka::Message msg = consumer.poll();
        if (!msg || msg.get_error()) {
            continue;
        }
        string data(msg.get_payload());
        if (streamsState->find(topicName) != streamsState->end()) {
            shouldStop = streamsState->find(topicName)->second;
        }
        if (data == "-1" || shouldStop) {  // Marks the end of stream
            kafka_client_logger.log("Received the end of stream", "info");
            break;
        }

        auto edgeJson = json::parse(data);
        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];

        std::string sourceID = std::string(sourceJson["id"]);
        std::string destinationID = std::string(destinationJson["id"]);

        partitionedEdge partitionedEdge = graphPartitioner.addEdge({sourceID, destinationID});
        edgeJson["source"]["pid"] = std::to_string(partitionedEdge[0].second);
        edgeJson["destination"]["pid"] = std::to_string(partitionedEdge[1].second);
        workerClients.at((int)partitionedEdge[0].second)->publish(edgeJson.dump());
        workerClients.at((int)partitionedEdge[1].second)->publish(edgeJson.dump());
    }
    graphPartitioner.printStats();
}
