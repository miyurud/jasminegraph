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

#include "StreamHandler.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <stdlib.h>

#include "../logger/Logger.h"
#include "../Utils.h"
#include "../../server/JasmineGraphServer.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;
Logger stream_handler_logger;

StreamHandler::StreamHandler(KafkaConnector *kstream, int numberOfPartitions,
                             vector<DataPublisher *> &workerClients, SQLiteDBInterface* sqlite,
                             int graphId, bool isDirected, spt::Algorithms algorithms)
        : kstream(kstream),
          graphId(graphId),
          workerClients(workerClients),
          graphPartitioner(numberOfPartitions, graphId, algorithms, sqlite, isDirected),
          stream_topic_name("stream_topic_name"),
          currentSnapshot(0),
          centralTemporalStore(nullptr),
          numberOfPartitions(numberOfPartitions) {
    std::string temporalEnabled = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.enabled");
    if (temporalEnabled == "true") {
        uint64_t timeThreshold = 60;
        uint64_t edgeThreshold = 10000;
        
        std::string timeStr = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.snapshot.time.seconds");
        if (!timeStr.empty()) {
            timeThreshold = std::stoull(timeStr);
        }
        
        std::string edgeStr = Utils::getJasmineGraphProperty("org.jasminegraph.temporal.snapshot.edge.count");
        if (!edgeStr.empty()) {
            edgeThreshold = std::stoull(edgeStr);
        }
        
        for (int partitionId = 0; partitionId < numberOfPartitions; partitionId++) {
            localTemporalStores[partitionId] = new TemporalStore(
                graphId, partitionId, timeThreshold, edgeThreshold, 
                SnapshotManager::SnapshotMode::HYBRID
            );
        }
        
        centralTemporalStore = new TemporalStore(
            graphId, numberOfPartitions, timeThreshold, edgeThreshold,
            SnapshotManager::SnapshotMode::HYBRID
        );
        
        stream_handler_logger.info("Temporal storage enabled for graph " + std::to_string(graphId) + 
                                  " with " + std::to_string(numberOfPartitions) + " partitions");
    }
}


// Polls kafka for a message.
cppkafka::Message StreamHandler::pollMessage() { return kstream->consumer.poll(std::chrono::milliseconds(1000)); }

// Checks if there's an error in Kafka's message.
bool StreamHandler::isErrorInMessage(const cppkafka::Message &msg) {
    if (!msg) {
        // Empty message (timeout) - not an error, just no data available yet
        return true;
    }
    if (msg.get_error()) {
        auto errorCode = msg.get_error().get_error();
        if (errorCode == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            // Reached end of partition - this is expected, not an error
            stream_handler_logger.info("Reached end of partition " + std::to_string(msg.get_partition()));
        } else if (errorCode == RD_KAFKA_RESP_ERR__TIMED_OUT) {
            // Poll timeout - normal, just no messages available
            return true;
        } else {
            stream_handler_logger.error("Kafka message error: " + msg.get_error().to_string() + 
                                       " (code: " + std::to_string(errorCode) + ")");
        }
        return true;
    }
    return false;
}

// Ends the stream if the end message("-1") has been received.
bool StreamHandler::isEndOfStream(const cppkafka::Message &msg) {
    if (!msg || !msg.get_payload()) {
        return false;  // Can't be end of stream if no message
    }
    std::string data(msg.get_payload());
    if (data == "-1") {
        frontend_logger.info("Received the end of `" + stream_topic_name + "` input kafka stream");
        return true;
    }
    return false;
}

void StreamHandler::listen_to_kafka_topic() {
    // get workers
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(workerClients.size());

    // assign partitions to workers
    for (int i = 0; i < workerClients.size(); i++) {
        Utils::assignPartitionToWorker(graphId, i, workers.at(i).hostname, workers.at(i).port);
    }

    uint64_t messagesProcessed = 0;
    uint64_t emptyPolls = 0;
    uint64_t localEdgesAdded = 0;
    uint64_t centralEdgesAdded = 0;
    uint64_t lastProgressCheck = 0;
    const uint64_t MAX_CONSECUTIVE_EMPTY_POLLS = 60;
    
    stream_handler_logger.info("Starting Kafka consumer loop for graph " + std::to_string(graphId));

    while (true) {
        cppkafka::Message msg = this->pollMessage();

        if (this->isEndOfStream(msg)) {
            stream_handler_logger.info("Received termination signal (-1) from Kafka");
            stream_handler_logger.info("Total messages processed: " + std::to_string(messagesProcessed));
            stream_handler_logger.info("Edges added: " + std::to_string(localEdgesAdded) + " local, " + 
                                      std::to_string(centralEdgesAdded) + " central");
            for (auto &workerClient : workerClients) {
                if (workerClient != nullptr) {
                    workerClient->publish("-1");
                }
            }
            break;
        }

        if (this->isErrorInMessage(msg)) {
            emptyPolls++;
            
            if (messagesProcessed > lastProgressCheck) {
                emptyPolls = 0;
                lastProgressCheck = messagesProcessed;
            }
            
            if (emptyPolls % 10 == 0 && emptyPolls > 0) {
                stream_handler_logger.info("Waiting for messages (empty polls: " + 
                                          std::to_string(emptyPolls) + ", processed: " + 
                                          std::to_string(messagesProcessed) + ")");
            }
            
            if (messagesProcessed > 0 && emptyPolls >= MAX_CONSECUTIVE_EMPTY_POLLS) {
                stream_handler_logger.info("Stream timeout: No messages for " + 
                                          std::to_string(MAX_CONSECUTIVE_EMPTY_POLLS) + " seconds");
                stream_handler_logger.info("Edges added: " + std::to_string(localEdgesAdded) + " local, " + 
                                          std::to_string(centralEdgesAdded) + " central");
                stream_handler_logger.warn("Did not receive termination signal (-1). Exiting due to timeout.");
                break;
            }
            continue;
        }
        
        emptyPolls = 0;
        messagesProcessed++;
        
        if (messagesProcessed > lastProgressCheck) {
            emptyPolls = 0;
            lastProgressCheck = messagesProcessed;
        }
        
        if (messagesProcessed % 1000 == 0) {
            stream_handler_logger.info("Progress: Processed " + std::to_string(messagesProcessed) + " messages");
        }
        
        string data(msg.get_payload());
        auto edgeJson = json::parse(data);

        auto prop = edgeJson["properties"];
        prop["graphId"] = to_string(this->graphId);
        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];
        string sId = std::string(sourceJson["id"]);
        string dId = std::string(destinationJson["id"]);
        
        partitionedEdge partitionedEdge = graphPartitioner.addEdge({sId, dId});
        long part_s = partitionedEdge[0].second;
        long part_d = partitionedEdge[1].second;
        
        sourceJson["pid"] = part_s;
        destinationJson["pid"] = part_d;
        string source = sourceJson.dump();
        string destination = destinationJson.dump();
        json obj;
        obj["source"] = sourceJson;
        obj["destination"] = destinationJson;
        obj["properties"] = prop;
        
        int n_workers = atoi((Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers")).c_str());
        long temp_s = part_s % n_workers;
        long temp_d = part_d % n_workers;

        if (!localTemporalStores.empty()) {
            if (part_s == part_d) {
                // Local edge: both nodes in same partition
                if (localTemporalStores.find(part_s) == localTemporalStores.end()) {
                    stream_handler_logger.error("Invalid partition ID " + std::to_string(part_s) + 
                                               " for edge " + sId + "-" + dId);
                    continue;
                }
                uint64_t partitionSnapshot = localTemporalStores[part_s]->getCurrentSnapshotId();
                localTemporalStores[part_s]->addEdge(sId, dId, partitionSnapshot);
                localEdgesAdded++;
                
                if (localTemporalStores[part_s]->shouldCreateSnapshot()) {
                    stream_handler_logger.info("Finalizing temporal snapshot " + std::to_string(partitionSnapshot) + 
                                              " for partition " + std::to_string(part_s));
                    
                    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
                    Utils::createDirectory(snapshotDir);
                    
                    if (localTemporalStores[part_s]->saveSnapshotToDisk(snapshotDir, false)) {
                        stream_handler_logger.info("Saved snapshot " + std::to_string(partitionSnapshot) + 
                                                  " partition " + std::to_string(part_s) + " to disk");
                        localTemporalStores[part_s]->openNewSnapshot();
                    }
                }
            } else {
                // Central edge: cross-partition
                if (centralTemporalStore == nullptr) {
                    stream_handler_logger.error("Central temporal store is null for edge " + sId + "-" + dId);
                    continue;
                }
                uint64_t centralSnapshot = centralTemporalStore->getCurrentSnapshotId();
                centralTemporalStore->addEdge(sId, dId, centralSnapshot);
                centralEdgesAdded++;
                
                if (centralTemporalStore->shouldCreateSnapshot()) {
                    stream_handler_logger.info("Finalizing central snapshot " + std::to_string(centralSnapshot));
                    
                    std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
                    Utils::createDirectory(snapshotDir);
                    
                    if (centralTemporalStore->saveSnapshotToDisk(snapshotDir, false)) {
                        stream_handler_logger.info("Saved central snapshot " + std::to_string(centralSnapshot) + " to disk");
                        centralTemporalStore->openNewSnapshot();
                    }
                }
            }
        }

        // Storing Node block
        if (part_s == part_d) {
            obj["EdgeType"] = "Local";
            obj["PID"] = part_s;
            workerClients.at(temp_s)->publish(obj.dump());
        } else {
            obj["EdgeType"] = "Central";
            obj["PID"] = part_s;
            workerClients.at(temp_s)->publish(obj.dump());
            obj["PID"] = part_d;
            workerClients.at(temp_d)->publish(obj.dump());
        }
    }
    
    stream_handler_logger.info("Edges added: " + std::to_string(localEdgesAdded) + " local, " + 
                              std::to_string(centralEdgesAdded) + " central, " +
                              std::to_string(localEdgesAdded + centralEdgesAdded) + " total");
    stream_handler_logger.info("Kafka consumption completed. Total messages processed: " + std::to_string(messagesProcessed));
    graphPartitioner.updateMetaDB();
    graphPartitioner.printStats();
    
    // Clean up temporal storage if enabled
    if (!localTemporalStores.empty()) {
        stream_handler_logger.info("Finalizing all temporal snapshots");
        
        std::string snapshotDir = Utils::getJasmineGraphHome() + "/env/data/temporal_snapshots";
        Utils::createDirectory(snapshotDir);
        
        // Save all local partition snapshots
        for (auto& [partitionId, store] : localTemporalStores) {
            if (store->saveSnapshotToDisk(snapshotDir, false)) {
                stream_handler_logger.info("Saved final snapshot for partition " + std::to_string(partitionId));
            } else {
                stream_handler_logger.error("Failed to save final snapshot for partition " + std::to_string(partitionId));
            }
            delete store;
        }
        localTemporalStores.clear();
        
        // Save central store snapshot
        if (centralTemporalStore != nullptr) {
            if (centralTemporalStore->saveSnapshotToDisk(snapshotDir, false)) {
                stream_handler_logger.info("Saved final central snapshot");
            } else {
                stream_handler_logger.error("Failed to save final central snapshot");
            }
            delete centralTemporalStore;
            centralTemporalStore = nullptr;
        }
    }
}
