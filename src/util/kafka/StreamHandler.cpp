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
#include <iomanip>
#include <sstream>

#include "../logger/Logger.h"
#include "../Utils.h"
#include "../../server/JasmineGraphServer.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;
Logger stream_handler_logger;

StreamHandler::StreamHandler(KafkaConnector *kstream, int numberOfPartitions,
                             vector<DataPublisher *> &workerClients, SQLiteDBInterface* sqlite,
                             int graphId, bool isDirected, spt::Algorithms algorithms, bool enableTemporal, bool enableOperationType)
        : kstream(kstream),
          graphId(graphId),
          workerClients(workerClients),
          graphPartitioner(numberOfPartitions, graphId, algorithms, sqlite, isDirected),
          stream_topic_name("stream_topic_name"),
          temporalStreamingEnabled(enableTemporal),
          operationTypeEnabled(true) { // Always enable operation type processing
    
    if (temporalStreamingEnabled) {
        stream_handler_logger.info("Temporal streaming enabled for graph " + to_string(graphId));
    } else {
        stream_handler_logger.info("Standard streaming mode for graph " + to_string(graphId));
    }
    
    // Operation type processing is always enabled - will check JSON for 'operation_type' field
    stream_handler_logger.info("Operation type processing enabled - will automatically detect ADD/EDIT/DELETE operations from JSON 'operation_type' field");
}


// Polls kafka for a message.
cppkafka::Message StreamHandler::pollMessage() { return kstream->consumer.poll(std::chrono::milliseconds(1000)); }

// Checks if there's an error in Kafka's message.
bool StreamHandler::isErrorInMessage(const cppkafka::Message &msg) {
    if (!msg || msg.get_error()) {
        frontend_logger.log("Couldn't retrieve message from Kafka.", "info");
        return true;
    }
    return false;
}

// Ends the stream if the end message("-1") has been received.
bool StreamHandler::isEndOfStream(const cppkafka::Message &msg) {
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

    while (true) {
        cppkafka::Message msg = this->pollMessage();

        if (this->isEndOfStream(msg)) {
            frontend_logger.info("Received the end of `" + stream_topic_name + "` input kafka stream");
            for (auto &workerClient : workerClients) {
                if (workerClient != nullptr) {
                    workerClient->publish("-1");
                }
            }
            break;
        }

        if (this->isErrorInMessage(msg)) {
            frontend_logger.log("Couldn't retrieve message from Kafka.", "info");
            continue;
        }
        string data(msg.get_payload());
        auto edgeJson = json::parse(data);

        // Add ingestion timestamp (system clock)
        auto ingestionTime = chrono::system_clock::now();
        auto ingestionTimeMs = chrono::duration_cast<chrono::milliseconds>(ingestionTime.time_since_epoch()).count();
        
        auto prop = edgeJson["properties"];
        prop["graphId"] = to_string(this->graphId);
        prop["ingestion_timestamp"] = ingestionTimeMs;
        
        // Handle temporal streaming if enabled
        if (temporalStreamingEnabled) {
            if (edgeJson.contains("event_timestamp")) {
                // Use provided event timestamp
                prop["event_timestamp"] = edgeJson["event_timestamp"];
                stream_handler_logger.debug("Processing edge with event_timestamp: " + 
                                          edgeJson["event_timestamp"].dump() + 
                                          ", ingestion_timestamp: " + to_string(ingestionTimeMs));
            } else {
                // Log warning if temporal streaming is enabled but no event timestamp provided
                stream_handler_logger.warn("Temporal streaming enabled but no 'event_timestamp' field found in message: " + data);
                // Use ingestion time as fallback for event time
                prop["event_timestamp"] = ingestionTimeMs;
            }
            
            // Calculate processing latency
            if (edgeJson.contains("event_timestamp")) {
                try {
                    uint64_t eventTime = 0;
                    if (edgeJson["event_timestamp"].is_number()) {
                        eventTime = edgeJson["event_timestamp"].get<uint64_t>();
                    } else if (edgeJson["event_timestamp"].is_string()) {
                        // Parse ISO timestamp or numeric string
                        string eventTimeStr = edgeJson["event_timestamp"].get<string>();
                        eventTime = stoull(eventTimeStr);
                    }
                    
                    if (eventTime > 0) {
                        int64_t latencyMs = ingestionTimeMs - eventTime;
                        prop["processing_latency_ms"] = latencyMs;
                        
                        if (latencyMs > 1000) { // Log if latency > 1 second
                            stream_handler_logger.warn("High processing latency detected: " + 
                                                     to_string(latencyMs) + "ms for edge");
                        }
                    }
                } catch (const exception& e) {
                    stream_handler_logger.error("Error processing event_timestamp: " + string(e.what()));
                    prop["event_timestamp"] = ingestionTimeMs; // Fallback to ingestion time
                }
            }
        } else {
            // Standard mode: only use ingestion timestamp
            prop["timestamp"] = ingestionTimeMs;
        }
        
        // Always check for operation type in JSON data
        string operationType = "ADD"; // Default operation
        if (edgeJson.contains("operation_type")) {
            string opType = edgeJson["operation_type"].get<string>();
            // Normalize operation type
            transform(opType.begin(), opType.end(), opType.begin(), ::toupper);
            
            if (opType == "ADD" || opType == "EDIT" || opType == "DELETE") {
                operationType = opType;
                stream_handler_logger.debug("Processing " + operationType + " operation for edge: " + 
                                           edgeJson["source"]["id"].get<string>() + " -> " + 
                                           edgeJson["destination"]["id"].get<string>());
            } else {
                stream_handler_logger.warn("Invalid operation_type '" + opType + "' found in message, defaulting to ADD");
                operationType = "ADD";
            }
        }
        // Always add operation type to properties for downstream processing
        prop["operation_type"] = operationType; else {
            // Standard mode: all operations are additions
            prop["operation_type"] = operationType;
        }
        
        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];
        string sId = std::string(sourceJson["id"]);
        string dId = std::string(destinationJson["id"]);
        partitionedEdge partitionedEdge = graphPartitioner.addEdge({sId, dId});
        sourceJson["pid"] = partitionedEdge[0].second;
        destinationJson["pid"] = partitionedEdge[1].second;
        string source = sourceJson.dump();
        string destination = destinationJson.dump();
        json obj;
        obj["source"] = sourceJson;
        obj["destination"] = destinationJson;
        obj["properties"] = prop;
        long part_s = partitionedEdge[0].second;
        long part_d = partitionedEdge[1].second;
        int n_workers = atoi((Utils::getJasmineGraphProperty("org.jasminegraph.server.nworkers")).c_str());
        long temp_s = part_s % n_workers;
        long temp_d = part_d % n_workers;

        // Route edge to appropriate workers with operation type
        if (part_s == part_d) {
            obj["EdgeType"] = "Local";
            obj["PID"] = part_s;
            obj["OperationType"] = operationType;
            
            // Log operation details
            stream_handler_logger.info("Routing " + operationType + " operation for Local edge (" + 
                                     sId + " -> " + dId + ") to worker " + to_string(temp_s) + 
                                     ", partition " + to_string(part_s));
            
            workerClients.at(temp_s)->publish(obj.dump());
        } else {
            obj["EdgeType"] = "Central";
            obj["OperationType"] = operationType;
            
            // Log operation details
            stream_handler_logger.info("Routing " + operationType + " operation for Central edge (" + 
                                     sId + " -> " + dId + ") to workers " + to_string(temp_s) + 
                                     " and " + to_string(temp_d) + ", partitions " + 
                                     to_string(part_s) + " and " + to_string(part_d));
            
            // Send to source partition worker
            obj["PID"] = part_s;
            workerClients.at(temp_s)->publish(obj.dump());
            
            // Send to destination partition worker
            obj["PID"] = part_d;
            workerClients.at(temp_d)->publish(obj.dump());
        }
    }
    graphPartitioner.updateMetaDB();
    graphPartitioner.printStats();
}
