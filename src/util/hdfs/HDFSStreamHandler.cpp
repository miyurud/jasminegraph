/*
 * Copyright 2025 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "HDFSStreamHandler.h"
#include "../../server/JasmineGraphServer.h"
#include "../../partitioner/stream/HDFSMultiThreadedHashPartitioner.h"
#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <mutex>
#include <queue>
#include <regex>
#include <thread>

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

Logger hdfs_stream_handler_logger;

const size_t MESSAGE_SIZE = 5 * 1024 * 1024;
const size_t MAX_BUFFER_SIZE = MESSAGE_SIZE * 512;
const std::string END_OF_STREAM_MARKER = "-1";

HDFSStreamHandler::HDFSStreamHandler(hdfsFS fileSystem, const std::string &filePath, int numberOfPartitions,
                                     int graphId, SQLiteDBInterface *sqlite, std::string masterIP, bool isDirected,
                                     bool isEdgeListType)
        : fileSystem(fileSystem),
          filePath(filePath),
          numberOfPartitions(numberOfPartitions),
          isReading(true),
          isProcessing(true),
          graphId(graphId),
          sqlite(sqlite),
          masterIP(masterIP),
          isDirected(isDirected),
          isEdgeListType(isEdgeListType) {}

void HDFSStreamHandler::streamFromHDFSIntoBuffer() {
    auto startTime = high_resolution_clock::now();
    hdfs_stream_handler_logger.info("Started streaming data from HDFS into data buffer...");

    hdfsFile file = hdfsOpenFile(fileSystem, filePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        hdfs_stream_handler_logger.error("Failed to open HDFS file.");
        isReading = false;
        dataBufferCV.notify_all();
        return;
    }

    std::vector<char> buffer(MESSAGE_SIZE);
    tSize numReadBytes = 0;
    std::string leftover;

    while ((numReadBytes = hdfsRead(fileSystem, file, buffer.data(), MESSAGE_SIZE)) > 0) {
        std::string data(buffer.data(), numReadBytes);
        data = leftover + data;
        leftover.clear();

        std::istringstream dataStream(data);
        std::string line;
        while (std::getline(dataStream, line)) {
            if (dataStream.eof() && data.back() != '\n') {
                leftover = line;
            } else {
                std::unique_lock<std::mutex> lock(dataBufferMutex);
                dataBufferCV.wait(lock, [this] { return dataBuffer.size() < MAX_BUFFER_SIZE || !isReading; });
                dataBuffer.push(line);
                lock.unlock();
                dataBufferCV.notify_one();
            }
        }
    }

    hdfsCloseFile(fileSystem, file);
    isReading = false;

    if (!leftover.empty()) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(leftover);
        lock.unlock();
        dataBufferCV.notify_all();
    }

    {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(END_OF_STREAM_MARKER);
        lock.unlock();
    }

    dataBufferCV.notify_all();
    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    hdfs_stream_handler_logger.debug("Successfully streamed data from HDFS into data buffer.");
    hdfs_stream_handler_logger.info("Time taken to read from HDFS: " + to_string(duration.count()) + " seconds");
}

void HDFSStreamHandler::streamFromBufferToProcessingQueueEdgeListGraph(HDFSMultiThreadedHashPartitioner &partitioner) {
    auto startTime = high_resolution_clock::now();

    while (isProcessing) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return !dataBuffer.empty() || !isReading; });

        if (!dataBuffer.empty()) {
            std::string line = dataBuffer.front();
            dataBuffer.pop();
            lock.unlock();

            // Check for end-of-stream marker
            if (line == END_OF_STREAM_MARKER) {
                hdfs_stream_handler_logger.debug("Received end-of-stream marker in one of the threads.");
                isProcessing = false;
                dataBufferCV.notify_all();
                break;
            }

            // Tokenize line by whitespace or comma
            std::regex delimiterRegex("\\s+|,");
            std::sregex_token_iterator iter(line.begin(), line.end(), delimiterRegex, -1);
            std::sregex_token_iterator end;
            std::vector<std::string> tokens(iter, end);

            if (tokens.size() == 2) {
                std::string sourceId = tokens[0];
                std::string destId = tokens[1];

                try {
                    int sourceIndex = std::stoi(sourceId) % this->numberOfPartitions;
                    int destIndex = std::stoi(destId) % this->numberOfPartitions;

                    // Construct source and destination node objects
                    json source = {
                        {"id", sourceId},
                        {"pid", sourceIndex},
                        {"properties", { {"id", sourceId} }}
                    };

                    json destination = {
                        {"id", destId},
                        {"pid", destIndex},
                        {"properties", { {"id", destId} }}
                    };

                    // Construct edge object
                    json edgeObj = {
                        {"source", source},
                        {"destination", destination},
                        {"properties", json::object()}  // Add edge-level properties if needed
                    };

                    // Add edge to partitioner
                    if (sourceIndex == destIndex) {
                        partitioner.addLocalEdge(edgeObj.dump(), sourceIndex);
                    } else {
                        // Add both directions for edge cut
                        partitioner.addEdgeCut(edgeObj.dump(), sourceIndex);

                        json reversedObj = {
                            {"source", destination},
                            {"destination", source},
                            {"properties", edgeObj["properties"]}
                        };
                        partitioner.addEdgeCut(reversedObj.dump(), destIndex);
                    }
                } catch (const std::invalid_argument &e) {
                    hdfs_stream_handler_logger.error("Invalid numeric node ID in line: " + line);
                } catch (const std::out_of_range &e) {
                    hdfs_stream_handler_logger.error("Node ID out of range in line: " + line);
                }
            } else {
                hdfs_stream_handler_logger.error("Malformed line (unexpected token count): " + line);
            }
        } else if (!isReading) {
            break;
        }
    }
}


void HDFSStreamHandler::streamFromBufferToProcessingQueuePropertyGraph(HDFSMultiThreadedHashPartitioner &partitioner) {
    auto startTime = high_resolution_clock::now();

    while (isProcessing) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return !dataBuffer.empty() || !isReading; });

        if (!dataBuffer.empty()) {
            std::string line = dataBuffer.front();
            dataBuffer.pop();
            lock.unlock();

            // Check for end-of-stream marker
            if (line == END_OF_STREAM_MARKER) {
                hdfs_stream_handler_logger.debug("Received end-of-stream marker in one of the threads.");
                isProcessing = false;
                dataBufferCV.notify_all();
                break;
            }

            try {
                auto jsonEdge = json::parse(line);
                auto source = jsonEdge["source"];
                auto destination = jsonEdge["destination"];
                std::string sourceId = source["id"];
                std::string destinationId = destination["id"];

                if (!sourceId.empty() && !destinationId.empty()) {
                    int sourceIndex = std::stoi(sourceId) % this->numberOfPartitions;
                    int destIndex = std::stoi(destinationId) % this->numberOfPartitions;

                    source["pid"] = sourceIndex;
                    destination["pid"] = destIndex;

                    json obj = {
                        {"source", source},
                        {"destination", destination},
                        {"properties", jsonEdge["properties"]}
                    };

                    if (sourceIndex == destIndex) {
                        partitioner.addLocalEdge(obj.dump(), sourceIndex);
                    } else {
                        partitioner.addEdgeCut(obj.dump(), sourceIndex);

                        // json reversedObj = {
                        //     {"source", destination},
                        //     {"destination", source},
                        //     {"properties", jsonEdge["properties"]}
                        // };
                        //
                        // partitioner.addEdgeCut(reversedObj.dump(), destIndex);
                    }
                } else {
                    hdfs_stream_handler_logger.error("Malformed line: missing source/destination ID: " + line);
                }
            } catch (const json::parse_error &e) {
                hdfs_stream_handler_logger.error("JSON parse error: " + std::string(e.what()) + " | Line: " + line);
            } catch (const std::invalid_argument &e) {
                hdfs_stream_handler_logger.error("Invalid node ID (not an integer) in line: " + line);
            } catch (const std::out_of_range &e) {
                hdfs_stream_handler_logger.error("Node ID out of range in line: " + line);
            }
        } else if (!isReading) {
            break;
        }
    }
}


void HDFSStreamHandler::startStreamingFromBufferToPartitions() {
    auto startTime = high_resolution_clock::now();

    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    std::vector<JasmineGraphServer::worker> workers = server->workers(numberOfPartitions);
    HDFSMultiThreadedHashPartitioner partitioner(numberOfPartitions, graphId, masterIP, isDirected, workers);

    std::thread readerThread(&HDFSStreamHandler::streamFromHDFSIntoBuffer, this);
    std::vector<std::thread> bufferProcessorThreads;

    if (isEdgeListType) {
        for (int i = 0; i < Conts::HDFS::EDGE_SEPARATION_LAYER_THREAD_COUNT; ++i) {
            bufferProcessorThreads.emplace_back(&HDFSStreamHandler::streamFromBufferToProcessingQueueEdgeListGraph,
                this, std::ref(partitioner));
        }
    } else {
        for (int i = 0; i < Conts::HDFS::EDGE_SEPARATION_LAYER_THREAD_COUNT; ++i) {
            bufferProcessorThreads.emplace_back(&HDFSStreamHandler::streamFromBufferToProcessingQueuePropertyGraph,
                this, std::ref(partitioner));
        }
    }

    readerThread.join();
    for (auto &thread : bufferProcessorThreads) {
        thread.join();
    }

    long vertices = partitioner.getVertexCount();
    long edges = partitioner.getEdgeCount();

    std::string sqlStatement = "UPDATE graph SET vertexcount = '" + std::to_string(vertices) +
                               "', centralpartitioncount = '" + std::to_string(this->numberOfPartitions) +
                               "', edgecount = '" + std::to_string(edges) +
                               "', graph_status_idgraph_status = '" + std::to_string(Conts::GRAPH_STATUS::OPERATIONAL) +
                               "' WHERE idgraph = '" + std::to_string(this->graphId) + "'";

    dbLock.lock();
    this->sqlite->runUpdate(sqlStatement);
    dbLock.unlock();

    partitioner.updatePartitionTable();

    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    hdfs_stream_handler_logger.info(
            "Total time taken for streaming from HDFS into partitions: " + to_string(duration.count()) + " seconds");
}
