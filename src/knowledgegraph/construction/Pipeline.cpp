/**
Copyright 2025 JasmineGraph Team
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

#include "Pipeline.h"

#include <hdfs.h>
#include <stdlib.h>

#include <chrono>
#include <functional>  // for std::hash
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <queue>
#include <regex>
#include <sstream>
#include <string>
#include <thread>

#include "../../partitioner/stream/HDFSMultiThreadedHashPartitioner.h"
#include "../../server/JasmineGraphServer.h"
class SQLiteDBInterface;

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

Logger kg_pipeline_stream_handler_logger;

const size_t MESSAGE_SIZE = 5 * 1024 * 1024;
const size_t MAX_BUFFER_SIZE = MESSAGE_SIZE * 512;
const size_t OVERLAP_BYTES = 1024;  // bytes to overlap between chunks to avoid splitting lines


const std::string END_OF_STREAM_MARKER = "-1";

Pipeline::Pipeline(int connFd, hdfsFS fileSystem, const std::string& filePath, int numberOfPartitions, int graphId,
                   std::string masterIP, vector<JasmineGraphServer::worker>& workerList,
                   std::vector<std::string> llmRunners, std::string llmInferenceEngine, std::string llm,
                   std::string chunkSize, std::string chunksPerBatch, long startFromBytes)
    : connFd(connFd),
      fileSystem(fileSystem),
      filePath(filePath),
      numberOfPartitions(numberOfPartitions),
      isReading(true),
      isProcessing(true),
      graphId(graphId),
      masterIP(masterIP),
      workerList(workerList),
      llmRunners(llmRunners),
      llmInferenceEngine(llmInferenceEngine),
      llm(llm),
      chunkSize(chunkSize),
      chunksPerBatch(chunksPerBatch),
      startFromBytes(startFromBytes) {}

void Pipeline::init() { startStreamingFromBufferToWorkers(); }

// ---------- document_reader ----------
// Reads chunks of fixed byte size from an AHDFS file and pushes them as chunks
// with generated doc_id
void Pipeline::streamFromHDFSIntoBuffer() {
    auto startTime = chrono::high_resolution_clock::now();
    kg_pipeline_stream_handler_logger.info("Started streaming data from HDFS into data buffer...");
    hdfsFileInfo* fileInfo = hdfsGetPathInfo(fileSystem, filePath.c_str());
    if (!fileInfo) {
        kg_pipeline_stream_handler_logger.error("Failed to get HDFS file info.");
        return;
    }
    int64_t total_file_size = fileInfo->mSize;
    hdfsFreeFileInfo(fileInfo, 1);
    hdfsFile file = hdfsOpenFile(fileSystem, filePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        kg_pipeline_stream_handler_logger.error("Failed to open HDFS file.");
        isReading = false;
        dataBufferCV.notify_all();
        return;
    }

    kg_pipeline_stream_handler_logger.info("Successfully opened HDFS file: " + filePath);

    std::vector<char> buffer(std::stol(chunkSize));
    int64_t read_bytes = 0;
    int chunk_idx = 0;
    std::string leftover;

    if (startFromBytes > 0) {
        if (startFromBytes >= total_file_size) {
            kg_pipeline_stream_handler_logger.error("startFromBytes exceeds total file size.");
            hdfsCloseFile(fileSystem, file);
            return;
        }

        if (hdfsSeek(fileSystem, file, startFromBytes) != 0) {
            kg_pipeline_stream_handler_logger.error("Failed to seek to startFromBytes: " +
                                                    std::to_string(startFromBytes));
            hdfsCloseFile(fileSystem, file);
            return;
        }
        bytes_read_so_far = startFromBytes;  // Initialize with startFromBytes
        realtime_bytes_read_so_far = startFromBytes;

        kg_pipeline_stream_handler_logger.info("Starting read from byte offset: " + std::to_string(startFromBytes));
    }
    while ((read_bytes = hdfsRead(fileSystem, file, buffer.data(), std::stol(chunkSize))) > 0) {
        if (stopFlag) {
            isReading = false;
            kg_pipeline_stream_handler_logger.info("Received Stop command , terminating");
            break;
        }

        int64_t remaining_bytes = total_file_size - bytes_read_so_far;
        double percent_read = (static_cast<double>(bytes_read_so_far) / total_file_size) * 100.0;

        kg_pipeline_stream_handler_logger.info("Chunk " + std::to_string(chunk_idx) +
                                               " read: " + std::to_string(read_bytes) + " bytes, " +
                                               "remaining bytes: " + std::to_string(remaining_bytes) + ", " +
                                               "progress: " + std::to_string(percent_read) + "%");
        kg_pipeline_stream_handler_logger.info("Starting to process chunk " + std::to_string(chunk_idx));
        std::string chunk_text = leftover + std::string(buffer.data(), read_bytes);

        kg_pipeline_stream_handler_logger.info("Read chunk " + std::to_string(chunk_idx) + " with " +
                                               std::to_string(read_bytes) + " bytes");
        kg_pipeline_stream_handler_logger.info("Current leftover size: " + std::to_string(leftover.size()));

        // Find last newline to keep only complete lines in chunk pushed to
        // dataBuffer
        size_t last_newline = chunk_text.find_last_of('\n');
        if (last_newline == std::string::npos) {
            kg_pipeline_stream_handler_logger.info("No newline found in chunk " + std::to_string(chunk_idx) +
                                                   ", storing as leftover");
            leftover = chunk_text;
            kg_pipeline_stream_handler_logger.info("Updated leftover size: " + std::to_string(leftover.size()));
            continue;
        }

        // Split into complete lines and leftover partial line
        std::string full_lines_chunk = chunk_text.substr(0, last_newline + 1);
        size_t overlap_start = (full_lines_chunk.size() > OVERLAP_BYTES) ? full_lines_chunk.size() - OVERLAP_BYTES : 0;

        // make sure leftover starts at a newline
        size_t newline_pos = full_lines_chunk.find('\n', overlap_start);
        if (newline_pos != std::string::npos && newline_pos + 1 < full_lines_chunk.size()) {
            leftover = full_lines_chunk.substr(newline_pos + 1);
        } else {
            leftover.clear();
        }

        kg_pipeline_stream_handler_logger.info("Full lines chunk size: " + std::to_string(full_lines_chunk.size()));
        kg_pipeline_stream_handler_logger.info("Leftover after split size: " + std::to_string(leftover.size()));
        kg_pipeline_stream_handler_logger.info("Pushing chunk " + std::to_string(chunk_idx) + " to dataBuffer");

        // Wait and push to dataBuffer safely
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        kg_pipeline_stream_handler_logger.info("Waiting to acquire lock for dataBuffer push");
        dataBufferCV.wait(lock, [this] { return dataBuffer.size() < workerList.size() || !isReading; });

        dataBuffer.push(Chunk{to_string(chunk_idx), std::move(full_lines_chunk), read_bytes});
        kg_pipeline_stream_handler_logger.info(
            "Chunk " + std::to_string(chunk_idx) +
            " pushed to dataBuffer. Current buffer size: " + std::to_string(dataBuffer.size()));
        lock.unlock();
        dataBufferCV.notify_all();

        chunk_idx++;
        kg_pipeline_stream_handler_logger.info("Finished processing chunk " + std::to_string(chunk_idx));
    }

    // Push leftover partial line if any (last chunk)
    if (!leftover.empty()) {
        chunk_idx++;
        kg_pipeline_stream_handler_logger.info("Pushing leftover data to dataBuffer");
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return dataBuffer.size() < workerList.size() || !isReading; });
        dataBuffer.push(Chunk{to_string(chunk_idx), std::move(leftover), read_bytes});

        lock.unlock();
        dataBufferCV.notify_one();
    }

    if (read_bytes < 0) {
        kg_pipeline_stream_handler_logger.error("Error reading from AHDFS file");
    }

    hdfsCloseFile(fileSystem, file);
    kg_pipeline_stream_handler_logger.info("Closed HDFS file: " + filePath);
    isReading = false;

    if (!leftover.empty()) {
        kg_pipeline_stream_handler_logger.info("Pushing leftover data again to data buffer after closing file");
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(Chunk{to_string(chunk_idx), std::move(leftover), read_bytes});

        lock.unlock();
        dataBufferCV.notify_all();
    }

    {
        kg_pipeline_stream_handler_logger.info("Pushing END_OF_STREAM_MARKER to data buffer");
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        // close all workers
        for (auto worker : workerList) {
            dataBuffer.push(Chunk{to_string(chunk_idx), END_OF_STREAM_MARKER, read_bytes});
        }
        lock.unlock();
    }

    dataBufferCV.notify_all();
    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    kg_pipeline_stream_handler_logger.debug("Successfully streamed data from HDFS into data buffer.");
    kg_pipeline_stream_handler_logger.info("Time taken to read from HDFS: " + to_string(duration.count()) + " seconds");
}

void Pipeline::startStreamingFromBufferToWorkers() {
    auto startTime = high_resolution_clock::now();

    std::thread readerThread(&Pipeline::streamFromHDFSIntoBuffer, this);
    std::vector<std::unique_ptr<SharedBuffer>> bufferPool;

    bufferPool.reserve(stoi(chunksPerBatch));  // Pre-allocate space for pointers
    for (size_t i = 0; i < stoi(chunksPerBatch); ++i) {
        bufferPool.emplace_back(std::make_unique<SharedBuffer>(MASTER_BUFFER_SIZE));
    }

    std::vector<std::thread> workerThreads;
    int count = 0;
    for (auto& worker : workerList) {
        if (count >= stoi(chunksPerBatch)) break;
        workerThreads.emplace_back(&Pipeline::extractTuples, this, worker.hostname, worker.port, masterIP, graphId,
                                   count, std::ref(dataBuffer), std::ref(*bufferPool[count]));
        count++;
    }

    string meta = processTupleAndSaveInPartition(bufferPool).dump();
    if (readerThread.joinable()) {
        readerThread.join();
    }

    for (auto& worker : workerThreads) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    isProcessing = false;

    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;

    kg_pipeline_stream_handler_logger.info(meta);
    char data[FED_DATA_LENGTH + 1];

    Utils::sendExpectResponse(connFd, data, INSTANCE_DATA_LENGTH, META.c_str(), JasmineGraphInstanceProtocol::OK);

    char ack3[ACK_MESSAGE_SIZE] = {0};
    int message_length = meta.length();
    int converted_number = htonl(message_length);
    kg_pipeline_stream_handler_logger.debug("Sending content length: " + to_string(converted_number));

    if (!Utils::sendIntExpectResponse(connFd, ack3, JasmineGraphInstanceProtocol::OK.length(), converted_number,
                                      JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::CLOSE);
        close(connFd);
        return;
    }

    if (!Utils::send_str_wrapper(connFd, meta)) {
        close(connFd);
        return;
    }
    Utils::sendExpectResponse(connFd, data, INSTANCE_DATA_LENGTH, DONE.c_str(), JasmineGraphInstanceProtocol::OK);

    kg_pipeline_stream_handler_logger.info(
        "Total time taken for streaming from HDFS into partitions: " + to_string(duration.count()) + " seconds");
}

json Pipeline::processTupleAndSaveInPartition(const std::vector<std::unique_ptr<SharedBuffer>>& tupleBuffer) {
    auto startTime = high_resolution_clock::now();
    HDFSMultiThreadedHashPartitioner partitioner(numberOfPartitions, graphId, masterIP, true, workerList, true, 5);
    std::hash<std::string> hasher;
    std::vector<std::thread> tupleThreads;
    for (size_t i = 0; i < tupleBuffer.size(); ++i) {
        SharedBuffer* tupleBufferRef = tupleBuffer[i].get();
        kg_pipeline_stream_handler_logger.info("Launching tuple thread for partition " + std::to_string(i));
        tupleThreads.emplace_back([&, tupleBufferRef, i]() {
            int realtimePartitionMetaUpdateIndicator = 0;
            kg_pipeline_stream_handler_logger.info("Tuple thread started for partition " + std::to_string(i));
            while (isProcessing) {
                if (!tupleBufferRef->empty()) {
                    realtimePartitionMetaUpdateIndicator++;
                    if (realtimePartitionMetaUpdateIndicator != 0 && realtimePartitionMetaUpdateIndicator % 5 == 0) {
                        std::unique_lock<std::mutex> lock(realTimeBytesUpdateMutex);

                        json meta;
                        json graph;
                        graph["vertexcount"] = partitioner.getVertexCount();
                        graph["edgecount"] = partitioner.getEdgeCount();
                        graph["centralpartitioncount"] = this->numberOfPartitions;
                        graph["graph_status_idgraph_status"] = Conts::GRAPH_STATUS::OPERATIONAL;

                        meta["graph"] = graph;
                        meta["partitions"] = partitioner.getPartitionsMeta();

                        char data[FED_DATA_LENGTH + 1];

                        Utils::send_str_wrapper(connFd, META);
                        string response = Utils::read_str_wrapper(connFd, data, FED_DATA_LENGTH);

                        char ack3[ACK_MESSAGE_SIZE] = {0};
                        string metaS = meta.dump();
                        int message_length = metaS.length();
                        int converted_number = htonl(message_length);
                        kg_pipeline_stream_handler_logger.debug("Sending content length: " +
                                                                to_string(converted_number));

                        if (!Utils::sendIntExpectResponse(connFd, ack3, JasmineGraphInstanceProtocol::OK.length(),
                                                          converted_number, JasmineGraphInstanceProtocol::OK)) {
                            Utils::send_str_wrapper(connFd, JasmineGraphInstanceProtocol::CLOSE);
                            close(connFd);
                            return;
                        }

                        if (!Utils::send_str_wrapper(connFd, metaS)) {
                            close(connFd);
                            return;
                        }
                        char ack2[FED_DATA_LENGTH + 1];
                        Utils::send_str_wrapper(connFd, std::to_string(realtime_bytes_read_so_far));

                        response = Utils::read_str_wrapper(connFd, ack2, FED_DATA_LENGTH);

                        if (response == "stop") {
                            kg_pipeline_stream_handler_logger.info("stop request:" + response);
                            stopFlag = true;
                        }
                        lock.unlock();
                    }

                    std::string line = tupleBufferRef->get();
                    kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) +
                                                            " processing line: " + line);
                    // Check for end-of-stream marker
                    if (line == END_OF_STREAM_MARKER) {
                        kg_pipeline_stream_handler_logger.debug("Received end-of-stream marker in thread " +
                                                                std::to_string(i));
                        break;
                    }
                    try {
                        auto jsonEdge = json::parse(line);
                        auto source = jsonEdge["source"];
                        auto destination = jsonEdge["destination"];
                        std::string sourceId = source["id"].get<std::string>();
                        std::string destinationId = destination["id"].get<std::string>();

                        if (nodeIndex.find(sourceId) == nodeIndex.end()) {
                            nodeIndex.insert({sourceId, nextNodeIndex});
                            source["id"]= to_string(nextNodeIndex);
                            source["properties"]["id"]= to_string(nextNodeIndex);

                            nextNodeIndex++;
                        } else {
                            source["id"]= to_string(nodeIndex[sourceId]);
                            source["properties"]["id"]= to_string(nodeIndex[sourceId]);
                        }

                        if (nodeIndex.find(destinationId) == nodeIndex.end()) {
                            nodeIndex.insert({destinationId, nextNodeIndex});
                            destination["id"]= to_string(nextNodeIndex);
                            destination["properties"]["id"]= to_string(nextNodeIndex);
                            nextNodeIndex++;
                        } else {
                            destination["id"]= to_string(nodeIndex[destinationId]);
                            destination["properties"]["id"]= to_string(nodeIndex[destinationId]);
                        }
                        kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) + " sourceId: " +
                                                                sourceId + ", destinationId: " + destinationId);
                        if (!sourceId.empty() && !destinationId.empty()) {
                            int sourceIndex = i % this->numberOfPartitions;
                            int destIndex = i % this->numberOfPartitions;
                            source["pid"] = sourceIndex;
                            destination["pid"] = destIndex;
                            json obj = {{"source", source},
                                        {"destination", destination},
                                        {"properties", jsonEdge["properties"]}};

                            if (sourceIndex == destIndex) {
                                kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) +
                                                                        " adding local edge to partition " +
                                                                        std::to_string(sourceIndex));
                                partitioner.addLocalEdge(obj.dump(), sourceIndex);
                            } else {
                                kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) +
                                                                        " adding edge cut to partition " +
                                                                        std::to_string(sourceIndex));
                                partitioner.addEdgeCut(obj.dump(), sourceIndex);
                            }
                        } else {
                            kg_pipeline_stream_handler_logger.error("Malformed line: missing source/destination ID: " +
                                                                    line);
                        }
                    } catch (const json::parse_error& e) {
                        kg_pipeline_stream_handler_logger.error("JSON parse error: " + std::string(e.what()) +
                                                                " | Line: " + line);
                    } catch (const std::invalid_argument& e) {
                        kg_pipeline_stream_handler_logger.error("Invalid node ID (not an integer) in line: " + line);
                    } catch (const std::out_of_range& e) {
                        kg_pipeline_stream_handler_logger.error("Node ID out of range in line: " + line);
                    } catch (const std::exception& e) {
                        kg_pipeline_stream_handler_logger.error("Unexpected exception in line: " +
                                                                std::string(e.what()));
                    }
                }
            }
            kg_pipeline_stream_handler_logger.info("Tuple thread finished for partition " + std::to_string(i));
        });
    }
    for (auto& t : tupleThreads) {
        t.join();
    }


    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    kg_pipeline_stream_handler_logger.info("processTupleAndSaveInPartition completed in " +
                                           std::to_string(duration.count()) + " seconds");

    json meta;

    json graph;
    graph["vertexcount"] = partitioner.getVertexCount();
    graph["edgecount"] = partitioner.getEdgeCount();
    graph["centralpartitioncount"] = this->numberOfPartitions;
    graph["graph_status_idgraph_status"] = Conts::GRAPH_STATUS::OPERATIONAL;

    meta["graph"] = graph;
    meta["partitions"] = partitioner.getPartitionsMeta();
    return meta;
}

void Pipeline::extractTuples(std::string host, int port, std::string masterIP, int graphID, int partitionId,
                             std::queue<Chunk>& dataBuffer, SharedBuffer& sharedBuffer) {
    kg_pipeline_stream_handler_logger.info("Starting extractTuples for host: " + host + ", port: " +
                                           std::to_string(port) + ", partitionId: " + std::to_string(partitionId));
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent* server;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        kg_pipeline_stream_handler_logger.error("Cannot create socket");
        return;
    }
    kg_pipeline_stream_handler_logger.info("Socket created successfully");

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
        kg_pipeline_stream_handler_logger.info("Host after split: " + host);
    }

    server = gethostbyname(host.c_str());
    if (!server) {
        kg_pipeline_stream_handler_logger.error("ERROR, no host named " + host);
        return;
    }
    kg_pipeline_stream_handler_logger.info("Host resolved: " + host);

    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        kg_pipeline_stream_handler_logger.error("Failed to connect to host: " + host +
                                                " on port: " + std::to_string(port));
        return;
    }
    kg_pipeline_stream_handler_logger.info("Connected to host: " + host + " on port: " + std::to_string(port));

    // 1. Perform handshake
    kg_pipeline_stream_handler_logger.info("Performing handshake with masterIP: " + masterIP);
    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        kg_pipeline_stream_handler_logger.error("Handshake failed");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    kg_pipeline_stream_handler_logger.info("Handshake successful");

    // 2. Send INITIATE_STREAMING_TUPLE_CONSTRUCTION
    kg_pipeline_stream_handler_logger.info("Sending INITIATE_STREAMING_TUPLE_CONSTRUCTION");
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                   JasmineGraphInstanceProtocol::INITIATE_STREAMING_TUPLE_CONSTRUCTION,
                                   JasmineGraphInstanceProtocol::OK)) {
        kg_pipeline_stream_handler_logger.error("Failed to initiate streaming tuple construction");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    kg_pipeline_stream_handler_logger.info("INITIATE_STREAMING_TUPLE_CONSTRUCTION successful");

    // 3. Send graph ID
    kg_pipeline_stream_handler_logger.info("Sending graphID: " + std::to_string(graphID));
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, std::to_string(graphID),
                                   JasmineGraphInstanceProtocol::OK)) {
        kg_pipeline_stream_handler_logger.error("Failed to send graphID");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    kg_pipeline_stream_handler_logger.info("GraphID sent successfully");

    // 3. Send LLM runner info
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, llmRunners[partitionId],
                                   JasmineGraphInstanceProtocol::OK)) {
        kg_pipeline_stream_handler_logger.error("Failed to send LLM runner info");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, llmInferenceEngine,
                                   JasmineGraphInstanceProtocol::OK)) {
        kg_pipeline_stream_handler_logger.error("Failed to send LLM runner info");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    kg_pipeline_stream_handler_logger.info("LLM runner sent successfully");

    // 3. Send LLM  info
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, llm, JasmineGraphInstanceProtocol::OK)) {
        kg_pipeline_stream_handler_logger.error("Failed to send LLM runner info");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    kg_pipeline_stream_handler_logger.info("LLM  sent successfully");
    // Streaming loop
    while (true) {
        if (stopFlag) {
            kg_pipeline_stream_handler_logger.info("Received END_OF_STREAM_MARKER for partitionId: " +
                                                   std::to_string(partitionId));
            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                           JasmineGraphInstanceProtocol::CHUNK_STREAM_END,
                                           JasmineGraphInstanceProtocol::OK)) {
                kg_pipeline_stream_handler_logger.error("Failed to send END_OF_STREAM");
            }
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            kg_pipeline_stream_handler_logger.info("Closed connection for partitionId: " + std::to_string(partitionId));
            sharedBuffer.add(END_OF_STREAM_MARKER);
            close(sockfd);
            break;
        }
        std::string chunk;

        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this, &dataBuffer] { return !dataBuffer.empty() || !this->isReading; });

        Chunk chunkData = dataBuffer.front();
        chunk = chunkData.text;

        dataBuffer.pop();

        kg_pipeline_stream_handler_logger.info("Processing chunk for partitionId: " + std::to_string(partitionId));
        lock.unlock();
        dataBufferCV.notify_all();

        if (chunk == END_OF_STREAM_MARKER) {
            kg_pipeline_stream_handler_logger.info("Received END_OF_STREAM_MARKER for partitionId: " +
                                                   std::to_string(partitionId));
            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                           JasmineGraphInstanceProtocol::CHUNK_STREAM_END,
                                           JasmineGraphInstanceProtocol::OK)) {
                kg_pipeline_stream_handler_logger.error("Failed to send END_OF_STREAM");
            }
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            kg_pipeline_stream_handler_logger.info("Closed connection for partitionId: " + std::to_string(partitionId));
            sharedBuffer.add(END_OF_STREAM_MARKER);
            close(sockfd);
            break;  // Exit loop if end of stream marker is received
        }
        // Send chunk
        kg_pipeline_stream_handler_logger.info("Sending QUERY_DATA_START for chunk of size: " +
                                               std::to_string(chunk.length()));
        if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                       JasmineGraphInstanceProtocol::QUERY_DATA_START,
                                       JasmineGraphInstanceProtocol::OK)) {
            kg_pipeline_stream_handler_logger.error("Failed to send QUERY_DATA_START");
            break;
        }

        char ack3[ACK_MESSAGE_SIZE] = {0};
        int converted_number = htonl(chunk.length());
        kg_pipeline_stream_handler_logger.info("Sending chunk length: " + std::to_string(chunk.length()));
        if (!Utils::sendIntExpectResponse(sockfd, ack3,
                                          JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(),
                                          converted_number, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
            kg_pipeline_stream_handler_logger.error("Failed to send chunk length");
            break;
        }

        kg_pipeline_stream_handler_logger.info("Sending chunk data");
        if (!Utils::send_str_wrapper(sockfd, chunk)) {
            kg_pipeline_stream_handler_logger.error("Failed to send chunk data");
            break;
        }
        Utils::expect_str_wrapper(sockfd, JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS);

        // Receive tuple from server
        kg_pipeline_stream_handler_logger.info("Waiting for QUERY_DATA_START from server");
        if (!Utils::expect_str_wrapper(sockfd, JasmineGraphInstanceProtocol::QUERY_DATA_START)) {
            kg_pipeline_stream_handler_logger.error("Did not receive QUERY_DATA_START from server");
            break;
        }
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK);
        int tuple_count = 0;  // to provide realtime updates
        while (true) {
            int tuple_length_net;
            ssize_t ret = recv(sockfd, &tuple_length_net, sizeof(int), 0);
            if (ret <= 0) {
                kg_pipeline_stream_handler_logger.error("Failed to receive tuple length, closing stream");
                break;
            }

            int tuple_length = ntohl(tuple_length_net);

            // ✅ Sanity checks
            if (tuple_length <= 0 || tuple_length > 10 * 1024 * 1024) {
                // limit to 10MB
                kg_pipeline_stream_handler_logger.error("Invalid tuple length: " + std::to_string(tuple_length));
                break;
            }

            kg_pipeline_stream_handler_logger.info("Received tuple length: " + std::to_string(tuple_length) +
                                                   " from: " + std::to_string(partitionId));
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK);

            std::string tuple(tuple_length, 0);
            size_t received = 0;
            while (received < static_cast<size_t>(tuple_length)) {
                ret = recv(sockfd, &tuple[received], tuple_length - received, 0);
                if (ret <= 0) {
                    kg_pipeline_stream_handler_logger.error("Error receiving tuple data");
                    break;
                }
                received += ret;
            }

            if (received != static_cast<size_t>(tuple_length)) {
                kg_pipeline_stream_handler_logger.error("Incomplete tuple received, expected " +
                                                        std::to_string(tuple_length) + " but got " +
                                                        std::to_string(received));
                break;
            }
            if (stopFlag) {
                Utils::send_str_wrapper(sockfd, "stop");
            } else {
                Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS);
            }

            if (tuple == END_OF_STREAM_MARKER) {
                realTimeBytesMutex.lock();
                bytes_read_so_far += chunkData.chunk_size;
                realtime_bytes_read_so_far = bytes_read_so_far;
                realTimeBytesMutex.unlock();


                break;
            }

            tuple_count++;
            if (tuple_count % 5 == 0) {
                realTimeBytesMutex.lock();
                realtime_bytes_read_so_far += (static_cast<long>(std::stod(chunkSize) * 0.2));
                realTimeBytesMutex.unlock();
            }
            kg_pipeline_stream_handler_logger.info("Received end of tuple stream marker");
            // Check for non-UTF8 safely
            if (!std::regex_search(tuple, std::regex(R"([^\x20-\x7E])"))) {
                // Log only if it looks printable ASCII
                kg_pipeline_stream_handler_logger.info("Tuple: " + tuple);
            } else {
                // Hex dump instead of logging raw binary
                std::ostringstream hexStream;
                for (unsigned char c : tuple) {
                    hexStream << std::hex << std::setw(2) << std::setfill('0') << (int)c;
                }
                kg_pipeline_stream_handler_logger.error("Tuple contains non-printable data (hex): " + hexStream.str());
            }

            sharedBuffer.add(tuple);
        }
    }

    kg_pipeline_stream_handler_logger.info("Closing connection for partitionId: " + std::to_string(partitionId));
}

bool Pipeline::streamGraphToDesignatedWorker(std::string host, int port, std::string masterIP, std::string graphId,
                                             int numberOfPartitions, std::string hdfsServerIp, std::string hdfsPort,
                                             std::string hostnamePort, std::string llmInferenceEngine, std::string llm,
                                             std::string chunkSize, std::string hdfsFilePath,
                                             bool continueKGConstruction, SQLiteDBInterface* sqlite,
                                             shared_ptr<atomic<bool>>& stopFlag,
                                             shared_ptr<KGConstructionRate>& kgConstructionRates) {
    kg_pipeline_stream_handler_logger.info("Connecting to worker Host:" + host + " Port:" + std::to_string(port));

    std::mutex dbLock;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    static const int ACK_MESSAGE_SIZE = 1024;
    struct sockaddr_in serv_addr;
    struct hostent* server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        kg_pipeline_stream_handler_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == nullptr) {
        kg_pipeline_stream_handler_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    if (Utils::connect_wrapper(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    // Perform handshake
    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    auto sendAndExpect = [&](const std::string& msg, const std::string& expected) -> bool {
        if (!Utils::send_str_wrapper(sockfd, msg)) {
            kg_pipeline_stream_handler_logger.error("Failed to send message: " + msg);
            return false;
        }
        char buffer[ACK_MESSAGE_SIZE] = {0};
        int bytes = recv(sockfd, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            kg_pipeline_stream_handler_logger.error("No response from worker for message: " + msg);
            return false;
        }
        std::string resp(buffer, bytes);
        resp = Utils::trim_copy(resp);
        if (resp != expected) {
            kg_pipeline_stream_handler_logger.error("Unexpected worker response. Sent: " + msg +
                                                    " Expected: " + expected + " Got: " + resp);
            return false;
        }
        kg_pipeline_stream_handler_logger.info("Worker responded '" + resp + "' for '" + msg + "'");
        return true;
    };
    std::vector<string> llmRunnerSockets;
    stringstream llm_(hostnamePort);
    string intermediate_llm;
    while (getline(llm_, intermediate_llm, ',')) {
        llmRunnerSockets.push_back(intermediate_llm);
    }
    const std::string chunksPerBatch = std::to_string(llmRunnerSockets.size());
    string workers = "";
    int counter = 0;
    int workerCount;

    if (llmRunnerSockets.size() < numberOfPartitions) {
        workerCount = numberOfPartitions;
    } else {
        workerCount = llmRunnerSockets.size();
    }
    for (JasmineGraphServer::worker worker : JasmineGraphServer::getWorkers(workerCount)) {
        counter++;
        kg_pipeline_stream_handler_logger.info("count " + std::to_string(counter));
        workers += worker.hostname + ":" + std::to_string(worker.port) + ":" + std::to_string(worker.dataPort);
        // append , only if not last
        if (counter < workerCount) {
            workers += ",";
        }
    }

    if (!sendAndExpect("initiate-streaming-kg-construction", "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(graphId, "ok")) {
        close(sockfd);
        return false;
    }
    if (continueKGConstruction) {
        kg_pipeline_stream_handler_logger.info("KG construction is continued");
        long upload_bytes;
        std::string query = "SELECT uploaded_bytes FROM graph WHERE idgraph = " + graphId + ";";
        auto result = sqlite->runSelect(query);

        if (result.empty() || result[0].empty()) {
            // No record found
            upload_bytes = 0;
        }

        try {
            kg_pipeline_stream_handler_logger.info(result[0][0].second);
            upload_bytes = std::stoi(result[0][0].second);
            kg_pipeline_stream_handler_logger.info("uploaded bytes: " + std::to_string(upload_bytes));
        } catch (std::exception& e) {
            kg_pipeline_stream_handler_logger.debug(e.what());
        }

        if (!sendAndExpect("y", "ok")) {
            close(sockfd);
            return false;
        }
        if (!sendAndExpect(std::to_string(upload_bytes), "ok")) {
            close(sockfd);
            return false;
        }
    } else {
        if (!sendAndExpect("n", "ok")) {
            close(sockfd);
            return false;
        }
    }

    if (!sendAndExpect(hostnamePort, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(llmInferenceEngine, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(llm, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(chunkSize, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(chunksPerBatch, "ok")) {
        close(sockfd);
        return false;
    }

    if (!sendAndExpect(std::to_string(numberOfPartitions), "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(hdfsServerIp, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(hdfsPort, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(masterIP, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(workers, "ok")) {
        close(sockfd);
        return false;
    }
    if (!sendAndExpect(hdfsFilePath, "ok")) {
        close(sockfd);
        return false;
    }
    char buffer[1024];
    long previousEdgeCount = 0;
    double previousBytesCount = 0;
    time_point previousTimeStampBytesPerSecond = std::chrono::system_clock::now();
    time_point previousTimeStampTriplesPerSecond = std::chrono::system_clock::now();

    int totalReceivedBytes = 0;
    while (true) {
        kg_pipeline_stream_handler_logger.info("Waiting for data...");
        bzero(buffer, sizeof(buffer));
        int bytes = recv(sockfd, buffer, sizeof(buffer), 0);
        if (bytes <= 0) break;

        std::string msg(buffer, bytes);
        msg = Utils::trim_copy(msg);

        if (msg == "meta") {
            Utils::send_str_wrapper(sockfd, "ok");
            int content_length;
            recv(sockfd, &content_length, sizeof(int), 0);
            content_length = ntohl(content_length);
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK);
            kg_pipeline_stream_handler_logger.info("Received content length: " + std::to_string(content_length));

            std::string meta(content_length, 0);
            recv(sockfd, &meta[0], content_length, 0);
            kg_pipeline_stream_handler_logger.info("Received meta: " + meta);

            json metaJSON = json::parse(meta);
            json graph = metaJSON["graph"];

            std::string sqlStatement =
                "UPDATE graph SET vertexcount = '" + graph["vertexcount"].dump() + "', centralpartitioncount = '" +
                graph["centralpartitioncount"].dump() + "', edgecount = '" + graph["edgecount"].dump() +
                "', graph_status_idgraph_status = '" + graph["graph_status_idgraph_status"].dump() +
                "' WHERE idgraph = '" + graphId + "'";

            dbLock.lock();
            sqlite->runUpdate(sqlStatement);
            auto elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                                                                   previousTimeStampTriplesPerSecond);

            double elapsed = static_cast<double>(elapsedSeconds.count());

            kgConstructionRates->triplesPerSecond =
                (std::stod(graph["edgecount"].dump()) - previousEdgeCount) / elapsed;
            previousEdgeCount = std::stod(graph["edgecount"].dump());
            previousTimeStampTriplesPerSecond = std::chrono::system_clock::now();
            json partitions = metaJSON["partitions"];

            for (auto partition : partitions) {
                kg_pipeline_stream_handler_logger.info(partition.dump());

                std::string sqlStatement =
                    "UPDATE partition SET "
                    "vertexcount = " +
                    partition["vertexcount"].dump() +
                    ", "
                    "central_vertexcount = " +
                    partition["central_vertexcount"].dump() +
                    ", "
                    "edgecount = " +
                    partition["edgecount"].dump() +
                    ", "
                    "central_edgecount_with_dups = " +
                    partition["central_edgecount_with_dups"].dump() +
                    ", "
                    "central_edgecount = " +
                    partition["central_edgecount"].dump() + " WHERE idpartition = " + partition["idpartition"].dump() +
                    " AND graph_idgraph = " + partition["graph_idgraph"].dump() +
                    "; "
                    "INSERT INTO partition (idpartition, graph_idgraph, vertexcount, "
                    "central_vertexcount, "
                    "edgecount, central_edgecount_with_dups, central_edgecount) "
                    "SELECT " +
                    partition["idpartition"].dump() + ", " + partition["graph_idgraph"].dump() + ", " +
                    partition["vertexcount"].dump() + ", " + partition["central_vertexcount"].dump() + ", " +
                    partition["edgecount"].dump() + ", " + partition["central_edgecount_with_dups"].dump() + ", " +
                    partition["central_edgecount"].dump() + " WHERE (SELECT changes() = 0)";
                int result = sqlite->runInsert(sqlStatement);
            }

            dbLock.unlock();
        } else if (msg == "done") {
            Utils::send_str_wrapper(sockfd, "ok");

            kg_pipeline_stream_handler_logger.info("Worker completed streaming for graph " + graphId);
            break;
        } else {
            try {
                double completedBytes = std::stod(msg);
                kg_pipeline_stream_handler_logger.info("Worker uploaded bytes: " + std::to_string(completedBytes));

                std::string updateQuery = "UPDATE graph SET uploaded_bytes = " + std::to_string(completedBytes) +
                                          " WHERE idgraph = " + graphId + ";";
                auto elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now() - previousTimeStampBytesPerSecond);

                double elapsed = static_cast<double>(elapsedSeconds.count());
                kgConstructionRates->bytesPerSecond = (completedBytes - previousBytesCount) / elapsed;
                previousBytesCount = completedBytes;
                previousTimeStampBytesPerSecond = std::chrono::system_clock::now();
                sqlite->runUpdate(updateQuery);

                if (*stopFlag) {
                    Utils::send_str_wrapper(sockfd, "stop");
                    *stopFlag = false;
                    kg_pipeline_stream_handler_logger.info("Stopping stream early as requested");
                } else {
                    Utils::send_str_wrapper(sockfd, "ok");
                }
            } catch (std::exception& e) {
                kg_pipeline_stream_handler_logger.error("Invalid progress message from worker: " + msg);
            }
        }
    }
    close(sockfd);
    kg_pipeline_stream_handler_logger.info("Worker completed streaming upload for graph " + (graphId));
    return true;
}
