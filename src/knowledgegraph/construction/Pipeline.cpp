//
// Created by sajeenthiran on 2025-08-11.
//

#include "Pipeline.h"
#include <hdfs.h>

#include <iostream>
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
#include <functional> // for std::hash
class SQLiteDBInterface;

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

Logger kg_pipeline_stream_handler_logger;

const size_t MESSAGE_SIZE = 5 * 1024 * 1024;
const size_t MAX_BUFFER_SIZE = MESSAGE_SIZE * 512;
const size_t CHUNCK_BYTE_SIZE = 1024; // 1 MB chunks
const std::string END_OF_STREAM_MARKER = "-1";

Pipeline::Pipeline(hdfsFS fileSystem, const std::string &filePath, int numberOfPartitions, int graphId,
                                     std::string masterIP , vector<JasmineGraphServer::worker> &workerList)
        : fileSystem(fileSystem),
          filePath(filePath),
          numberOfPartitions(numberOfPartitions),
          isReading(true),
          isProcessing(true),
          graphId(graphId),
          masterIP(masterIP),
workerList(workerList) {}


void Pipeline:: init()
{
    startStreamingFromBufferToWorkers();
}
// ---------- document_reader ----------
// Reads chunks of fixed byte size from an AHDFS file and pushes them as chunks with generated doc_id
void Pipeline::streamFromHDFSIntoBuffer() {
    auto startTime = chrono::high_resolution_clock::now();
    kg_pipeline_stream_handler_logger.info("Started streaming data from HDFS into data buffer...");

    hdfsFile file = hdfsOpenFile(fileSystem, filePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        kg_pipeline_stream_handler_logger.error("Failed to open HDFS file.");
        isReading = false;
        dataBufferCV.notify_all();
        return;
    }

    kg_pipeline_stream_handler_logger.info("Successfully opened HDFS file: " + filePath);

    std::vector<char> buffer(CHUNCK_BYTE_SIZE);
    int64_t read_bytes = 0;
    int chunk_idx = 0;
    std::string leftover;

  while ((read_bytes = hdfsRead(fileSystem, file, buffer.data(), CHUNCK_BYTE_SIZE)) > 0) {
      kg_pipeline_stream_handler_logger.info("Starting to process chunk " + std::to_string(chunk_idx));
      std::string chunk_text = leftover + std::string(buffer.data(), read_bytes);

      kg_pipeline_stream_handler_logger.info("Read chunk " + std::to_string(chunk_idx) + " with " + std::to_string(read_bytes) + " bytes");
      kg_pipeline_stream_handler_logger.info("Current leftover size: " + std::to_string(leftover.size()));

      // Find last newline to keep only complete lines in chunk pushed to dataBuffer
      size_t last_newline = chunk_text.find_last_of('\n');
      if (last_newline == std::string::npos) {
          kg_pipeline_stream_handler_logger.info("No newline found in chunk " + std::to_string(chunk_idx) + ", storing as leftover");
          leftover = chunk_text;
          kg_pipeline_stream_handler_logger.info("Updated leftover size: " + std::to_string(leftover.size()));
          continue;
      }

      // Split into complete lines and leftover partial line
      std::string full_lines_chunk = chunk_text.substr(0, last_newline + 1);
      leftover = chunk_text.substr(last_newline + 1);

      kg_pipeline_stream_handler_logger.info("Full lines chunk size: " + std::to_string(full_lines_chunk.size()));
      kg_pipeline_stream_handler_logger.info("Leftover after split size: " + std::to_string(leftover.size()));
      kg_pipeline_stream_handler_logger.info("Pushing chunk " + std::to_string(chunk_idx) + " to dataBuffer");

      // Wait and push to dataBuffer safely
      std::unique_lock<std::mutex> lock(dataBufferMutex);
      kg_pipeline_stream_handler_logger.info("Waiting to acquire lock for dataBuffer push");
      dataBufferCV.wait(lock, [this] { return dataBuffer.size() < workerList.size() || !isReading; });

      dataBuffer.push(std::move(full_lines_chunk));
      kg_pipeline_stream_handler_logger.info("Chunk " + std::to_string(chunk_idx) + " pushed to dataBuffer. Current buffer size: " + std::to_string(dataBuffer.size()));
      lock.unlock();
      dataBufferCV.notify_all();

      chunk_idx++;
  }

    // Push leftover partial line if any (last chunk)
    if (!leftover.empty()) {
        kg_pipeline_stream_handler_logger.info("Pushing leftover data to dataBuffer");
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return dataBuffer.size() <  workerList.size()|| !isReading; });

        dataBuffer.push(std::move(leftover));
        lock.unlock();
        dataBufferCV.notify_one();
    }

    if (read_bytes < 0) {
        kg_pipeline_stream_handler_logger.error("Error reading from AHDFS file");
        std::cerr << "Error reading from AHDFS file\n";
    }

    hdfsCloseFile(fileSystem, file);
    kg_pipeline_stream_handler_logger.info("Closed HDFS file: " + filePath);
    isReading = false;

    if (!leftover.empty()) {
        kg_pipeline_stream_handler_logger.info("Pushing leftover data again to dataBuffer after closing file");
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(leftover);
        lock.unlock();
        dataBufferCV.notify_all();
    }

    {
        kg_pipeline_stream_handler_logger.info("Pushing END_OF_STREAM_MARKER to dataBuffer");
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(END_OF_STREAM_MARKER);
        lock.unlock();
    }

    dataBufferCV.notify_all();
    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    kg_pipeline_stream_handler_logger.debug("Successfully streamed data from HDFS into data buffer.");
    kg_pipeline_stream_handler_logger.info("Time taken to read from HDFS: " + to_string(duration.count()) + " seconds");
}


void Pipeline::startStreamingFromBufferToWorkers()
{
    auto startTime = high_resolution_clock::now();
    // HDFSMultiThreadedHashPartitioner partitioner(numberOfPartitions, graphId, masterIP, isDirected);

    std::thread readerThread(&Pipeline::streamFromHDFSIntoBuffer, this);
    std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    bufferPool.reserve(numberOfPartitions);  // Pre-allocate space for pointers
    for (size_t i = 0; i < numberOfPartitions; ++i) {
        bufferPool.emplace_back(std::make_unique<SharedBuffer>(MASTER_BUFFER_SIZE));
    }

    std::vector<std::thread> workerThreads;
    int count = 0;
    for (auto &worker : workerList) {
        workerThreads.emplace_back(
            &Pipeline::extractTuples,
            this,
            worker.hostname, worker.port,
            masterIP, graphId, count,
            std::ref(dataBuffer), std::ref(*bufferPool[count]));
        count++;
    }
    processTupleAndSaveInPartition(bufferPool);
    readerThread.join();


    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    kg_pipeline_stream_handler_logger.info(
            "Total time taken for streaming from HDFS into partitions: " + to_string(duration.count()) + " seconds");
}

void Pipeline::processTupleAndSaveInPartition(const std::vector<std::unique_ptr<SharedBuffer>>& tupleBuffer) {
    auto startTime = high_resolution_clock::now();
    kg_pipeline_stream_handler_logger.info("Starting processTupleAndSaveInPartition");
    HDFSMultiThreadedHashPartitioner partitioner(numberOfPartitions, graphId, masterIP, true , workerList);
    std::hash<std::string> hasher;
    std::vector<std::thread> tupleThreads;
    for (size_t i = 0; i < tupleBuffer.size(); ++i) {
        SharedBuffer* tupleBufferRef = tupleBuffer[i].get();
        kg_pipeline_stream_handler_logger.info("Launching tuple thread for partition " + std::to_string(i));
        tupleThreads.emplace_back([&, tupleBufferRef, i]() {
            kg_pipeline_stream_handler_logger.info("Tuple thread started for partition " + std::to_string(i));
            while (isProcessing) {
                if (!tupleBufferRef->empty()) {
                    std::string line = tupleBufferRef->get();
                    kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) + " processing line: " + line);
                    // Check for end-of-stream marker
                    if (line == END_OF_STREAM_MARKER) {
                        kg_pipeline_stream_handler_logger.debug("Received end-of-stream marker in thread " + std::to_string(i));
                        isProcessing = false;
                        break;
                    }
                    try {
                        auto jsonEdge = json::parse(line);
                        auto source = jsonEdge["source"];
                        auto destination = jsonEdge["destination"];
                        std::string sourceId = std::to_string(hasher(source["id"])% 10000);
                        source["id"] = sourceId;
                        std::string destinationId = std::to_string(hasher(destination["id"])% 10000);
                        destination["id"] = destinationId;
                        kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) + " sourceId: " + sourceId + ", destinationId: " + destinationId);
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
                                kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) + " adding local edge to partition " + std::to_string(sourceIndex));
                                partitioner.addLocalEdge(obj.dump(), sourceIndex);
                            } else {
                                kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) + " adding edge cut to partition " + std::to_string(sourceIndex));
                                partitioner.addEdgeCut(obj.dump(), sourceIndex);
                                json reversedObj = {
                                    {"source", destination},
                                    {"destination", source},
                                    {"properties", jsonEdge["properties"]}
                                };
                                kg_pipeline_stream_handler_logger.debug("Thread " + std::to_string(i) + " adding reversed edge cut to partition " + std::to_string(destIndex));
                                partitioner.addEdgeCut(reversedObj.dump(), destIndex);
                            }
                        } else {
                            kg_pipeline_stream_handler_logger.error("Malformed line: missing source/destination ID: " + line);
                        }
                    } catch (const json::parse_error &e) {
                        kg_pipeline_stream_handler_logger.error("JSON parse error: " + std::string(e.what()) + " | Line: " + line);
                    } catch (const std::invalid_argument &e) {
                        kg_pipeline_stream_handler_logger.error("Invalid node ID (not an integer) in line: " + line);
                    } catch (const std::out_of_range &e) {
                        kg_pipeline_stream_handler_logger.error("Node ID out of range in line: " + line);
                    } catch (const std::exception &e) {
                        kg_pipeline_stream_handler_logger.error("Unexpected exception in line: " + std::string(e.what()));
                    }
                } else if (!isReading) {
                    kg_pipeline_stream_handler_logger.info("Thread " + std::to_string(i) + " exiting due to isReading=false");
                    break;
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
    kg_pipeline_stream_handler_logger.info("processTupleAndSaveInPartition completed in " + std::to_string(duration.count()) + " seconds");
}



void Pipeline::extractTuples(std::string host, int port, std::string masterIP,
                              int graphID, int partitionId,
                              std::queue<std::string> &dataBuffer,
                              SharedBuffer &sharedBuffer) {
    kg_pipeline_stream_handler_logger.info("Starting extractTuples for host: " + host + ", port: " + std::to_string(port) + ", partitionId: " + std::to_string(partitionId));
    char data[FED_DATA_LENGTH + 1];
    static const int ACK_MESSAGE_SIZE = 1024;
    struct sockaddr_in serv_addr;
    struct hostent *server;

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

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        kg_pipeline_stream_handler_logger.error("Failed to connect to host: " + host + " on port: " + std::to_string(port));
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
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                   std::to_string(graphID),
                                   JasmineGraphInstanceProtocol::OK)) {
        kg_pipeline_stream_handler_logger.error("Failed to send graphID");
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return;
    }
    kg_pipeline_stream_handler_logger.info("GraphID sent successfully");

    // Streaming loop
    while (true) {
        std::string chunk;

        std::unique_lock<std::mutex> lock(this->dataBufferMutexForWorker);
        this->dataBufferCV.wait(lock, [this, &dataBuffer] { return !dataBuffer.empty() || !this->isReading; });

        chunk = std::move(dataBuffer.front());
        dataBuffer.pop();

        kg_pipeline_stream_handler_logger.info("Processing chunk for partitionId: " + std::to_string(partitionId));
        lock.unlock();
        dataBufferCV.notify_all();

        if (chunk == END_OF_STREAM_MARKER) {
            kg_pipeline_stream_handler_logger.info("Received END_OF_STREAM_MARKER for partitionId: " + std::to_string(partitionId));
            if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                           JasmineGraphInstanceProtocol::CHUNK_STREAM_END,
                                           JasmineGraphInstanceProtocol::OK)) {
                kg_pipeline_stream_handler_logger.error("Failed to send END_OF_STREAM");
            }
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            kg_pipeline_stream_handler_logger.info("Closed connection for partitionId: " + std::to_string(partitionId));
            close(sockfd);
            break; // Exit loop if end of stream marker is received
        }
        // Send chunk
        kg_pipeline_stream_handler_logger.info("Sending QUERY_DATA_START for chunk of size: " + std::to_string(chunk.length()));
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
                                          converted_number,
                                          JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
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

        while (true)
        {

            // if ( Utils::expect_str_wrapper(sockfd, JasmineGraphInstanceProtocol::GRAPH_STREAM_END_OF_EDGE)) {
            //     kg_pipeline_stream_handler_logger.error("End of tuple stream");
            //     break;
            // }
            int tuple_length;
            recv(sockfd, &tuple_length, sizeof(int), 0);
            tuple_length = ntohl(tuple_length);
            kg_pipeline_stream_handler_logger.info("Received tuple length: " + std::to_string(tuple_length));
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK);

            std::string tuple(tuple_length, 0);
            recv(sockfd, &tuple[0], tuple_length, 0);
            kg_pipeline_stream_handler_logger.info("Received tuple data");
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS);
            kg_pipeline_stream_handler_logger.info(tuple);
            sharedBuffer.add(tuple);
        }
    }

    kg_pipeline_stream_handler_logger.info("Closing connection for partitionId: " + std::to_string(partitionId));
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
}


