#include "HDFSStreamHandler.h"
#include "../../server/JasmineGraphServer.h"
#include "../../partitioner/stream/HashPartitioner.h"
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
                                     int graphId, SQLiteDBInterface *sqlite, std::string masterIP)
        : fileSystem(fileSystem),
          filePath(filePath),
          numberOfPartitions(numberOfPartitions),
          isReading(true),
          isProcessing(true),
          graphId(graphId),
          sqlite(sqlite),
          masterIP(masterIP) {}

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

void HDFSStreamHandler::streamFromBufferToProcessingQueue(HashPartitioner &partitioner) {
    auto startTime = high_resolution_clock::now();
    while (isProcessing) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return !dataBuffer.empty() || !isReading; });

        if (!dataBuffer.empty()) {
            std::string line = dataBuffer.front();
            dataBuffer.pop();
            lock.unlock();

            // Check for the end-of-stream marker
            if (line == END_OF_STREAM_MARKER) {
                hdfs_stream_handler_logger.debug("Received end-of-stream marker in one of the threads.");

                // Set the flag to stop processing in all threads
                isProcessing = false;

                // Notify all waiting threads
                dataBufferCV.notify_all();
                break;
            }

            std::regex delimiterRegex("\\s+|,");
            std::sregex_token_iterator iter(line.begin(), line.end(), delimiterRegex, -1);
            std::sregex_token_iterator end;

            std::vector<std::string> tokens(iter, end);
            if (tokens.size() == 2) {
                std::string sourceId = tokens[0];
                std::string destId = tokens[1];
                if (!sourceId.empty() && !destId.empty()) {
                    int sourceIndex = std::hash<std::string>()(sourceId) % this->numberOfPartitions;
                    int destIndex = std::hash<std::string>()(destId) % this->numberOfPartitions;
                    if (sourceIndex == destIndex) {
                        partitioner.addLocalEdge({sourceId, destId}, sourceIndex);
                    } else {
                        partitioner.addEdgeCut({sourceId, destId}, sourceIndex);
                        partitioner.addEdgeCut({destId, sourceId}, destIndex);
                    }
                } else {
                    hdfs_stream_handler_logger.error("Malformed line: " + line);
                }
            } else {
                hdfs_stream_handler_logger.error("Malformed line (unexpected token count): " + line);
            }

        } else if (!isReading) {
            break;
        }
    }
}

void HDFSStreamHandler::startStreamingFromBufferToPartitions() {
    auto startTime = high_resolution_clock::now();
    HashPartitioner partitioner(numberOfPartitions, graphId, masterIP);

    std::thread readerThread(&HDFSStreamHandler::streamFromHDFSIntoBuffer, this);
    std::vector<std::thread> bufferProcessorThreads;
    for (int i = 0; i < 20; ++i) {
        bufferProcessorThreads.emplace_back(&HDFSStreamHandler::streamFromBufferToProcessingQueue, this,
                                            std::ref(partitioner));
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

    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    hdfs_stream_handler_logger.info(
            "Total time taken for streaming from HDFS into partitions: " + to_string(duration.count()) + " seconds");
}
