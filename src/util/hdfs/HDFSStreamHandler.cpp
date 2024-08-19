/*
 * Copyright 2024 JasminGraph Team
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
#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <mutex>
#include <queue>
#include <regex>

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

Logger hdfs_stream_handler_logger;

const size_t MESSAGE_SIZE = 1024;
const size_t MAX_BUFFER_SIZE = MESSAGE_SIZE * 1024;
const std::string END_OF_STREAM_MARKER = "-1";
const size_t FILE_SIZE_THRESHOLD = 10485760; // Example: 10 MB threshold

HDFSStreamHandler::HDFSStreamHandler(hdfsFS fileSystem, const std::string &filePath, int numberOfPartitions,
                                     int graphId, SQLiteDBInterface *sqlite,
                                     std::string masterIP)
        : fileSystem(fileSystem),
          filePath(filePath),
          numberOfPartitions(numberOfPartitions),
          isReading(true),
          isProcessing(true),
          graphId(graphId),
          currentFileSize(0),
          sqlite(sqlite),
          masterIP(masterIP),
          partitioner(sqlite),
          fileIndex(0) {}

void HDFSStreamHandler::stream_from_hdfs_into_buffer() {
    auto start_time = high_resolution_clock::now();

    hdfsFile file = hdfsOpenFile(fileSystem, filePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        hdfs_stream_handler_logger.error("Failed to open HDFS file.");
        isReading = false; // Signal completion
        dataBufferCV.notify_all(); // Notify any waiting consumers
        return;
    }

    char buffer[MESSAGE_SIZE];
    tSize num_read_bytes = 0;

    std::string leftover; // Store any leftover data between reads

    while ((num_read_bytes = hdfsRead(fileSystem, file, buffer, MESSAGE_SIZE)) > 0) {
        std::string data(buffer, num_read_bytes);

        // Prepend leftover data from the previous read
        data = leftover + data;
        leftover.clear();

        // Process data into complete lines
        std::istringstream dataStream(data);
        std::string line;
        while (std::getline(dataStream, line)) {
            if (dataStream.eof() && data.back() != '\n') {
                // If this is the last line but doesn't end with a newline, save it as leftover
                leftover = line;
            } else {
                std::unique_lock<std::mutex> lock(dataBufferMutex);
                dataBufferCV.wait(lock, [this] { return dataBuffer.size() < MAX_BUFFER_SIZE || !isReading; });
                dataBuffer.push(line);  // Push the line into the buffer
                lock.unlock();
                dataBufferCV.notify_one();  // Notify the processor thread
            }
        }
    }

    hdfsCloseFile(fileSystem, file);
    isReading = false;

    // Push any remaining leftover data if it's non-empty
    if (!leftover.empty()) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(leftover);  // Critical section
        lock.unlock();
        dataBufferCV.notify_all();  // Notify the processor thread that reading is done
    }

    // Push end-of-stream marker into the buffer
    {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(END_OF_STREAM_MARKER);  // Critical section
        lock.unlock(); // Ensure the mutex is unlocked
    }

    dataBufferCV.notify_all();  // Notify the processor thread that reading is done

    auto end_time = high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;
    hdfs_stream_handler_logger.info("Time taken to read from HDFS: " + to_string(duration.count()) + " seconds");
}

void HDFSStreamHandler::stream_from_buffer_to_processing_queue() {
    auto start_time = high_resolution_clock::now();

    while (true) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return !dataBuffer.empty() || !isReading; });

        if (!dataBuffer.empty()) {
            std::string line = dataBuffer.front();
            dataBuffer.pop();  // Critical section
            lock.unlock();  // Unlock the mutex

            // Check if the data is the end-of-stream marker
            if (line == END_OF_STREAM_MARKER) {
                hdfs_stream_handler_logger.info("Received end-of-stream marker");
                lineBuffer.push(END_OF_STREAM_MARKER);  // Push end-of-stream marker to the line buffer
                break; // Exit the loop after handling the end-of-stream marker
            }

            // Push complete line to line buffer
            std::unique_lock<std::mutex> lineLock(lineBufferMutex);
            lineBuffer.push(line);
            lineLock.unlock();
            lineBufferCV.notify_one();  // Notify the line processor thread
        } else if (!isReading) {
            break;
        }
    }

    isProcessing = false;
    lineBufferCV.notify_all();  // Notify the line processor thread that processing is done

    auto end_time = high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;
    hdfs_stream_handler_logger.info(
            "Time taken to read from buffer to processing queue: " + to_string(duration.count()) + " seconds");
}

void HDFSStreamHandler::process_lines() {
    auto start_time = high_resolution_clock::now();
    int edgeCount = 0;

    partitioner.setPathAndGraphID(graphId);

    while (true) {
        std::unique_lock<std::mutex> lineLock(lineBufferMutex);
        lineBufferCV.wait(lineLock, [this] { return !lineBuffer.empty() || !isProcessing; });

        if (!lineBuffer.empty()) {
            std::string line = lineBuffer.front();
            lineBuffer.pop();  // Critical section
            lineLock.unlock();  // Unlock the mutex

            // Check if the line is the end-of-stream marker
            if (line == END_OF_STREAM_MARKER) {
                load_data_and_close_file_chunk();
                break;
            }

            // Define regex pattern to match any of the delimiters (space, tab, comma)
            std::regex delimiter_regex("\\s+|,");
            std::sregex_token_iterator iter(line.begin(), line.end(), delimiter_regex, -1);
            std::sregex_token_iterator end;

            std::vector<std::string> tokens(iter, end);

            if (tokens.size() == 2) {
                std::string sourceId = tokens[0];
                std::string destId = tokens[1];

                if (!sourceId.empty() && !destId.empty()) {
                    edgeCount++;

                    // Check if file needs to be opened or if it exceeds size threshold
                    if (!currentFile.is_open() || currentFileSize >= FILE_SIZE_THRESHOLD) {
                        load_data_and_close_file_chunk();
                        open_new_file_chunk();
                    }

                    // Write the edge data to the file chunk
                    line += "\n";
                    currentFile.write(line.c_str(), line.size());
                    currentFileSize += line.size();
                } else {
                    hdfs_stream_handler_logger.error("Malformed line: " + line);
                }
            } else {
                hdfs_stream_handler_logger.error("Malformed line (unexpected token count): " + line);
            }
        } else if (!isProcessing) {
            break;
        }
    }
    vector<std::map<int, string>> fullFileList;
    fullFileList = partitioner.partitioneWithGPMetis(to_string(numberOfPartitions));
    hdfs_stream_handler_logger.info("Upload done");
    JasmineGraphServer *server = JasmineGraphServer::getInstance();
    server->uploadGraphLocally(graphId, Conts::GRAPH_TYPE_NORMAL, fullFileList, masterIP);
    Utils::deleteDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/" + to_string(graphId));
    JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(graphId), sqlite);

    auto end_time = high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;
    hdfs_stream_handler_logger.info("Time taken to process lines: " + to_string(duration.count()) + " seconds");
}

void HDFSStreamHandler::open_new_file_chunk() {
    currentFilePath = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.tempdatafolder") + "/aaa_" +
                      std::to_string(graphId) + std::to_string(fileIndex);
    hdfs_stream_handler_logger.info("Opening new file: " + currentFilePath);
    currentFile.open(currentFilePath, std::ios::out | std::ios::binary);
    if (!currentFile.is_open()) {
        hdfs_stream_handler_logger.error("Failed to open file: " + currentFilePath);
        return;
    }
    currentFileSize = 0;  // Reset the file size after opening a new file
}

void HDFSStreamHandler::load_data_and_close_file_chunk() {
    if (currentFile.is_open()) {
        currentFile.close();
        partitioner.loadFileChunks(currentFilePath);
        int result = partitioner.constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
        if (result == 0) {
            string reformattedFilePath = partitioner.reformatDataSet(currentFilePath, graphId);
            partitioner.loadDataSet(reformattedFilePath, graphId);
        }
    }
    fileIndex++;  // Increment the file index
}

void HDFSStreamHandler::start_streaming_data_from_hdfs_into_partitions() {
    auto start_time = high_resolution_clock::now();
    Utils::createDirectory(Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.tempdatafolder"));

    currentFileSize = 0;
    open_new_file_chunk();

    // Start threads for streaming and processing
    std::thread readerThread(&HDFSStreamHandler::stream_from_hdfs_into_buffer, this);
    std::thread bufferProcessorThread(&HDFSStreamHandler::stream_from_buffer_to_processing_queue, this);
    std::thread lineProcessorThread(&HDFSStreamHandler::process_lines, this);

    // Wait for the threads to finish
    readerThread.join();
    bufferProcessorThread.join();
    lineProcessorThread.join();

    // Close and publish the final file if needed
    if (currentFile.is_open()) {
        load_data_and_close_file_chunk();
    }

    auto end_time = high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;
    hdfs_stream_handler_logger.info(
            "Total time taken for streaming from HDFS into partitions: " + to_string(duration.count()) + " seconds");
    Utils::deleteDirectory(Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.tempdatafolder"));
}