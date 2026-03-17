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

#include "HDFSConnector.h"

HDFSConnector::HDFSConnector(const std::string &hdfsServerIP, const std::string &hdfsServerPort) {
    fileSystem = hdfsConnect(hdfsServerIP.c_str(), std::stoi(hdfsServerPort));
    if (!fileSystem) {
        frontend_logger.error("Failed to connect to HDFS server at " + hdfsServerIP);
    } else {
        frontend_logger.info("Connected to HDFS server at " + hdfsServerIP+":"+hdfsServerPort);
    }
}

bool HDFSConnector::isPathValid(const std::string &hdfsPath) {
    if (!fileSystem) {
        frontend_logger.error("HDFS connection is not established");
        return false;
    }

    int exists = hdfsExists(fileSystem, hdfsPath.c_str());
    if (exists == 0) {
        frontend_logger.info("Path exists: " + hdfsPath);
        return true;
    } else {
        frontend_logger.error("Invalid path: " + hdfsPath);
        return false;
    }
}

bool HDFSConnector::createDirectory(const std::string &hdfsPath) {
    if (!fileSystem) {
        frontend_logger.error("HDFS connection is not established");
        return false;
    }

    if (hdfsExists(fileSystem, hdfsPath.c_str()) == 0) {
        return true;
    }

    if (hdfsCreateDirectory(fileSystem, hdfsPath.c_str()) != 0) {
        frontend_logger.error("Failed to create HDFS directory: " + hdfsPath);
        return false;
    }

    frontend_logger.info("Created HDFS directory: " + hdfsPath);
    return true;
}

bool HDFSConnector::writeGraphToHDFS(const std::string &hdfsPath, const std::string &graphData) {
    if (!fileSystem) {
        frontend_logger.error("HDFS connection is not established");
        return false;
    }

    hdfsFile writeFile = hdfsOpenFile(fileSystem, hdfsPath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!writeFile) {
        frontend_logger.error("Failed to open HDFS file for writing: " + hdfsPath);
        return false;
    }

    tSize numWrittenBytes = hdfsWrite(fileSystem, writeFile, graphData.c_str(), graphData.length());
    if (numWrittenBytes == -1) {
        frontend_logger.error("Failed to write data to HDFS file: " + hdfsPath);
        hdfsCloseFile(fileSystem, writeFile);
        return false;
    }

    if (hdfsFlush(fileSystem, writeFile) != 0) {
        frontend_logger.error("Failed to flush HDFS file: " + hdfsPath);
        hdfsCloseFile(fileSystem, writeFile);
        return false;
    }

    if (hdfsCloseFile(fileSystem, writeFile) != 0) {
        frontend_logger.error("Failed to close HDFS file: " + hdfsPath);
        return false;
    }

    frontend_logger.info("Successfully wrote graph data to HDFS: " + hdfsPath);
    return true;
}

bool HDFSConnector::deletePath(const std::string &hdfsPath, bool recursive) {
    if (!fileSystem) {
        frontend_logger.error("HDFS connection is not established");
        return false;
    }

    if (hdfsDelete(fileSystem, hdfsPath.c_str(), recursive ? 1 : 0) != 0) {
        frontend_logger.error("Failed to delete HDFS path: " + hdfsPath);
        return false;
    }

    frontend_logger.info("Deleted HDFS path: " + hdfsPath);
    return true;
}

bool HDFSConnector::concatenateFiles(const std::vector<std::string> &sourcePaths, const std::string &destinationPath) {
    if (!fileSystem) {
        frontend_logger.error("HDFS connection is not established");
        return false;
    }

    if (sourcePaths.empty()) {
        frontend_logger.error("No HDFS source files provided for concatenation");
        return false;
    }

    if (!openFileForWrite(destinationPath)) {
        return false;
    }

    bool success = true;
    std::vector<char> buffer(64 * 1024);

    for (const auto &sourcePath : sourcePaths) {
        hdfsFile sourceFile = hdfsOpenFile(fileSystem, sourcePath.c_str(), O_RDONLY, 0, 0, 0);
        if (!sourceFile) {
            frontend_logger.error("Failed to open HDFS source file for reading: " + sourcePath);
            success = false;
            break;
        }

        while (true) {
            tSize bytesRead = hdfsRead(fileSystem, sourceFile, buffer.data(), static_cast<tSize>(buffer.size()));
            if (bytesRead < 0) {
                frontend_logger.error("Failed to read HDFS source file: " + sourcePath);
                success = false;
                break;
            }
            if (bytesRead == 0) {
                break;
            }

            if (!appendData(buffer.data(), static_cast<size_t>(bytesRead))) {
                frontend_logger.error("Failed to append merged data from HDFS source file: " + sourcePath);
                success = false;
                break;
            }
        }

        if (hdfsCloseFile(fileSystem, sourceFile) != 0) {
            frontend_logger.error("Failed to close HDFS source file: " + sourcePath);
            success = false;
        }

        if (!success) {
            break;
        }
    }

    bool closeSuccess = closeWriteFile();
    if (!closeSuccess) {
        success = false;
    }

    if (success) {
        frontend_logger.info("Successfully concatenated " + std::to_string(sourcePaths.size()) +
                             " HDFS files into " + destinationPath);
    }

    return success;
}

bool HDFSConnector::openFileForWrite(const std::string &hdfsPath) {
    if (!fileSystem) {
        frontend_logger.error("HDFS connection is not established");
        return false;
    }
    if (currentWriteFile) {
        frontend_logger.error("A file is already open for writing");
        return false;
    }
    currentWriteFile = hdfsOpenFile(fileSystem, hdfsPath.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
    if (!currentWriteFile) {
        frontend_logger.error("Failed to open HDFS file for writing: " + hdfsPath);
        return false;
    }
    return true;
}

bool HDFSConnector::appendData(const char *data, size_t length) {
    if (!currentWriteFile) {
        frontend_logger.error("No HDFS file open for writing");
        return false;
    }
    size_t totalWritten = 0;
    while (totalWritten < length) {
        tSize written = hdfsWrite(fileSystem, currentWriteFile,
                                  data + totalWritten, length - totalWritten);
        if (written == -1) {
            frontend_logger.error("Failed to write data to HDFS file");
            return false;
        }
        totalWritten += static_cast<size_t>(written);
    }
    return true;
}

bool HDFSConnector::closeWriteFile() {
    if (!currentWriteFile) {
        return true;
    }
    bool success = true;
    if (hdfsFlush(fileSystem, currentWriteFile) != 0) {
        frontend_logger.error("Failed to flush HDFS file");
        success = false;
    }
    if (hdfsCloseFile(fileSystem, currentWriteFile) != 0) {
        frontend_logger.error("Failed to close HDFS file");
        success = false;
    }
    currentWriteFile = nullptr;
    return success;
}

HDFSConnector::~HDFSConnector() {
    if (currentWriteFile) {
        hdfsCloseFile(fileSystem, currentWriteFile);
        currentWriteFile = nullptr;
    }
    if (fileSystem) {
        hdfsDisconnect(fileSystem);
        frontend_logger.info("Disconnected from HDFS server");
    }
}

hdfsFS HDFSConnector::getFileSystem() {
    return fileSystem;
}
