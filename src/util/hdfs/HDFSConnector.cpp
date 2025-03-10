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

HDFSConnector::~HDFSConnector() {
    if (fileSystem) {
        hdfsDisconnect(fileSystem);
        frontend_logger.info("Disconnected from HDFS server");
    }
}

hdfsFS HDFSConnector::getFileSystem() {
    return fileSystem;
}
