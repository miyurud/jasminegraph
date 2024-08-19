#include "HDFSConnector.h"

HDFSConnector::HDFSConnector(const std::string &hdfsServerIP, const std::string &hdfsServerPort) {
    fileSystem = hdfsConnect( hdfsServerIP.c_str(), std::stoi(hdfsServerPort));
    if (!fileSystem) {
        frontend_logger.error("Failed to connect to HDFS server at " + hdfsServerIP);
    } else {
        frontend_logger.info("Connected to HDFS server at " + hdfsServerIP+":"+hdfsServerPort);
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
