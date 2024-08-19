#ifndef HDFSCONNECTOR_H
#define HDFSCONNECTOR_H

#include <string>
#include "../logger/Logger.h"
#include "hdfs.h"

class HDFSConnector {
public:
    HDFSConnector(const std::string &hdfsServerIP, const std::string &hdfsServerPort);
    ~HDFSConnector();
    hdfsFS getFileSystem();

private:
    hdfsFS fileSystem;
    Logger frontend_logger;
};

#endif // HDFSCONNECTOR_H
