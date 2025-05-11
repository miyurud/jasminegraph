/**
Copyright 2019 JasmineGraph Team
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
#ifndef JASMINEGRAPH_UTILS_H
#define JASMINEGRAPH_UTILS_H

#include <arpa/inet.h>
#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "../metadb/SQLiteDBInterface.h"
#include "../performancedb/PerformanceSQLiteDBInterface.h"
#include "../frontend/JasmineGraphFrontEndProtocol.h"
#include "Conts.h"
#include "../query/processor/cypher/util/SharedBuffer.h"

using std::map;
using std::unordered_map;
using json = nlohmann::json;

class Utils {
 private:
    static unordered_map<std::string, std::string> propertiesMap;
    static std::mutex sqliteMutex;

 public:
    struct worker {
        std::string workerID;
        std::string hostname;
        std::string username;
        std::string port;
        std::string dataPort;
    };

    static std::string getJasmineGraphProperty(std::string key);

    static std::vector<worker> getWorkerList(SQLiteDBInterface *sqlite);

    static std::vector<std::string> getHostListFromProperties();

    static std::vector<std::string> getFileContent(std::string);

    static std::string getFileContentAsString(std::string);

    static std::string replaceAll(std::string content, const std::string &oldValue, const std::string &newValue);

    static void writeFileContent(const std::string &filePath, const std::string &content);

    static std::vector<std::string> split(const std::string &, char delimiter);

    static std::string trim_copy(const std::string &, const std::string &delimiters = " \f\n\r\t\v");

    static bool parseBoolean(const std::string str);

    static bool fileExists(std::string fileName);

    static bool fileExistsWithReadPermission(const std::string &path);

    static std::fstream *openFile(const std::string &path, std::ios_base::openmode mode);

    static int compressFile(const std::string filePath, std::string mode = "pigz");

    static bool is_number(const std::string &compareString);

    static int createDirectory(const std::string dirName);

    static std::vector<std::string> getListOfFilesInDirectory(std::string dirName);

    static int deleteDirectory(const std::string dirName);

    static int deleteAllMatchingFiles(const std::string fileNamePattern);

    static int deleteFile(const std::string fileName);

    static std::string getFileName(std::string filePath);

    static int getFileSize(std::string filePath);

    static std::string getJasmineGraphHome();

    static std::string getHomeDir();

    static int copyFile(const std::string sourceFilePath, const std::string destinationFilePath);

    static int unzipFile(std::string filePath, std::string mode = "pigz");

    static bool hostExists(std::string name, std::string ip, std::string workerPort, SQLiteDBInterface *sqlite);

    static int compressDirectory(const std::string filePath);

    static int unzipDirectory(std::string filePath);

    static int copyToDirectory(std::string currentPath, std::string copyPath);

    static std::string getHostID(std::string hostName, SQLiteDBInterface *sqlite);

    static void assignPartitionsToWorkers(int numberOfWorkers, SQLiteDBInterface *sqlite);

    static void updateSLAInformation(PerformanceSQLiteDBInterface *perfSqlite, std::string graphId, int partitionCount,
                                     long newSlaValue, std::string command, std::string category);

    static void editFlagZero(std::string flagPath);

    static void editFlagOne(std::string flagPath);

    static std::string checkFlag(std::string flagPath);

    static int connect_wrapper(int sock, const sockaddr *addr, socklen_t slen);

    /**
     * Wrapper to recv(2) to read a string.
     *
     * @param connFd connection file descriptor
     * @param buf writable buffer of size at least len+1
     * @param len maximum length of the string to read (excluding null terminator)
     * @param allowEmpty whether to allow reading empty string or not
     * @return The string read or "" on error. Logs error if recv failed. Also logs error if allowEmpty is false and
     * read length 0 string.
     */
    static std::string read_str_wrapper(int connFd, char *buf, size_t len, bool allowEmpty = false);

    /**
     * Wrapper to recv(2) to read a string and trim it.
     *
     * @param connFd connection file descriptor
     * @param buf writable buffer of size at least len+1
     * @param len maximum length of the string to read (excluding null terminator)
     * @return The trimmed string read or "" on error. Also rerurns "" if read length 0 string. This may return an
     * empty string if the string read is non-empty but contained only white-space characters.
     */
    static std::string read_str_trim_wrapper(int connFd, char *buf, size_t len);

    /**
     * Wrapper to send(2) to send data to socket.
     *
     * @param connFd connection file descriptor
     * @param buf readable buffer of size at least `size`
     * @param size size of data to send
     * @return true on success or false otherwise. Logs error if send() failed or sent less than the requested size.
     */
    static bool send_wrapper(int connFd, const char *buf, size_t size);

    /**
     * Wrapper to send(2) to send a std::string to socket.
     *
     * @param connFd connection file descriptor
     * @param str the string to send without any null terminator
     * @return true on success or false otherwise. Uses Utils::send_wrapper(int, const char *, size_t) internally.
     */
    static bool send_str_wrapper(int connFd, std::string str);
    static bool send_int_wrapper(int connFd, int* value, size_t datalength);

    static bool sendExpectResponse(int sockfd, char *data, size_t data_length, std::string sendMsg,
                                   std::string expectMsg);

    static bool performHandshake(int sockfd, char *data, size_t data_length, std::string masterIP);

    static std::string getCurrentTimestamp();

    static std::string getJsonStringFromYamlFile(const std::string &yamlFile);

    static int createDatabaseFromDDL(const char *dbLocation, const char *ddlFileLocation);

    static std::string downloadFile(const std::string& fileURL, const std::string& localFilePath);

    static std::string send_job(std::string job_group_name, std::string metric_name, std::string metric_value);

    static map<string, string> getMetricMap(string metricName);

    static bool uploadFileToWorker(std::string host, int port, int dataPort, int graphID, std::string filePath,
                                   std::string masterIP, std::string uploadType);

    static bool sendFileThroughService(std::string host, int dataPort, std::string fileName, std::string filePath);

    static bool transferPartition(std::string sourceWorker, int sourceWorkerPort, std::string destinationWorker,
                                  int destinationWorkerDataPort, std::string graphID, std::string partitionID,
                                  std::string workerID, SQLiteDBInterface *sqlite);
    static bool sendQueryPlanToWorker(std::string host, int port, std::string masterIP,
                                      int graphID, int PartitionId, std::string message, SharedBuffer &sharedBuffer);
    static bool sendIntExpectResponse(int sockfd, char *data, size_t data_length,
                                      int value, std::string expectMsg);

    static bool sendFileChunkToWorker(std::string host, int port, int dataPort, std::string filePath,
                                      std::string masterIP, std::string uploadType);

    static void assignPartitionToWorker(int graphId, int partitionIndex, string  hostname, int port);

    static string getFrontendInput(int connFd);
};

#endif  // JASMINEGRAPH_UTILS_H
