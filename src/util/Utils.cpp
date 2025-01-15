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

#include "Utils.h"

#include <curl/curl.h>
#include <dirent.h>
#include <jsoncpp/json/json.h>
#include <netdb.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <vector>

#include "../../globals.h"
#include "../k8s/K8sInterface.h"
#include "../server/JasmineGraphInstanceProtocol.h"
#include "../server/JasmineGraphServer.h"
#include "Conts.h"
#include "logger/Logger.h"

using namespace std;
Logger util_logger;

#ifdef UNIT_TEST
int jasminegraph_profile = PROFILE_K8S;
#endif

unordered_map<std::string, std::string> Utils::propertiesMap;

std::vector<std::string> Utils::split(const std::string &s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

std::vector<std::string> Utils::getFileContent(std::string file) {
    ifstream in(file);

    std::string str;
    vector<std::string> vec;
    if (!in.is_open()) return vec;
    while (std::getline(in, str)) {
        // now we loop back and get the next line in 'str'
        if (str.length() > 0) {
            vec.push_back(str);
        }
    }
    return vec;
};

std::string Utils::getFileContentAsString(std::string file) {
    if (!fileExists(file)) {
        util_logger.error("File not found : " + file);
        return "";
    }
    std::ifstream in(file);
    std::string fileContent((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();
    return fileContent;
}

std::string Utils::replaceAll(std::string content, const std::string &oldValue, const std::string &newValue) {
    size_t pos = 0;
    while ((pos = content.find(oldValue, pos)) != std::string::npos) {
        content.replace(pos, oldValue.length(), newValue);
        pos += newValue.length();
    }
    return content;
}

void Utils::writeFileContent(const std::string &filePath, const std::string &content) {
    std::ofstream out(filePath);
    if (!out.is_open()) {
        util_logger.error("Cannot write to file path: " + filePath);
        return;
    }
    out << content;
    out.close();
}

static std::mutex propertiesMapMutex;
static bool propertiesMapInitialized = false;
std::string Utils::getJasmineGraphProperty(std::string key) {
    if (!propertiesMapInitialized) {
        propertiesMapMutex.lock();
        if (!propertiesMapInitialized) {  // double-checking lock
            const vector<std::string> &vec = Utils::getFileContent(ROOT_DIR "conf/jasminegraph-server.properties");
            for (auto it = vec.begin(); it < vec.end(); it++) {
                std::string item = *it;
                if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
                    const std::vector<std::string> &vec2 = split(item, '=');
                    if (vec2.size() == 2) {
                        Utils::propertiesMap[vec2.at(0)] = vec2.at(1);
                    } else {
                        Utils::propertiesMap[vec2.at(0)] = string(" ");
                    }
                }
            }
        }
        propertiesMapInitialized = true;
        propertiesMapMutex.unlock();
    }
    auto it = Utils::propertiesMap.find(key);
    if (it != Utils::propertiesMap.end()) {
        return it->second;
    }
    return "";
}

std::vector<Utils::worker> Utils::getWorkerList(SQLiteDBInterface *sqlite) {
    vector<Utils::worker> workerVector;
    std::vector<vector<pair<string, string>>> v =
        sqlite->runSelect("SELECT idworker,user,ip,server_port,server_data_port FROM worker;");
    for (int i = 0; i < v.size(); i++) {
        string workerID = v[i][0].second;
        string user = v[i][1].second;
        string ip = v[i][2].second;
        string serverPort = v[i][3].second;
        string serverDataPort = v[i][4].second;

        worker workerInstance;
        workerInstance.workerID = workerID;
        workerInstance.username = user;
        workerInstance.hostname = ip;
        workerInstance.port = serverPort;
        workerInstance.dataPort = serverDataPort;

        workerVector.push_back(workerInstance);
    }

    return workerVector;
}

std::vector<std::string> Utils::getHostListFromProperties() {
    std::vector<std::string> result;
    std::vector<std::string>::iterator it;
    vector<std::string> vec = Utils::getFileContent("conf/hosts.txt");
    it = vec.begin();

    for (it = vec.begin(); it < vec.end(); it++) {
        std::string item = *it;
        if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
            result.insert(result.begin(), item);
        }
    }

    return result;
}

static inline std::string trim_right_copy(const std::string &s, const std::string &delimiters) {
    size_t end = s.find_last_not_of(delimiters);
    if (end == std::string::npos) {
        return "";  // Return empty string if all characters are delimiters
    }
    return s.substr(0, end + 1);
}

static inline std::string trim_left_copy(const std::string &s, const std::string &delimiters) {
    size_t start = s.find_first_not_of(delimiters);
    if (start == std::string::npos) {
        return "";  // Return empty string if all characters are delimiters
    }
    return s.substr(start);
}

std::string Utils::trim_copy(const std::string &s, const std::string &delimiters) {
    return trim_left_copy(trim_right_copy(s, delimiters), delimiters);
}

/**
 * This fuction is to convert string to boolean
 * @param str
 * @return
 */
bool Utils::parseBoolean(const std::string str) {
    if (str == "true" || str == "TRUE" || str == "True") {
        return true;
    }
    return false;
}

/**
 * This method checks if a file with the given path exists.
 * @param fileName
 * @return
 */
bool Utils::fileExists(std::string fileName) { return access(fileName.c_str(), F_OK) == 0; }

/**
 * This method creates a new directory if it does not exist
 * @param dirName
 */
int Utils::createDirectory(const std::string dirName) {
    if (dirName.empty()) {
        util_logger.error("Cannot mkdir empty dirName");
    }
    string command = "mkdir -p " + dirName;
    int status = system(command.c_str());
    if (status != 0) {
        util_logger.warn("Command failed: " + command + "      trying again");
        sleep(1);
        status = system(command.c_str());
    }
    return status;
}

std::vector<std::string> Utils::getListOfFilesInDirectory(std::string dirName) {
    std::vector<string> results;
    DIR *d = opendir(dirName.c_str());
    if (!d) {
        util_logger.error("Error opening directory " + dirName);
        return results;
    }
    if (dirName.back() != '/') {
        dirName.append("/");
    }
    const struct dirent *dir;
    while ((dir = readdir(d)) != nullptr) {
        const char *filename = dir->d_name;
        string fnamestr = filename;
        struct stat sb;
        string path = dirName + fnamestr;
        if (stat(path.c_str(), &sb) != 0) {
            util_logger.warn("stat() failed for " + path + " => skipping");
            continue;
        }
        if (S_ISREG(sb.st_mode)) {
            results.push_back(fnamestr);
        }
    }
    (void)closedir(d);
    return results;
}

/**
 * This method deletes a directory with all its content if the user has permission
 * @param dirName
 */
int Utils::deleteDirectory(const std::string dirName) {
    string command = "rm -rf " + dirName;
    int status = system(command.c_str());
    if (status == 0)
        util_logger.info(dirName + " deleted successfully");
    else
        util_logger.warn("Deleting " + dirName + " failed with exit code " + std::to_string(status));
    return status;
}

bool Utils::is_number(const std::string &compareString) {
    return !compareString.empty() && std::find_if(compareString.begin(), compareString.end(),
                                                  [](char c) { return !std::isdigit(c); }) == compareString.end();
}

/**
 * This method extracts the file name from file path
 * @param filePath
 * @return
 */
std::string Utils::getFileName(std::string filePath) {
    std::string filename = filePath.substr(filePath.find_last_of("/") + 1);
    return filename;
}

std::string Utils::getJasmineGraphHome() {
    std::string test = Conts::JASMINEGRAPH_HOME;
    std::string jasminegraph_home;

    char const *temp = getenv(test.c_str());
    if (temp != nullptr) {
        jasminegraph_home = std::string(temp);
    }
    if (jasminegraph_home.empty()) {
        util_logger.warn("Returning empty value for " + Conts::JASMINEGRAPH_HOME);
    }
    return jasminegraph_home;
}

/*
 * Get the current user's (caller of the program) home directory from the $HOME environment variable,
 * If it's not available get the home directory from /etc/passwd records.
 */
std::string Utils::getHomeDir() {
    const char *homedir;
    if ((homedir = getenv("HOME")) == NULL) {
        homedir = getpwuid(getuid())->pw_dir;
    }
    return string(homedir);
}

/**
 * This method copies the file
 * @param filePath
 */
int Utils::copyFile(const std::string sourceFilePath, const std::string destinationFilePath) {
    util_logger.info("Starting file copy source: " + sourceFilePath + " destination: " + destinationFilePath);
    std::string command = "cp " + sourceFilePath + " " + destinationFilePath;
    int status = system(command.c_str());
    if (status != 0) util_logger.error("Copy failed with exit code " + std::to_string(status));
    return status;
}

/**
 * This method returns the size of the file in bytes
 * @param filePath
 * @return
 */
int Utils::getFileSize(std::string filePath) {
    ifstream file(filePath.c_str(), ifstream::in | ifstream::binary);
    if (!file.is_open()) {
        return -1;
    }
    file.seekg(0, ios::end);
    int fileSize = file.tellg();
    file.close();
    return fileSize;
}

/**
 * This method compresses files using gzip
 * @param filePath
 */
int Utils::compressFile(const std::string filePath, std::string mode) {
    if (mode == "pigz") {
        if (access("/usr/bin/pigz", X_OK) != 0) {
            util_logger.info("pigz not found. Compressing using gzip");
            mode = "gzip";
        }
    }
    std::string command = mode + " -f " + filePath + " 2>&1";
    int status = system(command.c_str());
    if (status != 0) {
        util_logger.error("File compression failed with code " + std::to_string(status));
    }
    return status;
}

/**
 * this method extracts a gzip file
 * @param filePath
 */
int Utils::unzipFile(std::string filePath, std::string mode) {
    if (mode == "pigz") {
        if (access("/usr/bin/pigz", X_OK) != 0) {
            util_logger.info("pigz not found. Decompressing using gzip");
            mode = "gzip";
        }
    }
    std::string command = mode + " -f -d " + filePath + " 2>&1";
    int status = system(command.c_str());
    if (status != 0) {
        util_logger.error("File decompression failed with code " + std::to_string(status));
    }
    return status;
}

/**
 * This method checks if a host exists in JasmineGraph MetaBD.
 * This method uses the name and ip of the host.
 */
bool Utils::hostExists(string name, string ip, std::string workerPort, SQLiteDBInterface *sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM worker WHERE name LIKE '" + name + "' AND ip LIKE '" + ip +
                  "' AND server_port LIKE '" + workerPort + "';";
    if (ip == "") {
        stmt = "SELECT COUNT( * ) FROM worker WHERE name LIKE '" + name + "';";
    }
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

string Utils::getHostID(string hostName, SQLiteDBInterface *sqlite) {
    map<string, string> hostIDMap;
    std::vector<vector<pair<string, string>>> v =
        sqlite->runSelect("SELECT idhost FROM host where name LIKE '" + hostName + "';");
    string id = v[0][0].second;

    return id;
}

/**
 * This method compresses directories using tar
 * @param filePath
 */
int Utils::compressDirectory(const std::string filePath) {
    std::string command = "tar -czf " + filePath + ".tar.gz " + filePath;
    int status = system(command.c_str());
    if (status != 0) {
        util_logger.error("Directory compression failed with code " + std::to_string(status));
    }
    return status;
}

/**
 * this method extracts a tar.gz directory
 * @param filePath
 */
int Utils::unzipDirectory(std::string filePath) {
    std::string command = "tar -xzf " + filePath;
    int status = system(command.c_str());
    if (status != 0) {
        util_logger.error("Directory compression failed with code " + std::to_string(status));
    }
    return status;
}

void Utils::assignPartitionsToWorkers(int numberOfWorkers, SQLiteDBInterface *sqlite) {
    // Not used in K8s mode
    sqlite->runUpdate("DELETE FROM worker_has_partition");

    std::vector<vector<pair<string, string>>> v =
        sqlite->runSelect("SELECT idpartition, graph_idgraph FROM partition;");
    int workerCounter = 0;
    string valueString;
    string sqlStatement =
        "INSERT INTO worker_has_partition (partition_idpartition, partition_graph_idgraph, worker_idworker) VALUES ";
    std::stringstream ss;
    if (v.size() > 0) {
        for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
            int counter = 0;
            ss << "(";
            for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
                ss << j->second << ",";
            }

            ss << workerCounter << "),";
            valueString = valueString + ss.str();
            ss.str(std::string());
            workerCounter++;
            if (workerCounter >= numberOfWorkers) {
                workerCounter = 0;
            }
        }
        valueString = valueString.substr(0, valueString.length() - 1);
        sqlStatement = sqlStatement + valueString;
        sqlite->runInsert(sqlStatement);
    }
}

void Utils::updateSLAInformation(PerformanceSQLiteDBInterface *perfSqlite, std::string graphId, int partitionCount,
                                 long newSlaValue, std::string command, std::string category) {
    std::string categoryQuery =
        "SELECT id from sla_category where command='" + command + "' and category='" + category + "'";

    std::vector<vector<pair<string, string>>> categoryResults = perfSqlite->runSelect(categoryQuery);

    if (categoryResults.size() == 1) {
        string slaCategoryId = categoryResults[0][0].second;

        std::string query = "SELECT id, sla_value, attempt from graph_sla where graph_id='" + graphId +
                            "' and partition_count='" + std::to_string(partitionCount) + "' and id_sla_category='" +
                            slaCategoryId + "';";

        std::vector<vector<pair<string, string>>> results = perfSqlite->runSelect(query);

        if (results.size() == 1) {
            std::string slaId = results[0][0].second;
            std::string slaValueString = results[0][1].second;
            std::string attemptString = results[0][2].second;

            long slaValue = atol(slaValueString.c_str());
            int attempts = atoi(attemptString.c_str());

            if (attempts < Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
                long newSla = ((slaValue * attempts) + newSlaValue) / (attempts + 1);

                attempts++;

                std::string updateQuery = "UPDATE graph_sla set sla_value='" + std::to_string(newSla) + "', attempt='" +
                                          std::to_string(attempts) + "' where id = '" + slaId + "'";

                perfSqlite->runUpdate(updateQuery);
            }
        } else {
            std::string insertQuery =
                "insert into graph_sla (id_sla_category, graph_id, partition_count, sla_value, attempt) VALUES ('" +
                slaCategoryId + "','" + graphId + "'," + std::to_string(partitionCount) + "," +
                std::to_string(newSlaValue) + ",0);";

            perfSqlite->runInsert(insertQuery);
        }
    } else {
        util_logger.error("Invalid SLA " + category + " for " + command + " command");
    }
}

int Utils::copyToDirectory(std::string currentPath, std::string destinationDir) {
    if (access(destinationDir.c_str(), F_OK)) {
        std::string createDirCommand = "mkdir -p " + destinationDir;
        if (system(createDirCommand.c_str())) {
            util_logger.error("Creating directory " + destinationDir + " failed");
            return -1;
        }
    }
    std::string copyCommand = "cp " + currentPath + " " + destinationDir;
    int status = system(copyCommand.c_str());
    if (status != 0) {
        util_logger.error("Copying " + currentPath + " to directory " + destinationDir + " failed with code " +
                          std::to_string(status));
    }
    return status;
}

void Utils::editFlagOne(std::string flagPath) {
    std::string filePath = flagPath;
    ofstream stream;
    char flag[] = "1";

    stream.open(filePath);
    stream << flag << endl;
    stream.close();
}

void Utils::editFlagZero(std::string flagPath) {
    std::string filePath = flagPath;
    ofstream stream;
    char flag[] = "0";

    stream.open(filePath);
    stream << flag << endl;
    stream.close();
}

std::string Utils::checkFlag(std::string flagPath) {
    std::string filePath = flagPath;
    std::string bitVal;
    ifstream infile(filePath);

    if (infile.good()) {
        getline(infile, bitVal);
    }

    infile.close();
    return bitVal;
}

int Utils::connect_wrapper(int sock, const sockaddr *addr, socklen_t slen) {
    int retry = 0;

    struct timeval tv = {1, 0};

    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv, sizeof(tv)) == -1) {
        util_logger.error("Failed to set send timeout option for socket");
    }

    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv)) == -1) {
        util_logger.error("Failed to set receive timeout option for socket");
    }

    do {
        if (retry) sleep(retry * 2);
        util_logger.info("Trying to connect to [" + to_string(retry) +
                         "]: " + string(inet_ntoa(((const struct sockaddr_in *)addr)->sin_addr)) + ":" +
                         to_string(ntohs(((const struct sockaddr_in *)addr)->sin_port)));
        if (connect(sock, addr, slen) == 0) {
            tv = {0, 0};
            if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv)) == -1) {
                util_logger.error("Failed to set receive timeout option for socket after successful connection");
            }
            return 0;
        }
    } while (retry++ < 4);
    util_logger.error("Error connecting to " + string(inet_ntoa(((const struct sockaddr_in *)addr)->sin_addr)) + ":" +
                      to_string(ntohs(((const struct sockaddr_in *)addr)->sin_port)));
    return -1;
}

std::string Utils::read_str_wrapper(int connFd, char *buf, size_t len, bool allowEmpty) {
    ssize_t result = recv(connFd, buf, len, 0);
    if (result < 0) {
        util_logger.error("Read failed: recv returned " + std::to_string((int)result));
        return "";
    } else if (!allowEmpty && result == 0) {
        util_logger.error("Read failed: recv empty string");
        return "";
    }
    buf[result] = 0;  // null terminator for string
    string str = buf;
    return str;
}

std::string Utils::read_str_trim_wrapper(int connFd, char *buf, size_t len) {
    string str = read_str_wrapper(connFd, buf, len);
    if (!str.empty()) str = trim_copy(str);
    return str;
}

bool Utils::send_wrapper(int connFd, const char *buf, size_t size) {
    ssize_t sz = send(connFd, buf, size, 0);
    if (sz < size) {
        util_logger.error("Send failed");
        return false;
    }
    return true;
}

bool Utils::send_str_wrapper(int connFd, std::string str) {
    return send_wrapper(connFd, str.c_str(), str.length());
}

bool Utils::send_int_wrapper(int connFd, int *value, size_t datalength) {
    ssize_t sz = send(connFd, value, datalength, 0);
    if (sz < datalength) {
        util_logger.error("Send failed");
        return false;
    }
    return true;
}

bool Utils::sendIntExpectResponse(int sockfd, char *data, size_t data_length,
                                  int value, std::string expectMsg) {
    if (!Utils::send_int_wrapper(sockfd, &value, sizeof(value))) {
        return false;
    }
    util_logger.info("Sent: " + to_string(value));
    std::string response = Utils::read_str_trim_wrapper(sockfd, data, data_length);
    if (response.compare(expectMsg) != 0) {
        util_logger.error("Incorrect response. Expected: " + expectMsg + " ; Received: " + response);
        return false;
    }
    util_logger.info("Received: " + response);
    return true;
}

bool Utils::sendExpectResponse(int sockfd, char *data, size_t data_length, std::string sendMsg, std::string expectMsg) {
    if (!Utils::send_str_wrapper(sockfd, sendMsg)) {
        return false;
    }
    util_logger.info("Sent: " + sendMsg);

    std::string response = Utils::read_str_trim_wrapper(sockfd, data, data_length);
    if (response.compare(expectMsg) != 0) {
        util_logger.error("Incorrect response. Expected: " + expectMsg + " ; Received: " + response);
        return false;
    }
    util_logger.info("Received: " + response);
    return true;
}

bool Utils::performHandshake(int sockfd, char *data, size_t data_length, std::string masterIP) {
    if (!Utils::sendExpectResponse(sockfd, data, data_length, JasmineGraphInstanceProtocol::HANDSHAKE,
                                   JasmineGraphInstanceProtocol::HANDSHAKE_OK)) {
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, data_length, masterIP, JasmineGraphInstanceProtocol::HOST_OK)) {
        return false;
    }
    return true;
}

std::string Utils::getCurrentTimestamp() {
    auto now = chrono::system_clock::now();
    time_t time = chrono::system_clock::to_time_t(now);
    tm tm_time;
    localtime_r(&time, &tm_time);
    stringstream timestamp;
    timestamp << put_time(&tm_time, "%y%m%d_%H%M%S");  // Format can be customized

    return timestamp.str();
}

static inline json parse_scalar(const YAML::Node &node) {
    int i;
    double d;
    bool b;
    std::string s;

    // Check whether the node is quoted. parse it as a string
    if (node.Tag() == "!") {
        s = node.as<std::string>();
        return s;
    }

    if (YAML::convert<int>::decode(node, i)) return i;
    if (YAML::convert<double>::decode(node, d)) return d;
    if (YAML::convert<bool>::decode(node, b)) return b;
    if (YAML::convert<std::string>::decode(node, s)) return s;

    return nullptr;
}

static inline json yaml2json(const YAML::Node &root) {
    json j{};

    switch (root.Type()) {
        case YAML::NodeType::Null:
            break;
        case YAML::NodeType::Scalar:
            return parse_scalar(root);
        case YAML::NodeType::Sequence:
            for (auto &&node : root) j.emplace_back(yaml2json(node));
            break;
        case YAML::NodeType::Map:
            for (auto &&it : root) j[it.first.as<std::string>()] = yaml2json(it.second);
            break;
        default:
            break;
    }
    return j;
}

std::string Utils::getJsonStringFromYamlFile(const std::string &yamlFile) {
    YAML::Node yamlNode = YAML::LoadFile(yamlFile);
    return to_string(yaml2json(yamlNode));
}

int Utils::createDatabaseFromDDL(const char *dbLocation, const char *ddlFileLocation) {
    if (!Utils::fileExists(ddlFileLocation)) {
        util_logger.error("DDL file not found: " + string(ddlFileLocation));
        return -1;
    }
    ifstream ddlFile(ddlFileLocation);

    stringstream buffer;
    buffer << ddlFile.rdbuf();
    ddlFile.close();

    sqlite3 *tempDatabase;
    int rc = sqlite3_open(dbLocation, &tempDatabase);
    if (rc) {
        util_logger.error("Cannot create database: " + string(sqlite3_errmsg(tempDatabase)));
        return -1;
    }

    rc = sqlite3_exec(tempDatabase, buffer.str().c_str(), 0, 0, 0);
    if (rc) {
        util_logger.error("DDL execution failed: " + string(sqlite3_errmsg(tempDatabase)));
        sqlite3_close(tempDatabase);
        return -1;
    }

    sqlite3_close(tempDatabase);
    util_logger.info("Database created successfully");
    return 0;
}

static size_t write_callback(void *contents, size_t size, size_t nmemb, std::string *output) {
    size_t totalSize = size * nmemb;
    output->append(static_cast<char *>(contents), totalSize);
    return totalSize;
}
std::string Utils::send_job(std::string job_group_name, std::string metric_name, std::string metric_value) {
    CURL *curl;
    CURLcode res;
    std::string pushGatewayJobAddr;
    if (jasminegraph_profile == PROFILE_K8S) {
        std::unique_ptr<K8sInterface> interface(new K8sInterface());
        pushGatewayJobAddr = interface->getJasmineGraphConfig("pushgateway_address");
    } else {
        pushGatewayJobAddr = getJasmineGraphProperty("org.jasminegraph.collector.pushgateway");
    }

    pushGatewayJobAddr += "metrics/job/";

    std::string response_string;
    curl = curl_easy_init();
    if (curl) {
        std::string hostPGAddr;
        const char *hostAddress = getenv("HOST_NAME");
        const char *port = getenv("PORT");

        std::string uniqueWorkerID;
        if (hostAddress) {
            uniqueWorkerID = std::string(hostAddress) + ":" + std::string(port);
        } else {
            uniqueWorkerID = "Master";
        }
        hostPGAddr = pushGatewayJobAddr + uniqueWorkerID;
        curl_easy_setopt(curl, CURLOPT_URL, hostPGAddr.c_str());

        // Set the callback function to handle the response data
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
        std::string job_data = metric_name + " " + metric_value + "\n";
        const char *data = job_data.c_str();

        curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/x-prometheus-remote-write-v1");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
        curl_easy_setopt(curl, CURLOPT_POST, 1);

        res = curl_easy_perform(curl);
        long code = -1;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        if (res != CURLE_OK || code != 200) {
            util_logger.error("curl failed: " + std::string(curl_easy_strerror(res)) + "| url: " + hostPGAddr +
                              "| data: " + job_data);
        }

        curl_easy_cleanup(curl);
    }
    return response_string;
}

std::map<std::string, std::string> Utils::getMetricMap(std::string metricName) {
    std::map<std::string, std::string> map;
    CURL *curl;
    CURLcode res;
    std::string response_cpu_usages;

    std::string prometheusAddr;
    if (jasminegraph_profile == PROFILE_K8S) {
        std::unique_ptr<K8sInterface> interface(new K8sInterface());
        prometheusAddr = interface->getJasmineGraphConfig("prometheus_address");
    } else {
        prometheusAddr = getJasmineGraphProperty("org.jasminegraph.collector.prometheus");
    }
    std::string prometheusQueryAddr = prometheusAddr + "api/v1/query?query=" + metricName;

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, prometheusQueryAddr.c_str());

        // Set the callback function to handle the response data
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_cpu_usages);
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0);
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);

        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            util_logger.error("cURL failed: " + string(curl_easy_strerror(res)));
        } else {
            util_logger.info(response_cpu_usages);
            Json::Value root;
            Json::Reader reader;
            reader.parse(response_cpu_usages, root);
            const Json::Value results = root["data"]["result"];
            Json::Value currentExportedJobName;
            for (int i = 0; i < results.size(); i++) {
                currentExportedJobName = results[i]["metric"]["exported_job"];
                map[(currentExportedJobName.asString().c_str())] = (results[i]["value"][1]).asString();
            }
            curl_easy_cleanup(curl);
        }
    }

    return map;
}

bool Utils::fileExistsWithReadPermission(const string &path) { return access(path.c_str(), R_OK) == 0; }

std::fstream *Utils::openFile(const string &path, std::ios_base::openmode mode) {
    if (!fileExistsWithReadPermission(path)) {
        // Create the file if it doesn't exist
        std::ofstream dummyFile(path, std::ios::out | std::ios::binary);
        dummyFile.close();
    }
    // Now open the file in the desired mode
    return new std::fstream(path, mode | std::ios::binary);
}

bool Utils::uploadFileToWorker(std::string host, int port, int dataPort, int graphID, std::string filePath,
                               std::string masterIP, std::string uploadType) {
    util_logger.info("Host:" + host + " Port:" + to_string(port) + " DPort:" + to_string(dataPort));
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        util_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        util_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, uploadType, JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    std::string fileName = Utils::getFileName(filePath);
    int fileSize = Utils::getFileSize(filePath);
    std::string fileLength = to_string(fileSize);

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, std::to_string(graphID),
                                   JasmineGraphInstanceProtocol::SEND_FILE_NAME)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, fileName,
                                   JasmineGraphInstanceProtocol::SEND_FILE_LEN)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, fileLength,
                                   JasmineGraphInstanceProtocol::SEND_FILE_CONT)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    util_logger.info("Going to send file" + filePath + "/" + fileName + "through file transfer service to worker");
    Utils::sendFileThroughService(host, dataPort, fileName, filePath);

    string response;
    int count = 0;
    while (true) {
        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            return false;
        }
        util_logger.info("Sent: " + JasmineGraphInstanceProtocol::FILE_RECV_CHK);

        util_logger.info("Checking if file is received");
        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
            util_logger.info("Received: " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT);
            util_logger.info("Checking file status : " + to_string(count));
            count++;
            sleep(1);
            continue;
        } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
            util_logger.info("Received: " + JasmineGraphInstanceProtocol::FILE_ACK);
            util_logger.info("File transfer completed for file : " + filePath);
            break;
        }
    }
    // Next we wait till the batch upload completes
    while (true) {
        if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK)) {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            return false;
        }
        util_logger.info("Sent: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK);

        response = Utils::read_str_trim_wrapper(sockfd, data, FED_DATA_LENGTH);
        if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
            util_logger.info("Received: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT);
            sleep(1);
            continue;
        } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
            util_logger.info("Received: " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK);
            util_logger.info("Batch upload completed: " + fileName);
            break;
        }
    }
    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

bool Utils::sendFileThroughService(std::string host, int dataPort, std::string fileName, std::string filePath) {
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    util_logger.info("Sending file " + filePath + " through port " + std::to_string(dataPort));
    FILE *fp = fopen(filePath.c_str(), "r");
    if (fp == NULL) {
        return false;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        util_logger.error("Cannot create socket");
        return false;
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        util_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(dataPort);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, fileName,
                                   JasmineGraphInstanceProtocol::SEND_FILE_LEN)) {
        close(sockfd);
        return false;
    }

    int fsize = Utils::getFileSize(filePath);
    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, to_string(fsize),
                                   JasmineGraphInstanceProtocol::SEND_FILE)) {
        close(sockfd);
        return false;
    }

    bool status = true;
    while (true) {
        unsigned char buff[1024];
        int nread = fread(buff, 1, sizeof(buff), fp);

        /* If read was success, send data. */
        if (nread > 0) {
            write(sockfd, buff, nread);
        } else {
            if (feof(fp)) util_logger.info("End of file");
            if (ferror(fp)) {
                status = false;
                util_logger.error("Error reading file: " + filePath);
            }
            break;
        }
    }

    fclose(fp);
    close(sockfd);
    return status;
}

/*
 * Function to transfer a partition from one worker to another
 * Caller should ensure that the partition exists in the source worker
 * and the destination worker is ready to accept the partition.
 * */
bool Utils::transferPartition(std::string sourceWorker, int sourceWorkerPort, std::string destinationWorker,
                              int destinationWorkerDataPort, std::string graphID, std::string partitionID,
                              std::string workerID, SQLiteDBInterface *sqlite) {
    util_logger.info("### Transferring partition " + partitionID + " of graph " + graphID + " from " + sourceWorker +
                     " to " + destinationWorker);

    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        util_logger.error("Cannot create socket");
        return false;
    }

    server = gethostbyname(sourceWorker.c_str());
    if (server == NULL) {
        util_logger.error("ERROR, no host named " + sourceWorker);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(sourceWorkerPort);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, destinationWorker)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, JasmineGraphInstanceProtocol::PUSH_PARTITION,
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                   destinationWorker + ":" + to_string(destinationWorkerDataPort),
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH, graphID + "," + partitionID,
                                   JasmineGraphInstanceProtocol::OK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    util_logger.info("### Transfer partition completed");
    sqlite->runInsert(
        "INSERT INTO worker_has_partition "
        "(partition_idpartition, partition_graph_idgraph, worker_idworker) VALUES ('" +
        partitionID + "', '" + graphID + "', '" + workerID + "')");

    Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
    close(sockfd);
    return true;
}

bool Utils::sendQueryPlanToWorker(std::string host, int port, std::string masterIP,
                                  int graphID, int partitionId, std::string message, SharedBuffer &sharedBuffer){
    util_logger.info("Host:" + host + " Port:" + to_string(port));
    bool result = true;
    int sockfd;
    char data[FED_DATA_LENGTH + 1];
    static const int ACK_MESSAGE_SIZE = 1024;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        util_logger.error("Cannot create socket");
        return false;
    }

    if (host.find('@') != std::string::npos) {
        host = Utils::split(host, '@')[1];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        util_logger.error("ERROR, no host named " + host);
        return false;
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return false;
    }

    if (!Utils::performHandshake(sockfd, data, FED_DATA_LENGTH, masterIP)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if (!Utils::sendExpectResponse(sockfd, data, INSTANCE_DATA_LENGTH,
                                   JasmineGraphInstanceProtocol::QUERY_START,
                                   JasmineGraphInstanceProtocol::QUERY_START_ACK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }
    char ack1[ACK_MESSAGE_SIZE] = {0};
    int message_length = std::to_string(graphID).length();
    int converted_number = htonl(message_length);
    util_logger.info("Sending content length: "+ to_string(converted_number));
    if (!Utils::sendIntExpectResponse(sockfd, ack1,
                                      JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(),
                                   converted_number,
                                   JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if(!Utils::send_str_wrapper(sockfd, to_string(graphID))) {
        close(sockfd);
        return false;
    }

    char ack2[ACK_MESSAGE_SIZE] = {0};
    message_length = std::to_string(partitionId).length();
    converted_number = htonl(message_length);
    util_logger.info("Sending content length: "+to_string(converted_number));

    if (!Utils::sendIntExpectResponse(sockfd, ack2,
                                      JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(),
                                      converted_number,
                                      JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if(!Utils::send_str_wrapper(sockfd, to_string(partitionId))) {
        close(sockfd);
        return false;
    }

    char ack3[ACK_MESSAGE_SIZE] = {0};
    message_length = message.length();
    converted_number = htonl(message_length);
    util_logger.info("Sending content length: "+to_string(converted_number));

    if (!Utils::sendIntExpectResponse(sockfd, ack3,
                                      JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(),
                                      converted_number,
                                      JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK)) {
        Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
        close(sockfd);
        return false;
    }

    if(!Utils::send_str_wrapper(sockfd, message)) {
        close(sockfd);
        return false;
    }

    while(true){
        char start[ACK_MESSAGE_SIZE] = {0};
        recv(sockfd, &start, sizeof(start), 0);
        std::string start_msg(start);
        if (JasmineGraphInstanceProtocol::QUERY_DATA_START != start_msg) {
            util_logger.error("Error while receiving start command ack : "+ start_msg);
            continue;
        }
        util_logger.info(start);
        send(sockfd, JasmineGraphInstanceProtocol::QUERY_DATA_ACK.c_str(),
             JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);

        int content_length;
        ssize_t return_status = recv(sockfd, &content_length, sizeof(int), 0);
        if (return_status > 0) {
            content_length = ntohl(content_length);
            util_logger.info("Received int =" + std::to_string(content_length));
        } else {
            util_logger.error("Error while receiving content length");
            return false;
        }
        send(sockfd, JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.c_str(),
             JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);

        std::string data(content_length, 0);
        return_status = recv(sockfd, &data[0], content_length, 0);
        if (return_status > 0) {
            util_logger.info("Received graph data: ");
            send(sockfd, JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.c_str(),
                 JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);
        } else {
            util_logger.info("Error while reading graph data");
            return false;
        }
        if(data == "-1"){
            sharedBuffer.add(data);
            break;
        }
//        auto str = json::parse(data);
//        std::ostringstream row;
//        row << "| " << std::left
//            << std::setw(10) << str.value("id", "-")
//            << "| " << std::setw(30) << str.value("name", "-")
//            << "| " << std::setw(30) << str.value("occupation", str.value("category", "-"))
//            << "| " << std::setw(10) << str.value("type", "-")
//            << "|";
        sharedBuffer.add(data);
    }

    return true;
}