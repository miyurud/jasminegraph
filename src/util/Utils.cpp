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

#include <dirent.h>
#include <pwd.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <vector>

#include "Conts.h"
#include "logger/Logger.h"

using namespace std;
Logger util_logger;

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
    vector<std::string> *vec = new vector<std::string>();
    while (std::getline(in, str)) {
        // now we loop back and get the next line in 'str'

        if (str.length() > 0) {
            vec->insert(vec->begin(), str);
        }
    }

    return *vec;
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

std::string Utils::replaceAll(std::string content, const std::string& oldValue, const std::string& newValue) {
    size_t pos = 0;
    while ((pos = content.find(oldValue, pos)) != std::string::npos) {
        content.replace(pos, oldValue.length(), newValue);
        pos += newValue.length();
    }
    return content;
}

void Utils::writeFileContent(const std::string& filePath, const std::string& content) {
    std::ofstream out(filePath);
    if (!out.is_open()) {
        util_logger.error("Cannot write to file path: " + filePath);
    }
    out << content;
    out.close();
}

std::string Utils::getJasmineGraphProperty(std::string key) {
    if (Utils::propertiesMap.empty()) {
        std::vector<std::string>::iterator it;
        vector<std::string> vec = Utils::getFileContent(ROOT_DIR "conf/jasminegraph-server.properties");
        it = vec.begin();

        for (it = vec.begin(); it < vec.end(); it++) {
            std::string item = *it;
            if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
                std::vector<std::string> vec2 = split(item, '=');
                if (vec2.size() == 2) {
                    Utils::propertiesMap[vec2.at(0)] = vec2.at(1);
                } else {
                    Utils::propertiesMap[vec2.at(0)] = string(" ");
                }
            }
        }
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

static inline std::string trim_right_copy(const std::string &s, const std::string &delimiters = " \f\n\r\t\v") {
    return s.substr(0, s.find_last_not_of(delimiters) + 1);
}

static inline std::string trim_left_copy(const std::string &s, const std::string &delimiters = " \f\n\r\t\v") {
    return s.substr(s.find_first_not_of(delimiters));
}

std::string Utils::trim_copy(const std::string &s, const std::string &delimiters = " \f\n\r\t\v") {
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
void Utils::createDirectory(const std::string dirName) {
    // TODO: check if directory exists before creating
    // TODO: check return value
    mkdir(dirName.c_str(), 0777);
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
    std::string filename = filePath.substr(filePath.find_last_of("/\\") + 1);
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
        util_logger.log("Returning empty value for " + Conts::JASMINEGRAPH_HOME, "warn");
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
    util_logger.log("Starting file copy source: " + sourceFilePath + " destination: " + destinationFilePath, "info");
    std::string command = "cp " + sourceFilePath + " " + destinationFilePath;
    int status = system(command.c_str());
    if (status != 0) util_logger.warn("Copy failed with exit code " + std::to_string(status));
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
            util_logger.info("pigz not found. Compressing using gzip");
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
        util_logger.log("Invalid SLA " + category + " for " + command + " command", "error");
    }
}

int Utils::copyToDirectory(std::string currentPath, std::string copyPath) {
    std::string command = "mkdir -p " + copyPath + "&& " + "cp " + currentPath + " " + copyPath;
    int status = system(command.c_str());
    if (status != 0) {
        util_logger.error("Copying " + currentPath + " to directory " + copyPath + " failed with code " +
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
    string str = read_str_wrapper(connFd, buf, len, false);
    if (!str.empty()) str = trim_copy(str, " \f\n\r\t\v");
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

bool Utils::send_str_wrapper(int connFd, std::string str) { return send_wrapper(connFd, str.c_str(), str.length()); }

std::string Utils::getCurrentTimestamp() {
    auto now = chrono::system_clock::now();
    time_t time = chrono::system_clock::to_time_t(now);
    tm tm_time;
    localtime_r(&time, &tm_time);
    stringstream timestamp;
    timestamp << put_time(&tm_time, "%y%m%d_%H%M%S");  // Format can be customized

    return timestamp.str();
}

inline json parse_scalar(const YAML::Node &node) {
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

inline json yaml2json(const YAML::Node &root) {
    json j{};

    switch (root.Type()) {
        case YAML::NodeType::Null: break;
        case YAML::NodeType::Scalar: return parse_scalar(root);
        case YAML::NodeType::Sequence:
            for (auto &&node : root)
                j.emplace_back(yaml2json(node));
            break;
        case YAML::NodeType::Map:
            for (auto &&it : root)
                j[it.first.as<std::string>()] = yaml2json(it.second);
            break;
        default: break;
    }
    return j;
}

std::string Utils::getJsonStringFromYamlFile(const std::string &yamlFile) {
    YAML::Node yamlNode = YAML::LoadFile(yamlFile);
    return to_string(yaml2json(yamlNode));

}