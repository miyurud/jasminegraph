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

#include <vector>
#include <sstream>
#include <sys/stat.h>
#include <pwd.h>
#include <unistd.h>
#include "Utils.h"
#include "../frontend/JasmineGraphFrontEnd.h"
#include "Conts.h"
#include "logger/Logger.h"

using namespace std;
Logger util_logger;

map<std::string, std::string> Utils::getBatchUploadFileList(std::string file) {
    std::vector<std::string> batchUploadFileContent = getFileContent(file);
    std::vector<std::string>::iterator iterator1 = batchUploadFileContent.begin();
    map<std::string, std::string> *result = new map<std::string, std::string>();
    while (iterator1 != batchUploadFileContent.end()) {
        std::string str = *iterator1;

        if (str.length() > 0 && !(str.rfind("#", 0) == 0)) {

            std::vector<std::string> vec = split(str, ':');

//            ifstream batchUploadConfFile(vec.at(1));
//            string line;
//
//            if (batchUploadConfFile.is_open()) {
//                while (getline(batchUploadConfFile, line)) {
//                    cout << line << '\n';
//                }
//            }

            result->insert(std::pair<std::string, std::string>(vec.at(0), vec.at(1)));

        }

        iterator1++;
    }

    return *result;
}

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
    //map<std::string, std::string>* result = new map<std::string, std::string>();
    vector<std::string> *vec = new vector<std::string>();
    while (std::getline(in, str)) {
        // output the line
        //std::cout << str << std::endl;

        // now we loop back and get the next line in 'str'

        //if (str.length() > 0 && !(str.rfind("#", 0) == 0)) {
        if (str.length() > 0) {
            //std::vector<std::string> vec = split(str, '=');
            //std::cout << vec.size() << std::endl;
            //result->insert(std::pair<std::string, std::string>(vec.at(0), vec.at(1)));
            vec->insert(vec->begin(), str);
        }
    }

    return *vec;
};

std::string Utils::getJasmineGraphProperty(std::string key) {
    std::vector<std::string>::iterator it;
    vector<std::string> vec = getFileContent("conf/jasminegraph-server.properties");
    it = vec.begin();

    for (it = vec.begin(); it < vec.end(); it++) {
        std::string item = *it;
        if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
            std::vector<std::string> vec2 = split(item, '=');
            if (vec2.at(0).compare(key) == 0) {
                if (item.substr(item.length() - 1, item.length()).compare("=") != 0) {
                    return vec2.at(1);
                } else {
                    return " ";
                }
            }
        }
    }

    return NULL;
}

std::vector<Utils::worker> Utils::getWorkerList(SQLiteDBInterface sqlite) {
    vector<worker> workerVector;
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(
            "SELECT idworker,user,ip,server_port,server_data_port FROM worker;");
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
    vector<std::string> vec = getFileContent("conf/hosts.txt");
    it = vec.begin();

    for (it = vec.begin(); it < vec.end(); it++) {
        std::string item = *it;
        if (item.length() > 0 && !(item.rfind("#", 0) == 0)) {
            result.insert(result.begin(), item);
        }
    }

    return result;
}


inline std::string trim_right_copy(
        const std::string &s,
        const std::string &delimiters = " \f\n\r\t\v") {
    return s.substr(0, s.find_last_not_of(delimiters) + 1);
}

inline std::string trim_left_copy(
        const std::string &s,
        const std::string &delimiters = " \f\n\r\t\v") {
    return s.substr(s.find_first_not_of(delimiters));
}

std::string Utils::trim_copy(
        const std::string &s,
        const std::string &delimiters = " \f\n\r\t\v") {
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
bool Utils::fileExists(std::string fileName) {
    std::ifstream infile(fileName);
    return infile.good();
}

/**
 * This method creates a new directory if it does not exist
 * @param dirName
 */
void Utils::createDirectory(const std::string dirName) {
    if (mkdir(dirName.c_str(), 0777) == -1) {
        //std::cout << "Error : " << strerror(errno) << endl;
    } else {
        //util_logger.log("Directory " + dirName + " created successfully", "info");
    }
}

std::vector<std::string> Utils::getListOfFilesInDirectory(const std::string dirName) {
    char buffer[128];
    std::vector<string> results;
    std::string result = "";
    std::string command = "ls -l " + dirName;
    char *commandChar = new char[command.length() + 1];
    strcpy(commandChar, command.c_str());
    FILE *input = popen(commandChar, "r");
    if (input) {
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty()) {
            std::vector<std::string> vec = split(result, '\r\n');
            for(std::vector<std::string>::iterator it = vec.begin(); it != vec.end(); ++it){
                std::string line = it->c_str();
                if (line.rfind("-", 0) == 0) {
                    std::string file = line.substr(line.find_last_of(' ') + 1);
                    results.push_back(file);
                }
            }
        }
    } else {
        perror("popen");
        // handle error
    }

    return results;
}

/**
 * This method deletes a directory with all its content
 * @param dirName
 */
//TODO :: find a possible solution to handle the permission denied error when trying to delete a protected directory. popen does not work either
void Utils::deleteDirectory(const std::string dirName) {
    string command = "rm -rf " + dirName;
    system(command.c_str());
    util_logger.log(dirName + " deleted successfully", "info");
}

bool Utils::is_number(const std::string& compareString) {
    return !compareString.empty() && std::find_if(compareString.begin(),
                                                  compareString.end(), [](char c) { return !std::isdigit(c); }) == compareString.end();
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

    char const* temp = getenv(test.c_str());
    if(temp != NULL)
    {
        jasminegraph_home = std::string(temp);
    }
    if(jasminegraph_home.empty()) {
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
    if ((homedir = getenv("HOME")) == NULL)
    {
        homedir = getpwuid(getuid())->pw_dir;
    }
    return string(homedir);
}

/**
 * This method returns the size of the file in bytes
 * @param filePath
 * @return
 */
int Utils::getFileSize(std::string filePath) {
    //const clock_t begin_time = clock();
    ifstream file(filePath.c_str(), ifstream::in | ifstream::binary);
    if (!file.is_open()) {
        return -1;
    }
    file.seekg(0, ios::end);
    int fileSize = file.tellg();
    file.close();
    //std::cout << "TIME FOR READ : "<<float( clock () - begin_time ) /  CLOCKS_PER_SEC << std::endl;
    return fileSize;
}

/**
 * This method compresses files using gzip
 * @param filePath
 */
void Utils::compressFile(const std::string filePath, const std::string mode) {
    char buffer[128];
    std::string result = "";
    std::string command = mode + " -f " + filePath + " 2>&1";
    char *commandChar = new char[command.length() + 1];
    strcpy(commandChar, command.c_str());
    FILE *input = popen(commandChar, "r");
    if (input) {
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty()) {
            if (result.find("pigz: not found") != std::string::npos) {
                util_logger.log("pigz not found. Compressing using gzip", "info");
                compressFile(filePath, "gzip");
            } else {
                util_logger.log("File compression failed with error: " + result, "error");
            }
        }
    } else {
        perror("popen");
        // handle error
    }
}

/**
 * this method extracts a gzip file
 * @param filePath
 */
void Utils::unzipFile(std::string filePath, const std::string mode ) {
    char buffer[128];
    std::string result = "";
    std::string command = mode + " -f -d " + filePath + " 2>&1";
    char *commandChar = new char[command.length() + 1];
    strcpy(commandChar, command.c_str());
    FILE *input = popen(commandChar, "r");
    if (input) {
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty()) {
            if (result.find("pigz: not found") != std::string::npos) {
                util_logger.log("pigz not found. Decompressing using gzip", "info");
                unzipFile(filePath, "gzip");
            } else {
                util_logger.log("File decompression failed with error: " + result, "error");
            }
        }
    } else {
        perror("popen");
        // handle error
    }
}

int Utils::parseARGS(char **args, char *line){
    int tmp=0;
    args[tmp] = strtok( line, ":" );
    while ( (args[++tmp] = strtok(NULL, ":" ) ) != NULL );
    return tmp - 1;
}

/**
 * This method checks if a host exists in JasmineGraph MetaBD.
 * This method uses the name and ip of the host.
 */
bool Utils::hostExists(string name, string ip, std::string workerPort, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM worker WHERE name LIKE '" + name + "' AND ip LIKE '" + ip + "' AND server_port LIKE '"+ workerPort +"';";
    if (ip == ""){
        stmt = "SELECT COUNT( * ) FROM worker WHERE name LIKE '" + name + "';";
    }
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

string Utils::getHostID(string hostName, SQLiteDBInterface sqlite) {
    map<string, string> hostIDMap;
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect("SELECT idhost FROM host where name LIKE '" +
            hostName + "';");
    string id = v[0][0].second;

    return id;
}


/**
 * This method compresses directories using tar
 * @param filePath
 */
void Utils::compressDirectory(const std::string filePath) {
    char buffer[128];
    std::string result = "";
    std::string command = "tar -czvf " + filePath + ".tar.gz "+ filePath;
    char *commandChar = new char[command.length() + 1];
    strcpy(commandChar, command.c_str());
    FILE *input = popen(commandChar, "r");
    if (input) {
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty()) {
            util_logger.log("Directory compression failed with error: " + result, "error");
        } else {
            //util_logger.log("File in " + filePath + " compressed with gzip", "info");
        }
    } else {
        perror("popen");
        // handle error
    }
}

/**
 * this method extracts a tar.gz directory
 * @param filePath
 */
void Utils::unzipDirectory(std::string filePath){

    char buffer[128];
    std::string result = "";
    std::string command = "tar -xzvf " + filePath ;
    char *commandChar = new char[command.length() + 1];
    strcpy(commandChar, command.c_str());
    FILE *input = popen(commandChar, "r");
    if (input) {
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        pclose(input);
        if (!result.empty()) {
            util_logger.log("Directory decompression failed with error : " + result, "error");
        } else {
            //util_logger.log("File in " + filePath + " extracted with gzip", "info");
        }
    } else {
        perror("popen");
        // handle error
    }
}

void Utils::assignPartitionsToWorkers(int numberOfWorkers, SQLiteDBInterface sqlite) {
    sqlite.runUpdate("DELETE FROM worker_has_partition");

    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(
            "SELECT idpartition, graph_idgraph FROM partition;");
    int workerCounter = 0;
    string valueString;
    string sqlStatement = "INSERT INTO worker_has_partition (partition_idpartition, partition_graph_idgraph, worker_idworker) VALUES ";
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
        sqlite.runInsert(sqlStatement);
    }
}


string Utils::replace(string str, string old, string replacement) {
    size_t index = 0;
    while (true) {
        /* Locate the substring to replace. */
        index = str.find(old, index);
        if (index == std::string::npos) {
            return str;
        }

        /* Make the replacement. */
        str.replace(index, 1, replacement);

        /* Advance index forward so the next iteration doesn't pick it up as well. */
        index += 2;
    }
}
