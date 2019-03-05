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
#include "Utils.h"
#include "../frontend/JasmineGraphFrontEnd.h"
#include "Conts.h"

using namespace std;

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
                return vec2.at(1);
            }
        }
    }

    return NULL;
}

std::vector<std::string> Utils::getHostList() {
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
bool Utils::fileExists(const std::string fileName, frontendservicesessionargs *ptr) {
    std::ifstream infile(fileName);
    return infile.good();
}

/**
 * This method compresses files using gzip
 * @param filePath
 */
void Utils::compressFile(const std::string filePath) {
    char buffer[128];
    std::string result = "";
    std::string command = "gzip -f " + filePath + " 2>&1";
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
            std::cout << "File compression failed with error : " << result << endl;
        } else {
            cout << "File in " << filePath << " compressed with gzip" << endl;
        }
    } else {
        perror("popen");
        // handle error
    }
}

/**
 * This method creates a new directory if it does not exist
 * @param dirName
 */
void Utils::createDirectory(const std::string dirName) {
    if (mkdir(dirName.c_str(), 0777) == -1) {
        //std::cout << "Error : " << strerror(errno) << endl;
    } else {
        std::cout << "Directory created" << endl;
    }
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

    return jasminegraph_home;
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
