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

#include <vector>
#include <map>
#include <iostream>
#include <fstream>
#include "../frontend/JasmineGraphFrontEnd.h"
#include <algorithm>

using std::map;

class Utils
{
  public:
    map<std::string, std::string> getBatchUploadFileList(std::string file);

    std::string getJasmineGraphProperty(std::string key);

    std::vector<std::string> getHostList();

    std::vector<std::string> getFileContent(std::string);

    static std::vector<std::string> split(const std::string &, char delimiter);

    std::string trim_copy(const std::string &,
                          const std::string &);

    bool parseBoolean(const std::string str);

    bool fileExists(const std::string fileName, frontendservicesessionargs *ptr);

    void compressFile(const std::string filePath);
    bool is_number(const std::string &compareString);

    void createDirectory(const std::string dirName);

    // Static method to get running users home directory
    static std::string getHomeDir();
};

#endif //JASMINEGRAPH_UTILS_H
