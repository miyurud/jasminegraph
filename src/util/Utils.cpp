/**
Copyright 2018 JasminGraph Team
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

using namespace std;

map<std::string, std::string> Utils::getBatchUploadFileList()
{
    map<std::string, std::string>* result = getFileContentAsMap("conf/batch-upload.txt");
    std::map<std::string, std::string>::iterator iterator1 = result->begin();
    while (iterator1 != result->begin())
    {
        std::string fileName = iterator1->first;
        std::string filePath = iterator1->second;
        ifstream batchUploadConfFile (filePath);
        string line;

        if (batchUploadConfFile.is_open())
        {
            while (getline(batchUploadConfFile, line))
            {
                cout << line << '\n';
            }
        }

        iterator1++;
    }


}

map<std::string, std::string>* Utils::getFileContentAsMap(std::string file)
{
    ifstream in(file);

    std::string str;
    while (std::getline(in, str)) {
        // output the line
        std::cout << str << std::endl;

        // now we loop back and get the next line in 'str'
    }

    map<std::string, std::string>* result = new map<std::string, std::string>();

    return result;
};


