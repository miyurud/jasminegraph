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

#include <iostream>
#include <map>
#include "JasminGraphServer.h"
#include "../metadb/SQLiteDBInterface.h"
#include "JasminGraphInstance.h"
#include "../util/Utils.h"

JasminGraphServer::JasminGraphServer()
{

}

JasminGraphServer::~JasminGraphServer()
{
    puts ("Freeing up server resources.");
    sqlite.finalize();
}

int JasminGraphServer::run()
{
    std::cout << "Running the server..." << std::endl;

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();
    init();
    return 0;
}

bool JasminGraphServer::isRunning() {
    return true;
}

void JasminGraphServer::init()
{
    Utils utils;
    std::map<string, string> result = utils.getBatchUploadFileList(utils.getJasminGraphProperty("org.jasmingraph.batchupload.file.path"));

    if (result.size() != 0) {
        std::map<std::string, std::string>::iterator iterator1 = result.begin();
        while (iterator1 != result.end()) {
            std::string fileName = iterator1->first;
            std::string filePath = iterator1->second;
            //Next, we need to implement the batch upload logic here.
            iterator1++;
        }
    }

    this->frontend = new JasminGraphFrontEnd(sqlite);
    this->frontend->run();
}
