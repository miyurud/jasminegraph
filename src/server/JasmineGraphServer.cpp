/**
Copyright 2018 JasmineGraph Team
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
#include "JasmineGraphServer.h"
#include "../metadb/SQLiteDBInterface.h"
#include "JasmineGraphInstance.h"
#include "../frontend/JasmineGraphFrontEnd.h"
#include "../util/Utils.h"

JasmineGraphServer::JasmineGraphServer()
{

}

JasmineGraphServer::~JasmineGraphServer()
{
    puts ("Freeing up server resources.");
    sqlite.finalize();
}

int JasmineGraphServer::run()
{
    std::cout << "Running the server..." << std::endl;

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();
    init();
    return 0;
}

bool JasmineGraphServer::isRunning() {
    return true;
}

void JasmineGraphServer::init()
{
    Utils utils;
    std::map<string, string> result = utils.getBatchUploadFileList(utils.getJasmineGraphProperty("org.jasminegraph.batchupload.file.path"));
    IS_DISTRIBUTED = utils.parseBoolean(utils.getJasmineGraphProperty("org.jasminegraph.server.mode.isdistributed"));

    if (result.size() != 0) {
        std::map<std::string, std::string>::iterator iterator1 = result.begin();
        while (iterator1 != result.end()) {
            std::string fileName = iterator1->first;
            std::string filePath = iterator1->second;
            //Next, we need to implement the batch upload logic here.
            iterator1++;
        }
    }

    this->frontend = new JasmineGraphFrontEnd(this->sqlite);
    this->frontend->run();
}
