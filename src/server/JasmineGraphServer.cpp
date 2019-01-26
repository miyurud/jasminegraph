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

void *runfrontend(void *dummyPt){
    JasmineGraphServer* refToServer = (JasmineGraphServer*) dummyPt;
    refToServer->frontend = new JasmineGraphFrontEnd(refToServer->sqlite);
    refToServer->frontend->run();
}


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
    start_workers();
    return 0;
}

bool JasmineGraphServer::isRunning() {
    return true;
}

void JasmineGraphServer::init()
{
    Utils utils;
    std::map<string, string> result = utils.getBatchUploadFileList(utils.getJasmineGraphProperty("org.jasminegraph.batchupload.file.path"));

    if (result.size() != 0) {
        std::map<std::string, std::string>::iterator iterator1 = result.begin();
        while (iterator1 != result.end()) {
            std::string fileName = iterator1->first;
            std::string filePath = iterator1->second;
            //Next, we need to implement the batch upload logic here.
            iterator1++;
        }
    }

    pthread_t frontendthread;
    pthread_create(&frontendthread, NULL, runfrontend, this);
}

void JasmineGraphServer::start_workers(){
    Utils utils;
    std::vector<std::string> hostsList = utils.getHostList();
    std::vector<std::string>::iterator it;
    it = hostsList.begin();

    for (it=hostsList.begin(); it<hostsList.end(); it++) {
        std::string item = *it;
        std::cout << "===>>>" << item << std::endl;

    }
}
