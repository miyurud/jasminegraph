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

#ifndef JASMINEGRAPH_JASMINGRAPHLINKPREDICTOR_H
#define JASMINEGRAPH_JASMINGRAPHLINKPREDICTOR_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <map>
#include "src/server/JasmineGraphServer.h"

class JasminGraphLinkPredictor {
public:

    int initiateLinkPrediction(std::string graphID, std::string path, std::string masterIP);

    int sendQueryToWorker(std::string host, int port, int dataPort, int selectedHostPartitionsNo, std::string graphID,
                          std::string vertexCount,
                          std::string filePath,
                          std::string hostsList, std::string masterIP);
};


#endif //JASMINEGRAPH_JASMINGRAPHLINKPREDICTOR_H
