/**
Copyright 2019 JasminGraph Team
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

#ifndef JASMINEGRAPH_JASMINEGRAPHFRONTEND_H
#define JASMINEGRAPH_JASMINEGRAPHFRONTEND_H


#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <pthread.h>
#include "../metadb/SQLiteDBInterface.h"

void *frontendservicesesion(void *dummyPt);

class JasmineGraphFrontEnd {
public:
    JasmineGraphFrontEnd(SQLiteDBInterface db);

    int run();

    static bool graphExists(std::string basic_string, void *dummyPt);

    static bool graphExistsByID(std::string id, void *dummyPt);

    static void removeGraph(std::string graphID, void *dummyPt);

    static bool isGraphActiveAndTrained(std::string graphID, void *dummyPt);

private:
    SQLiteDBInterface sqlite;
};

struct frontendservicesessionargs {
    SQLiteDBInterface sqlite;
    int connFd;
};

#endif //JASMINGRAPH_JASMINGRAPHFRONTEND_H
