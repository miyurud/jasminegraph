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

#ifndef JASMINEGRAPH_JASMINEGRAPHBACKEND_H
#define JASMINEGRAPH_JASMINEGRAPHBACKEND_H

#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "../metadb/SQLiteDBInterface.h"

void *backendservicesesion(void *dummyPt);

class JasmineGraphBackend {
 public:
    JasmineGraphBackend(SQLiteDBInterface *db, int numberOfWorkers);

    int run();

 private:
    SQLiteDBInterface *sqlite;
    int workerCount;
};

struct backendservicesessionargs {
    SQLiteDBInterface *sqlite;
    int connFd;
};

#endif  // JASMINEGRAPH_JASMINEGRAPHBACKEND_H
