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

#include <dirent.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>

#include "../metadb/SQLiteDBInterface.h"
#include "../performancedb/PerformanceSQLiteDBInterface.h"
#include "../query/algorithms/triangles/Triangles.h"
#include "core/scheduler/JobScheduler.h"

class JasmineGraphHashMapCentralStore;

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface *sqlite,
                            PerformanceSQLiteDBInterface *perfSqlite, JobScheduler *jobScheduler);

class JasmineGraphFrontEnd {
 public:
    JasmineGraphFrontEnd(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, std::string masterIP,
                         JobScheduler *jobScheduler);

    int run();

    static void scheduleStrianJobs(JobRequest &jobDetails, std::priority_queue<JobRequest> &jobQueue,
                                    JobScheduler *jobScheduler, bool *strian_exist);

    static int getRunningHighPriorityTaskCount();
    static bool areRunningJobsForSameGraph();
    static bool constructKGStreamHDFSCommand(std::string masterIP, int connFd,
                                             int numberOfPartitions,
                                             SQLiteDBInterface* sqlite, bool* loop_exit_p);

    static bool strian_exit;
    std::map<std::string, std::atomic<bool>> *streamsState;
    std::map<std::string, std::thread> streamingThreads;

 private:
    SQLiteDBInterface *sqlite;
    std::string masterIP;
    PerformanceSQLiteDBInterface *perfSqlite;
    JobScheduler *jobScheduler;
};

struct frontendservicesessionargs {
    std::string masterIP;
    int connFd;
    SQLiteDBInterface *sqlite;
    PerformanceSQLiteDBInterface *perfSqlite;
    JobScheduler *jobScheduler;
};

#endif  // JASMINGRAPH_JASMINGRAPHFRONTEND_H
