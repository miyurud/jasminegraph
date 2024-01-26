/**
Copyright 2021 JasmineGraph Team
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

#ifndef JASMINEGRAPH_PAGERANKEXECUTOR_H
#define JASMINEGRAPH_PAGERANKEXECUTOR_H


#include "../AbstractExecutor.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../JasmineGraphFrontEndProtocol.h"
#include "../../../../performance/metrics/PerformanceUtil.h"
#include "../../../../server/JasmineGraphServer.h"

class PagerankExecutor : public AbstractExecutor{
 public:
    PagerankExecutor();

    PagerankExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest);
    static void doPageRank(std::string graphID, double alpha, int iterations, string partition,
                          string host, int port, int dataPort, std::string workerList);
    void execute();
    int getUid();

 private:
    SQLiteDBInterface *sqlite;
    PerformanceSQLiteDBInterface *perfDB;
};


#endif  //  JASMINEGRAPH_PAGERANKEXECUTOR_H
