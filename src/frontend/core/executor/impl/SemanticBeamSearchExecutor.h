/**
Copyright 2025 JasmineGraph Team
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
#ifndef JASMINEGRAPH_SEMANTICBEAMSEARCHEXECUTOR_H
#define JASMINEGRAPH_SEMANTICBEAMSEARCHEXECUTOR_H
#include "../AbstractExecutor.h"

class SemanticBeamSearchExecutor : public AbstractExecutor {
 public:
    SemanticBeamSearchExecutor();

    SemanticBeamSearchExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest);

    void execute() override;
    static void doSemanticBeamSearch(std::string host, int port, std::string masterIP, int graphID, int partitionId,
                                     std::string query, SharedBuffer &sharedBuffer, int noOfPartitions);
    static int getUid();

 private:
    SQLiteDBInterface *sqlite;
    PerformanceSQLiteDBInterface *perfDB;
};

#endif  // JASMINEGRAPH_SEMANTICBEAMSEARCHEXECUTOR_H
