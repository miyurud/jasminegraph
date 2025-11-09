//
// Created by sajeenthiran on 2025-08-26.
//

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