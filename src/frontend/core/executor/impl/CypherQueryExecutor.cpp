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
#include <fstream>

#include "CypherQueryExecutor.h"
#include "antlr4-runtime.h"
#include "../../../../../src/query/processor/cypher/astbuilder/ASTBuilder.h"
#include "../../../../../src/query/processor/cypher/astbuilder/ASTNode.h"
#include "../../../../../src/query/processor/cypher/semanticanalyzer/SemanticAnalyzer.h"
#include "../../../../../src/query/processor/cypher/queryplanner/QueryPlanner.h"
#include "../../../../../src/server/JasmineGraphServer.h"

#include "/home/ubuntu/software/antlr/CypherLexer.h"
#include "/home/ubuntu/software/antlr/CypherParser.h"

Logger cypher_logger;

// std::mutex processStatusMutex;
// std::mutex responseVectorMutex;

CypherQueryExecutor::CypherQueryExecutor() {}

CypherQueryExecutor::CypherQueryExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
}

void CypherQueryExecutor::execute() {
    cypher_logger.info("Executing Cypher Query");

    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
    std::string graphSLAString = request.getParameter(Conts::PARAM_KEYS::GRAPH_SLA);
    std::string queryString = request.getParameter(Conts::PARAM_KEYS::CYPHER_QUERY::QUERY_STRING);
    int numberOfPartitions = std::stoi(request.getParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS));
    int connFd = std::stoi(request.getParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR));

    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);

    antlr4::ANTLRInputStream input(queryString);
    // Create a lexer from the input
    CypherLexer lexer(&input);
    cypher_logger.info("Created lexer from input");

    // Create a token stream from the lexer
    antlr4::CommonTokenStream tokens(&lexer);
    cypher_logger.info("Created tokens from lexer");

    // Create a parser from the token stream
    CypherParser parser(&tokens);
    cypher_logger.info("Created parser from tokens");

    ASTBuilder astBuilder;
    auto* ast = any_cast<ASTNode*>(astBuilder.visitOC_Cypher(parser.oC_Cypher()));

    SemanticAnalyzer semanticAnalyzer;
    string queryPlan;
    if (semanticAnalyzer.analyze(ast)) {
        cypher_logger.info("AST is successfully analyzed");
        QueryPlanner queryPlanner;
        Operator *executionPlan = queryPlanner.createExecutionPlan(ast);
        queryPlan = executionPlan->execute();
    } else {
        cypher_logger.error("query isn't semantically correct: "+queryString);
    }

    SharedBuffer sharedBuffer(3);
    std::vector<std::future<void>> intermRes;
    std::vector<std::future<int>> statResponse;

    auto begin = chrono::high_resolution_clock::now();

    const auto &workerList = JasmineGraphServer::getWorkers(numberOfPartitions);

    std::vector<std::thread> workerThreads;
    int count = 0;
    for (auto worker : workerList) {
        workerThreads.emplace_back(
            doCypherQuery,
            worker.hostname, worker.port,
            masterIP, std::stoi(graphId), count++,
            queryPlan, std::ref(sharedBuffer)
        );
    }

    PerformanceUtil::init();

    std::string query =
        "SELECT attempt from graph_sla INNER JOIN sla_category where graph_sla.id_sla_category=sla_category.id and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(numberOfPartitions) +
        "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY + "' and sla_category.command='" + CYPHER +
        "';";

    std::vector<vector<pair<string, string>>> queryResults = perfDB->runSelect(query);

    if (queryResults.size() > 0) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());

        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        cypher_logger.info("###CYPHER-QUERY-EXECUTOR### Inserting initial record for SLA ");
        Utils::updateSLAInformation(perfDB, graphId, numberOfPartitions, 0, CYPHER, Conts::SLA_CATEGORY::LATENCY);
        statResponse.push_back(std::async(std::launch::async, AbstractExecutor::collectPerformaceData, perfDB,
                                          graphId.c_str(), CYPHER, Conts::SLA_CATEGORY::LATENCY, numberOfPartitions,
                                          masterIP, autoCalibrate));
        isStatCollect = true;
    }

    // int result_wr;
    int closeFlag = 0;
    while(true){
        if (closeFlag == numberOfPartitions) {
            break;
        }
        std::string data = sharedBuffer.get();
        if (data == "-1") {
            closeFlag++;
        } else {
            write(connFd, data.c_str(), data.length());
            write(connFd, "\r\n", 2);
        }
    }

    for (auto &thread : workerThreads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    cypher_logger.info("###CYPHER-QUERY-EXECUTOR### Executing Query : Completed");

    workerResponded = true;
    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    responseVector.push_back(jobResponse);

    responseVectorMutex.lock();
    responseMap[request.getJobId()] = jobResponse;
    responseVectorMutex.unlock();

    auto end = chrono::high_resolution_clock::now();
    auto dur = end - begin;
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

    std::string durationString = std::to_string(msDuration);

    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, numberOfPartitions, msDuration, CYPHER,
                                    Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    for (auto processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;

        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
         }
    processStatusMutex.unlock();
}

void CypherQueryExecutor::doCypherQuery(std::string host, int port, std::string masterIP, int graphID,
                                               int PartitionId, std::string message, SharedBuffer &sharedBuffer) {
    Utils::sendQueryPlanToWorker(host, port, masterIP, graphID, PartitionId, message, sharedBuffer);
}


int CypherQueryExecutor::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}



