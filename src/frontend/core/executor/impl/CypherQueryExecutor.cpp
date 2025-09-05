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
#include "../../../../../src/query/processor/cypher/runtime/AggregationFactory.h"
#include "../../../../../src/query/processor/cypher/runtime/Aggregation.h"
#include "../../../../../src/server/JasmineGraphServer.h"

#include "/home/ubuntu/software/antlr/CypherLexer.h"
#include "/home/ubuntu/software/antlr/CypherParser.h"

Logger cypher_logger;

inline const json* getNestedValuePtr(const json& obj, const std::string& dottedKey) {
    // Direct lookup first
    auto it = obj.find(dottedKey);
    if (it != obj.end()) {
        return &(*it);
    }

    // Nested resolution
    const json* current = &obj;
    size_t start = 0;
    while (start < dottedKey.size()) {
        size_t dot = dottedKey.find('.', start);
        std::string key = dottedKey.substr(start, dot - start);
        if (!current->is_object()) {
            cypher_logger.error("Current JSON is not an object at key: '" + key + "'");
            return nullptr;
        }
        auto nested = current->find(key);
        if (nested == current->end()) {
            cypher_logger.error("Key '" + key + "' not found");
            return nullptr;
        }
        current = &(*nested);
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return current;
}

CypherQueryExecutor::CypherQueryExecutor() {}

CypherQueryExecutor::CypherQueryExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
    JobRequest jobRequest) {
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
    bool *loop_exit = reinterpret_cast<bool*>(static_cast<std::uintptr_t>(std::stoull(
        request.getParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER))));


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
        cypher_logger.error("Query isn't semantically correct: " + queryString);
    }

    std::vector<std::future<void>> intermRes;
    std::vector<std::future<int>> statResponse;

    auto begin = chrono::high_resolution_clock::now();

    const auto &workerList = JasmineGraphServer::getWorkers(numberOfPartitions);

    std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
    bufferPool.reserve(numberOfPartitions);  // Pre-allocate space for pointers
    for (size_t i = 0; i < numberOfPartitions; ++i) {
        bufferPool.emplace_back(std::make_unique<SharedBuffer>(MASTER_BUFFER_SIZE));
    }

    std::vector<std::thread> workerThreads;
    int count = 0;
    for (auto worker : workerList) {
        workerThreads.emplace_back(
            doCypherQuery,
            worker.hostname, worker.port,
            masterIP, std::stoi(graphId), count,
            queryPlan, std::ref(*bufferPool[count]));
        count++;
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

    int result_wr;
    int closeFlag = 0;
    if (Operator::isAggregate) {
        auto startTime = std::chrono::high_resolution_clock::now();
        if (Operator::aggregateType == AggregationFactory::AVERAGE) {
            Aggregation* aggregation = AggregationFactory::getAggregationMethod(AggregationFactory::AVERAGE);
            while (true) {
                if (closeFlag == numberOfPartitions) {
                    break;
                }
                for (size_t i = 0; i < bufferPool.size(); ++i) {
                    std::string data;
                    if (bufferPool[i]->tryGet(data)) {
                        if (data == "-1") {
                            closeFlag++;
                        } else {
                            aggregation->insert(data);
                        }
                    }
                }
            }
            aggregation->getResult(connFd);
        } else if (Operator::aggregateType == AggregationFactory::ASC ||
                   Operator::aggregateType == AggregationFactory::DESC) {
            struct BufferEntry {
                std::string value;
                size_t bufferIndex;
                json data;
                bool isAsc;
                BufferEntry(const std::string& v, size_t idx, const json& parsed, bool asc)
                        : value(v), bufferIndex(idx), data(parsed), isAsc(asc) {}
                bool operator<(const BufferEntry& other) const {
                    const json* val1 = getNestedValuePtr(data, Operator::aggregateKey);
                    if (!val1) {
                        cypher_logger.error("Missing key in val1 for comparison: " + Operator::aggregateKey);
                        return false;  // or decide what fallback you want
                    }
                    const json* val2 = getNestedValuePtr(other.data, Operator::aggregateKey);
                    if (!val2) {
                        cypher_logger.error("Missing key in val2 for comparison: " + Operator::aggregateKey);
                        return false;
                    }
                    bool result;
                    if (val1->is_number_integer() && val2->is_number_integer()) {
                        result = val1->get<int>() > val2->get<int>();
                    } else if (val1->is_string() && val2->is_string()) {
                        result = val1->get<std::string>() > val2->get<std::string>();
                    } else {
                        result = val1->dump() > val2->dump();  // fallback comparison
                    }
                    return isAsc ? result : !result;
                }
            };
            bool isAsc = (Operator::aggregateType == AggregationFactory::ASC);
            auto cmp = [](const BufferEntry& a, const BufferEntry& b) { return a < b; };
            std::priority_queue<BufferEntry, std::vector<BufferEntry>, decltype(cmp)> mergeQueue(cmp);

            cypher_logger.info("START MASTER STREAMING MERGE");
            for (size_t i = 0; i < bufferPool.size(); ++i) {
                std::string value = bufferPool[i]->get();
                if (!value.empty()) {
                    if (value == "-1") {
                        closeFlag++;
                    } else {
                        try {
                            json parsed = json::parse(value);
                            BufferEntry entry{value, i, parsed, isAsc};
                            mergeQueue.push(entry);
                        } catch (const json::exception &e) {
                            cypher_logger.error("JSON parse error in init: " + std::string(e.what()));
                        }
                    }
                }
            }

            // Streaming merge loop
            while (!mergeQueue.empty()) {
                BufferEntry top = mergeQueue.top();
                mergeQueue.pop();
                result_wr = write(connFd, top.value.c_str(), top.value.length());
                write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());

                // Try to get next element from the same worker buffer
                std::string nextValue = bufferPool[top.bufferIndex]->get();
                if (nextValue == "-1") {
                    closeFlag++;
                } else {
                    try {
                        json parsed = json::parse(nextValue);
                        BufferEntry nextEntry{nextValue, top.bufferIndex, parsed, isAsc};
                        mergeQueue.push(nextEntry);
                    } catch (const json::exception& e) {
                        cypher_logger.error("JSON parse error: " + std::string(e.what()));
                    }
                }
                if (closeFlag >= bufferPool.size()) break;
            }
            cypher_logger.info("MASTER STREAMING MERGE COMPLETED");

        } else {
            std::string log = "Query is recongnized as Aggreagation, but method doesnot have implemented yet";
            result_wr = write(connFd, log.c_str(), log.length());
            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                              Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                cypher_logger.error("Error writing to socket");
                *loop_exit = true;
                return;
            }
        }
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        int totalTime = duration.count();
        cypher_logger.info("Total time taken for aggregation: " + std::to_string(totalTime) + " ms");
        Operator::isAggregate = false;
    } else {
        int count = 0;
        while (true) {
            if (closeFlag == numberOfPartitions) {
                break;
            }
            for (size_t i = 0; i < bufferPool.size(); ++i) {
                std::string data;
                if (bufferPool[i]->tryGet(data)) {
                    if (data == "-1") {
                        closeFlag++;
                    } else {
                        count++;
                        result_wr = write(connFd, data.c_str(), data.length());
                        result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                                          Conts::CARRIAGE_RETURN_NEW_LINE.size());
                        if (result_wr < 0) {
                            cypher_logger.error("Error writing to socket");
                            *loop_exit = true;
                            return;
                        }
                    }
                }
            }
        }
        cypher_logger.info("Total records returned: " + std::to_string(count));
    }

    for (auto& thread : workerThreads) {
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

