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

#include "OperatorExecutor.h"
#include "InstanceHandler.h"
#include "../util/Const.h"
#include "../../../../util/logger/Logger.h"
#include "Helpers.h"
#include <thread>
#include <future>
#include <queue>
#include <chrono>
#include <random>

Logger execution_logger;
std::unordered_map<std::string,
    std::function<void(OperatorExecutor&, SharedBuffer&, std::string, GraphConfig)>> OperatorExecutor::methodMap;

// Initialize static parallel executor (shared across all instances)
std::unique_ptr<IntraPartitionParallelExecutor> OperatorExecutor::parallelExecutor = nullptr;

// Helper function to extract node data and manage memory
static json extractNodeDataAndCleanup(NodeBlock* node) {
    json nodeData;
    std::string pid(node->getMetaPropertyHead()->value);
    nodeData["partitionID"] = pid;
    std::map<std::string, char*> rawProps = node->getAllProperties();
    
    // Use RAII to automatically cleanup allocated memory
    std::vector<std::unique_ptr<char[]>> cleanup;
    cleanup.reserve(rawProps.size());
    
    for (const auto& [key, value] : rawProps) {
        nodeData[key] = value;
        cleanup.emplace_back(value);
    }
    return nodeData;
}

// Helper function to extract relation data and manage memory
static json extractRelationDataAndCleanup(RelationBlock* relation) {
    json relationData;
    std::map<std::string, char*> rawProps = relation->getAllProperties();
    
    // Use RAII to automatically cleanup allocated memory
    std::vector<std::unique_ptr<char[]>> cleanup;
    cleanup.reserve(rawProps.size());
    
    for (const auto& [key, value] : rawProps) {
        relationData[key] = std::string(value);
        cleanup.emplace_back(value);
    }
    return relationData;
}

// Helper to safely close a database stream
static void closeDBStream(std::fstream*& db) {
    if (db) {
        db->close();
        db = nullptr;
    }
}

// Helper to open database files
static void openDatabaseFiles(const std::string& dbPrefix) {
    std::ios_base::openmode openMode = std::ios::in | std::ios::out | std::ios::binary;
    NodeBlock::nodesDB = Utils::openFile(dbPrefix + "_nodes.db", openMode);
    RelationBlock::relationsDB = Utils::openFile(dbPrefix + "_relations.db", openMode);
    RelationBlock::centralRelationsDB = Utils::openFile(dbPrefix + "_central_relations.db", openMode);
    PropertyLink::propertiesDB = Utils::openFile(dbPrefix + "_properties.db", openMode);
    MetaPropertyLink::metaPropertiesDB = Utils::openFile(dbPrefix + "_meta_properties.db", openMode);
    PropertyEdgeLink::edgePropertiesDB = Utils::openFile(dbPrefix + "_edge_properties.db", openMode);
    MetaPropertyEdgeLink::metaEdgePropertiesDB = Utils::openFile(dbPrefix + "_meta_edge_properties.db", openMode);
}

// Initialize thread-local database connections
void initializeThreadLocalDBs(const GraphConfig& gc) {
    // Use function-local static variables (thread-safe in C++11+)
    static thread_local int currentPartitionID = -1;
    static thread_local int currentGraphID = -1;
    
    // Check if already initialized for this partition
    if (bool needsInit = (currentPartitionID != gc.partitionID || 
                          currentGraphID != gc.graphID || 
                          RelationBlock::relationsDB == nullptr); !needsInit) {
        return;
    }

    // Close existing connections
    closeDBStream(NodeBlock::nodesDB);
    closeDBStream(RelationBlock::relationsDB);
    closeDBStream(RelationBlock::centralRelationsDB);
    closeDBStream(PropertyLink::propertiesDB);
    closeDBStream(MetaPropertyLink::metaPropertiesDB);
    closeDBStream(PropertyEdgeLink::edgePropertiesDB);
    closeDBStream(MetaPropertyEdgeLink::metaEdgePropertiesDB);

    std::string instanceDataFolderLocation =
        Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    std::string dbPrefix = instanceDataFolderLocation + "/g" + std::to_string(gc.graphID) + 
                          "_p" + std::to_string(gc.partitionID);

    openDatabaseFiles(dbPrefix);

    currentPartitionID = gc.partitionID;
    currentGraphID = gc.graphID;
}

// Helper to add filtered results to buffer
static void addFilteredResults(const std::vector<std::vector<std::string>>& results, SharedBuffer& buffer) {
    for (const auto& chunkResults : results) {
        for (const auto& item : chunkResults) {
            buffer.add(item);
        }
    }
}

// Helper to process batch sequentially
static void processSequentially(const std::vector<std::string>& batch, 
                                FilterHelper& filterHelper,
                                SharedBuffer& buffer) {
    for (const auto& item : batch) {
        if (filterHelper.evaluate(item)) {
            buffer.add(item);
        }
    }
}

// Helper to process batch in parallel
static bool tryProcessInParallel(const std::vector<std::string>& batch,
                                 FilterHelper& filterHelper,
                                 SharedBuffer& buffer,
                                 IntraPartitionParallelExecutor* parallelExecutor) {
    auto processor = [&filterHelper, &batch](const WorkChunk& chunk) {
        std::vector<std::string> localResults;
        for (long i = chunk.startIndex - 1; i < chunk.endIndex && i < (long)batch.size(); ++i) {
            if (filterHelper.evaluate(batch[i])) {
                localResults.push_back(batch[i]);
            }
        }
        return localResults;
    };

    try {
        auto results = parallelExecutor->processInParallel<decltype(processor), std::vector<std::string>>(
            static_cast<long>(batch.size()), processor);
        addFilteredResults(results, buffer);
        return true;
    } catch (const std::runtime_error& e) {
        execution_logger.warn("Parallel filter failed, using sequential: " + std::string(e.what()));
        return false;
    }
}

// Helper function to process batch filtering (reduce nesting complexity)
static void processBatch(const std::vector<std::string>& batch, 
                        FilterHelper& filterHelper,
                        SharedBuffer& buffer,
                        IntraPartitionParallelExecutor* parallelExecutor) {
    bool useParallel = parallelExecutor && batch.size() > 500;
    
    if (useParallel && tryProcessInParallel(batch, filterHelper, buffer, parallelExecutor)) {
        return;
    }
    
    processSequentially(batch, filterHelper, buffer);
}

// Helper function to process a single relationship chunk
static std::vector<std::string> processRelationshipChunk(
    const WorkChunk& chunk, 
    const std::string& jsonPlan, 
    const GraphConfig& gc, 
    const std::string& masterIP,
    long maxRelations) {
    
    initializeThreadLocalDBs(gc);
    json queryJson = json::parse(jsonPlan);
    std::vector<std::string> results;

    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = (graphDirection == "TRUE");
    bool isDirectionRight = (queryJson["direction"] == "right");

    for (long i = chunk.startIndex; i <= chunk.endIndex && i < maxRelations; ++i) {
        std::unique_ptr<RelationBlock> relation(RelationBlock::getLocalRelation(i * RelationBlock::BLOCK_SIZE));
        if (relation->getLocalRelationshipType() != queryJson["relType"]) {
            continue;
        }
        
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        json startNodeData = extractNodeDataAndCleanup(startNode);
        json destNodeData = extractNodeDataAndCleanup(destNode);
        json relationData = extractRelationDataAndCleanup(relation.get());

        json directionData;
        string start = queryJson["sourceVariable"];
        string dest = queryJson["destVariable"];
        string rel = queryJson["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        results.push_back(directionData.dump());
    }
    return results;
}

OperatorExecutor::OperatorExecutor(GraphConfig gc, std::string queryPlan, std::string masterIP):
    queryPlan(queryPlan), gc(gc), masterIP(masterIP) {
    this->query = json::parse(queryPlan);

    // Initialize parallel executor safely
    if (!parallelExecutor) {
        try {
            parallelExecutor = std::make_unique<IntraPartitionParallelExecutor>();
        } catch (const std::bad_alloc& e) {
            execution_logger.error("Failed to allocate parallel executor: " + std::string(e.what()));
            parallelExecutor = nullptr;
        } catch (const std::runtime_error& e) {
            execution_logger.error("Failed to initialize parallel executor: " + std::string(e.what()));
            parallelExecutor = nullptr;
        }
    }
};

void OperatorExecutor::initializeMethodMap() {
    methodMap["AllNodeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.AllNodeScan(buffer, jsonPlan, gc);
    };

    methodMap["ProduceResult"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.ProduceResult(buffer, jsonPlan, gc);
    };

    methodMap["Filter"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.Filter(buffer, jsonPlan, gc);
    };

    methodMap["ExpandAll"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.ExpandAll(buffer, jsonPlan, gc);
    };

    methodMap["UndirectedRelationshipTypeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.UndirectedRelationshipTypeScan(buffer, jsonPlan, gc);
    };

    methodMap["UndirectedAllRelationshipScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.UndirectedAllRelationshipScan(buffer, jsonPlan, gc);
    };

    methodMap["DirectedRelationshipTypeScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                     std::string jsonPlan, GraphConfig gc) {
        executor.DirectedRelationshipTypeScan(buffer, jsonPlan, gc);
    };

    methodMap["DirectedAllRelationshipScan"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                    std::string jsonPlan, GraphConfig gc) {
        executor.DirectedAllRelationshipScan(buffer, jsonPlan, gc);
    };

    methodMap["NodeByIdSeek"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                                    std::string jsonPlan, GraphConfig gc) {
        executor.NodeByIdSeek(buffer, jsonPlan, gc);
    };

    methodMap["Projection"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
            std::string jsonPlan, GraphConfig gc) {
        executor.Projection(buffer, jsonPlan, gc);
    };

    methodMap["AggregationFunction"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                   std::string jsonPlan, GraphConfig gc) {
        executor.AggregationFunction(buffer, jsonPlan, gc);
    };

    methodMap["Create"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                    std::string jsonPlan, GraphConfig gc) {
        executor.Create(buffer, jsonPlan, gc);
    };

    methodMap["CartesianProduct"] = [](OperatorExecutor &executor, SharedBuffer &buffer,
                                     std::string jsonPlan, GraphConfig gc) {
        executor.CartesianProduct(buffer, jsonPlan, gc);
    };

    methodMap["Distinct"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.Distinct(buffer, jsonPlan, gc);
    };

    methodMap["OrderBy"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
        executor.OrderBy(buffer, jsonPlan, gc);
    };

    methodMap["NodeScanByLabel"] = [](OperatorExecutor &executor, SharedBuffer &buffer, std::string jsonPlan,
            GraphConfig gc) {
        executor.NodeScanByLabel(buffer, jsonPlan, gc);
    };
}

void OperatorExecutor::AllNodeScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    // Use parallel processing for datasets with 1000+ nodes
    if (size_t nodeCount = nodeManager.nodeIndex.size(); parallelExecutor && nodeCount > 1000 && parallelExecutor->shouldUseParallelProcessing(nodeCount)) {
        try {
            AllNodeScanParallel(buffer, jsonPlan, gc);
            return;
        } catch (const std::runtime_error& e) {
            execution_logger.warn("Parallel AllNodeScan failed, falling back to sequential: " + std::string(e.what()));
        }
    }

    // Use sequential processing for small datasets (original code)
    for (auto it : nodeManager.nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager.get(nodeId);
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID)) {
            nodeData["partitionID"] = value;
            std::map<std::string, char*> properties = node->getAllProperties();
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            for (auto& [key, value] : properties) {
                delete[] value;  // Free each allocated char* array
            }
            properties.clear();

            json data;
            string variable = query["variables"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::NodeScanByLabel(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    size_t nodeCount = nodeManager.nodeIndex.size();

    // Use parallel processing for datasets with 1000+ nodes
    if (parallelExecutor && nodeCount > 1000 && parallelExecutor->shouldUseParallelProcessing(nodeCount)) {
        try {
            NodeScanByLabelParallel(buffer, jsonPlan, gc);
            return;
        } catch (const std::exception& e) {
            execution_logger.warn("Parallel processing failed, falling back to sequential: " + std::string(e.what()));
        }
    }

    // Use sequential processing for small datasets (original code)
    for (auto it : nodeManager.nodeIndex) {
        json nodeData;
        auto nodeId = it.first;
        NodeBlock *node = nodeManager.get(nodeId);
        string label = node->getLabel();
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID) && label == query["Label"]) {
            nodeData["partitionID"] = value;
            std::map<std::string, char*> properties = node->getAllProperties();
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            for (auto& [key, value] : properties) {
                delete[] value;  // Free each allocated char* array
            }
            properties.clear();

            json data;
            string variable = query["variable"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::ProduceResult(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(raw);
            result.join();
            break;
        }
        std::vector<std::string> values = query["variable"].get<std::vector<std::string>>();
        json data;
        json rawObj = json::parse(raw);
        for (auto value : values) {
            data[value] = rawObj[value];
        }
        buffer.add(data.dump());
    }
}

void OperatorExecutor::Filter(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    auto condition = query["condition"];
    FilterHelper filterHelper(condition.dump());

    std::vector<std::string> batch;
    batch.reserve(100);

    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            // Process remaining batch using helper function
            IntraPartitionParallelExecutor* executor = parallelExecutor.get();
            processBatch(batch, filterHelper, buffer, executor);
            buffer.add(raw);
            result.join();
            break;
        }

        batch.push_back(raw);
        if (batch.size() >= 100) {
            // Process and flush batch using helper function
            IntraPartitionParallelExecutor* executor = parallelExecutor.get();
            processBatch(batch, filterHelper, buffer, executor);
            batch.clear();
        }
    }
}

void OperatorExecutor::UndirectedRelationshipTypeScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                   "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string direction = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (direction == "TRUE") {
        isDirected = true;
    }
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        if (relation->getLocalRelationshipType() != query["relType"]) {
            continue;
        }
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i * RelationBlock::CENTRAL_BLOCK_SIZE);
        if (relation->getCentralRelationshipType() != query["relType"]) {
            continue;
        }

        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);

        if (startPid != to_string(gc.partitionID)) {
            continue;
        }
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;

        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::UndirectedAllRelationshipScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                    "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string direction = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (direction == "TRUE") {
        isDirected = true;
    }
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();


        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i*RelationBlock::CENTRAL_BLOCK_SIZE);
        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json rightDirectionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        rightDirectionData[start] = startNodeData;
        rightDirectionData[dest] = destNodeData;
        rightDirectionData[rel] = relationData;
        buffer.add(rightDirectionData.dump());

        if (!isDirected) {
            json leftDirectionData;
            leftDirectionData[start] = destNodeData;
            leftDirectionData[dest] = startNodeData;
            leftDirectionData[rel] = relationData;
            buffer.add(leftDirectionData.dump());
        }
        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::DirectedRelationshipTypeScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    string direction = query["direction"];
    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                   "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (graphDirection == "TRUE") {
        isDirected = true;
    }
    bool isDirectionRight = query["direction"] == "right";
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i*RelationBlock::BLOCK_SIZE);
        if (relation->getLocalRelationshipType() != query["relType"]) {
            continue;
        }
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i*RelationBlock::CENTRAL_BLOCK_SIZE);
        if (relation->getCentralRelationshipType() != query["relType"]) {
            continue;
        }

        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());

        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::DirectedAllRelationshipScan(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);

    // Check threshold for parallel execution
    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = nodeManager.dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;

    if (parallelExecutor && localRelationCount > 100000 &&
        parallelExecutor->shouldUseParallelProcessing(localRelationCount)) {
        try {
            DirectedAllRelationshipScanParallel(buffer, jsonPlan, gc);
            return;
        } catch (const std::runtime_error& e) {
            execution_logger.warn("Parallel relationship scan failed, falling back to sequential: " + std::string(e.what()));
        }
    }

    string direction = query["direction"];
    long centralRelationCount = nodeManager.dbSize(dbPrefix +
                                                   "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (graphDirection == "TRUE") {
        isDirected = true;
    }
    bool isDirectionRight = query["direction"] == "right";
    int count = 1;
    for (long i = 1; i < localRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getLocalRelation(i * RelationBlock::BLOCK_SIZE);
        if (relation->getLocalRelationshipType() != query["relType"]) {
            continue;
        }
        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());
        count++;
    }

    int central = 1;
    for (long i = 1; i < centralRelationCount; i++) {
        json startNodeData;
        json destNodeData;
        json relationData;
        RelationBlock* relation = RelationBlock::getCentralRelation(i * RelationBlock::CENTRAL_BLOCK_SIZE);
        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(gc.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        startNodeData["partitionID"] = startPid;
        std::map<std::string, char*> startProperties = startNode->getAllProperties();
        for (auto property : startProperties) {
            startNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : startProperties) {
            delete[] value;  // Free each allocated char* array
        }
        startProperties.clear();

        std::string destPid(destNode->getMetaPropertyHead()->value);
        destNodeData["partitionID"] = destPid;
        std::map<std::string, char*> destProperties = destNode->getAllProperties();
        for (auto property : destProperties) {
            destNodeData[property.first] = property.second;
        }
        for (auto& [key, value] : destProperties) {
            delete[] value;  // Free each allocated char* array
        }
        destProperties.clear();

        std::map<std::string, char*> relProperties = relation->getAllProperties();
        for (auto property : relProperties) {
            relationData[property.first] = property.second;
        }
        for (auto& [key, value] : relProperties) {
            delete[] value;  // Free each allocated char* array
        }
        relProperties.clear();

        json directionData;
        string start = query["sourceVariable"];
        string dest = query["destVariable"];
        string rel = query["relVariable"];

        if (isDirectionRight) {
            directionData[start] = startNodeData;
            directionData[dest] = destNodeData;
        } else if (!isDirected) {
            directionData[start] = destNodeData;
            directionData[dest] = startNodeData;
        }
        directionData[rel] = relationData;
        buffer.add(directionData.dump());

        central++;
    }
    buffer.add("-1");
}

void OperatorExecutor::NodeByIdSeek(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    NodeManager nodeManager(gc);
    NodeBlock* node = nodeManager.get(query["id"]);
    if (node) {
        json nodeData;
        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(gc.partitionID)) {
            std::map<std::string, char*> properties = node->getAllProperties();
            nodeData["partitionID"] = value;
            for (auto property : properties) {
                nodeData[property.first] = property.second;
            }
            json data;
            string variable = query["variable"];
            data[variable] = nodeData;
            buffer.add(data.dump());
        }
    }
    buffer.add("-1");
}

void OperatorExecutor::ExpandAll(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    string sourceVariable = query["sourceVariable"];
    string destVariable = query["destVariable"];
    string relVariable = query["relVariable"];

    string relType = "";
    if (query.contains("relType")) {
        relType = query["relType"];
    }
    string graphDirection = Utils::getGraphDirection(to_string(gc.graphID), masterIP);
    bool isDirected = false;
    if (graphDirection == "TRUE") {
        isDirected = true;
    }
    bool isDirectionRight = false;
    if (query.contains("direction")) {
        isDirectionRight = query["direction"] == "right";
    }

    string queryString;

    NodeManager nodeManager(gc);

    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(raw);
            result.join();
            break;
        }
        json rawObj = json::parse(raw);
        string nodeId = rawObj[sourceVariable]["id"];
        if (rawObj[sourceVariable]["partitionID"] == to_string(gc.partitionID)) {
            NodeBlock* node = nodeManager.get(nodeId);
            if (node) {
                RelationBlock *relation = RelationBlock::getLocalRelation(node->edgeRef);
                if (relation) {
                    RelationBlock *nextRelation = relation;
                    bool isSource;
                    while (nextRelation) {
                        if (to_string(nextRelation->source.nodeId) == nodeId) {
                            isSource = true;
                        } else {
                            isSource = false;
                        }

                        json relationData;
                        json destNodeData;
                        std::map<std::string, char*> relProperties = nextRelation->getAllProperties();
                        for (auto property : relProperties) {
                            relationData[property.first] = property.second;
                        }

                        if (relType != "" && relationData["relationship"] != relType) {
                            if (isSource) {
                                nextRelation = nextRelation->nextLocalSource();
                            } else {
                                nextRelation = nextRelation->nextLocalDestination();
                            }
                            continue;
                        }
                        if (isDirected && !isSource) {
                            nextRelation = nextRelation->nextLocalDestination();
                            continue;
                        }
                        for (auto& [key, value] : relProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        relProperties.clear();
                        NodeBlock *destNode;
                        if (isSource) {
                            destNode = nextRelation->getDestination();
                        } else {
                            destNode = nextRelation->getSource();
                        }
                        std::string value(destNode->getMetaPropertyHead()->value);
                        destNodeData["partitionID"] = value;
                        std::map<std::string, char*> destProperties = destNode->getAllProperties();
                        for (auto property : destProperties) {
                            destNodeData[property.first] = property.second;
                        }
                        for (auto& [key, value] : destProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        destProperties.clear();
                        rawObj[relVariable] = relationData;
                        rawObj[destVariable] = destNodeData;

                        buffer.add(rawObj.dump());
                        if (isSource) {
                            nextRelation = nextRelation->nextLocalSource();
                        } else {
                            nextRelation = nextRelation->nextLocalDestination();
                        }
                    }
                }

                relation = RelationBlock::getCentralRelation(node->centralEdgeRef);
                if (relation) {
                    RelationBlock *nextRelation = relation;
                    bool isSource;

                    while (nextRelation) {
                        if (to_string(nextRelation->source.nodeId) == nodeId) {
                            isSource = true;
                        } else {
                            isSource = false;
                        }

                        json relationData;
                        json destNodeData;
                        std::map<std::string, char*> relProperties = nextRelation->getAllProperties();
                        for (auto property : relProperties) {
                            relationData[property.first] = property.second;
                        }

                        if (relType != "" && relationData["relationship"] != relType) {
                            if (isSource) {
                                nextRelation = nextRelation->nextCentralSource();
                            } else {
                                nextRelation = nextRelation->nextCentralDestination();
                            }
                            continue;
                        }

                        if (isDirected && !isSource) {
                            nextRelation = nextRelation->nextCentralDestination();
                            continue;
                        }

                        for (auto& [key, value] : relProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        relProperties.clear();
                        NodeBlock *destNode;
                        if (isSource) {
                            destNode = nextRelation->getDestination();
                        } else {
                            destNode = nextRelation->getSource();
                        }
                        std::string value(destNode->getMetaPropertyHead()->value);
                        destNodeData["partitionID"] = value;
                        std::map<std::string, char*> destProperties = destNode->getAllProperties();
                        for (auto property : destProperties) {
                            destNodeData[property.first] = property.second;
                        }
                        for (auto& [key, value] : destProperties) {
                            delete[] value;  // Free each allocated char* array
                        }
                        destProperties.clear();
                        rawObj[relVariable] = relationData;
                        rawObj[destVariable] = destNodeData;
                        buffer.add(rawObj.dump());
                        if (isSource) {
                            nextRelation = nextRelation->nextCentralSource();
                        } else {
                            nextRelation = nextRelation->nextCentralDestination();
                        }
                    }
                }
            }
        } else {
            if (query.contains("relType")) {
                queryString = ExpandAllHelper::generateSubQuery(query["sourceVariable"],
                                                                query["destVariable"],
                                                                query["relVariable"],
                                                                isDirected,
                                                                rawObj[sourceVariable]["id"],
                                                                query["relType"]);
            } else {
                queryString = ExpandAllHelper::generateSubQuery(query["sourceVariable"],
                                                                query["destVariable"],
                                                                query["relVariable"],
                                                                isDirected,
                                                                rawObj[sourceVariable]["id"]);
            }
            string queryPlan = ExpandAllHelper::generateSubQueryPlan(queryString);
            SharedBuffer temp(INTER_OPERATOR_BUFFER_SIZE);
            std::thread t(Utils::sendDataFromWorkerToWorker,
                          masterIP,
                          gc.graphID,
                          rawObj[sourceVariable]["partitionID"],
                          std::ref(queryPlan),
                          std::ref(temp));
            while (true) {
                string tmpRaw = temp.get();
                if (tmpRaw == "-1") {
                    t.join();
                    break;
                }
                json tmpData = json::parse(tmpRaw);
                rawObj[relVariable] = tmpData[relVariable];
                rawObj[destVariable] = tmpData[destVariable];
                buffer.add(rawObj.dump());
            }
        }
    }
}

void OperatorExecutor::AggregationFunction(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];
    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    AverageAggregationHelper* averageAggregationHelper =
            new AverageAggregationHelper(query["variable"], query["property"]);
    while (true) {
        string raw = sharedBuffer.get();
        if (raw == "-1") {
            buffer.add(averageAggregationHelper->getFinalResult());
            buffer.add(raw);
            result.join();
            break;
        }
        averageAggregationHelper->insertData(raw);
    }
}

void OperatorExecutor::Projection(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];

    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    if (!query.contains("project") || !query["project"].is_array()) {
        while (true) {
            string raw = sharedBuffer.get();
            buffer.add(raw);
            if (raw == "-1") {
                result.join();
                break;
            }
        }
    } else {
        while (true) {
            string raw = sharedBuffer.get();
            if (raw == "-1") {
                buffer.add(raw);
                result.join();
                break;
            }
            auto data = json::parse(raw);
            for (const auto& operand : query["project"]) {
                for (auto& [key, value] : data.items()) {
                    if (operand.contains("variable") && key == operand["variable"]) {
                        string assign = operand["assign"];
                        string property = operand["property"];
                        data[assign] = value[property];
                    } else if (operand.contains("functionName") && key == operand["functionName"]) {
                        string assign = operand["assign"];
                        data["variable"] = assign;
                        data[assign] = value;
                    }
                }
            }
            buffer.add(data.dump());
        }
    }
}

void OperatorExecutor::Create(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    string partitionAlgo = Utils::getPartitionAlgorithm(to_string(gc.graphID), masterIP);
    CreateHelper createHelper(query["elements"], partitionAlgo, gc, masterIP);
    if (query.contains("NextOperator")) {
        std::string nextOpt = query["NextOperator"];
        json next = json::parse(nextOpt);
        auto method = OperatorExecutor::methodMap[next["Operator"]];
        // Launch the method in a new thread
        std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
        while (true) {
            string raw = sharedBuffer.get();
            if (raw == "-1") {
                buffer.add(raw);
                result.join();
                break;
            }
            createHelper.insertFromData(raw, std::ref(buffer));
        }
    } else {
        createHelper.insertWithoutData(std::ref(buffer));
        buffer.add("-1");
    }
}

void OperatorExecutor::CartesianProduct(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer left(INTER_OPERATOR_BUFFER_SIZE);
    SharedBuffer right(INTER_OPERATOR_BUFFER_SIZE);
    std::string leftOpt = query["left"];
    std::string rightOpt = query["right"];
    json leftJson = json::parse(leftOpt);
    json rightJson = json::parse(rightOpt);
    auto leftMethod = OperatorExecutor::methodMap[leftJson["Operator"]];
    auto rightMethod = OperatorExecutor::methodMap[rightJson["Operator"]];
    // Launch the method in a new thread
    std::thread leftThread(leftMethod, std::ref(*this), std::ref(left), query["left"], gc);
    while (true) {
        string leftRaw = left.get();
        if (leftRaw == "-1") {
            buffer.add(leftRaw);
            leftThread.join();
            break;
        }

        string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
        int numberOfPartitions = std::stoi(partitionCount);
        std::vector<std::thread> workerThreads;

        for (int i = 0; i < numberOfPartitions; i++) {
            if (i == gc.partitionID) {
                continue;
            }
            workerThreads.emplace_back(
                    Utils::sendDataFromWorkerToWorker,
                    masterIP,
                    gc.graphID,
                    to_string(i),
                    query["right"],
                    std::ref(right));
        }

        std::thread rightThread(rightMethod, std::ref(*this), std::ref(right), query["right"], gc);
        int count = 0;
        while (true) {
            string rightRaw = right.get();
            if (rightRaw == "-1") {
                count++;
                if (count == numberOfPartitions) {
                    buffer.add("-1");
                    rightThread.join();
                    for (auto& t : workerThreads) {
                        if (t.joinable()) {
                            t.join();
                        }
                    }
                }
                continue;
            }



            json leftData = json::parse(leftRaw);
            json rightData = json::parse(rightRaw);

            for (auto& [key, value] : rightData.items()) {
                leftData[key] = value;
            }
            buffer.add(leftData.dump());
        }
    }
}

void OperatorExecutor::Distinct(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];

    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);
    if (!query.contains("project") || !query["project"].is_array()) {
        while (true) {
            string raw = sharedBuffer.get();
            buffer.add(raw);
            if (raw == "-1") {
                result.join();
                break;
            }
        }
    } else {
        while (true) {
            string raw = sharedBuffer.get();
            if (raw == "-1") {
                buffer.add(raw);
                result.join();
                break;
            }
            auto data = json::parse(raw);
            for (const auto& operand : query["project"]) {
                for (auto& [key, value] : data.items()) {
                    if (operand.contains("variable") && key == operand["variable"]) {
                        string assign = operand["assign"];
                        string property = operand["property"];
                        data[assign] = value[property];
                    } else if (operand.contains("functionName") && key == operand["functionName"]) {
                        string assign = operand["assign"];
                        data["variable"] = assign;
                        data[assign] = value;
                    }
                }
            }
            buffer.add(data.dump());
        }
    }
}

struct Row {
    json data;
    std::string jsonStr;
    std::string sortKey;
    bool isAsc;
    size_t sourceFileIndex;

    Row(const std::string& str, const std::string& key, bool asc, size_t srcIdx = 0)
            : jsonStr(str), sortKey(key), isAsc(asc), sourceFileIndex(srcIdx) {
        data = json::parse(str);
    }

    size_t memoryUsage() const {
        // Rough estimate: strings + json overhead
        return sizeof(Row)
               + jsonStr.size()
               + sortKey.size()
               + sizeof(bool)
               + sizeof(size_t);
    }

    json getNestedValue(const json& obj, const std::string& key) const {
        json current = obj;
        std::stringstream ss(key);
        std::string token;

        while (std::getline(ss, token, '.')) {
            if (!current.contains(token)) {
                return nullptr;
            }
            current = current[token];
        }
        return current;
    }

    bool operator<(const Row& other) const {
        json val1 = getNestedValue(data, sortKey);
        json val2 = getNestedValue(other.data, sortKey);

        bool result;
        if (val1.is_number_integer() && val2.is_number_integer()) {
            result = val1.get<int>() > val2.get<int>();
        } else if (val1.is_string() && val2.is_string()) {
            result = val1.get<std::string>() > val2.get<std::string>();
        } else {
            result = val1.dump() > val2.dump();
        }
        return isAsc ? result : !result;  // Flip for DESC
    }
};

// Define comparator for ASC
struct RowAscComparator {
    bool operator()(const Row& a, const Row& b) const {
        return a < b;  // Uses your Row::operator<
    }
};

// Define comparator for DESC
struct RowDescComparator {
    bool operator()(const Row& a, const Row& b) const {
        return b < a;  // Reverses your Row::operator<
    }
};

struct DynamicComparator {
    bool isAsc;
    bool operator()(const Row& a, const Row& b) const {
        return isAsc ? RowAscComparator {}(a, b) : RowDescComparator {}(a, b);
    }
};

std::string generateUniqueFilename() {
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();

    std::stringstream uuid;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    for (int i = 0; i < 8; i++) {
        uuid << std::hex << dis(gen);
    }

    std::ostringstream filename;
    filename << "run_" << millis << "_" << uuid.str() << ".dat";
    return filename.str();
}

// Flush heap into a run file
void flushHeapToRunFile(
        std::priority_queue<Row, std::vector<Row>, DynamicComparator> &pq,
        std::vector<std::string> &runFiles
) {
    if (pq.empty()) return;

    std::string workerPath = Utils::getJasmineGraphProperty("org.jasminegraph.worker.path");
    std::string filename = workerPath + generateUniqueFilename();
    std::ofstream outFile(filename, std::ios::out | std::ios::binary);

    if (!outFile) {
        execution_logger.error("OrderBy: Failed to create run file: " + filename);
        return;
    }

    execution_logger.info("OrderBy: Flushing heap to run file: " + filename);

    while (!pq.empty()) {
        const Row &row = pq.top();
        outFile << row.jsonStr << "\n";
        pq.pop();
    }

    outFile.close();
    runFiles.push_back(filename);
    execution_logger.info("OrderBy: Finished flushing heap to run file: " + filename);
}

void OperatorExecutor::OrderBy(SharedBuffer &buffer, std::string jsonPlan, GraphConfig gc) {
    json query = json::parse(jsonPlan);
    SharedBuffer sharedBuffer(INTER_OPERATOR_BUFFER_SIZE);
    std::string nextOpt = query["NextOperator"];
    json next = json::parse(nextOpt);
    auto method = OperatorExecutor::methodMap[next["Operator"]];

    // Launch the method in a new thread
    std::thread result(method, std::ref(*this), std::ref(sharedBuffer), query["NextOperator"], gc);

    std::string sortKey = query["variable"];
    std::string order = query["order"];
    const size_t MAX_MEMORY_BYTES = 1024 * 1024;  // 1 MB
    size_t heapMemoryUsage = 0;
    bool isAsc = (order == "ASC");

    auto heap = std::priority_queue<Row, std::vector<Row>, DynamicComparator>(
            DynamicComparator{isAsc});
    std::vector<std::string> runFiles;  // store generated run file paths
    size_t runCounter = 0;

    auto localKWayMergeToBuffer = [&](
            const std::vector<std::string> &runFiles,
                                      SharedBuffer &buffer,
                                      bool isAsc,
                                      const std::string &sortKey) {
        // Comparator for heap
        auto cmp = [&](const Row &a, const Row &b) {
            json val1 = a.getNestedValue(a.data, sortKey);
            json val2 = b.getNestedValue(b.data, sortKey);

            bool result;
            if (val1.is_number_integer() && val2.is_number_integer()) {
                result = val1.get<int>() > val2.get<int>();
            } else if (val1.is_string() && val2.is_string()) {
                result = val1.get<std::string>() > val2.get<std::string>();
            } else {
                result = val1.dump() > val2.dump();
            }
            return isAsc ? result : !result;
        };

        std::priority_queue<Row, std::vector<Row>, decltype(cmp)> pq(cmp);
        std::vector<std::ifstream> files(runFiles.size());

        // push first row from each run
        for (size_t i = 0; i < runFiles.size(); ++i) {
            files[i].open(runFiles[i]);
            std::string line;
            if (std::getline(files[i], line)) {
                pq.emplace(line, sortKey, isAsc, i);
            }
        }

        // Merge directly to buffer
        while (!pq.empty()) {
            Row top = pq.top();
            pq.pop();
            buffer.add(top.jsonStr);

            size_t fileIndex = top.sourceFileIndex;
            std::string line;
            if (std::getline(files[fileIndex], line)) {
                pq.emplace(line, sortKey, isAsc, fileIndex);
            }
        }
        buffer.add("-1");  // close flag
        for (auto &f : files) {
            f.close();
        }
        for (const auto &file : runFiles) {
            if (std::remove(file.c_str()) != 0) {
                execution_logger.warn("OrderBy: Failed to delete temporary run file: " + file);
            } else {
                execution_logger.info("OrderBy: Deleted temporary run file: " + file);
            }
        }
    };

    while (true) {
        std::string jsonStr = sharedBuffer.get();
        if (jsonStr == "-1") {
            // Final flush: If heap still has data, either to run file (if other runs exist) or directly to buffer
            if (!runFiles.empty()) {
                flushHeapToRunFile(heap, runFiles);
                localKWayMergeToBuffer(runFiles, buffer, isAsc, sortKey);
            } else {
                while (!heap.empty()) {
                    buffer.add(heap.top().jsonStr);
                    heap.pop();
                }
                buffer.add(jsonStr);  // -1 close flag
                result.join();
                break;
            }
        }

        try {
            Row row(jsonStr, sortKey, isAsc);
            json nestedVal = row.getNestedValue(row.data, sortKey);

            if (nestedVal.is_null()) {
                execution_logger.warn("OrderBy: Sort key '" + sortKey + "' not found in row: " + jsonStr);
                continue;
            }
            heapMemoryUsage += row.memoryUsage();
            heap.push(row);
            if (heapMemoryUsage > MAX_MEMORY_BYTES) {
                execution_logger.info("OrderBy: Heap memory exceeded limit (" + std::to_string(heapMemoryUsage)
                + " bytes), flushing to runfile");
                flushHeapToRunFile(heap, runFiles);
                heapMemoryUsage = 0;  // reset after flush
            }
        } catch (const std::exception& e) {
            execution_logger.error("OrderBy: Error parsing row: " + std::string(e.what()));
        }
    }
}

// Helper function for parallel node scanning (to avoid large lambda)
static std::vector<std::string> processNodeScanChunk(
    const WorkChunk& chunk,
    const std::vector<std::pair<std::string, unsigned int>>& nodeIndices,
    const std::string& variable,
    const GraphConfig& graphConfig) {
    
    initializeThreadLocalDBs(graphConfig);  // Initialize DBs for this thread

    std::vector<std::string> results;
    results.reserve(chunk.endIndex - chunk.startIndex + 1);

    long start = std::max(0L, chunk.startIndex - 1);
    long end = std::min<long>(static_cast<long>(nodeIndices.size()) - 1, chunk.endIndex - 1);

    for (long i = start; i <= end; ++i) {
        const auto& pair = nodeIndices[static_cast<size_t>(i)];
        std::string nodeId = pair.first;
        unsigned int addressIndex = pair.second;

        std::unique_ptr<NodeBlock> node(NodeManager::get(addressIndex, nodeId));
        if (node == nullptr) continue;

        std::string value(node->getMetaPropertyHead()->value);
        if (value == to_string(graphConfig.partitionID)) {
            json nodeData;
            nodeData["partitionID"] = value;
            std::map<std::string, char*> rawProps = node->getAllProperties();
            
            // Use RAII to automatically cleanup allocated memory
            std::vector<std::unique_ptr<char[]>> cleanup;
            cleanup.reserve(rawProps.size());
            
            for (const auto& [key, val] : rawProps) {
                nodeData[key] = std::string(val);
                cleanup.emplace_back(val);
            }

            json data;
            data[variable] = nodeData;
            results.push_back(data.dump());
        }
    }
    return results;
}

// Helper function to process node scan by label chunk
static std::vector<std::string> processNodeByLabelChunk(
    const WorkChunk& chunk,
    const std::vector<std::pair<std::string, unsigned int>>& nodeIndices,
    const std::string& targetLabel,
    const GraphConfig& graphConfig) {
    
    initializeThreadLocalDBs(graphConfig);

    std::vector<std::string> results;
    results.reserve(chunk.endIndex - chunk.startIndex + 1);

    long start = std::max(0L, chunk.startIndex - 1);
    long end = std::min<long>(static_cast<long>(nodeIndices.size()) - 1, chunk.endIndex - 1);

    for (long i = start; i <= end; ++i) {
        const auto& [nodeId, addressIndex] = nodeIndices[static_cast<size_t>(i)];

        std::unique_ptr<NodeBlock> node(NodeManager::get(addressIndex, nodeId));
        if (node == nullptr) continue;

        string label = node->getLabel();
        std::string partitionValue(node->getMetaPropertyHead()->value);

        if (partitionValue == to_string(graphConfig.partitionID) && label == targetLabel) {
            json nodeData;
            nodeData["partitionID"] = partitionValue;
            std::map<std::string, char*> rawProps = node->getAllProperties();
            
            // Use RAII to automatically cleanup allocated memory
            std::vector<std::unique_ptr<char[]>> cleanup;
            cleanup.reserve(rawProps.size());
            
            for (const auto& [key, val] : rawProps) {
                nodeData[key] = std::string(val);
                cleanup.emplace_back(val);
            }

            results.push_back(nodeData.dump());
        }
    }
    return results;
}

void OperatorExecutor::AllNodeScanParallel(SharedBuffer &buffer, std::string jsonPlan, GraphConfig graphConfig) {
    // Use thread pool executor with deterministic merge
    try {
        json queryJson = json::parse(jsonPlan);
        NodeManager nodeManager(graphConfig);
        string variable = queryJson["variables"].get<std::string>();

        // Collect node indices (ID, AddressIndex)
        std::vector<std::pair<std::string, unsigned int>> nodeIndices;
        nodeIndices.reserve(nodeManager.nodeIndex.size());

        for (const auto& [nodeId, addressIdx] : nodeManager.nodeIndex) {
            nodeIndices.emplace_back(nodeId, addressIdx);
        }

        // Define processor using named function
        auto processor = [&nodeIndices, &variable, graphConfig](const WorkChunk& chunk) {
            return processNodeScanChunk(chunk, nodeIndices, variable, graphConfig);
        };

        // Execute using the thread pool with adaptive chunking
        std::vector<std::vector<std::string>> chunkResults =
            parallelExecutor->processInParallel<decltype(processor), std::vector<std::string>>(
                static_cast<long>(nodeIndices.size()),
                processor);

        // Deterministic merge in chunk order and emit
        for (const auto& chunkVec : chunkResults) {
            for (const auto& row : chunkVec) {
                buffer.add(row);
            }
        }
        buffer.add("-1");
    } catch (const std::runtime_error& e) {
        execution_logger.error("AllNodeScanParallel failed: " + std::string(e.what()));
        buffer.add("-1");
    } catch (const json::exception& e) {
        execution_logger.error("JSON parsing failed in AllNodeScanParallel: " + std::string(e.what()));
        buffer.add("-1");
    }
}

void OperatorExecutor::NodeScanByLabelParallel(SharedBuffer &buffer, std::string jsonPlan, const GraphConfig& graphConfig) {
    try {
        json queryParsed = json::parse(jsonPlan);
        NodeManager nodeManager(graphConfig);
        string targetLabel = queryParsed["Label"].get<std::string>();

        // Collect node indices (ID, AddressIndex)
        std::vector<std::pair<std::string, unsigned int>> nodeIndices;
        nodeIndices.reserve(nodeManager.nodeIndex.size());

        for (const auto& [nodeId, addressIdx] : nodeManager.nodeIndex) {
            nodeIndices.emplace_back(nodeId, addressIdx);
        }

        auto processor = [&nodeIndices, &targetLabel, graphConfig](const WorkChunk& chunk) {
            return processNodeByLabelChunk(chunk, nodeIndices, targetLabel, graphConfig);
        };

        std::vector<std::vector<std::string>> chunkResults =
            parallelExecutor->processInParallel<decltype(processor), std::vector<std::string>>(
                static_cast<long>(nodeIndices.size()), processor);

        for (const auto& chunkVec : chunkResults) {
            for (const auto& row : chunkVec) {
                buffer.add(row);
            }
        }
        buffer.add("-1");
    } catch (const std::runtime_error& e) {
        execution_logger.error("NodeScanByLabelParallel failed: " + std::string(e.what()));
        buffer.add("-1");
    } catch (const json::exception& e) {
        execution_logger.error("JSON parsing failed in NodeScanByLabelParallel: " + std::string(e.what()));
        buffer.add("-1");
    }
}

void OperatorExecutor::DirectedAllRelationshipScanParallel(SharedBuffer &buffer, std::string jsonPlan, GraphConfig graphConfig) {
    json queryParsed = json::parse(jsonPlan);
    NodeManager nodeManager(graphConfig);
    const std::string& dbPrefix = nodeManager.getDbPrefix();
    long localRelationCount = NodeManager::dbSize(dbPrefix + "_relations.db") / RelationBlock::BLOCK_SIZE;

    if (parallelExecutor && localRelationCount > 0) {
        // Use helper function instead of large lambda
        auto processor = [jsonPlan, graphConfig, masterIP = this->masterIP, localRelationCount](const WorkChunk& chunk) {
            return processRelationshipChunk(chunk, jsonPlan, graphConfig, masterIP, localRelationCount);
        };

        std::vector<std::vector<std::string>> chunkResults =
            parallelExecutor->processInParallel<decltype(processor), std::vector<std::string>>(
                localRelationCount - 1,
                processor);

        for (const auto& chunkVec : chunkResults) {
            for (const auto& row : chunkVec) {
                buffer.add(row);
            }
        }
    }

    // Central relations - sequential
    long centralRelationCount =
        NodeManager::dbSize(dbPrefix + "_central_relations.db") / RelationBlock::CENTRAL_BLOCK_SIZE;
    string graphDirection = Utils::getGraphDirection(to_string(graphConfig.graphID), masterIP);
    bool isDirected = (graphDirection == "TRUE");
    bool isDirectionRight = (queryParsed["direction"] == "right");

    for (long i = 1; i < centralRelationCount; i++) {
        std::unique_ptr<RelationBlock> relation(RelationBlock::getCentralRelation(i*RelationBlock::CENTRAL_BLOCK_SIZE));
        if (relation->getCentralRelationshipType() != queryParsed["relType"]) {
            continue;
        }

        std::string pid(relation->getMetaPropertyHead()->value);
        if (pid != to_string(graphConfig.partitionID)) {
            continue;
        }

        NodeBlock* startNode = relation->getSource();
        NodeBlock* destNode = relation->getDestination();

        std::string startPid(startNode->getMetaPropertyHead()->value);
        if (startPid != to_string(graphConfig.partitionID)) {
            continue;
        }

        json startNodeData = extractNodeDataAndCleanup(startNode);
        json destNodeData = extractNodeDataAndCleanup(destNode);
        json relationData = extractRelationDataAndCleanup(relation.get());

        json directionData;
        string startVar = queryParsed["sourceVariable"];
        string destVar = queryParsed["destVariable"];
        string relVar = queryParsed["relVariable"];

        if (isDirectionRight) {
            directionData[startVar] = startNodeData;
            directionData[destVar] = destNodeData;
        } else if (!isDirected) {
            directionData[startVar] = destNodeData;
            directionData[destVar] = startNodeData;
        }
        directionData[relVar] = relationData;
        buffer.add(directionData.dump());
    }
    buffer.add("-1");
}
