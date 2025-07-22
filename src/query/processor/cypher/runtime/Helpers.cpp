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

#include "Helpers.h"


FilterHelper::FilterHelper(string condition) : condition(condition) {};

bool FilterHelper::evaluate(std::string data) {
    return evaluateCondition(condition, data);
}

bool FilterHelper::evaluateCondition(std::string condition, std::string data) {
    json predicate = json::parse(condition);
    std::string type = predicate["type"];
    if (type == "COMPARISON") {
        return evaluateComparison(condition, data);
    } else if (type == "PREDICATE_EXPRESSIONS") {
        return evaluatePredicateExpression(condition, data);
    } else if (type == "AND" || type == "OR" || type == "XOR" || type == "NOT") {
        return evaluateLogical(condition, data);
    }

    return false;
}

bool FilterHelper::evaluateComparison(std::string condition, std::string raw) {
    json predicate = json::parse(condition);
    json data = json::parse(raw);
    if (!typeCheck(predicate["left"]["type"], predicate["right"]["type"])) {
        return false;
    }

    ValueType leftValue;
    ValueType rightValue;

    if (predicate["left"]["type"] == "VARIABLE" && predicate["right"]["type"] == "VARIABLE") {
        string left = predicate["left"]["value"];
        string right = predicate["right"]["value"];
        return evaluateNodes(data[left].dump(),
                             data[right].dump());
    }

    if (predicate["left"]["type"] == "PROPERTY_LOOKUP") {
        std::string variable = predicate["left"]["variable"];
        leftValue = evaluatePropertyLookup(predicate["left"].dump(),
                                           data[variable].dump(), predicate["right"]["type"]);
    } else if (predicate["left"]["type"] == Const::FUNCTION) {
        leftValue = evaluateFunction(predicate["left"].dump(),
                                     data.dump(), predicate["right"]["type"]);
    } else {
        //  only evaluating string, decimal, boolean, null for now
        leftValue = evaluateOtherTypes(predicate["left"].dump());
    }

    if (predicate["right"]["type"] == "PROPERTY_LOOKUP") {
        std::string variable = predicate["right"]["variable"];
        rightValue = evaluatePropertyLookup(predicate["right"].dump(),
                                            data[variable].dump(), predicate["left"]["type"]);

    } else if (predicate["right"]["type"] == Const::FUNCTION) {
        rightValue = evaluateFunction(predicate["right"].dump(),
                                      data.dump(), predicate["left"]["type"]);
    } else {
        //  only evaluating string, decimal, boolean, null for now
        rightValue = evaluateOtherTypes(predicate["right"].dump());
    }
    string op = predicate["operator"];

    return std::visit([&op](auto&& lhs, auto&& rhs) -> bool {
        using LType = std::decay_t<decltype(lhs)>;
        using RType = std::decay_t<decltype(rhs)>;

        if constexpr (std::is_same_v<LType, RType>) {
            if (op == "==") return lhs == rhs;
            if (op == "<>") return lhs != rhs;
            if constexpr (std::is_arithmetic_v<LType>) {
                if (op == "<") return lhs < rhs;
                if (op == ">") return lhs > rhs;
                if (op == "<=") return lhs <= rhs;
                if (op == ">=") return lhs >= rhs;
            }
        }
        return false;  // Default if types are incompatible
    }, leftValue, rightValue);
    return false;
}

bool FilterHelper::evaluatePredicateExpression(std::string condition, std::string raw) {
    json predicate = json::parse(condition);
    json data = json::parse(raw);
    if (!typeCheck(predicate["left"]["type"], predicate["right"]["type"])) {
        return false;
    }

    ValueType leftValue;
    ValueType rightValue;

    if (predicate["left"]["type"] == "VARIABLE" && predicate["right"]["type"] == "VARIABLE") {
        string left = predicate["left"]["value"];
        string right = predicate["right"]["value"];
        return evaluateNodes(data[left].dump(),
                             data[right].dump());
    }

    if (predicate["left"]["type"] == "PROPERTY_LOOKUP") {
        std::string variable = predicate["left"]["variable"];
        leftValue = evaluatePropertyLookup(predicate["left"].dump(),
                                           data[variable].dump(), predicate["right"]["type"]);
    } else if (predicate["left"]["type"] == Const::FUNCTION) {
        leftValue = evaluateFunction(predicate["left"].dump(),
                                     data.dump(), predicate["right"]["type"]);
    } else {
        // only evaluating string, decimal, boolean, null for now
        leftValue = evaluateOtherTypes(predicate["left"].dump());
    }
    rightValue = evaluateOtherTypes(predicate["right"].dump());

    string op = predicate["operator"];

    return std::visit([&op](auto&& lhs, auto&& rhs) -> bool {
        using LType = std::decay_t<decltype(lhs)>;
        using RType = std::decay_t<decltype(rhs)>;

        if constexpr (std::is_same_v<LType, RType>) {
            if (op == "==") return lhs == rhs;
            if (op == "<>") return lhs != rhs;
            if constexpr (std::is_arithmetic_v<LType>) {
                if (op == "<") return lhs < rhs;
                if (op == ">") return lhs > rhs;
                if (op == "<=") return lhs <= rhs;
                if (op == ">=") return lhs >= rhs;
            }
        }

        return false;  // Default if types are incompatible
    }, leftValue, rightValue);
    return false;
}

bool FilterHelper::evaluateLogical(std::string condition, std::string data) {
    json predicate = json::parse(condition);
    std::string type = predicate["type"];
    vector<json> comparisons = predicate["comparisons"];

    if (type == "AND") {
        for (auto comp : comparisons) {
            if (!evaluateCondition(comp.dump(), data)) {
                return false;
            }
        }
        return true;
    } else if (type == "OR") {
        for (auto comp : comparisons) {
            if (evaluateCondition(comp.dump(), data)) {
                return true;
            }
        }
        return false;
    } else if (type == "XOR") {
        if (comparisons.size() < 2) {
            return false;
        }
        bool result = evaluateCondition(comparisons[0].dump(),
                                        data) ^ evaluateCondition(comparisons[1].dump(), data);

        for (size_t i = 2; i < comparisons.size(); ++i) {
            result = result ^ evaluateCondition(comparisons[i].dump(), data);
        }
        return result;
    } else if (type == "NOT") {
        return evaluateCondition(comparisons[0].dump(), data);
    }

    return false;
}

bool FilterHelper::evaluateNodes(std::string left, std::string right) {
    json leftNode = json::parse(left);
    json rightNode = json::parse(right);
    if (left == "null" || right == "null") {
        return true;
    }

    if (leftNode["id"] != rightNode["id"]) {
        return true;
    }
    return false;
}

bool FilterHelper::typeCheck(std::string left, std::string right) {
    if (left == Const::PROPERTY_LOOKUP
        || right == Const::PROPERTY_LOOKUP
        || left == Const::FUNCTION
        || right == Const::FUNCTION) {
        return true;
    } else if (left == right) {
        return true;
    } else {
        return false;
    }
}

ValueType FilterHelper::evaluatePropertyLookup(std::string property, std::string data, string type) {
    json prop = json::parse(property);
    json raw = json::parse(data);
    vector<string> properties = prop["property"];
    string value;
    // should be implemented to lookup nested properties after persisting that kind of properties
    // for now only one level of properties are supported (size of properties vector should be 1)
    for (auto p : properties) {
        if (!raw.contains(p)) {
            value = "null";
        } else {
            value = raw[p];
        }
    }

    try {
        if (type == "STRING") {
            return value;
        } else if (type == "DECIMAL") {
            size_t pos;
            int num = stoi(value, &pos);
            if (pos != value.size()) throw invalid_argument("Invalid number format");
            return num;
        } else if (type == "BOOLEAN") {
            string lowerValue;
            std::transform(value.begin(), value.end(), back_inserter(lowerValue), ::tolower);
            if (lowerValue == "true") return true;
            if (lowerValue == "false") return false;
            throw invalid_argument("Invalid boolean format");
        } else if (type == "NULL") {
            if (value == "null") return "null";
            return value;
        } else if ("PROPERTY_LOOKUP") {
            return value;
        }
    } catch (const exception& e) {
        return "null";
    }
    return "null";
}

ValueType FilterHelper::evaluateFunction(std::string function, std::string data, std::string type) {
    json func = json::parse(function);
    json raw = json::parse(data);
    vector<string> args = func["arguments"];
    string arg = args[0];
    string value;

    if (func["functionName"] == "id") {
        value = raw[arg]["id"];
    }

    try {
        if (type == "STRING") {
            return value;
        } else if (type == "DECIMAL") {
            size_t pos;
            int num = stoi(value, &pos);
            if (pos != value.size()) throw invalid_argument("Invalid number format");
            return num;
        } else if (type == "BOOLEAN") {
            string lowerValue;
            std::transform(value.begin(), value.end(), back_inserter(lowerValue), ::tolower);
            if (lowerValue == "true") return true;
            if (lowerValue == "false") return false;
            throw invalid_argument("Invalid boolean format");
        } else if (type == "NULL") {
            return "null";
        } else if ("PROPERTY_LOOKUP") {
            return value;
        }
    } catch (const exception& e) {
        return "null";
    }
    return "null";
}

ValueType FilterHelper::evaluateOtherTypes(std::string data) {
    json val = json::parse(data);
    if (val["type"] == "STRING") {
        string str = val["value"];
        if (str.size() >= 2 && str.front() == '\'' && str.back() == '\'') {
            return str.substr(1, str.size() - 2);  // Remove first and last character ' '
        }
        return val["value"];
    } else if (val["type"] == "DECIMAL") {
        return stoi(val["value"].get<std::string>());
    } else if (val["type"] == "BOOLEAN") {
        return val["value"] == "TRUE";
    } else if (val["type"] == "NULL") {
        return "null";
    }
    return "";
}

string ExpandAllHelper::generateSubQueryPlan(std::string query) {
    antlr4::ANTLRInputStream input(query);
    // Create a lexer from the input
    CypherLexer lexer(&input);

    // Create a token stream from the lexer
    antlr4::CommonTokenStream tokens(&lexer);

    // Create a parser from the token stream
    CypherParser parser(&tokens);

    ASTBuilder ast_builder;
    auto* ast = any_cast<ASTNode*>(ast_builder.visitOC_Cypher(parser.oC_Cypher()));

    SemanticAnalyzer semantic_analyzer;
    string obj;
    QueryPlanner query_planner;
    Operator *opr = query_planner.createExecutionPlan(ast);
    obj = opr->execute();
    return obj;
}

string ExpandAllHelper::generateSubQuery(std::string startVar, std::string destVar, string relVar, bool isDirected,
                                         std::string id, std::string relType) {
    if (relType == "") {
        if (isDirected) {
            return "match (" + startVar + ")-[" + relVar + "]->(" + destVar + ") where id("
                   + startVar + ") = " + id + " return " + relVar + "," + destVar;
        }
        return "match (" + startVar + ")-[" + relVar + "]-(" + destVar + ") where id("
                + startVar + ") = " + id + " return " + relVar + "," + destVar;
    }
    if (isDirected) {
        return "match (" + startVar + ")-[" + relVar + ":" + relType + "]->(" + destVar + ") where id("
               + startVar + ") = " + id + " return " + relVar + "," + destVar;
    }
    return "match (" + startVar + ")-[" + relVar + ":" + relType + "]-(" + destVar + ") where id("
            + startVar + ") = " + id + " return " + relVar + "," + destVar;
}

void AverageAggregationHelper::insertData(std::string data) {
    json rawObj = json::parse(data);
    string value = rawObj[this->variable][this->property];
    float property = stof(value);
    this->numberOfData++;
    this->localAverage = (this->localAverage * (this->numberOfData - 1) + property) / this->numberOfData;
}

string AverageAggregationHelper::getFinalResult() {
    json data;
    data["avg"] = this->localAverage;
    data["numberOfData"] = this->numberOfData;
    return data.dump();
}

CreateHelper::CreateHelper(vector<json> elements, std::string partitionAlgo, GraphConfig gc, string masterIP) :
    elements(elements), gc(gc), masterIP(masterIP) {
    std::string partitionCount = Utils::getJasmineGraphProperty("org.jasminegraph.server.npartitions");
    int numberOfPartitions = std::stoi(partitionCount);
    this->graphPartitioner = new Partitioner(stoi(partitionCount), gc.graphID,
                                             spt::getPartitioner(partitionAlgo), nullptr, true);
};

void CreateHelper::insertFromData(std::string data, SharedBuffer &buffer) {
    json rawData = json::parse(data);
    NodeManager nodeManager(this->gc);
    for (json insert : this->elements) {
        if (insert["type"] == "Relationships") {
            for (json rel : insert["relationships"]) {
                auto rawObj = rawData;
                auto source = rel["source"];
                auto dest = rel["dest"];
                auto relation = rel["rel"];
                string sourceVariable;
                string destVariable;
                string sourceId;
                string destId;
                bool sourceFromData = false;
                bool destFromData = false;
                if (source.contains("variable")
                        && rawObj.contains(source["variable"])) {
                    sourceVariable = source["variable"];
                    sourceId = rawObj[sourceVariable]["id"];
                    sourceFromData = true;
                } else if (source.contains("variable")
                        && !rawObj.contains(source["variable"])
                        && source.contains("properties")
                        && source["properties"].contains("id")) {
                    sourceVariable = source["variable"];
                    sourceId = source["properties"]["id"];
                } else if ((source.contains("variable")
                           && !rawObj.contains(source["variable"])
                           && !source.contains("properties"))
                           || (source.contains("variable")
                              && !rawObj.contains(source["variable"])
                              && source.contains("properties")
                              && !source["properties"].contains("id"))) {
                    return;
                } else if (!source.contains("variable")
                           && source.contains("properties")
                           && !source["properties"].contains("id")) {
                    sourceId = source["properties"]["id"];
                }

                if (dest.contains("variable") && rawObj.contains(dest["variable"])) {
                    destVariable = dest["variable"];
                    destId = rawObj[destVariable]["id"];
                    destFromData = true;
                } else if (dest.contains("variable")
                           && !rawObj.contains(dest["variable"])
                           && dest.contains("properties")
                           && dest["properties"].contains("id")) {
                    destVariable = dest["variable"];
                    destId = dest["properties"]["id"];
                } else if ((dest.contains("variable")
                           && !rawObj.contains(dest["variable"])
                           && !dest.contains("properties"))
                            || (dest.contains("variable")
                               && !rawObj.contains(dest["variable"])
                               && dest.contains("properties")
                               && !dest["properties"].contains("id"))) {
                    return;
                } else if (!dest.contains("variable")
                            && dest.contains("properties")
                            && !dest["properties"].contains("id")) {
                    destId = dest["properties"]["id"];
                }

                json edgeProps;
                if (relation.contains("properties")) {
                    edgeProps = relation["properties"];
                }
                edgeProps["graphId"] = to_string(gc.graphID);

                json sourceProps;
                if (sourceFromData) {
                    sourceProps = rawObj[sourceVariable];
                } else if (source.contains("properties")) {
                    sourceProps = source["properties"];
                }

                json destProps;
                if (destFromData) {
                    destProps = rawObj[destVariable];
                } else if (dest.contains("properties")) {
                    destProps = dest["properties"];
                }

                partitionedEdge partitionedEdge = graphPartitioner->addEdge({sourceId, destId});
                // this Json object are used to create edge string to send to the other workers
                json edge;
                json sourceJson;
                json destJson;

                if (sourceProps.contains("partitionID")) {
                    sourceProps.erase("partitionID");
                }

                if (destProps.contains("partitionID")) {
                    destProps.erase("partitionID");
                }

                if (partitionedEdge[0].second == partitionedEdge[1].second) {
                    edge["EdgeType"] = "Local";
                } else {
                    edge["EdgeType"] = "Central";
                }

                sourceJson["properties"] = sourceProps;
                sourceJson["id"] = sourceId;
                destJson["properties"] = destProps;
                destJson["id"] = destId;
                sourceJson["pid"] = partitionedEdge[0].second;
                destJson["pid"] = partitionedEdge[1].second;
                edge["source"] = sourceJson;
                edge["destination"] = destJson;
                edge["properties"] = edgeProps;
                RelationBlock* newRelation;

                std::string host;
                int port;
                int dataPort;

                if (partitionedEdge[0].second == partitionedEdge[1].second &&
                partitionedEdge[0].second == gc.partitionID) {
                    newRelation = nodeManager.addLocalEdge({sourceId, destId});
                } else if (partitionedEdge[0].second == gc.partitionID) {
                    newRelation = nodeManager.addCentralEdge({sourceId, destId});
                    auto destWorker = Utils::getWorker(to_string(partitionedEdge[1].second), masterIP,
                                                       Conts::JASMINEGRAPH_BACKEND_PORT);
                    edge["PID"] = partitionedEdge[1].second;
                    if (destWorker) {
                        std::tie(host, port, dataPort) = *destWorker;
                    } else {
                        return;
                    }
                    auto dataPublisher = new DataPublisher(port, host, dataPort);
                    dataPublisher->publish(edge.dump());
                } else if (partitionedEdge[1].second == gc.partitionID) {
                    newRelation = nodeManager.addCentralEdge({sourceId, destId});
                    auto sourceWorker = Utils::getWorker(to_string(partitionedEdge[0].second), masterIP,
                                                         Conts::JASMINEGRAPH_BACKEND_PORT);
                    edge["PID"] = partitionedEdge[0].second;
                    if (sourceWorker) {
                        std::tie(host, port, dataPort) = *sourceWorker;
                    } else {
                        return;
                    }
                    auto dataPublisher = new DataPublisher(port, host, dataPort);
                    dataPublisher->publish(edge.dump());
                } else if (partitionedEdge[0].second == partitionedEdge[1].second) {
                    auto worker = Utils::getWorker(to_string(partitionedEdge[0].second), masterIP,
                                                           Conts::JASMINEGRAPH_BACKEND_PORT);
                    edge["PID"] = partitionedEdge[1].second;
                    if (worker) {
                        std::tie(host, port, dataPort) = *worker;
                    } else {
                        return;
                    }
                    auto dataPublisher = new DataPublisher(port, host, dataPort);
                    dataPublisher->publish(edge.dump());
                    continue;
                } else {
                    auto sourceWorker = Utils::getWorker(to_string(partitionedEdge[0].second), masterIP,
                                                   Conts::JASMINEGRAPH_BACKEND_PORT);
                    edge["PID"] = partitionedEdge[0].second;
                    if (sourceWorker) {
                        std::tie(host, port, dataPort) = *sourceWorker;
                    } else {
                        return;
                    }
                    auto sourceDataPublisher = new DataPublisher(port, host, dataPort);
                    sourceDataPublisher->publish(edge.dump());
                    auto destWorker = Utils::getWorker(to_string(partitionedEdge[1].second), masterIP,
                                                       Conts::JASMINEGRAPH_BACKEND_PORT);
                    edge["PID"] = partitionedEdge[1].second;
                    if (destWorker) {
                        std::tie(host, port, dataPort) = *destWorker;
                    } else {
                        return;
                    }
                    auto destDataPublisher = new DataPublisher(port, host, dataPort);
                    destDataPublisher->publish(edge.dump());
                    continue;
                }

                char value[PropertyLink::MAX_VALUE_SIZE] = {0};
                char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
                char metaEdge[MetaPropertyEdgeLink::MAX_VALUE_SIZE] = {0};

                for (auto it = edgeProps.begin(); it != edgeProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    if (partitionedEdge[0].second == partitionedEdge[1].second) {
                        newRelation->addLocalProperty(std::string(it.key()), &value[0]);
                    } else {
                        newRelation->addCentralProperty(std::string(it.key()), &value[0]);
                    }
                }

                if (partitionedEdge[0].second != partitionedEdge[1].second) {
                    std::string edgePid = to_string(partitionedEdge[0].second);
                    strcpy(metaEdge, edgePid.c_str());
                    newRelation->addMetaProperty(MetaPropertyEdgeLink::PARTITION_ID, &metaEdge[0]);
                }

                for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newRelation->getSource()->addProperty(std::string(it.key()), &value[0]);
                }
                std::string sourcePid = to_string(partitionedEdge[0].second);
                strcpy(meta, sourcePid.c_str());
                newRelation->getSource()->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);

                for (auto it = destProps.begin(); it != destProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newRelation->getDestination()->addProperty(std::string(it.key()), &value[0]);
                }

                std::string destPid = to_string(partitionedEdge[1].second);;
                strcpy(meta, destPid.c_str());
                newRelation->getDestination()->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);

                if (source.contains("variable")) {
                    string variable = source["variable"];
                    rawObj[variable] = sourceProps;
                }

                if (dest.contains("variable")) {
                    string variable = dest["variable"];
                    rawObj[variable] = destProps;
                }

                if (relation.contains("variable")) {
                    string variable = relation["variable"];
                    rawObj[variable] = edgeProps;
                }
                buffer.add(rawObj.dump());
            }
        } else if (insert["type"] == "Node") {
            auto rawObj = rawData;
            std::string host;
            int port;
            int dataPort;
            if (insert.contains("properties") && insert["properties"].contains("id")) {
                string sourceId = insert["properties"]["id"];
                string destId = Const::DUMMY_ID;
                partitionedEdge partitionedEdge = graphPartitioner->addEdge({sourceId, destId});
                NodeBlock* newNode = nullptr;
                if (partitionedEdge[0].second == gc.partitionID) {
                    newNode = nodeManager.addNode(sourceId);
                } else {
                    auto worker = Utils::getWorker(to_string(partitionedEdge[0].second), masterIP,
                                                   Conts::JASMINEGRAPH_BACKEND_PORT);
                    if (worker) {
                        std::tie(host, port, dataPort) = *worker;
                    } else {
                        return;
                    }
                    auto dataPublisher = new DataPublisher(port, host, dataPort);
                    json node;
                    node["id"] = insert["properties"]["id"];
                    node["properties"] = insert["properties"];
                    node["pid"] = partitionedEdge[0].second;
                    node["isNode"] = true;
                    dataPublisher->publish(node.dump());
                    return;
                }

                if (!newNode) {
                    return;
                }

                char value[PropertyLink::MAX_VALUE_SIZE] = {0};
                char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
                json sourceProps = insert["properties"];
                for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newNode->addProperty(std::string(it.key()), &value[0]);
                }

                std::string sourcePid = to_string(partitionedEdge[0].second);;
                strcpy(meta, sourcePid.c_str());
                newNode->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);

                if (insert.contains("variable")) {
                    string variable = insert["variable"];
                    rawObj[variable] = sourceProps;
                }

                buffer.add(rawObj.dump());
            }
            return;
        }
    }
}

void CreateHelper::insertWithoutData(SharedBuffer &buffer) {
    NodeManager nodeManager(this->gc);
    for (json insert : this->elements) {
        if (insert["type"] == "Relationships") {
            for (json rel : insert["relationships"]) {
                json rawObj;
                auto source = rel["source"];
                auto dest = rel["dest"];
                auto relation = rel["rel"];
                string sourceVariable;
                string destVariable;
                string sourceId;
                string destId;
                if (source.contains("properties")
                        && source["properties"].contains("id")) {
                    sourceId = source["properties"]["id"];
                } else {
                    return;
                }

                if (dest.contains("properties")
                        && dest["properties"].contains("id")) {
                    destId = dest["properties"]["id"];
                } else {
                    return;
                }

                json edgeProps;
                if (relation.contains("properties")) {
                    edgeProps = relation["properties"];
                }
                json sourceProps = source["properties"];
                json destProps = dest["properties"];
                partitionedEdge partitionedEdge = graphPartitioner->addEdge({sourceId, destId});
                RelationBlock* newRelation;

                if (partitionedEdge[0].second == partitionedEdge[1].second &&
                    partitionedEdge[0].second == gc.partitionID) {
                    newRelation = nodeManager.addLocalEdge({sourceId, destId});
                } else if (partitionedEdge[0].second == gc.partitionID ||
                           partitionedEdge[1].second == gc.partitionID) {
                    newRelation = nodeManager.addCentralEdge({sourceId, destId});
                } else {
                    return;
                }

                char value[PropertyLink::MAX_VALUE_SIZE] = {0};
                char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
                char metaEdge[MetaPropertyEdgeLink::MAX_VALUE_SIZE] = {0};

                for (auto it = edgeProps.begin(); it != edgeProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    if (partitionedEdge[0].second == partitionedEdge[1].second) {
                        newRelation->addLocalProperty(std::string(it.key()), &value[0]);
                    } else {
                        newRelation->addCentralProperty(std::string(it.key()), &value[0]);
                    }
                }

                if (partitionedEdge[0].second != partitionedEdge[1].second) {
                    std::string edgePid = to_string(partitionedEdge[0].second);
                    strcpy(metaEdge, edgePid.c_str());
                    newRelation->addMetaProperty(MetaPropertyEdgeLink::PARTITION_ID, &metaEdge[0]);
                }

                for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newRelation->getSource()->addProperty(std::string(it.key()), &value[0]);
                }
                std::string sourcePid = to_string(partitionedEdge[0].second);
                strcpy(meta, sourcePid.c_str());
                newRelation->getSource()->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);

                for (auto it = destProps.begin(); it != destProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newRelation->getDestination()->addProperty(std::string(it.key()), &value[0]);
                }

                std::string destPid = to_string(partitionedEdge[1].second);;
                strcpy(meta, destPid.c_str());
                newRelation->getDestination()->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);

                if (source.contains("variable")) {
                    string variable = source["variable"];
                    rawObj[variable] = sourceProps;
                }

                if (dest.contains("variable")) {
                    string variable = dest["variable"];
                    rawObj[variable] = destProps;
                }

                if (relation.contains("variable")) {
                    string variable = relation["variable"];
                    rawObj[variable] = edgeProps;
                }

                buffer.add(rawObj.dump());
            }
        } else if (insert["type"] == "Node") {
            json rawObj;
            if (insert.contains("properties") && insert["properties"].contains("id")) {
                string sourceId = insert["properties"]["id"];
                string destId = Const::DUMMY_ID;
                partitionedEdge partitionedEdge = graphPartitioner->addEdge({sourceId, destId});
                RelationBlock* newRelation;
                NodeBlock* newNode = nullptr;
                if (partitionedEdge[0].second == gc.partitionID) {
                    newNode = nodeManager.addNode(sourceId);
                }

                if (!newNode) {
                    return;
                }

                char value[PropertyLink::MAX_VALUE_SIZE] = {0};
                char meta[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
                json sourceProps = insert["properties"];
                for (auto it = sourceProps.begin(); it != sourceProps.end(); it++) {
                    strcpy(value, it.value().get<std::string>().c_str());
                    newNode->addProperty(std::string(it.key()), &value[0]);
                }

                std::string sourcePid = to_string(partitionedEdge[0].second);;
                strcpy(meta, sourcePid.c_str());
                newNode->addMetaProperty(MetaPropertyLink::PARTITION_ID, &meta[0]);

                if (insert.contains("variable")) {
                    string variable = insert["variable"];
                    rawObj[variable] = insert["properties"];
                }
                buffer.add(rawObj.dump());
            }
            return;
        }
    }
}
