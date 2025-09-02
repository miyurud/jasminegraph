/**
Copyright 2024 JasmineGraph Team
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
#include <iostream>
#include <fstream>
#include <set>
#include <string>
#include <regex>
#include <map>
#include <vector>
#include <thread>
#include <chrono>
#include <nlohmann/json.hpp>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "../../../src/util/logger/Logger.h"

using json = nlohmann::json;
using namespace std;

Logger evaluation_logger;

const string HOST = "127.0.0.1";
const int PORT = 7777;
const int BUFFER_SIZE = 4096;
const string CARRIAGE_RETURN_NEWLINE = "\r\n";
const string CYPHER = "cypher";


map<string, int> nodeLabelCounts;
map<string, int> relLabelCounts;

string safeExtractId(const json &obj) {
    try {
        if (!obj.contains("id")) return "";
        if (obj["id"].is_string()) return obj["id"].get<string>();
        if (obj["id"].is_number_integer()) return to_string(obj["id"].get<int>());
    } catch (const exception &e) {
        evaluation_logger.error("[ERROR] Failed to extract ID: " + string(e.what()));
    }
    return "";
}

set<string> extractAllLabels(const string &graphPath, set<string> &relLabels) {
    evaluation_logger.info("[INFO] Extracting all labels from graph: " + graphPath);
    set<string> nodeLabels;
    set<string> scannedNodeIds;
    ifstream file(graphPath);
    string line;

    if (!file.is_open()) {
        evaluation_logger.error("[ERROR] Failed to open file: " + graphPath);
        return {};
    }

    while (getline(file, line)) {
        try {
            if (line.empty()) continue;
            auto entry = json::parse(line);

            string srcId = entry["source"].value("id", "");
            string dstId = entry["destination"].value("id", "");
            string srcLabel = entry["source"]["properties"].value("label", "");
            string dstLabel = entry["destination"]["properties"].value("label", "");
            string relLabel = entry["properties"].value("type", "");

            if (!srcId.empty() && scannedNodeIds.find(srcId) == scannedNodeIds.end() && !srcLabel.empty()) {
                nodeLabels.insert(srcLabel);
                nodeLabelCounts[srcLabel]++;
                scannedNodeIds.insert(srcId);
            }
            if (!dstId.empty() && scannedNodeIds.find(dstId) == scannedNodeIds.end() && !dstLabel.empty()) {
                nodeLabels.insert(dstLabel);
                nodeLabelCounts[dstLabel]++;
                scannedNodeIds.insert(dstId);
            }
            if (!relLabel.empty()) {
                relLabels.insert(relLabel);
                relLabelCounts[relLabel]++;
            }
        } catch (const exception &e) {
            evaluation_logger.error("[ERROR] Failed to parse line in label extraction: " + string(e.what()));
            evaluation_logger.error("[LINE] " + line);
        }
    }

    return nodeLabels;
}

int connectToServer() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST.c_str(), &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        evaluation_logger.error("[ERROR] Connection failed");
        return -1;
    }

    return sock;
}

string recvUntilDone(int sock) {
    string response;
    char buffer[BUFFER_SIZE];
    while (true) {
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes <= 0) break;
        response.append(buffer, bytes);
        if (response.find("done") != string::npos) break;
    }
    return response;
}

void validateNodeLabel(const string &graphId, const string &label, int expectedCount) {
    evaluation_logger.info("[INFO] Validating node label: " + label +
                             " (expected: " + to_string(expectedCount) + ")");

    auto start = chrono::steady_clock::now();
    int sock = connectToServer();
    if (sock < 0) return;

    send(sock, (CYPHER + CARRIAGE_RETURN_NEWLINE).c_str(), CYPHER.size() + CARRIAGE_RETURN_NEWLINE.size(), 0);
    recv(sock, new char[1024], 1024, 0);
    send(sock, (graphId + CARRIAGE_RETURN_NEWLINE).c_str(), graphId.size() + CARRIAGE_RETURN_NEWLINE.size(), 0);
    recv(sock, new char[1024], 1024, 0);

    string query = "MATCH(n:" + label + ") RETURN n" + CARRIAGE_RETURN_NEWLINE;
    send(sock, query.c_str(), query.size(), 0);

    string response = recvUntilDone(sock);
    close(sock);

    int count = 0;
    istringstream stream(response);
    string line;

    while (getline(stream, line)) {
        if (line == "done\r" || line == "done" || line == "\r" || line.empty()) continue;
        try {
            line = regex_replace(line, regex(R"(,$)"), "");
            json obj = json::parse(line);
            if (obj.contains("n")) {
                count++;
            }
        } catch (const exception &e) {
            evaluation_logger.error("[ERROR] Failed to parse response line: " + string(e.what()));
            evaluation_logger.error("[LINE] " + line);
        }
    }

    auto end = chrono::steady_clock::now();
    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();

    if (count != expectedCount) {
        evaluation_logger.error("[MISMATCH] ❌ Node label '" + label + "' has " + to_string(count) +
                                 " in graph, but " + to_string(expectedCount) + " in file.");
    } else {
        evaluation_logger.info("[MATCH] ✅ Node label '" + label + "' count matches: " + to_string(count));
    }

    evaluation_logger.info("[TIME] Node label '" + label + "' query took " + to_string(elapsed_ms) + " ms");
}

void validateRelationshipLabel(const string &graphId, const string &label, int expectedCount) {
    evaluation_logger.info("[INFO] Validating relationship label: " + label +
                            " (expected: " + to_string(expectedCount) + ")");

    auto start = chrono::steady_clock::now();
    int sock = connectToServer();
    if (sock < 0) return;

    send(sock, (CYPHER + CARRIAGE_RETURN_NEWLINE).c_str(), CYPHER.size() + CARRIAGE_RETURN_NEWLINE.size(), 0);
    this_thread::sleep_for(chrono::milliseconds(100));
    recv(sock, new char[1024], 1024, 0);
    send(sock, (graphId + CARRIAGE_RETURN_NEWLINE).c_str(), graphId.size() + CARRIAGE_RETURN_NEWLINE.size(), 0);
    recv(sock, new char[1024], 1024, 0);

    string query = "MATCH(n)-[r:" + label + "]-(m) RETURN r" + CARRIAGE_RETURN_NEWLINE;
    send(sock, query.c_str(), query.size(), 0);

    string response = recvUntilDone(sock);
    close(sock);

    int count = 0;
    istringstream stream(response);
    string line;

    while (getline(stream, line)) {
        if (line == "done\r" || line == "done" || line == "\r" || line.empty()) continue;
        try {
            line = regex_replace(line, regex(R"(,$)"), "");
            json obj = json::parse(line);
            if (obj.contains("r")) {
                count++;
            }
        } catch (const exception &e) {
            evaluation_logger.error("[ERROR] Failed to parse response line: " + string(e.what()));
            evaluation_logger.error("[LINE] " + line);
        }
    }

    auto end = chrono::steady_clock::now();
    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();

    if (count != expectedCount) {
        evaluation_logger.error("[MISMATCH] ❌ Relationship label '" + label + "' has " +
                                    to_string(count) + " in graph, but " + to_string(expectedCount) + " in file.");
    } else {
        evaluation_logger.info("[MATCH] ✅ Relationship label '" + label + "' count matches: " + to_string(count));
    }

    evaluation_logger.info("[TIME] Relationship label '" + label + "' query took " +
                             to_string(elapsed_ms) + " ms");}

int main(int argc, char *argv[]) {
    string graphPath = "../tests/integration/env_init/data/graph_with_properties_test2.txt";
    string graphId;

    if (argc > 1) {
        graphPath = argv[1];
        if (argc > 2) {
            graphId = argv[2];

        } else {
            evaluation_logger.error("[USAGE] ./label_evaluation <graph_data_file_path> <graph_id>");
            return 1;
        }
    } else {
       evaluation_logger.error("[USAGE] ./label_evaluation <graph_data_file_path> <graph_id>");
       return 1;
    }

    set<string> relLabels;
    auto nodeLabels = extractAllLabels(graphPath, relLabels);

    for (const auto &label : nodeLabels) {
        validateNodeLabel(graphId, label, nodeLabelCounts[label]);
    }

    for (const auto &label : relLabels) {
        validateRelationshipLabel(graphId, label, relLabelCounts[label]);
    }

    return 0;
}
