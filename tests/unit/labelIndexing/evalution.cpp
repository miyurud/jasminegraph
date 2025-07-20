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
#include <cstdlib>
#include <cstring>

// =============================================================
// Relationship & Node Label Validation + Sample Edge Queries
// =============================================================
// This program:
//   1. Parses a line-delimited JSON edge list file.
//   2. Extracts node labels and relationship (edge) labels and counts.
//   3. Retrieves all nodes / relationships by label via Cypher queries to the server and compares counts.
//   4. For each relationship label, stores up to N sample relationships and runs a targeted MATCH query:
//        MATCH (n)-[r:TYPE]-(x) WHERE n.id='...' AND x.id='...' [AND r.description='...'] RETURN n,r,x
//      to verify those specific edges exist (catching missing individual edges even if counts match).
//   5. Reports missing node / relationship IDs.
//
// Adjust constants (HOST, PORT, file path, graphId) as needed.
//
// Compile: g++ -std=c++17 -O2 -pthread -I/path/to/nlohmann relationship_sample_validation.cpp -o rel_validator
// Run: ./rel_validator /path/to/graph_data.txt <graphId>
// =============================================================

using json = nlohmann::json;
using namespace std;

// ---------------- Configuration ----------------
const string HOST = "127.0.0.1";   // Server host
const int    PORT = 7777;            // Server port
const string LINE_END = "\r\n";   // Protocol line terminator
const string CYPHER = "cypher";     // Initial mode token

// Maximum number of sample relationships to store per label for targeted validation
const size_t MAX_SAMPLES_PER_LABEL = 5;

// ---------------- Global State ----------------
map<string, int> nodeLabelCounts;               // label -> count (from file)
map<string, int> relLabelCounts;                // relationship type -> count (from file)
map<string, set<string>> fileNodeIdsByLabel;    // label -> set(node IDs) (from file)
map<string, set<string>> graphNodeIdsByLabel;   // label -> set(node IDs) (retrieved from server)
map<string, set<string>> fileRelIdsByLabel;     // rel label -> set(relationship IDs) (from file)
map<string, set<string>> graphRelIdsByLabel;    // rel label -> set(relationship IDs) (from server)

struct RelSample {
    string relId;
    string srcId;
    string dstId;
    string label;        // relationship type
    string description;  // optional description property
};
map<string, vector<RelSample>> relSamplesByLabel;  // rel label -> sample relationships

// ---------------- Utility Functions ----------------

string safeExtractId(const json &obj) {
    try {
        if (!obj.contains("id")) return "";
        if (obj["id"].is_string()) return obj["id"].get<string>();
        if (obj["id"].is_number_integer()) return to_string(obj["id"].get<long long>());
        if (obj["id"].is_number()) return to_string(obj["id"].get<double>()); // fallback (not ideal)
    } catch (const exception &e) {
        cerr << "[ERROR] Failed to extract ID: " << e.what() << endl;
    }
    return "";
}

// Escape single quotes for Cypher string literals. If your engine uses backslash escaping, adjust accordingly.
// Neo4j-style escaping: duplicate single quotes.
string cypherEscape(const string &s) {
    string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '\'') out += "''"; else out += c;
    }
    return out;
}

// Parse the file and populate label/ID maps and collect relationship samples.
set<string> extractAllLabels(const string &graphPath, set<string> &relLabels) {
    cout << "[INFO] Extracting all labels from graph: " << graphPath << endl;
    set<string> nodeLabels;
    set<string> scannedNodeIds; // to count each node label only once per ID

    ifstream file(graphPath);
    string line;
    if (!file.is_open()) {
        cerr << "[ERROR] Failed to open file: " << graphPath << endl;
        return {};
    }

    size_t lineNo = 0;

    while (getline(file, line)) {
        ++lineNo;
        try {
            if (line.empty()) continue;
            auto entry = json::parse(line);

            string srcId    = entry["source"].value("id", "");
            string dstId    = entry["destination"].value("id", "");
            string srcLabel = entry["source"]["properties"].value("label", "");
            string dstLabel = entry["destination"]["properties"].value("label", "");
            string relLabel = entry["properties"].value("type", "DEFAULT");
            string relId    = entry.value("id", "");

            if (!srcId.empty() && scannedNodeIds.insert(srcId).second && !srcLabel.empty()) {
                nodeLabels.insert(srcLabel);
                nodeLabelCounts[srcLabel]++;
                fileNodeIdsByLabel[srcLabel].insert(srcId);
            }
            if (!dstId.empty() && scannedNodeIds.insert(dstId).second && !dstLabel.empty()) {
                nodeLabels.insert(dstLabel);
                nodeLabelCounts[dstLabel]++;
                fileNodeIdsByLabel[dstLabel].insert(dstId);
            }
            if (!relLabel.empty()) {
                relLabels.insert(relLabel);
                relLabelCounts[relLabel]++;
                fileRelIdsByLabel[relLabel].insert(relId);

                // Capture sample relationship (up to MAX_SAMPLES_PER_LABEL)
                if (relSamplesByLabel[relLabel].size() < MAX_SAMPLES_PER_LABEL) {
                    string relDesc;
                    try {
                        if (entry["properties"].contains("description") && entry["properties"]["description"].is_string()) {
                            relDesc = entry["properties"]["description"].get<string>();
                        }
                    } catch (...) {}
                    relSamplesByLabel[relLabel].push_back(RelSample{relId, srcId, dstId, relLabel, relDesc});
                }
            }
        } catch (const exception &e) {
            cerr << "[ERROR] Failed to parse line " << lineNo << ": " << e.what() << endl;
            // Optionally: cerr << "[LINE] " << line << endl;
        }
    }

    return nodeLabels;
}

int connectToServer() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        cerr << "[ERROR] socket() failed" << endl;
        return -1;
    }
    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, HOST.c_str(), &serv_addr.sin_addr) <= 0) {
        cerr << "[ERROR] inet_pton() failed" << endl;
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        cerr << "[ERROR] Connection failed to " << HOST << ":" << PORT << endl;
        close(sock);
        return -1;
    }

    return sock;
}

string recvUntilDone(int sock) {
    string response;
    char buffer[8192];
    while (true) {
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes <= 0) break;
        response.append(buffer, bytes);
        if (response.find("done") != string::npos) break; // simple protocol sentinel
    }
    return response;
}

// Generic query executor returning raw response
string runCypherQuery(const string &graphId, const string &query) {
    int sock = connectToServer();
    if (sock < 0) return "";

    // handshake
    {
        string cmd = CYPHER + LINE_END;
        send(sock, cmd.c_str(), cmd.size(), 0);
        this_thread::sleep_for(chrono::milliseconds(50));
        char dummy[1024]; recv(sock, dummy, sizeof(dummy), 0); // read ack
        string gid = graphId + LINE_END;
        send(sock, gid.c_str(), gid.size(), 0);
        recv(sock, dummy, sizeof(dummy), 0); // read ack
    }

    string fullQuery = query + LINE_END;
    send(sock, fullQuery.c_str(), fullQuery.size(), 0);

    string resp = recvUntilDone(sock);
    close(sock);
    return resp;
}

void validateNodeLabel(const string &graphId, const string &label, int expectedCount) {
    cout << "[INFO] Validating node label: " << label << " (expected: " << expectedCount << ")\n";
    auto start = chrono::steady_clock::now();

    string query = "MATCH(n:" + label + ") RETURN n";
    string response = runCypherQuery(graphId, query);

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
                string id = safeExtractId(obj["n"]);
                if (!id.empty()) graphNodeIdsByLabel[label].insert(id);
            }
        } catch (const exception &e) {
            cerr << "[ERROR] Failed to parse response line (node label " << label << "): " << e.what() << endl;
            // cerr << "[LINE] " << line << endl;
        }
    }

    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();

    if (count != expectedCount) {
        cerr << "[MISMATCH] ❌ Node label '" << label << "' has " << count << " in graph, but " << expectedCount << " in file." << endl;
    } else {
        cout << "[MATCH] ✅ Node label '" << label << "' count matches: " << count << endl;
    }
    cout << "[TIME] Node label '" << label << "' query took " << elapsed_ms << " ms" << endl;
}

void validateRelationshipLabel(const string &graphId, const string &label, int expectedCount) {
    cout << "[INFO] Validating relationship label: " << label << " (expected: " << expectedCount << ")\n";
    auto start = chrono::steady_clock::now();

    string query = "MATCH(n)-[r:" + label + "]-(m) RETURN r"; // undirected style
    string response = runCypherQuery(graphId, query);

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
                string id = safeExtractId(obj["r"]);
                if (!id.empty()) graphRelIdsByLabel[label].insert(id);
            }
        } catch (const exception &e) {
            cerr << "[ERROR] Failed to parse response line (rel label " << label << "): " << e.what() << endl;
        }
    }

    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();

    if (count != expectedCount) {
        cerr << "[MISMATCH] ❌ Relationship label '" << label << "' has " << count << " in graph, but " << expectedCount << " in file." << endl;
    } else {
        cout << "[MATCH] ✅ Relationship label '" << label << "' count matches: " << count << endl;
    }
    cout << "[TIME] Relationship label '" << label << "' query took " << elapsed_ms << " ms" << endl;
}

void validateRelationshipSamples(const string &graphId) {
    cout << "[INFO] Validating specific relationship samples" << endl;
    for (const auto &kv : relSamplesByLabel) {
        const string &label = kv.first;
        const auto &samples = kv.second;
        for (const auto &s : samples) {
            string whereClause = "n.id='" + cypherEscape(s.srcId) + "' AND x.id='" + cypherEscape(s.dstId) + "'";
            if (!s.description.empty()) {
                whereClause += " AND r.description='" + cypherEscape(s.description) + "'";
            }
            string query = "MATCH (n)-[r:" + label + "]-(x) WHERE " + whereClause + " RETURN n, r, x";

            auto start = chrono::steady_clock::now();
            string response = runCypherQuery(graphId, query);
            auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();

            bool found = false;
            istringstream stream(response);
            string line;
            while (getline(stream, line)) {
                if (line == "done\r" || line == "done" || line == "\r" || line.empty()) continue;
                try {
                    line = regex_replace(line, regex(R"(,$)"), "");
                    json obj = json::parse(line);
                    if (obj.contains("n") && obj.contains("x")) {
                        string nId = safeExtractId(obj["n"]);
                        string xId = safeExtractId(obj["x"]);
                        if ((nId == s.srcId && xId == s.dstId) || (nId == s.dstId && xId == s.srcId)) {
                            found = true;
                            break;
                        }
                    }
                } catch (...) {
                    // ignore individual line errors for sample query
                }
            }

            if (found) {
                cout << "[SAMPLE MATCH] ✅ (" << label << ") " << s.srcId << " -- " << s.dstId
                     << (s.description.empty() ? "" : (" | desc: " + s.description))
                     << " (" << elapsed_ms << " ms)" << endl;
            } else {
                cerr << "[SAMPLE MISSING] ❌ (" << label << ") " << s.srcId << " -- " << s.dstId
                     << (s.description.empty() ? "" : (" | desc: " + s.description))
                     << " (" << elapsed_ms << " ms)" << endl;
            }
        }
    }
}

map<string, vector<string>> getMissingNodeIds() {
    map<string, vector<string>> missingNodeIds;
    for (const auto &entry : fileNodeIdsByLabel) {
        const string &label = entry.first;
        const auto &expected = entry.second;
        const auto &retrieved = graphNodeIdsByLabel[label];
        for (const auto &id : expected) {
            if (retrieved.find(id) == retrieved.end()) {
                cout << "[MISSING NODE] Label: " << label << ", Node ID: " << id << endl;
                missingNodeIds[label].push_back(id);
            }
        }
    }
    return missingNodeIds;
}

map<string, vector<string>> getMissingRelationshipIds() {
    map<string, vector<string>> missingRelIds;
    for (const auto &entry : fileRelIdsByLabel) {
        const string &label = entry.first;
        const auto &expected = entry.second;
        const auto &retrieved = graphRelIdsByLabel[label];
        for (const auto &id : expected) {
            if (id == "DEFAULT") continue; // skip placeholder
            if (retrieved.find(id) == retrieved.end()) {
                cout << "[MISSING REL] Label: " << label << ", Relationship ID: " << id << endl;
                missingRelIds[label].push_back(id);
            }
        }
    }
    return missingRelIds;
}

int main(int argc, char *argv[]) {
    ios::sync_with_stdio(false);

    string graphPath = "/home/ubuntu/software/jasminegraph/tests/integration/env_init/data/graph_data_0.45GB.txt";
    string graphId   = "4"; // default graph ID for server

    if (argc > 1) {
        graphPath = argv[1];
        if (argc > 2) graphId = argv[2];
    }

    set<string> relLabels;
    auto nodeLabels = extractAllLabels(graphPath, relLabels);

    cout << "[INFO] Found node labels: ";
    for (const auto &l : nodeLabels) cout << l << ' ';
    cout << "\n";

    cout << "[INFO] Found relationship labels: ";
    for (const auto &l : relLabels) cout << l << ' ';
    cout << "\n";

    // Validate node labels
    for (const auto &label : nodeLabels) {
        validateNodeLabel(graphId, label, nodeLabelCounts[label]);
    }

    // Validate relationship labels (counts)
    for (const auto &label : relLabels) {
        validateRelationshipLabel(graphId, label, relLabelCounts[label]);
    }

    // Validate specific sample relationships
    validateRelationshipSamples(graphId);

    // Compute missing IDs
    auto missingNodeIds = getMissingNodeIds();
    auto missingRelIds  = getMissingRelationshipIds();

    // Summary
    for (const auto &[label, ids] : missingNodeIds) {
        cout << "[SUMMARY] Missing " << ids.size() << " node(s) under label: " << label << endl;
        for (const auto &id : ids) cout << "  - Node ID: " << id << endl;
    }
    for (const auto &[label, ids] : missingRelIds) {
        cout << "[SUMMARY] Missing " << ids.size() << " relationship(s) under label: " << label << endl;
        for (const auto &id : ids) cout << "  - Relationship ID: " << id << endl;
    }

    cout << "[DONE] Validation complete." << endl;
    return 0;
}
