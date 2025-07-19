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
// Now optimized so that *all* validation queries (node labels, relationship labels, samples)
// are executed inside *one* persistent telnet / Cypher session (single handshake).
// This drastically reduces overhead when there are many labels.
//
// Compile: g++ -std=c++17 -O2 -pthread -I/path/to/nlohmann relationship_sample_validation.cpp -o rel_validator
// Run: ./rel_validator /path/to/graph_data.txt <graphId>
// =============================================================

using json = nlohmann::json;
using namespace std;

// ---------------- Configuration ----------------
const string HOST = "127.0.0.1";   // Server host
const int    PORT = 7777;           // Server port
const string LINE_END = "\r\n";   // Protocol line terminator
const string CYPHER = "cypher";    // Initial mode token

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
    string out; out.reserve(s.size());
    for (char c : s) { if (c == '\'') out += "''"; else out += c; }
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

// ---------------- Persistent Session Helpers ----------------

int startCypherSession(const string &graphId) {
    int sock = connectToServer();
    if (sock < 0) return -1;

    // Send 'cypher'
    string cmd = CYPHER + LINE_END;
    if (send(sock, cmd.c_str(), cmd.size(), 0) < 0) {
        cerr << "[ERROR] Failed to send CYPHER command" << endl;
        close(sock);
        return -1;
    }
    this_thread::sleep_for(chrono::milliseconds(50));
    char tmp[1024]; memset(tmp, 0, sizeof(tmp));
    recv(sock, tmp, sizeof(tmp), 0); // discard banner/ack (best effort)

    // Send graphId
    string gid = graphId + LINE_END;
    if (send(sock, gid.c_str(), gid.size(), 0) < 0) {
        cerr << "[ERROR] Failed to send graphId" << endl;
        close(sock);
        return -1;
    }
    memset(tmp, 0, sizeof(tmp));
    recv(sock, tmp, sizeof(tmp), 0); // discard ack (best effort)

    return sock;
}

// Execute a single query on an already open session, returning collected response lines
bool executeQueryOnSession(int sock, const string &query, string &responseOut) {
    responseOut.clear();
    string q = query + LINE_END;
    if (send(sock, q.c_str(), q.size(), 0) < 0) {
        cerr << "[ERROR] Failed to send query: " << query << endl;
        return false;
    }

    char buffer[8192];
    string accum;
    while (true) {
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            if (bytes < 0) cerr << "[ERROR] recv() failed during query: " << query << endl;
            break; // connection closed or error
        }
        accum.append(buffer, bytes);

        // Check if we have a full 'done' line.
        size_t searchStart = 0;
        bool doneFound = false;
        while (true) {
            size_t nl = accum.find('\n', searchStart);
            if (nl == string::npos) break;
            size_t lineStart = (searchStart == 0 ? 0 : searchStart);
            string line = accum.substr(lineStart, nl - lineStart);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line == "done") { doneFound = true; break; }
            searchStart = nl + 1;
        }
        if (doneFound) break;
    }
    responseOut = std::move(accum);
    return true;
}

// ---------------- Validation Functions (Persistent Session) ----------------

void validateNodeLabel(int sock, const string &label, int expectedCount) {
    cout << "[INFO] Validating node label: " << label << " (expected: " << expectedCount << ")\n";
    auto start = chrono::steady_clock::now();

    string query = "MATCH(n:" + label + ") RETURN n";
    string response;
    if (!executeQueryOnSession(sock, query, response)) {
        cerr << "[ERROR] Query failed for node label: " << label << endl;
        return;
    }

    int count = 0; string line; istringstream stream(response);
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

void validateRelationshipLabel(int sock, const string &label, int expectedCount) {
    cout << "[INFO] Validating relationship label: " << label << " (expected: " << expectedCount << ")\n";
    auto start = chrono::steady_clock::now();

    string query = "MATCH(n)-[r:" + label + "]-(m) RETURN r"; // undirected style
    string response;
    if (!executeQueryOnSession(sock, query, response)) {
        cerr << "[ERROR] Query failed for relationship label: " << label << endl;
        return;
    }

    int count = 0; string line; istringstream stream(response);
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

void validateRelationshipSamples(int sock) {
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
            string response;
            if (!executeQueryOnSession(sock, query, response)) {
                cerr << "[ERROR] Sample query send failed for label " << label << endl;
                continue;
            }
            auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();

            bool found = false; string line; istringstream stream(response);
            while (getline(stream, line)) {
                if (line == "done\r" || line == "done" || line == "\r" || line.empty()) continue;
                try {
                    line = regex_replace(line, regex(R"(,$)"), "");
                    json obj = json::parse(line);
                    if (obj.contains("n") && obj.contains("x")) {
                        string nId = safeExtractId(obj["n"]);
                        string xId = safeExtractId(obj["x"]);
                        if ((nId == s.srcId && xId == s.dstId) || (nId == s.dstId && xId == s.srcId)) { found = true; break; }
                    }
                } catch (...) { /* ignore malformed sample line */ }
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

// ---------------- Missing ID Reports ----------------

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

// ---------------- Main ----------------

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

    // Start single persistent session
    int sessionSock = startCypherSession(graphId);
    if (sessionSock < 0) {
        cerr << "[FATAL] Could not start Cypher session. Exiting." << endl;
        return 1;
    }

    // Validate node labels in the same session
    for (const auto &label : nodeLabels) {
        validateNodeLabel(sessionSock, label, nodeLabelCounts[label]);
    }

    // Validate relationship labels (counts)
    for (const auto &label : relLabels) {
        validateRelationshipLabel(sessionSock, label, relLabelCounts[label]);
    }

    // Validate specific sample relationships
    validateRelationshipSamples(sessionSock);

    // Close session now that all queries are done
    close(sessionSock);

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
