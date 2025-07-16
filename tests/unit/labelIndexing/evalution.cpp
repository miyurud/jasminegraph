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

using json = nlohmann::json;
using namespace std;

const string HOST = "127.0.0.1";
const int PORT = 7777;
const string LINE_END = "\r\n";
const string CYPHER = "cypher";

string safeExtractId(const json &obj) {
    try {
        if (!obj.contains("id")) return "";
        if (obj["id"].is_string()) return obj["id"].get<string>();
        if (obj["id"].is_number_integer()) return to_string(obj["id"].get<int>());
    } catch (const exception &e) {
        cerr << "[ERROR] Failed to extract ID: " << e.what() << endl;
    }
    return "";
}

set<string> extractAllLabels(const string &graphPath, set<string> &relLabels) {
    cout << "[INFO] Extracting all labels from graph: " << graphPath << endl;
    set<string> nodeLabels;
    ifstream file(graphPath);
    string line;

    if (!file.is_open()) {
        cerr << "[ERROR] Failed to open file: " << graphPath << endl;
        return {};
    }

    while (getline(file, line)) {
        try {
            if (line.empty()) continue;
            auto entry = json::parse(line);
            string srcLabel = entry["source"]["properties"].value("label", "");
            string dstLabel = entry["destination"]["properties"].value("label", "");
            string relLabel = entry["properties"].value("type", "");

            if (!srcLabel.empty()) nodeLabels.insert(srcLabel);
            if (!dstLabel.empty()) nodeLabels.insert(dstLabel);
            if (!relLabel.empty()) relLabels.insert(relLabel);
        } catch (const exception &e) {
            cerr << "[ERROR] Failed to parse line in label extraction: " << e.what() << endl;
            cerr << "[LINE] " << line << endl;
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
        cerr << "[ERROR] Connection failed\n";
        return -1;
    }

    return sock;
}

string recvUntilDone(int sock) {
    string response;
    char buffer[4096];
    while (true) {
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes <= 0) break;
        response.append(buffer, bytes);
        if (response.find("done") != string::npos) break;
    }
    return response;
}
void validateNodeLabel(const string &graphId, const string &label) {
    cout << "[INFO] Validating node label: " << label << endl;

    auto start = chrono::steady_clock::now();

    int sock = connectToServer();
    if (sock < 0) return;

    send(sock, (CYPHER + LINE_END).c_str(), CYPHER.size() + LINE_END.size(), 0);
    this_thread::sleep_for(chrono::milliseconds(100));
    recv(sock, new char[1024], 1024, 0);
    send(sock, (graphId + LINE_END).c_str(), graphId.size() + LINE_END.size(), 0);
    recv(sock, new char[1024], 1024, 0);

    string query = "MATCH(n:" + label + ") RETURN n" + LINE_END;
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
            cerr << "[ERROR] Failed to parse response line: " << e.what() << endl;
            cerr << "[LINE] " << line << endl;
        }
    }

    auto end = chrono::steady_clock::now();
    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();

    if (count > 0) {
        cout << "[SUCCESS] ✅ Found " << count << " nodes with label '" << label << "'\n";
    } else {
        cerr << "[WARN] ⚠️  No nodes found for label '" << label << "'\n";
    }

    cout << "[TIME] Node label '" << label << "' query took " << elapsed_ms << " ms\n";
}
void validateRelationshipLabel(const string &graphId, const string &label) {
    cout << "[INFO] Validating relationship label: " << label << endl;

    auto start = chrono::steady_clock::now();

    int sock = connectToServer();
    if (sock < 0) return;

    send(sock, (CYPHER + LINE_END).c_str(), CYPHER.size() + LINE_END.size(), 0);
    this_thread::sleep_for(chrono::milliseconds(100));
    recv(sock, new char[1024], 1024, 0);
    send(sock, (graphId + LINE_END).c_str(), graphId.size() + LINE_END.size(), 0);
    recv(sock, new char[1024], 1024, 0);

    string query = "MATCH (n)-[r:" + label + "]-(m) RETURN r" + LINE_END;
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
            cerr << "[ERROR] Failed to parse response line: " << e.what() << endl;
            cerr << "[LINE] " << line << endl;
        }
    }

    auto end = chrono::steady_clock::now();
    auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();

    if (count > 0) {
        cout << "[SUCCESS] ✅ Found " << count << " relationships with label '" << label << "'\n";
    } else {
        cerr << "[WARN] ⚠️  No relationships found for label '" << label << "'\n";
    }

    cout << "[TIME] Relationship label '" << label << "' query took " << elapsed_ms << " ms\n";
}

int main(int argc, char *argv[]) {
    string graphPath = "/home/ubuntu/software/jasminegraph/tests/integration/env_init/data/graph_with_properties_test2.txt";
    string graphId = "8";

    if (argc > 1) {
        graphPath = argv[1];
        if (argc > 2) {
            graphId = argv[2];
        }
    }

    set<string> relLabels;
    auto nodeLabels = extractAllLabels(graphPath, relLabels);

    cout << "[INFO] Found node labels: ";
    for (const auto &l : nodeLabels) cout << l << " ";
    cout << "\n";

    cout << "[INFO] Found relationship labels: ";
    for (const auto &l : relLabels) cout << l << " ";
    cout << "\n";


    for (const auto &label : nodeLabels) {
        validateNodeLabel(graphId, label);
    }


    for (const auto &label : relLabels) {
        validateRelationshipLabel(graphId, label);
    }

    return 0;
}
