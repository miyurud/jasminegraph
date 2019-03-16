/**
Copyright 2019 JasminGraph Team
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

#include <sstream>
#include <ctime>
#include <chrono>
#include "JasmineGraphFrontEnd.h"
#include "../util/Conts.h"
#include "../util/Utils.h"
#include "JasmineGraphFrontEndProtocol.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../partitioner/local/MetisPartitioner.h"
#include "../partitioner/local/RDFPartitioner.h"
#include "../util/logger/Logger.h"
#include "../server/JasmineGraphServer.h"

using namespace std;

static int connFd;
Logger frontend_logger;

void *frontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *) dummyPt;
    frontend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    char data[300];
    bzero(data, 301);
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(sessionargs->connFd, data, 300);

        string line(data);
        frontend_logger.log("Command received: " + line, "info");

        Utils utils;
        line = utils.trim_copy(line, " \f\n\r\t\v");

        if (line.compare(EXIT) == 0) {
            break;
        } else if (line.compare(LIST) == 0) {
            SQLiteDBInterface *sqlite = &sessionargs->sqlite;
            std::stringstream ss;
            std::vector<vector<pair<string, string>>> v = sqlite->runSelect(
                    "SELECT idgraph, name, upload_path FROM graph;");
            for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
                ss << "|";
                for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
                    ss << j->second << "|";
                }
                ss << "\n";
            }
            string result = ss.str();
            write(sessionargs->connFd, result.c_str(), result.length());

        } else if (line.compare(SHTDN) == 0) {
            close(sessionargs->connFd);
            exit(0);
        } else if (line.compare(ADRDF) == 0) {
            // add RDF graph
            write(sessionargs->connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(sessionargs->connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[300];
            bzero(graph_data, 301);
            string name = "";
            string path = "";

            read(sessionargs->connFd, graph_data, 300);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                frontend_logger.log("Message format not recognized", "error");
                break;
            }

            name = strArr[0];
            path = strArr[1];

            if (JasmineGraphFrontEnd::graphExists(path, dummyPt)) {
                frontend_logger.log("Graph exists", "error");
                break;
            }

            if (utils.fileExists(path)) {
                std::cout << "Path exists" << endl;

                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + path +
                        "\", \"" + uploadStartTime + "\", \"\",\"UPLOADING\", \"\", \"\", \"\")";
                int newGraphID = sqlite->runInsert(sqlStatement);
                RDFPartitioner *rdfPartitioner = new RDFPartitioner(&sessionargs->sqlite);
                rdfPartitioner->loadDataSet(path, utils.getJasmineGraphProperty(
                        "org.jasminegraph.server.runtime.location").c_str(), newGraphID);

            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }

        } else if (line.compare(ADGR) == 0) {
            write(sessionargs->connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(sessionargs->connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[300];
            bzero(graph_data, 301);
            string name = "";
            string path = "";

            read(sessionargs->connFd, graph_data, 300);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                frontend_logger.log("Message format not recognized", "error");
                break;
            }

            name = strArr[0];
            path = strArr[1];

            if (JasmineGraphFrontEnd::graphExists(path, dummyPt)) {
                frontend_logger.log("Graph exists" ,"error");
                break;
            }

            if (utils.fileExists(path)) {
                std::cout << "Path exists" << endl;

                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + path +
                        "\", \"" + uploadStartTime + "\", \"\",\"UPLOADING\", \"\", \"\", \"\")";
                //int newGraphID = sqlite->runInsert(sqlStatement);
                int newGraphID = 3;
                MetisPartitioner *partitioner = new MetisPartitioner(&sessionargs->sqlite);
                //partitioner->loadDataSet(path, utils.getJasmineGraphProperty("org.jasminegraph.server.runtime.location").c_str());
                partitioner->loadDataSet(path, newGraphID);

                partitioner->constructMetisFormat();
                partitioner->partitioneWithGPMetis();
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                jasmineServer->uploadGraphLocally(newGraphID);
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }
        } else {
            frontend_logger.log("Message format not recognized", "error");
        }
    }
    frontend_logger.log("Closing thread " + to_string(pthread_self()) + " and connection", "info");
    close(sessionargs->connFd);
}

JasmineGraphFrontEnd::JasmineGraphFrontEnd(SQLiteDBInterface db) {
    this->sqlite = db;
}

int JasmineGraphFrontEnd::run() {
    int pId;
    int portNo = Conts::JASMINEGRAPH_FRONTEND_PORT;;
    int listenFd;
    socklen_t len;
    bool loop = false;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    //TODO: This seems there is only 3 front end instances can be kept running once. Need to double check this.
    pthread_t threadA[3];

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenFd < 0) {
        frontend_logger.log("Cannot open socket", "error");
        return 0;
    }

    bzero((char *) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(portNo);

    int yes = 1;

    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if (bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0) {
        frontend_logger.log("Cannot bind", "error");
        return 0;
    }

    listen(listenFd, 5);

    len = sizeof(clntAdd);

    int noThread = 0;

    while (noThread < 3) {
        frontend_logger.log("Frontend Listening", "info");

        //this is where client connects. svr will hang in this mode until client conn
        connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

        if (connFd < 0) {
            frontend_logger.log("Cannot accept connection", "error");
            return 0;
        } else {
            frontend_logger.log("Connection successful", "info");
        }

        struct frontendservicesessionargs frontendservicesessionargs1;
        frontendservicesessionargs1.sqlite = this->sqlite;
        frontendservicesessionargs1.connFd = connFd;


        pthread_create(&threadA[noThread], NULL, frontendservicesesion,
                       &frontendservicesessionargs1);

        noThread++;
    }

    for (int i = 0; i < 3; i++) {
        pthread_join(threadA[i], NULL);
    }


}

/**
 * This method checks if a graph exists in JasmineGraph.
 * This method uses the unique path of the graph.
 * @param basic_string
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::graphExists(string path, void *dummyPt) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE upload_path LIKE '" + path + "';";
    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

/**
 * This method checks if a graph exists in JasmineGraph with the same unique ID.
 * @param id
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::graphExistsByID(string id, void *dummyPt) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + id + "';";
    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}
