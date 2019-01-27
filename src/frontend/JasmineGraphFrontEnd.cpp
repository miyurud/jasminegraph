/**
Copyright 2018 JasminGraph Team
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

using namespace std;

Utils utils;
static int connFd;

void *frontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *) dummyPt;
    cout << "Thread No: " << pthread_self() << endl;
    char data[300];
    bzero(data, 301);
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(sessionargs->connFd, data, 300);

        string line(data);
        cout << line << endl;

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
            std::cout << SEND << endl;
            write(sessionargs->connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(sessionargs->connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[300];
            bzero(graph_data, 301);
            string name = "";
            string path = "";

            read(sessionargs->connFd, graph_data, 300);
            string gData(graph_data);
            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            std::cout << "data received : " << gData << endl;

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                std::cout << ERROR << ":Message format not recognized" << endl;
                break;
            }

            name = strArr[0];
            path = strArr[1];

            if (JasmineGraphFrontEnd::graphExists(path, dummyPt)) {
                std::cout << ERROR << ":Graph exists" << endl;
                break;
            }

            if (utils.fileExists(path, sessionargs)) {
                std::cout << "Path exists" << endl;
                //call rdf partitioner
            } else {
                std::cout << ERROR << ":Graph data file does not exist on the specified path" << endl;
                break;
            }

        } else if (line.compare(ADGR) == 0) {
            std::cout << SEND << endl;
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
            std::cout << "data received : " << gData << endl;

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                std::cout << ERROR << ":Message format not recognized" << endl;
                break;
            }

            name = strArr[0];
            path = strArr[1];

            if (JasmineGraphFrontEnd::graphExists(path, dummyPt)) {
                std::cout << ERROR << ":Graph exists" << endl;
                break;
            }

            if (utils.fileExists(path, sessionargs)) {
                std::cout << "Path exists" << endl;

                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + path +
                        "\", \"" + uploadStartTime + "\", \"\",\"UPLOADING\", \"\", \"\", \"\")";
                int newGraphID = sqlite->runInsert(sqlStatement);
                MetisPartitioner *partitioner = new MetisPartitioner(&sessionargs->sqlite);
                //partitioner->loadDataSet(path, utils.getJasmineGraphProperty("org.jasminegraph.server.runtime.location").c_str());
                partitioner->loadDataSet(path, utils.getJasmineGraphProperty(
                        "org.jasminegraph.server.runtime.location").c_str(), newGraphID);

                partitioner->constructMetisFormat();
                partitioner->partitioneWithGPMetis();
            } else {
                std::cout << ERROR << ":Graph data file does not exist on the specified path" << endl;
                break;
            }
        } else {
            std::cout << ERROR << ":Message format not recognized" << endl;
        }
    }
    cout << "\nClosing thread " << pthread_self() << " and connection" << endl;
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
        cerr << "Cannot open socket" << endl;
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
        cerr << "Cannot bind" << endl;
        return 0;
    }

    listen(listenFd, 5);

    len = sizeof(clntAdd);

    int noThread = 0;

    while (noThread < 3) {
        cout << "Listening" << endl;

        //this is where client connects. svr will hang in this mode until client conn
        connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

        if (connFd < 0) {
            cerr << "Cannot accept connection" << endl;
            return 0;
        } else {
            cout << "Connection successful" << endl;
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
    std::cout << "No of columns  : " << count << endl;
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
    std::cout << "No of columns  : " << count << endl;
    if (count == 0) {
        result = false;
    }
    return result;
}

/**
 * This method checks if a file with the given path exists.
 * @param fileName
 * @return
 */
bool JasmineGraphFrontEnd::fileExists(const string fileName) {
    std::ifstream infile(fileName);
    return infile.good();
}