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
#include "../util/kafka/KafkaCC.h"
#include "JasmineGraphFrontEndProtocol.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../partitioner/local/MetisPartitioner.h"
#include "../partitioner/stream/Partitioner.h"
#include "../partitioner/local/RDFPartitioner.h"
#include "../util/logger/Logger.h"
#include "../server/JasmineGraphServer.h"
#include "../partitioner/local/RDFParser.h"
#include "../partitioner/local/JSONParser.h"


using namespace std;

static int connFd;
Logger frontend_logger;

void *frontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *) dummyPt;
    frontend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    int connFd = sessionargs->connFd;
    char data[300];
    bzero(data, 301);
    bool loop = false;
    while (!loop) {
        bzero(data, 301);
        read(connFd, data, 300);

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
            write(connFd, result.c_str(), result.length());

        } else if (line.compare(SHTDN) == 0) {
            close(connFd);
            exit(0);
        } else if (line.compare(ADRDF) == 0) {

            // add RDF graph
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[300];
            bzero(graph_data, 301);
            string name = "";
            string path = "";

            read(connFd, graph_data, 300);

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
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite->runInsert(sqlStatement);

                GetConfig appConfig;
                appConfig.readConfigFile(path, newGraphID);

                MetisPartitioner *metisPartitioner = new MetisPartitioner(&sessionargs->sqlite);
                vector<std::map<int,string>> fullFileList;
                string input_file_path = utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID) + "/" +
                                         to_string(newGraphID);
                metisPartitioner->loadDataSet(input_file_path, newGraphID);

                metisPartitioner->constructMetisFormat(Conts::GRAPH_TYPE_RDF);
                fullFileList = metisPartitioner->partitioneWithGPMetis();
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));

            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }

        } else if (line.compare(ADGR) == 0) {
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[300];
            bzero(graph_data, 301);
            string name = "";
            string path = "";

            read(connFd, graph_data, 300);

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
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite->runInsert(sqlStatement);
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                MetisPartitioner *partitioner = new MetisPartitioner(&sessionargs->sqlite);
                vector<std::map<int,string>> fullFileList;

                partitioner->loadDataSet(path, newGraphID);
                int result = partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
                if (result == 0) {
                    string reformattedFilePath = partitioner->reformatDataSet(path, newGraphID);
                    partitioner->loadDataSet(reformattedFilePath, newGraphID);
                    partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
                    fullFileList = partitioner->partitioneWithGPMetis();
                } else {
                    fullFileList = partitioner->partitioneWithGPMetis();
                }
                frontend_logger.log("Upload done", "info");
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_TYPE_NORMAL, fullFileList);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }
        } else if (line.compare(ADGR_CUST) == 0) {
            string message = "Select a custom graph upload option\n";
            write(connFd, message.c_str(), message.size());
            write(connFd, Conts::GRAPH_WITH::TEXT_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::TEXT_ATTRIBUTES.size());
            write(connFd, "\n", 2);
            write(connFd, Conts::GRAPH_WITH::JSON_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::TEXT_ATTRIBUTES.size());
            write(connFd, "\n", 2);
            write(connFd, Conts::GRAPH_WITH::XML_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::TEXT_ATTRIBUTES.size());
            write(connFd, "\n", 2);

            //TODO :: Handle user inputting a value out of range
            char type[20];
            bzero(type, 21);
            read(connFd, type, 20);
            string graphType(type);

            // We get the name and the path to graph edge list and attribute list as a triplet separated by | .
            // (<name>|<path to edge list>|<path to attribute file>)
            message = "Send <name>|<path to edge list>|<path to attribute file>\n";
            write(connFd, message.c_str(), message.size());
            char graph_data[300];
            bzero(graph_data, 301);
            string name = "";
            string edgeListPath = "";
            string attributeListPath = "";

            read(connFd, graph_data, 300);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 3) {
                frontend_logger.log("Message format not recognized", "error");
                break;
            }

            name = strArr[0];
            edgeListPath = strArr[1];
            attributeListPath = strArr[2];

            if (JasmineGraphFrontEnd::graphExists(edgeListPath, dummyPt)) {
                frontend_logger.log("Graph exists", "error");
                break;
            }

            if (utils.fileExists(edgeListPath) && utils.fileExists(attributeListPath)) {
                std::cout << "Paths exists" << endl;

                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + edgeListPath +
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite->runInsert(sqlStatement);
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                MetisPartitioner *partitioner = new MetisPartitioner(&sessionargs->sqlite);
                vector<std::map<int,string>> fullFileList;
                partitioner->loadContentData(attributeListPath, "1");
                partitioner->loadDataSet(edgeListPath, newGraphID);
                int result = partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
                if (result == 0) {
                    string reformattedFilePath = partitioner->reformatDataSet(edgeListPath, newGraphID);
                    partitioner->loadDataSet(reformattedFilePath, newGraphID);
                    partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
                    fullFileList = partitioner->partitioneWithGPMetis();
                } else {
                    fullFileList = partitioner->partitioneWithGPMetis();
                }
                //Graph type should be changed to identify graphs with attributes
                //because this graph type has additional attribute files to be uploaded
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }
        } else if (line.compare(ADD_STREAM_KAFKA) == 0) {
            frontend_logger.log("Start serving `" + ADD_STREAM_KAFKA + "` command", "info");
            string message = "send kafka topic name"; 
            write(connFd, message.c_str(), message.length());
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char topic_name[300];
            bzero(topic_name, 301);

            read(connFd, topic_name, 300);

            Utils utils;
            string topic_name_s(topic_name);
            topic_name_s = utils.trim_copy(topic_name_s, " \f\n\r\t\v");
            // After getting the topic name , need to close the connection and ask the user to send the data to given topic

            cppkafka::Configuration configs = {{"metadata.broker.list", "127.0.0.1:9092"},
                                               {"group.id",             "knnect"}};
            KafkaConnector kstream(configs);
            int numberOfPartitions = 4;
            Partitioner graphPartitioner(numberOfPartitions);

            kstream.Subscribe(topic_name_s);
            frontend_logger.log("Start listning to " + topic_name_s, "info");
            while (true) {
                cppkafka::Message msg = kstream.consumer.poll();
                if (!msg || msg.get_error()) {
                    continue;
                }
                string data(msg.get_payload());
                // cout << "Payload = " << data << endl;
                if (data == "-1") {  // Marks the end of stream
                    frontend_logger.log("Received the end of stream", "info");
                    break;
                }
                std::pair<long, long> edge = Partitioner::deserialize(data);
                frontend_logger.log("Received edge >> " + std::to_string(edge.first) + " --- " + std::to_string(edge.second) , "info");
                graphPartitioner.addEdge(edge);
            }
            graphPartitioner.printStats();

        } else if (line.compare(RMGR) == 0) {
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_id[300];
            bzero(graph_id, 301);
            string name = "";
            string path = "";

            read(connFd, graph_id, 300);

            string graphID(graph_id);

            Utils utils;
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            frontend_logger.log("Graph ID received: " + graphID, "info");

            if (JasmineGraphFrontEnd::graphExistsByID(graphID, dummyPt)) {
                frontend_logger.log("Graph with ID " + graphID + " is being deleted now", "info");
                JasmineGraphFrontEnd::removeGraph(graphID, dummyPt);
            } else {
                frontend_logger.log("Graph does not exist or cannot be deleted with the current hosts setting",
                                    "error");
            }

        } else if (line.compare(REFORMAT) == 0) {
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[300];
            bzero(graph_data, 301);


            read(connFd, graph_data, 300);

            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                frontend_logger.log("Message format not recognized", "error");
                break;
            }
            string name = strArr[0];
            string path = strArr[1];

            if (utils.fileExists(path)) {
                std::cout << "Path exists" << endl;

                JSONParser *jsonParser = new JSONParser();
                jsonParser->readFile(path);
                frontend_logger.log("Reformatted files created on /home/.jasminegraph/tmp/JSONParser/output", "info");


            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }
        } else {
            frontend_logger.log("Message format not recognized " + line, "error");
        }
    }
    frontend_logger.log("Closing thread " + to_string(pthread_self()) + " and connection", "info");
    close(connFd);
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

    listen(listenFd, 10);

    pthread_t threadA[20];
    len = sizeof(clntAdd);

    int noThread = 0;

    while (noThread < 20) {
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

    for (int i = 0; i < noThread; i++) {
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
    string stmt =
            "SELECT COUNT( * ) FROM graph WHERE upload_path LIKE '" + path + "' AND graph_status_idgraph_status = '" +
            to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

/**
 * This method checks if an accessible graph exists in JasmineGraph with the same unique ID.
 * @param id
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::graphExistsByID(string id, void *dummyPt) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph = " + id + " and graph_status_idgraph_status = " +
                  to_string(Conts::GRAPH_STATUS::OPERATIONAL);
    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

/**
 * This method removes a graph from JasmineGraph
 */
void JasmineGraphFrontEnd::removeGraph(std::string graphID, void *dummyPt) {
    vector<pair<string, string>> hostHasPartition;
    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    vector<vector<pair<string, string>>> hostPartitionResults = sqlite->runSelect(
            "SELECT name, partition_idpartition FROM host_has_partition INNER JOIN host ON host_idhost = idhost WHERE "
            "partition_graph_idgraph = '" + graphID + "'");
    for (vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin();
         i != hostPartitionResults.end(); ++i) {
        int count = 0;
        string hostname;
        string partitionID;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (count == 0) {
                hostname = j->second;
            } else {
                partitionID = j->second;
                hostHasPartition.push_back(pair<string, string>(hostname, partitionID));
            }
            count++;
        }
    }
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        cout << "HOST ID : " << j->first << " Partition ID : " << j->second << endl;
    }
    sqlite->runUpdate("UPDATE graph SET graph_status_idgraph_status = " + to_string(Conts::GRAPH_STATUS::DELETING) +
                      " WHERE idgraph = " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->removeGraph(hostHasPartition, graphID);

    sqlite->runUpdate("DELETE FROM host_has_partition WHERE partition_graph_idgraph = " + graphID);
    sqlite->runUpdate("DELETE FROM partition WHERE graph_idgraph = " + graphID);
    sqlite->runUpdate("DELETE FROM graph WHERE idgraph = " + graphID);
}

