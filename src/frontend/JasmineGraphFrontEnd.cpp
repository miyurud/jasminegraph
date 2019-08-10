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
#include <iostream>
#include <map>
#include <set>
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
#include "../server/JasmineGraphInstanceProtocol.h"

using namespace std;

static int connFd;
Logger frontend_logger;

void *frontendservicesesion(void *dummyPt) {
    frontendservicesessionargs *sessionargs = (frontendservicesessionargs *) dummyPt;
    frontend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    int connFd = sessionargs->connFd;
    char data[FRONTEND_DATA_LENGTH];
    bzero(data, FRONTEND_DATA_LENGTH + 1);
    bool loop = false;
    while (!loop) {
        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(connFd, data, FRONTEND_DATA_LENGTH);

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
            char graph_data[FRONTEND_DATA_LENGTH];
            bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string path = "";

            read(connFd, graph_data, FRONTEND_DATA_LENGTH);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            name = strArr[0];
            path = strArr[1];

            if (JasmineGraphFrontEnd::graphExists(path, dummyPt)) {
                frontend_logger.log("Graph exists", "error");
                continue;
            }

            if (utils.fileExists(path)) {
                frontend_logger.log("Path exists", "info");

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
                vector<std::map<int, string>> fullFileList;
                string input_file_path = utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID) + "/" +
                                         to_string(newGraphID);
                metisPartitioner->loadDataSet(input_file_path, newGraphID);

                metisPartitioner->constructMetisFormat(Conts::GRAPH_TYPE_RDF);
                fullFileList = metisPartitioner->partitioneWithGPMetis();
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), dummyPt);
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }

        } else if (line.compare(ADGR) == 0) {
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[FRONTEND_DATA_LENGTH];
            bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string path = "";

            read(connFd, graph_data, FRONTEND_DATA_LENGTH);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 2) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            name = strArr[0];
            path = strArr[1];

            if (JasmineGraphFrontEnd::graphExists(path, dummyPt)) {
                frontend_logger.log("Graph exists", "error");
                continue;
            }

            if (utils.fileExists(path)) {
                frontend_logger.log("Path exists", "info");

                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + path +
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite->runInsert(sqlStatement);
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                MetisPartitioner *partitioner = new MetisPartitioner(&sessionargs->sqlite);
                vector<std::map<int, string>> fullFileList;

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
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), dummyPt);
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
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

            char type[20];
            bzero(type, 21);
            read(connFd, type, 20);
            string graphType(type);
            graphType = utils.trim_copy(graphType, " \f\n\r\t\v");

            std::unordered_set<std::string> s = {"1", "2", "3"};
            if (s.find(graphType) == s.end()) {
                frontend_logger.log("Graph type not recognized", "error");
                continue;
            }

            string graphAttributeType = "";
            if (graphType == "1") {
                graphAttributeType = Conts::GRAPH_WITH_TEXT_ATTRIBUTES;
            } else if (graphType == "2") {
                graphAttributeType = Conts::GRAPH_WITH_JSON_ATTRIBUTES;
            } else if (graphType == "3") {
                graphAttributeType = Conts::GRAPH_WITH_XML_ATTRIBUTES;
            }

            // We get the name and the path to graph edge list and attribute list as a triplet separated by | .
            // (<name>|<path to edge list>|<path to attribute file>)
            message = "Send <name>|<path to edge list>|<path to attribute file>\n";
            write(connFd, message.c_str(), message.size());
            char graph_data[FRONTEND_DATA_LENGTH];
            bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string edgeListPath = "";
            string attributeListPath = "";

            read(connFd, graph_data, FRONTEND_DATA_LENGTH);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 3) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            name = strArr[0];
            edgeListPath = strArr[1];
            attributeListPath = strArr[2];

            if (JasmineGraphFrontEnd::graphExists(edgeListPath, dummyPt)) {
                frontend_logger.log("Graph exists", "error");
                continue;
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
                vector<std::map<int, string>> fullFileList;
                partitioner->loadContentData(attributeListPath, graphAttributeType);
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
                //utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                //utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), dummyPt);
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }
        } else if (line.compare(ADD_STREAM_KAFKA) == 0) {
            frontend_logger.log("Start serving `" + ADD_STREAM_KAFKA + "` command", "info");
            string message = "send kafka topic name";
            write(connFd, message.c_str(), message.length());
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char topic_name[FRONTEND_DATA_LENGTH];
            bzero(topic_name, FRONTEND_DATA_LENGTH + 1);

            read(connFd, topic_name, FRONTEND_DATA_LENGTH);

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
                frontend_logger.log(
                        "Received edge >> " + std::to_string(edge.first) + " --- " + std::to_string(edge.second),
                        "info");
                graphPartitioner.addEdge(edge);
            }
            graphPartitioner.printStats();

        } else if (line.compare(RMGR) == 0){
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_id[FRONTEND_DATA_LENGTH];
            bzero(graph_id, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string path = "";

            read(connFd, graph_id, FRONTEND_DATA_LENGTH);

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

        } else if (line.compare(PROCESS_DATASET) == 0) {
            
        } else if (line.compare(TRIANGLES) == 0) {
            // add RDF graph
            write(connFd, GRAPHID_SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_id_data[300];
            bzero(graph_id_data, 301);
            string name = "";

            read(connFd, graph_id_data, 300);

            string graph_id(graph_id_data);
            graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\n'),
                           graph_id.end());
            graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\r'),
                           graph_id.end());

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id,dummyPt)) {
                string error_message = "The specified graph id does not exist";
                write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                write(connFd, "\r\n", 2);
            } else {
                auto begin = chrono::high_resolution_clock::now();
                vector<string> hostsList = utils.getHostList();
                int hostListLength = hostsList.size();
                long triangleCount = JasmineGraphFrontEnd::countTriangles(graph_id,dummyPt);
                auto end = chrono::high_resolution_clock::now();
                auto dur = end - begin;
                auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
                frontend_logger.log("Triangle Count: " + to_string(triangleCount),"info");
            }
        } else if (line.compare(VCOUNT) == 0) {
            write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
            write(connFd, "\r\n", 2);

            char graph_id_data[300];
            bzero(graph_id_data, 301);
            string name = "";

            read(connFd, graph_id_data, 300);

            string graph_id(graph_id_data);

            graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\n'),
                           graph_id.end());
            graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\r'),
                           graph_id.end());

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id,dummyPt)) {
                string error_message = "The specified graph id does not exist";
                write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                write(connFd, "\r\n", 2);
            } else {
                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement = "SELECT vertexcount from graph where idgraph=" + graph_id;

                std::vector<vector<pair<string, string>>> output = sqlite->runSelect(sqlStatement);

                int vertexCount = std::stoi(output[0][0].second);
                frontend_logger.log("Vertex Count: " + to_string(vertexCount),"info");
            }
        } else if (line.compare(ECOUNT) == 0) {
            write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
            write(connFd, "\r\n", 2);

            char graph_id_data[300];
            bzero(graph_id_data, 301);
            string name = "";

            read(connFd, graph_id_data, 300);

            string graph_id(graph_id_data);

            graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\n'),
                           graph_id.end());
            graph_id.erase(std::remove(graph_id.begin(), graph_id.end(), '\r'),
                           graph_id.end());

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id,dummyPt)) {
                string error_message = "The specified graph id does not exist";
                write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                write(connFd, "\r\n", 2);
            } else {
                SQLiteDBInterface *sqlite = &sessionargs->sqlite;
                string sqlStatement = "SELECT edgecount from graph where idgraph=" + graph_id;

                std::vector<vector<pair<string, string>>> output = sqlite->runSelect(sqlStatement);

                int vertexCount = std::stoi(output[0][0].second);
                frontend_logger.log("Edge Count: " + to_string(vertexCount),"info");
            }
        } else {
            frontend_logger.log("Command not recognized " + line, "error");
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
    bool result = false;
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


long JasmineGraphFrontEnd::countTriangles(std::string graphId, void *dummyPt) {
    long result= 0;
    Utils utils;
    vector<std::string> hostList = utils.getHostList();
    int hostListSize = hostList.size();
    int counter = 0;
    std::vector<std::future<long>> intermRes;
    map<std::string,std::future<std::string>> remoteCopyRes;
    PlacesToNodeMapper placesToNodeMapper;

    string sqlStatement = "SELECT name,partition_idpartition FROM host_has_partition INNER JOIN host ON host_idhost=idhost WHERE partition_graph_idgraph=" + graphId + ";";

    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    std::vector<vector<pair<string, string>>> results = sqlite->runSelect(sqlStatement);

    std::map<string, std::vector<string>> map;

    // Implement Logic to Decide the Worker node which Acts as the centralstore triangle count aggregator
    std::string aggregatorWorker = "localhost@7780";
    std::string aggregatorWorkerHost;
    std::string aggregatorWorkerPort;
    std::string aggregatorPartitionId;

    if (aggregatorWorker.find('@') != std::string::npos) {
        aggregatorWorkerHost = utils.split(aggregatorWorker, '@')[0];
        aggregatorWorkerPort = utils.split(aggregatorWorker, '@')[1];
    }

    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        string name = rowData.at(0).second;
        string partitionId = rowData.at(1).second;

        std::vector<string> partitionList = map[name];

        partitionList.push_back(partitionId);

        map[name] = partitionList;
    }

    for (int i=0;i<hostListSize;i++) {
        int k = counter;
        string host = placesToNodeMapper.getHost(i);
        std::vector<int> instancePorts = placesToNodeMapper.getInstancePort(i);
        string partitionId;

        std::vector<string> partitionList = map[host];

        std::vector<int>::iterator portsIterator;

        for (portsIterator = instancePorts.begin(); portsIterator != instancePorts.end(); ++portsIterator) {
            int port = *portsIterator;

            if (partitionList.size() > 0) {
                auto iterator = partitionList.begin();
                partitionId = *iterator;
                partitionList.erase(partitionList.begin());
            }

            intermRes.push_back(std::async(std::launch::async,JasmineGraphFrontEnd::getTriangleCount,atoi(graphId.c_str()),host,port,atoi(partitionId.c_str())));
            if (!(aggregatorWorkerHost == host && aggregatorWorkerPort == std::to_string(port))) {
                remoteCopyRes.insert(std::make_pair(host,std::async(std::launch::async, JasmineGraphFrontEnd::copyCentralStoreToAggregator, aggregatorWorkerHost,aggregatorWorkerPort,host,std::to_string(port),atoi(graphId.c_str()),atoi(partitionId.c_str()))));
            } else {
                aggregatorPartitionId = partitionId;
            }

        }

    }


    for (auto&& futureCall:intermRes) {
        result += futureCall.get();
    }



    long aggregatedTriangleCount = JasmineGraphFrontEnd::countCentralStoreTriangles(aggregatorWorkerHost,aggregatorWorkerPort,aggregatorWorker,aggregatorPartitionId,graphId);
    result += aggregatedTriangleCount;
    return result;
}


long JasmineGraphFrontEnd::getTriangleCount(int graphId, std::string host, int port, int partitionId) {

    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;
    long triangleCount;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[0];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        frontend_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::TRIANGLES.c_str(),
              JasmineGraphInstanceProtocol::TRIANGLES.size());
        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::TRIANGLES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());
            frontend_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());
            frontend_logger.log("Sent : Partition ID " + std::to_string(partitionId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
            triangleCount = atol(response.c_str());
            return triangleCount;
        }
    } else {
        frontend_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }

}


std::string JasmineGraphFrontEnd::copyCentralStoreToAggregator(std::string aggregatorHostName,
                                                               std::string aggregatorPort, std::string host,
                                                               std::string port, int graphId, int partitionId) {
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[0];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(atoi(port.c_str()));
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        frontend_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.c_str(),
              JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.size());
        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());
            frontend_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());
            frontend_logger.log("Sent : Partition ID " + std::to_string(partitionId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, aggregatorHostName.c_str(), aggregatorHostName.size());
            frontend_logger.log("Sent : Aggregator Host Name " + aggregatorHostName, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, aggregatorPort.c_str(), aggregatorPort.size());
            frontend_logger.log("Sent : Aggregator Port " + aggregatorPort, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, host.c_str(), host.size());
            frontend_logger.log("Sent : Host " + host, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }
    } else {
        frontend_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }
    return response;
}


long JasmineGraphFrontEnd::countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort, std::string host, std::string partitionId, std::string graphId) {
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (host.find('@') != std::string::npos) {
        host = utils.split(host, '@')[0];
    }

    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(atoi(aggregatorPort.c_str()));
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());
    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        string server_host = utils.getJasmineGraphProperty("org.jasminegraph.server.host");
        write(sockfd, server_host.c_str(), server_host.size());
        frontend_logger.log("Sent : " + server_host, "info");

        write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.c_str(),
              JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.size());
        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, graphId.c_str(), graphId.size());
            frontend_logger.log("Sent : Graph ID " + graphId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            write(sockfd, partitionId.c_str(), partitionId.size());
            frontend_logger.log("Sent : Partition ID " + partitionId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }


    } else {
        frontend_logger.log("There was an error in the upload process and the response is :: " + response, "error");
    }
    return atol(response.c_str());
}

void JasmineGraphFrontEnd::getAndUpdateUploadTime(std::string graphID, void *dummyPt) {
    SQLiteDBInterface *sqlite = (SQLiteDBInterface *) dummyPt;
    struct tm tm;
    vector<vector<pair<string, string>>> uploadStartFinishTimes = sqlite->runSelect(
            "SELECT upload_start_time,upload_end_time FROM graph WHERE idgraph = '" + graphID + "'");
    string startTime = uploadStartFinishTimes[0][0].second;
    string endTime = uploadStartFinishTimes[0][1].second;
    string sTime = startTime.substr(startTime.size()-14,startTime.size()-5);
    string eTime = endTime.substr(startTime.size()-14,startTime.size()-5);
    strptime(sTime.c_str(), "%H:%M:%S", &tm);
    time_t start = mktime(&tm);
    strptime(eTime.c_str(), "%H:%M:%S", &tm);
    time_t end = mktime(&tm);
    double difTime = difftime(end,start);
    sqlite->runUpdate("UPDATE graph SET upload_time = " + to_string(difTime) + " WHERE idgraph = " + graphID);
    frontend_logger.log("Upload time updated in the database" , "info");
}
