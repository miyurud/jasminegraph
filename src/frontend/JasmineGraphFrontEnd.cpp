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
#include "../trainer/python-c-api/Python_C_API.h"
#include "../trainer/JasminGraphTrainingInitiator.h"
#include "../trainer/JasminGraphLinkPredictor.h"
#include "../trainer/JasmineGraphTrainingSchedular.h"


#include "../trainer/python-c-api/Python_C_API.h"
#include "../trainer/JasminGraphTrainingInitiator.h"
#include "../trainer/JasminGraphLinkPredictor.h"

using namespace std;

static int connFd;
Logger frontend_logger;

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface sqlite) {
    frontend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    frontend_logger.log("Master IP: " + masterIP, "info");
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
           std::stringstream ss;
            std::vector<vector<pair<string, string>>> v = sqlite.runSelect(
                    "SELECT idgraph, name, upload_path FROM graph;");
            for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
                ss << "|";
                int counter = 0;
                for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
                    if (counter == 3) {
                        if (std::stoi(j->second) == Conts::GRAPH_STATUS::LOADING) {
                                ss << "loading|";
                        } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::DELETING) {
                            ss << "deleting|";
                        } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::NONOPERATIONAL) {
                            ss << "nop|";
                        } else if (std::stoi(j->second) == Conts::GRAPH_STATUS::OPERATIONAL) {
                            ss << "op|";
                        }
                    } else {
                        ss << j->second << "|";
                    }
                    counter++;
                }
                ss << "\n";
            }
            string result = ss.str();
            write(connFd, result.c_str(), result.length());

        } else if (line.compare(SHTDN) == 0) {
            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            jasmineServer->shutdown_workers();
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

            if (JasmineGraphFrontEnd::graphExists(path, sqlite)) {
                frontend_logger.log("Graph exists", "error");
                continue;
            }

            if (utils.fileExists(path)) {
                frontend_logger.log("Path exists", "info");

                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + path +
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite.runInsert(sqlStatement);

                GetConfig appConfig;
                appConfig.readConfigFile(path, newGraphID);

                MetisPartitioner *metisPartitioner = new MetisPartitioner(&sqlite);
                vector<std::map<int, string>> fullFileList;
                string input_file_path = utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID) + "/" +
                                         to_string(newGraphID);
                metisPartitioner->loadDataSet(input_file_path, newGraphID);

                metisPartitioner->constructMetisFormat(Conts::GRAPH_TYPE_RDF);
                fullFileList = metisPartitioner->partitioneWithGPMetis("");
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList,masterIP);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }

        } else if (line.compare(ADGR) == 0) {
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[FRONTEND_DATA_LENGTH];
            char partition_count[FRONTEND_DATA_LENGTH];
            bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string path = "";
            string partitionCount = "";

            read(connFd, graph_data, FRONTEND_DATA_LENGTH);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() < 2) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            name = strArr[0];
            path = strArr[1];

            if (strArr.size() == 3) {
                partitionCount = strArr[2];
            }

            if (JasmineGraphFrontEnd::graphExists(path, sqlite)) {
                frontend_logger.log("Graph exists", "error");
                continue;
            }

            if (utils.fileExists(path)) {
                frontend_logger.log("Path exists", "info");

                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + path +
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite.runInsert(sqlStatement);
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                MetisPartitioner *partitioner = new MetisPartitioner(&sqlite);
                vector<std::map<int, string>> fullFileList;

                partitioner->loadDataSet(path, newGraphID);
                int result = partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
                if (result == 0) {
                    string reformattedFilePath = partitioner->reformatDataSet(path, newGraphID);
                    partitioner->loadDataSet(reformattedFilePath, newGraphID);
                    partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
                    fullFileList = partitioner->partitioneWithGPMetis(partitionCount);
                } else {

                    fullFileList = partitioner->partitioneWithGPMetis(partitionCount);
                }
                frontend_logger.log("Upload done", "info");
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_TYPE_NORMAL, fullFileList, masterIP);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
                write(connFd, DONE.c_str(), DONE.size());
                write(connFd, "\n", 2);
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

            if (JasmineGraphFrontEnd::graphExists(edgeListPath, sqlite)) {
                frontend_logger.log("Graph exists", "error");
                continue;
            }

            if (utils.fileExists(edgeListPath) && utils.fileExists(attributeListPath)) {
                std::cout << "Paths exists" << endl;

                string sqlStatement =
                        "INSERT INTO graph (name,upload_path,upload_start_time,upload_end_time,graph_status_idgraph_status,"
                        "vertexcount,centralpartitioncount,edgecount) VALUES(\"" + name + "\", \"" + edgeListPath +
                        "\", \"" + uploadStartTime + "\", \"\",\"" + to_string(Conts::GRAPH_STATUS::LOADING) +
                        "\", \"\", \"\", \"\")";
                int newGraphID = sqlite.runInsert(sqlStatement);
                JasmineGraphServer *jasmineServer = new JasmineGraphServer();
                MetisPartitioner *partitioner = new MetisPartitioner(&sqlite);
                vector<std::map<int, string>> fullFileList;
                partitioner->loadContentData(attributeListPath, graphAttributeType, newGraphID);
                partitioner->loadDataSet(edgeListPath, newGraphID);
                int result = partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL);
                if (result == 0) {
                    string reformattedFilePath = partitioner->reformatDataSet(edgeListPath, newGraphID);
                    partitioner->loadDataSet(reformattedFilePath, newGraphID);
                    partitioner->constructMetisFormat(Conts::GRAPH_TYPE_NORMAL_REFORMATTED);
                    fullFileList = partitioner->partitioneWithGPMetis("");
                } else {
                    fullFileList = partitioner->partitioneWithGPMetis("");
                }
                //Graph type should be changed to identify graphs with attributes
                //because this graph type has additional attribute files to be uploaded
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList, masterIP);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
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

        } else if (line.compare(RMGR) == 0) {
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

            if (JasmineGraphFrontEnd::graphExistsByID(graphID, sqlite)) {
                frontend_logger.log("Graph with ID " + graphID + " is being deleted now", "info");
                JasmineGraphFrontEnd::removeGraph(graphID, sqlite, masterIP);
                write(connFd, DONE.c_str(), DONE.size());
                write(connFd, "\n", 2);
            } else {
                frontend_logger.log("Graph does not exist or cannot be deleted with the current hosts setting",
                                    "error");
                write(connFd, ERROR.c_str(), ERROR.size());
                write(connFd, "\n", 2);
            }

        } else if (line.compare(PROCESS_DATASET) == 0) {
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            // We get the name and the path to graph as a pair separated by |.
            char graph_data[FRONTEND_DATA_LENGTH];
            bzero(graph_data, FRONTEND_DATA_LENGTH + 1);


            read(connFd, graph_data, FRONTEND_DATA_LENGTH);

            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            if (gData.length() == 0) {
                frontend_logger.log("Message format not recognized", "error");
                break;
            }
            string path = gData;


            if (utils.fileExists(path)) {
                frontend_logger.log("Path exists", "info");

                JSONParser *jsonParser = new JSONParser();
                jsonParser->jsonParse(path);
                frontend_logger.log("Reformatted files created on /home/.jasminegraph/tmp/JSONParser/output", "info");


            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }
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

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id,sqlite)) {
                string error_message = "The specified graph id does not exist";
                write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                write(connFd, "\r\n", 2);
            } else {
                auto begin = chrono::high_resolution_clock::now();
                long triangleCount = JasmineGraphFrontEnd::countTriangles(graph_id,sqlite,masterIP);
                auto end = chrono::high_resolution_clock::now();
                auto dur = end - begin;
                auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
                frontend_logger.log("Triangle Count: " + to_string(triangleCount), "info");
                write(connFd, to_string(triangleCount).c_str(), (int)to_string(triangleCount).length());
                write(connFd, "\r\n", 2);
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

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id,sqlite)) {
                string error_message = "The specified graph id does not exist";
                write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                write(connFd, "\r\n", 2);
            } else {
                string sqlStatement = "SELECT vertexcount from graph where idgraph=" + graph_id;

                std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);

                int vertexCount = std::stoi(output[0][0].second);
                frontend_logger.log("Vertex Count: " + to_string(vertexCount), "info");
                write(connFd, to_string(vertexCount).c_str(), to_string(vertexCount).length());
                write(connFd, "\r\n", 2);
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

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id,sqlite)) {
                string error_message = "The specified graph id does not exist";
                write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                write(connFd, "\r\n", 2);
            } else {
                string sqlStatement = "SELECT edgecount from graph where idgraph=" + graph_id;

                std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);

                int edgeCount = std::stoi(output[0][0].second);
                frontend_logger.log("Edge Count: " + to_string(edgeCount), "info");
                write(connFd, to_string(edgeCount).c_str(), to_string(edgeCount).length());
                write(connFd, "\r\n", 2);
            }
        } else if (line.compare(TRAIN) == 0) {
            string message = "Available main flags:\n";
            write(connFd, message.c_str(), message.size());
            string flags =
                    Conts::FLAGS::GRAPH_ID + " " + Conts::FLAGS::LEARNING_RATE + " " + Conts::FLAGS::BATCH_SIZE + " " +
                    Conts::FLAGS::VALIDATE_ITER + " " + Conts::FLAGS::EPOCHS;
            write(connFd, flags.c_str(), flags.size());
            write(connFd, "\n", 2);
            message = "Send --<flag1> <value1> --<flag2> <value2> .. \n";
            write(connFd, message.c_str(), message.size());

            char train_data[300];
            bzero(train_data, 301);

            read(connFd, train_data, 300);

            string trainData(train_data);

            Utils utils;
            trainData = utils.trim_copy(trainData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + trainData, "info");

            std::vector<std::string> trainargs = Utils::split(trainData, ' ');
            std::vector<std::string>::iterator itr = std::find(trainargs.begin(), trainargs.end(), "--graph_id");
            std::string graphID;
            if (itr != trainargs.cend()) {
                int index = std::distance(trainargs.begin(), itr);
                graphID = trainargs[index + 1];
            } else {
                frontend_logger.log("graph_id should be given as an argument", "error");
                continue;
            }

            if (trainargs.size() == 0) {
                frontend_logger.log("Message format not recognized", "error");
                break;
            }

            JasminGraphTrainingInitiator *jasminGraphTrainingInitiator = new JasminGraphTrainingInitiator();
            jasminGraphTrainingInitiator->initiateTrainingLocally(graphID,trainData);
        } else if (line.compare(PREDICT) == 0){
            write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            write(connFd, "\r\n", 2);

            char predict_data[300];
            bzero(predict_data, 301);
            string graphID = "";
            string path = "";

            read(connFd, predict_data, 300);
            string predictData(predict_data);

            Utils utils;
            predictData = utils.trim_copy(predictData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + predictData, "info");

            std::vector<std::string> strArr = Utils::split(predictData, '|');

            if (strArr.size() != 2) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            graphID = strArr[0];
            path = strArr[1];

            if(JasmineGraphFrontEnd::isGraphActiveAndTrained(graphID, sqlite)) {
                if (utils.fileExists(path)) {
                    std::cout << "Path exists" << endl;
                    JasminGraphLinkPredictor *jasminGraphLinkPredictor = new JasminGraphLinkPredictor();
                    jasminGraphLinkPredictor->initiateLinkPrediction(graphID, path, masterIP);
                } else {
                    frontend_logger.log("Graph edge file does not exist on the specified path", "error");
                    continue;
                }
            } else {
                frontend_logger.log("The graph is not fully accessible or not fully trained.", "error");
                continue;
            }
        } else {
            frontend_logger.log("Message format not recognized " + line, "error");
        }
    }
    frontend_logger.log("Closing thread " + to_string(pthread_self()) + " and connection", "info");
    close(connFd);
}

JasmineGraphFrontEnd::JasmineGraphFrontEnd(SQLiteDBInterface db, std::string masterIP) {
    this->sqlite = db;
    this->masterIP = masterIP;
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
        frontend_logger.log("Cannot bind on port " + portNo, "error");
        return 0;
    }

    listen(listenFd, 10);

    std::thread* myThreads = new std::thread[20];
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

        frontend_logger.log("Master IP" + masterIP, "info");

        struct frontendservicesessionargs *frontendservicesessionargs1 =(struct frontendservicesessionargs*) malloc(sizeof(struct frontendservicesessionargs)*1 );;
        frontendservicesessionargs1->sqlite = this->sqlite;
        frontendservicesessionargs1->connFd = connFd;

        myThreads[noThread] = std::thread(frontendservicesesion, masterIP, connFd, this->sqlite);

        std::thread();

        noThread++;
    }

    for (int i = 0; i < noThread; i++) {
        myThreads[i].join();
    }


}

/**
 * This method checks if a graph exists in JasmineGraph.
 * This method uses the unique path of the graph.
 * @param basic_string
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::graphExists(string path, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt =
            "SELECT COUNT( * ) FROM graph WHERE upload_path LIKE '" + path + "' AND graph_status_idgraph_status = '" +
            to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
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
bool JasmineGraphFrontEnd::graphExistsByID(string id, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph = " + id + " and graph_status_idgraph_status = " +
                  to_string(Conts::GRAPH_STATUS::OPERATIONAL);
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    std::cout << "AAAAAAAAA1:" << count << std::endl;
    if (count == 0) {
        result = false;
    }

    std::cout << "AAAAAAAAA2:" << result << std::endl;

    return result;
}

/**
 * This method removes a graph from JasmineGraph
 */
void JasmineGraphFrontEnd::removeGraph(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP) {
    vector<pair<string, string>> hostHasPartition;
    vector<vector<pair<string, string>>> hostPartitionResults = sqlite.runSelect(
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
    sqlite.runUpdate("UPDATE graph SET graph_status_idgraph_status = " + to_string(Conts::GRAPH_STATUS::DELETING) +
                      " WHERE idgraph = " + graphID);

    JasmineGraphServer *jasmineServer = new JasmineGraphServer();
    jasmineServer->removeGraph(hostHasPartition, graphID, masterIP);

    sqlite.runUpdate("DELETE FROM host_has_partition WHERE partition_graph_idgraph = " + graphID);
    sqlite.runUpdate("DELETE FROM partition WHERE graph_idgraph = " + graphID);
    sqlite.runUpdate("DELETE FROM graph WHERE idgraph = " + graphID);
}

/**
 * This method checks whether the graph is active and trained
 * @param graphID
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface sqlite) {
    bool result = true;
    string stmt =
            "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + graphID + "' AND graph_status_idgraph_status = '" +
            to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "' AND train_status = '"+(Conts::TRAIN_STATUS::TRAINED) +"';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count == 0) {
        result = false;
    }
    return result;
}

long JasmineGraphFrontEnd::countTriangles(std::string graphId, SQLiteDBInterface sqlite, std::string masterIP) {
    long result= 0;
    Utils utils;
    Utils::worker aggregatorWorker;
    vector<Utils::worker> workerList = utils.getHostList(sqlite);
    int hostListSize = workerList.size();
    int counter = 0;
    std::vector<std::future<long>> intermRes;
    std::map<std::string, std::future<std::string>> remoteCopyRes;
    PlacesToNodeMapper placesToNodeMapper;

    string sqlStatement = "SELECT name,ip,user,server_port,server_data_port,partition_idpartition FROM host_has_partition INNER JOIN host ON host_idhost=idhost WHERE partition_graph_idgraph=" + graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite.runSelect(sqlStatement);

    std::map<string, std::vector<string>> map;

    // Implement Logic to Decide the Worker node which Acts as the centralstore triangle count aggregator
    aggregatorWorker = workerList.at(0);
    string aggregatorWorkerHost = aggregatorWorker.hostname;
    std::string aggregatorWorkerPort = aggregatorWorker.port;

    std::string aggregatorPartitionId;

    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;
        string host = "";

        string name = rowData.at(0).second;
        string ip = rowData.at(1).second;
        string user = rowData.at(2).second;
        string serverPort = rowData.at(3).second;
        string serverDataPort = rowData.at(4).second;
        string partitionId = rowData.at(5).second;

        if (ip.find("localhost") != std::string::npos) {
            host = ip;
        } else {
            host = user + "@" + ip;;
        }

        frontend_logger.log("###FRONTEND### Getting Triangle Count : Host " + host + " Server Port " + serverPort + " PartitionId " + partitionId, "info");

        intermRes.push_back(std::async(std::launch::async,JasmineGraphFrontEnd::getTriangleCount,atoi(graphId.c_str()),host,atoi(serverPort.c_str()),atoi(partitionId.c_str()),masterIP));

        if (!(aggregatorWorkerHost == host && aggregatorWorker.port == serverPort)) {
            remoteCopyRes.insert(std::make_pair(host,std::async(std::launch::async, JasmineGraphFrontEnd::copyCentralStoreToAggregator, aggregatorWorkerHost,aggregatorWorker.port,host,serverPort,atoi(graphId.c_str()),atoi(partitionId.c_str()),masterIP)));
        } else {
            aggregatorPartitionId = partitionId;
        }
    }

    for (int i = 0; i < hostListSize; i++) {
        int k = counter;
        string host = placesToNodeMapper.getHost(i);
        std::vector<int> instancePorts = placesToNodeMapper.getInstancePortsList(i);

        string partitionId;

        std::vector<string> partitionList = map[host];

        std::vector<int>::iterator portsIterator;

        for (portsIterator = instancePorts.begin(); portsIterator != instancePorts.end(); ++portsIterator) {
            int port = (int) *portsIterator;
            frontend_logger.log("====>PORT:" + std::to_string(port), "info");

            if (partitionList.size() > 0) {
                auto iterator = partitionList.begin();
                partitionId = *iterator;
                partitionList.erase(partitionList.begin());
            }

//            std::future<long> f2 = std::async(std::launch::async, JasmineGraphFrontEnd::getTriangleCount, atoi(graphId.c_str()), host,
//                                                     port, atoi(partitionId.c_str()));
//            f2.wait();
//            intermRes.push_back(f2);

            intermRes.push_back(
                    std::async(std::launch::async, JasmineGraphFrontEnd::getTriangleCount, atoi(graphId.c_str()), host,
                               port, atoi(partitionId.c_str()), masterIP));
//            if (!(aggregatorWorkerHost == host && aggregatorWorkerPort == std::to_string(port))) {
//                remoteCopyRes.insert(std::make_pair(host, std::async(std::launch::async,
//                                                                     JasmineGraphFrontEnd::copyCentralStoreToAggregator,
//                                                                     aggregatorWorkerHost, aggregatorWorkerPort, host,
//                                                                     std::to_string(port), atoi(graphId.c_str()),
//                                                                     atoi(partitionId.c_str()))));
//            } else {
//                aggregatorPartitionId = partitionId;
//            }
        }
    }

    for (auto &&futureCall:intermRes) {
        futureCall.wait();
    }

    for (auto &&futureCall:intermRes) {
        result += futureCall.get();
        //frontend_logger.log("====>result:" + std::to_string(result), "info");
    }

    long aggregatedTriangleCount = JasmineGraphFrontEnd::countCentralStoreTriangles(aggregatorWorkerHost,aggregatorWorkerPort,aggregatorWorkerHost,aggregatorPartitionId,graphId,masterIP);
    result += aggregatedTriangleCount;
    frontend_logger.log("###FRONTEND### Getting Triangle Count : Completed: Triangles " + to_string(result), "info");
    return result;
}


long JasmineGraphFrontEnd::getTriangleCount(int graphId, std::string host, int port, int partitionId, std::string masterIP) {

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
        host = utils.split(host, '@')[1];
    }

    frontend_logger.log("###FRONTEND### Get Host By Name : " + host, "info");

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
        write(sockfd, masterIP.c_str(), masterIP.size());
        frontend_logger.log("Sent : " + masterIP, "info");

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
            frontend_logger.log("Got response : |" + response + "|", "info");
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
                                                               std::string port, int graphId, int partitionId, std::string masterIP) {
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
        write(sockfd, masterIP.c_str(), masterIP.size());
        frontend_logger.log("Sent : " + masterIP, "info");

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


long JasmineGraphFrontEnd::countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort, std::string host, std::string partitionId, std::string graphId, std::string masterIP) {
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
        write(sockfd, masterIP.c_str(), masterIP.size());
        frontend_logger.log("Sent : " + masterIP, "info");

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

void JasmineGraphFrontEnd::getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface sqlite) {
    struct tm tm;
    vector<vector<pair<string, string>>> uploadStartFinishTimes = sqlite.runSelect(
            "SELECT upload_start_time,upload_end_time FROM graph WHERE idgraph = '" + graphID + "'");
    string startTime = uploadStartFinishTimes[0][0].second;
    string endTime = uploadStartFinishTimes[0][1].second;
    string sTime = startTime.substr(startTime.size() - 14, startTime.size() - 5);
    string eTime = endTime.substr(startTime.size() - 14, startTime.size() - 5);
    strptime(sTime.c_str(), "%H:%M:%S", &tm);
    time_t start = mktime(&tm);
    strptime(eTime.c_str(), "%H:%M:%S", &tm);
    time_t end = mktime(&tm);
    double difTime = difftime(end, start);
    sqlite.runUpdate("UPDATE graph SET upload_time = " + to_string(difTime) + " WHERE idgraph = " + graphID);
    frontend_logger.log("Upload time updated in the database", "info");
}
