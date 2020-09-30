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

#include <ctime>
#include <chrono>
#include <iostream>
#include <map>
#include <set>
#include "JasmineGraphFrontEnd.h"
#include "../util/Conts.h"
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
#include "../ml/trainer/JasminGraphTrainingInitiator.h"
#include "../query/algorithms/linkprediction/JasminGraphLinkPredictor.h"
#include "../ml/trainer/JasmineGraphTrainingSchedular.h"
#include "../ml/trainer/python-c-api/Python_C_API.h"

using namespace std;

static int connFd;
Logger frontend_logger;
static map<string,int> aggregatorWeightMap;

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
        if (line.compare("\r\n") == 0) {
            continue;
        }
        frontend_logger.log("Command received: " + line, "info");

        Utils utils;
        line = utils.trim_copy(line, " \f\n\r\t\v");

        if (line.compare(EXIT) == 0) {
            break;
        } else if (line.compare(LIST) == 0) {
           std::stringstream ss;
            std::vector<vector<pair<string, string>>> v = sqlite.runSelect(
                    "SELECT idgraph, name, upload_path, graph_status_idgraph_status FROM graph;");
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
            if (result.size() == 0) {
                int result_wr = write(connFd, EMPTY.c_str(), EMPTY.length());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\r\n", 2);

                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }

            } else {
                int result_wr = write(connFd, result.c_str(), result.length());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            }

        } else if (line.compare(SHTDN) == 0) {
            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            jasmineServer->shutdown_workers();
            close(connFd);
            exit(0);
        } else if (line.compare(ADRDF) == 0) {

            // add RDF graph
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

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
                jasmineServer->uploadGraphLocally(newGraphID, Conts::GRAPH_WITH_ATTRIBUTES, fullFileList, masterIP);
                utils.deleteDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/" + to_string(newGraphID));
                utils.deleteDirectory("/tmp/" + std::to_string(newGraphID));
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }

        } else if (line.compare(ADGR) == 0) {
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

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
                string workerCountQuery = "select count(*) from worker";
                std::vector<vector<pair<string, string>>> results = sqlite.runSelect(workerCountQuery);
                string workerCount = results[0][0].second;
                int nWorkers = atoi(workerCount.c_str());
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
                int result_wr = write(connFd, DONE.c_str(), DONE.size());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }
        } else if (line.compare(ADGR_CUST) == 0) {
            string message = "Select a custom graph upload option\n";
            int result_wr = write(connFd, message.c_str(), message.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, Conts::GRAPH_WITH::TEXT_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::TEXT_ATTRIBUTES.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, Conts::GRAPH_WITH::JSON_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::JSON_ATTRIBUTES.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, Conts::GRAPH_WITH::XML_ATTRIBUTES.c_str(), Conts::GRAPH_WITH::XML_ATTRIBUTES.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\n", 2);

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

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
            // <name>|<path to edge list>|<path to attribute file>|(optional)<attribute data type: int8. int16, int32 or float>
            // Data types based on numpy array data types for numerical values with int8 referring to 8bit integers etc.
            // If data type is not specified, it will be inferred from values present in the first line of the attribute file
            // The provided data type should be the largest in the following order: float > int32 > int16 > int8
            // Inferred data type will be the largest type based on the values present in the attribute file first line
            message = "Send <name>|<path to edge list>|<path to attribute file>|(optional)<attribute data type: int8. int16, int32 or float>\n";
            result_wr = write(connFd, message.c_str(), message.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            char graph_data[FRONTEND_DATA_LENGTH];
            bzero(graph_data, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string edgeListPath = "";
            string attributeListPath = "";
            string attrDataType = "";

            read(connFd, graph_data, FRONTEND_DATA_LENGTH);

            std::time_t time = chrono::system_clock::to_time_t(chrono::system_clock::now());
            string uploadStartTime = ctime(&time);
            string gData(graph_data);

            Utils utils;
            gData = utils.trim_copy(gData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + gData, "info");

            std::vector<std::string> strArr = Utils::split(gData, '|');

            if (strArr.size() != 3 && strArr.size() != 4) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            name = strArr[0];
            edgeListPath = strArr[1];
            attributeListPath = strArr[2];
            //If data type is specified
            if (strArr.size() == 4) {
                attrDataType = strArr[3];
                if (attrDataType != "int8" && attrDataType != "int16" && attrDataType != "int32" && attrDataType != "float") {
                    frontend_logger.log("Data type not recognized", "error");
                    continue;
                }
            }

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
                partitioner->loadContentData(attributeListPath, graphAttributeType, newGraphID, attrDataType);
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
                result_wr = write(connFd, DONE.c_str(), DONE.size());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }
        } else if (line.compare(ADD_STREAM_KAFKA) == 0) {
            frontend_logger.log("Start serving `" + ADD_STREAM_KAFKA + "` command", "info");
            string message = "send kafka topic name";
            int result_wr = write(connFd, message.c_str(), message.length());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

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
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

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
                result_wr = write(connFd, DONE.c_str(), DONE.size());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            } else {
                frontend_logger.log("Graph does not exist or cannot be deleted with the current hosts setting",
                                    "error");
                result_wr = write(connFd, ERROR.c_str(), ERROR.size());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            }

        } else if (line.compare(PROCESS_DATASET) == 0) {
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

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
                frontend_logger.log("Reformatted files created on /home/.jasminegraph/tmp/JSONParser/output",
                        "info");


            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                break;
            }
        } else if (line.compare(TRIANGLES) == 0) {
            // add RDF graph
            int result_wr = write(connFd, GRAPHID_SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

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
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);

                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }

                result_wr = write(connFd, "\r\n", 2);

                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            } else {
                auto begin = chrono::high_resolution_clock::now();
                long triangleCount = JasmineGraphFrontEnd::countTriangles(graph_id,sqlite,masterIP);
                auto end = chrono::high_resolution_clock::now();
                auto dur = end - begin;
                auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
                frontend_logger.log("Triangle Count: " + to_string(triangleCount) + " Time Taken: " + to_string(msDuration) +
                " milliseconds", "info");
                result_wr = write(connFd, to_string(triangleCount).c_str(), (int)to_string(triangleCount).length());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            }
        } else if (line.compare(VCOUNT) == 0) {
            int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

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
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            } else {
                string sqlStatement = "SELECT vertexcount from graph where idgraph=" + graph_id;

                std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);

                int vertexCount = std::stoi(output[0][0].second);
                frontend_logger.log("Vertex Count: " + to_string(vertexCount), "info");
                result_wr = write(connFd, to_string(vertexCount).c_str(), to_string(vertexCount).length());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            }
        } else if (line.compare(ECOUNT) == 0) {
            int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

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
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            } else {
                string sqlStatement = "SELECT edgecount from graph where idgraph=" + graph_id;

                std::vector<vector<pair<string, string>>> output = sqlite.runSelect(sqlStatement);

                int edgeCount = std::stoi(output[0][0].second);
                frontend_logger.log("Edge Count: " + to_string(edgeCount), "info");
                result_wr = write(connFd, to_string(edgeCount).c_str(), to_string(edgeCount).length());
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            }
        } else if (line.compare(TRAIN) == 0) {
            string message = "Available main flags:\n";
            int result_wr = write(connFd, message.c_str(), message.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            string flags =
                    Conts::FLAGS::GRAPH_ID + " " + Conts::FLAGS::LEARNING_RATE + " " + Conts::FLAGS::BATCH_SIZE + " " +
                    Conts::FLAGS::VALIDATE_ITER + " " + Conts::FLAGS::EPOCHS;
            result_wr = write(connFd, flags.c_str(), flags.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            message = "Send --<flag1> <value1> --<flag2> <value2> .. \n";
            result_wr = write(connFd, message.c_str(), message.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

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

            if (!JasmineGraphFrontEnd::isGraphActive(graphID, sqlite)) {

                string error_message = "Graph is not in the active status";
                frontend_logger.log(error_message, "error");
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                continue;
            }

            JasminGraphTrainingInitiator *jasminGraphTrainingInitiator = new JasminGraphTrainingInitiator();
            jasminGraphTrainingInitiator->initiateTrainingLocally(graphID,trainData);
        } else if (line.compare(PREDICT) == 0){
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

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

        struct frontendservicesessionargs *frontendservicesessionargs1 =(struct frontendservicesessionargs*) malloc(
                sizeof(struct frontendservicesessionargs)*1 );;
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
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph = " + id;
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);

    if (count == 0) {
        result = false;
    }

    return result;
}

/**
 * This method removes a graph from JasmineGraph
 */
void JasmineGraphFrontEnd::removeGraph(std::string graphID, SQLiteDBInterface sqlite, std::string masterIP) {
    vector<pair<string, string>> hostHasPartition;
    vector<vector<pair<string, string>>> hostPartitionResults = sqlite.runSelect(
            "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON "
            "worker_has_partition.worker_idworker = worker.idworker WHERE partition_graph_idgraph = " + graphID + ";");
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

    sqlite.runUpdate("DELETE FROM worker_has_partition WHERE partition_graph_idgraph = " + graphID);
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

/**
 * This method checks whether the graph is active
 * @param graphID
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEnd::isGraphActive(std::string graphID, SQLiteDBInterface sqlite) {
    bool result = false;
    string stmt =
            "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + graphID + "' AND graph_status_idgraph_status = '" +
            to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite.runSelect(stmt);
    int count = std::stoi(v[0][0].second);
    if (count != 0) {
        result = true;
    }
    return result;
}

long JasmineGraphFrontEnd::countTriangles(std::string graphId, SQLiteDBInterface sqlite, std::string masterIP) {
    long result= 0;
    Utils utils;
    Utils::worker aggregatorWorker;
    vector<Utils::worker> workerList = utils.getWorkerList(sqlite);
    int workerListSize = workerList.size();
    int counter = 0;
    std::vector<std::future<long>> intermRes;
    std::vector<std::future<string>> remoteCopyRes;
    PlacesToNodeMapper placesToNodeMapper;

    string sqlStatement = "SELECT worker_idworker, name,ip,user,server_port,server_data_port,partition_idpartition "
                          "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
                          "WHERE partition_graph_idgraph=" + graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite.runSelect(sqlStatement);

    std::map<string, std::vector<string>> partitionMap;

    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;
        string host = "";

        string workerID = rowData.at(0).second;
        string name = rowData.at(1).second;
        string ip = rowData.at(2).second;
        string user = rowData.at(3).second;
        string serverPort = rowData.at(4).second;
        string serverDataPort = rowData.at(5).second;
        string partitionId = rowData.at(6).second;

        if ((ip.find("localhost") != std::string::npos) || ip == masterIP) {
            host = ip;
        } else {
            host = user + "@" + ip;;
        }

        if( partitionMap.find(workerID) == partitionMap.end()){
            std::vector<string> partitionVec;
            partitionVec.push_back(partitionId);
            partitionMap.insert(std::pair<string, std::vector<string>>(workerID, partitionVec));
        } else {
            std::vector<string> partitionVec = partitionMap.find(workerID)->second;
            partitionVec.push_back(partitionId);
        }

        frontend_logger.log("###FRONTEND### Getting Triangle Count : Host " + host + " Server Port " +
        serverPort + " PartitionId " + partitionId, "info");

    }

    for (auto &&futureCall:remoteCopyRes) {
        futureCall.wait();
    }

    for (int i = 0; i < workerListSize; i++) {
        Utils::worker currentWorker = workerList.at(i);
        int k = counter;
        string host = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        string partitionId;

        std::vector<string> partitionList = partitionMap[workerID];

        std::vector<string>::iterator partitionIterator;

        for (partitionIterator = partitionList.begin(); partitionIterator != partitionList.end(); ++partitionIterator) {
            int workerPort = atoi(string(currentWorker.port).c_str());
            frontend_logger.log("====>PORT:" + std::to_string(workerPort), "info");

            partitionId = *partitionIterator;
            intermRes.push_back(
                    std::async(std::launch::async, JasmineGraphFrontEnd::getTriangleCount, atoi(graphId.c_str()), host,
                               workerPort, atoi(partitionId.c_str()), masterIP));
        }
    }

    for (auto &&futureCall:intermRes) {
        result += futureCall.get();
    }

    long aggregatedTriangleCount = JasmineGraphFrontEnd::aggregateCentralStoreTriangles(sqlite, graphId,masterIP);
    result += aggregatedTriangleCount;
    frontend_logger.log("###FRONTEND### Getting Triangle Count : Completed: Triangles " + to_string(result),
            "info");
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
        host = utils.split(host, '@')[0];
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
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }

    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            frontend_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::TRIANGLES.c_str(),
              JasmineGraphInstanceProtocol::TRIANGLES.size());

        if(result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::TRIANGLES, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(graphId).c_str(), std::to_string(graphId).size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : Graph ID " + std::to_string(graphId), "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, std::to_string(partitionId).c_str(), std::to_string(partitionId).size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

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
        frontend_logger.log("There was an error in the upload process and the response is :: " + response,
                "error");
    }

}

std::vector<std::vector<string>> JasmineGraphFrontEnd::getWorkerCombination(SQLiteDBInterface sqlite, std::string graphId) {

    std::set<string> workerIdSet;
    std::vector<std::vector<int>> combinations;
    std::vector<std::vector<string>> workerIdCombination;

    string sqlStatement = "SELECT worker_idworker "
                          "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
                          "WHERE partition_graph_idgraph=" + graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite.runSelect(sqlStatement);


    for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        string workerId = rowData.at(0).second;

        workerIdSet.insert(workerId);
    }

    std::vector<string> workerIdVector(workerIdSet.begin(), workerIdSet.end());

    //Below algorithm will get all the combinations of 3 workers for given set of workers
    std::string bitmask(3, 1);
    bitmask.resize(workerIdVector.size(), 0);

    do {
        std::vector<int> combination;
        for (int i = 0; i < workerIdVector.size(); ++i)
        {
            if (bitmask[i]) {
                combination.push_back(i);
            }
        }
        combinations.push_back(combination);
    } while (std::prev_permutation(bitmask.begin(), bitmask.end()));

    for (std::vector<std::vector<int>>::iterator combinationsIterator = combinations.begin() ; combinationsIterator != combinations.end(); ++combinationsIterator) {
        std::vector<int> combination = *combinationsIterator;
        std::vector<string> tempWorkerIdCombination;

        for (std::vector<int>::iterator combinationIterator = combination.begin();combinationIterator != combination.end(); ++combinationIterator) {
            int index = *combinationIterator;

            tempWorkerIdCombination.push_back(workerIdVector.at(index));
        }

        workerIdCombination.push_back(tempWorkerIdCombination);
    }

    return workerIdCombination;

}


std::string JasmineGraphFrontEnd::copyCentralStoreToAggregator(std::string aggregatorHostName,
                                                               std::string aggregatorPort, std::string aggregatorDataPort, int graphId, int partitionId,
                                                               std::string masterIP) {
    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    Utils utils;
    std::string aggregatorFilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.aggregatefolder");
    std::string fileName = std::to_string(graphId) + "_centralstore_" + std::to_string(partitionId) + ".gz";
    std::string centralStoreFile = aggregatorFilePath + "/" + fileName;
    JasmineGraphServer *jasmineServer = new JasmineGraphServer();

    int fileSize = utils.getFileSize(centralStoreFile);
    std::string fileLength = to_string(fileSize);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    if (aggregatorHostName.find('@') != std::string::npos) {
        aggregatorHostName = utils.split(aggregatorHostName, '@')[0];
    }

    server = gethostbyname(aggregatorHostName.c_str());
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
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }

    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            frontend_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.c_str(),
              JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR.size());

        if(result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::SEND_CENTRALSTORE_TO_AGGREGATOR,
                "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_NAME, "info");
            result_wr = write(sockfd, fileName.c_str(), fileName.size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : File Name " + fileName, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_LEN, "info");
                result_wr = write(sockfd, fileLength.c_str(), fileLength.size());

                if(result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }

                frontend_logger.log("Sent : File Length: " + fileLength, "info");

                bzero(data, 301);
                read(sockfd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                    frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::SEND_FILE_CONT, "info");
                    frontend_logger.log("Going to send file through service", "info");
                    jasmineServer->sendFileThroughService(aggregatorHostName, std::atoi(aggregatorDataPort.c_str()), fileName, centralStoreFile, masterIP);
                }
            }
        }

        int count = 0;

        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(),
                              JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::FILE_RECV_CHK, "info");
            frontend_logger.log("Checking if file is received", "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            //response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_RECV_WAIT, "info");
                frontend_logger.log("Checking file status : " + to_string(count), "info");
                count++;
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::FILE_ACK) == 0) {
                frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::FILE_ACK, "info");
                frontend_logger.log("File transfer completed for file : " + centralStoreFile, "info");
                break;
            }
        }

        //Next we wait till the batch upload completes
        while (true) {
            result_wr = write(sockfd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(),
                              JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK, "info");
            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);

            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT, "info");
                sleep(1);
                continue;
            } else if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0) {
                frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK, "info");
                frontend_logger.log("CentralStore partition file upload completed", "info");
                break;
            }
        }
    } else {
        frontend_logger.log("There was an error in the upload process and the response is :: " + response,
                "error");
    }
    return response;
}


string JasmineGraphFrontEnd::countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
        std::string host, std::string partitionId, std::string partitionIdList, std::string graphId, std::string masterIP) {
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
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    if(result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }

    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK, "info");
        result_wr = write(sockfd, masterIP.c_str(), masterIP.size());

        if(result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + masterIP, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);

        if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::HOST_OK, "info");
        } else {
            frontend_logger.log("Received : " + response, "error");
        }
        result_wr = write(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.c_str(),
              JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES.size());

        if(result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::AGGREGATE_CENTRALSTORE_TRIANGLES,
                "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, graphId.c_str(), graphId.size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : Graph ID " + graphId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionId.c_str(), partitionId.size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : Partition ID " + partitionId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
            result_wr = write(sockfd, partitionIdList.c_str(), partitionIdList.size());

            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            frontend_logger.log("Sent : Partition ID List : " + partitionId, "info");

            bzero(data, 301);
            read(sockfd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");
        }


    } else {
        frontend_logger.log("There was an error in the upload process and the response is :: " + response,
                "error");
    }
    return response;
}

long JasmineGraphFrontEnd::aggregateCentralStoreTriangles(SQLiteDBInterface sqlite, std::string graphId, std::string masterIP) {

    std::vector<std::vector<string>> workerCombinations = getWorkerCombination(sqlite,graphId);
    std::map<string, int> workerWeightMap;
    std::vector<std::vector<string>>::iterator workerCombinationsIterator;
    std::vector<std::future<string>> triangleCountResponse;
    std::string result = "";
    long aggregatedTriangleCount = 0;

    for (workerCombinationsIterator = workerCombinations.begin(); workerCombinationsIterator != workerCombinations.end(); ++workerCombinationsIterator) {
        std::vector<string> workerCombination = *workerCombinationsIterator;
        std::map<string, int>::iterator workerWeightMapIterator;
        std::vector<std::future<string>> remoteGraphCopyResponse;
        int minimumWeight = 0;
        std::string minWeightWorker;
        string aggregatorHost = "";
        std::string partitionIdList="";

        std::vector<string>::iterator workerCombinationIterator;
        std::vector<string>::iterator aggregatorCopyCombinationIterator;

        for (workerCombinationIterator = workerCombination.begin();workerCombinationIterator != workerCombination.end(); ++workerCombinationIterator) {
            std::string workerId = *workerCombinationIterator;

            workerWeightMapIterator = workerWeightMap.find(workerId);

            if (workerWeightMapIterator != workerWeightMap.end()) {
                int weight = workerWeightMap.at(workerId);

                if (minimumWeight > weight) {
                    minimumWeight = weight;
                    minWeightWorker = workerId;
                }
            } else {
                minWeightWorker = workerId;
            }
        }

        string aggregatorSqlStatement = "SELECT ip,user,server_port,server_data_port,partition_idpartition "
                                        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
                                        "WHERE partition_graph_idgraph=" + graphId + " and idworker=" + minWeightWorker + ";";

        std::vector<vector<pair<string, string>>> result = sqlite.runSelect(aggregatorSqlStatement);

        vector<pair<string, string>> aggregatorData = result.at(0);

        std::string aggregatorIp = aggregatorData.at(0).second;
        std::string aggregatorUser = aggregatorData.at(1).second;
        std::string aggregatorPort = aggregatorData.at(2).second;
        std::string aggregatorDataPort = aggregatorData.at(3).second;
        std::string aggregatorPartitionId = aggregatorData.at(4).second;

        if ((aggregatorIp.find("localhost") != std::string::npos) || aggregatorIp == masterIP) {
            aggregatorHost = aggregatorIp;
        } else {
            aggregatorHost = aggregatorUser + "@" + aggregatorIp;
        }

        for (aggregatorCopyCombinationIterator = workerCombination.begin();aggregatorCopyCombinationIterator != workerCombination.end(); ++aggregatorCopyCombinationIterator) {
            std::string workerId = *aggregatorCopyCombinationIterator;
            string host = "";

            if (workerId != minWeightWorker) {
                string sqlStatement = "SELECT ip,user,server_port,server_data_port,partition_idpartition "
                                      "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
                                      "WHERE partition_graph_idgraph=" + graphId + " and idworker=" + workerId + ";";

                std::vector<vector<pair<string, string>>> result = sqlite.runSelect(sqlStatement);

                vector<pair<string, string>> workerData = result.at(0);

                std::string workerIp = workerData.at(0).second;
                std::string workerUser = workerData.at(1).second;
                std::string workerPort = workerData.at(2).second;
                std::string workerDataPort = workerData.at(3).second;
                std::string partitionId = workerData.at(4).second;

                if ((workerIp.find("localhost") != std::string::npos) || workerIp == masterIP) {
                    host = workerIp;
                } else {
                    host = workerUser + "@" + workerIp;
                }

                partitionIdList += partitionId + ",";

                remoteGraphCopyResponse.push_back(
                        std::async(std::launch::async, JasmineGraphFrontEnd::copyCentralStoreToAggregator, aggregatorHost, aggregatorPort, aggregatorDataPort,
                                   atoi(graphId.c_str()), atoi(partitionId.c_str()), masterIP));
            }

        }

        for (auto &&futureCallCopy:remoteGraphCopyResponse) {
            futureCallCopy.get();
        }

        std::string adjustedPartitionIdList = partitionIdList.substr(0, partitionIdList.size()-1);
        minimumWeight = minimumWeight + 1;
        workerWeightMap[minWeightWorker] = minimumWeight;

        triangleCountResponse.push_back(
                std::async(std::launch::async, JasmineGraphFrontEnd::countCentralStoreTriangles, aggregatorHost, aggregatorPort, aggregatorHost,
                           aggregatorPartitionId, adjustedPartitionIdList, graphId, masterIP));


    }

    for (auto &&futureCall:triangleCountResponse) {
        result = result + ":" + futureCall.get();
    }

    std::vector<std::string> triangles = Utils::split(result, ':');
    std::vector<std::string>::iterator triangleIterator;
    std::set<std::string> uniqueTriangleSet;

    for (triangleIterator = triangles.begin();triangleIterator!=triangles.end();++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            uniqueTriangleSet.insert(triangle);
        }
    }

    aggregatedTriangleCount = uniqueTriangleSet.size();

    return aggregatedTriangleCount;

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

JasmineGraphHashMapCentralStore JasmineGraphFrontEnd::loadCentralStore(std::string centralStoreFileName) {
    frontend_logger.log("Loading Central Store File : Started " + centralStoreFileName,"info");
    JasmineGraphHashMapCentralStore *jasmineGraphHashMapCentralStore = new JasmineGraphHashMapCentralStore();
    jasmineGraphHashMapCentralStore->loadGraph(centralStoreFileName);
    frontend_logger.log("Loading Central Store File : Completed","info");
    return *jasmineGraphHashMapCentralStore;
}

map<long, long> JasmineGraphFrontEnd::getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap) {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = graphMap.begin(); it != graphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap.insert(std::make_pair(it->first, distribution));
    }
    return distributionHashMap;
}
