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
#include <nlohmann/json.hpp>
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
#include "../centralstore/incremental/DataPublisher.h"
#include "core/scheduler/JobScheduler.h"
#include "../util/performance/PerformanceUtil.h"
#include "core/CoreConstants.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

std::atomic<int> highPriorityTaskCount;
static int connFd;
static int currentFESession;
static bool canCalibrate = true;
Logger frontend_logger;
std::set<ProcessInfo> processData;

void *frontendservicesesion(std::string masterIP, int connFd, SQLiteDBInterface sqlite,
                            PerformanceSQLiteDBInterface perfSqlite, JobScheduler jobScheduler) {
    frontend_logger.log("Thread No: " + to_string(pthread_self()), "info");
    frontend_logger.log("Master IP: " + masterIP, "info");
    char data[FRONTEND_DATA_LENGTH];
    bzero(data, FRONTEND_DATA_LENGTH + 1);
    Utils utils;
    vector<Utils::worker> workerList = utils.getWorkerList(sqlite);
    vector<DataPublisher*> workerClients;
    
    for (int i = 0; i < workerList.size(); i++) {
        Utils::worker currentWorker = workerList.at(i);
        string workerHost = currentWorker.hostname;
        string workerID = currentWorker.workerID;
        int workerPort = atoi(string(currentWorker.port).c_str());
        DataPublisher* workerClient = new DataPublisher(workerPort, workerHost);
        workerClients.push_back(workerClient);        
    }
    bool loop = false;
    while (!loop) {
        if(currentFESession == Conts::MAX_FE_SESSIONS + 1) {
            currentFESession--;
            std::string errorResponse = "Jasminegraph Server is Busy. Please try again later.";
            int result_wr = write(connFd, errorResponse.c_str(), errorResponse.length());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            break;
        }

        bzero(data, FRONTEND_DATA_LENGTH + 1);
        read(connFd, data, FRONTEND_DATA_LENGTH);

        string line(data);
        if (line.compare("\r\n") == 0) {
            continue;
        }
        frontend_logger.log("Command received: " + line, "info");

        if (line.empty()) {
            currentFESession--;
            break;
        }

        Utils utils;
        line = utils.trim_copy(line, " \f\n\r\t\v");

        if (currentFESession > 1) {
            canCalibrate = false;
        } else {
            canCalibrate = true;
            workerResponded = false;
        }


        if (line.compare(EXIT) == 0) {
            currentFESession--;
            break;
        } else if (line.compare(LIST) == 0) {
            std::stringstream ss;
            std::vector < vector < pair < string, string>>> v = sqlite.runSelect(
                    "SELECT idgraph, name, upload_path, graph_status_idgraph_status FROM graph;");
            for (std::vector < vector < pair < string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
                ss << "|";
                int counter = 0;
                for (std::vector < pair < string, string >> ::iterator j = (i->begin()); j != i->end(); ++j) {
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
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\r\n", 2);

                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }

            } else {
                int result_wr = write(connFd, result.c_str(), result.length());
                if (result_wr < 0) {
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
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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

            std::vector <std::string> strArr = Utils::split(gData, '|');

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
                vector <std::map<int, string>> fullFileList;
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
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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

            std::vector <std::string> strArr = Utils::split(gData, '|');

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
                vector <std::map<int, string>> fullFileList;

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
                std::vector < vector < pair < string, string>>> results = sqlite.runSelect(workerCountQuery);
                string workerCount = results[0][0].second;
                int nWorkers = atoi(workerCount.c_str());
                JasmineGraphFrontEnd::getAndUpdateUploadTime(to_string(newGraphID), sqlite);
                int result_wr = write(connFd, DONE.c_str(), DONE.size());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if (result_wr < 0) {
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
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, Conts::GRAPH_WITH::TEXT_ATTRIBUTES.c_str(),
                              Conts::GRAPH_WITH::TEXT_ATTRIBUTES.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, Conts::GRAPH_WITH::JSON_ATTRIBUTES.c_str(),
                              Conts::GRAPH_WITH::JSON_ATTRIBUTES.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, Conts::GRAPH_WITH::XML_ATTRIBUTES.c_str(),
                              Conts::GRAPH_WITH::XML_ATTRIBUTES.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\n", 2);

            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            char type[20];
            bzero(type, 21);
            read(connFd, type, 20);
            string graphType(type);
            graphType = utils.trim_copy(graphType, " \f\n\r\t\v");

            std::unordered_set <std::string> s = {"1", "2", "3"};
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
            if (result_wr < 0) {
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

            std::vector <std::string> strArr = Utils::split(gData, '|');

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
                if (attrDataType != "int8" && attrDataType != "int16" && attrDataType != "int32" &&
                    attrDataType != "float") {
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
                vector <std::map<int, string>> fullFileList;
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
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            } else {
                frontend_logger.log("Graph data file does not exist on the specified path", "error");
                continue;
            }
        } else if (line.compare(ADD_STREAM_KAFKA) == 0) {
            bool TESTING = false; // Test graph data bypassing kafka stream
            if (TESTING) {
                std::string testData[] = {
                    "{\"source\":{\"id\":\"0x97e58c7d37cba1a1e2ecbb2a5b23f8d127b6892d\",\"properties\":{"
                    "\"blockNumber\":\"448028\",\"timestamp\":\"1445954881\",\"tokenId\":\"4422\"}},\"destination\":{"
                    "\"id\":\"0xb1a2b43a7433dd150bb82227ed519cd6b142d382\"},\"properties\":{\"blockNumber\":\"448028\","
                    "\"timestamp\":\"1445954881\",\"tokenId\":\"4422\",\"graphId\":\"1\"}}",
                    "{\"source\":{\"id\":\"0x97e58c7d37cba1a1e2ecbb2a5b23f8d127b6892d\",\"properties\":{"
                    "\"blockNumber\":\"447862\",\"timestamp\":\"1445951915\",\"tokenId\":\"1343\"}},\"destination\":{"
                    "\"id\":\"0x9b22a80d5c7b3374a05b446081f97d0a34079e7f\"},\"properties\":{\"blockNumber\":\"447862\","
                    "\"timestamp\":\"1445951915\",\"tokenId\":\"1343\",\"graphId\":\"1\"}}",
                    "{\"source\":{\"id\":\"0x97e58c7d37cba1a1e2ecbb2a5b23f8d127b6892d\",\"properties\":{"
                    "\"blockNumber\":\"447786\",\"timestamp\":\"1445950900\",\"tokenId\":\"10000\"}},\"destination\":{"
                    "\"id\":\"0x9b22a80d5c7b3374a05b446081f97d0a34079e7f\"},\"properties\":{\"blockNumber\":\"447786\","
                    "\"timestamp\":\"1445950900\",\"tokenId\":\"10000\",\"graphId\":\"1\"}}",
                    "{\"source\":{\"id\":\"0xb1a2b43a7433dd150bb82227ed519cd6b142d382\",\"properties\":{"
                    "\"blockNumber\":\"447767\",\"timestamp\":\"1445950646\",\"tokenId\":\"20000\"}},\"destination\":{"
                    "\"id\":\"0x97e58c7d37cba1a1e2ecbb2a5b23f8d127b6892d\"},\"properties\":{\"blockNumber\":\"447767\","
                    "\"timestamp\":\"1445950646\",\"tokenId\":\"20000\",\"graphId\":\"1\"}}"};
                Partitioner graphPartitioner(1, 0, spt::Algorithms::HASH);

                for (auto data : testData) {
                    auto edgeJson = json::parse(data);
                    auto sourceJson = edgeJson["source"];
                    auto destinationJson = edgeJson["destination"];

                    std::string sourceID = std::string(sourceJson["id"]);
                    std::string destinationID = std::string(destinationJson["id"]);

                    partitionedEdge partitionedEdge = graphPartitioner.addEdge({sourceID, destinationID});
                    edgeJson["source"]["pid"] = std::to_string(partitionedEdge[0].second);
                    edgeJson["destination"]["pid"] = std::to_string(partitionedEdge[1].second);
                    workerClients.at((int)partitionedEdge[0].second)->publish(edgeJson.dump());
                    workerClients.at((int)partitionedEdge[1].second)->publish(edgeJson.dump());
                }
                continue;
            }

            frontend_logger.log("Start serving `" + ADD_STREAM_KAFKA + "` command", "info");
            string message = "send kafka topic name";
            int result_wr = write(connFd, message.c_str(), message.length());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);

            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            // We get the name and the path to graph as a pair separated by |.
            char topic_name[FRONTEND_DATA_LENGTH];
            bzero(topic_name, FRONTEND_DATA_LENGTH + 1);

            read(connFd, topic_name, FRONTEND_DATA_LENGTH);

            string topic_name_s(topic_name);
            topic_name_s = utils.trim_copy(topic_name_s, " \f\n\r\t\v");
            
            //std::thread streamingThread(KafkaConnector::startStream,topic_name_s, workerClients, streamsState);
            //TODO(miyurud):Temporarily commenting this line to enable building the project. Asked tmkasun to provide a
            // permanent fix later when he is available.
            //streamsState->insert(topic_name_s, false);

        } else if (line.compare(STOP_STREAM_KAFKA) == 0) {
            frontend_logger.log("Start serving `" + STOP_STREAM_KAFKA + "` command", "info");
            string message = "send kafka topic name";
            int result_wr = write(connFd, message.c_str(), message.length());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);

            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            // Get the Kafka topic name
            char topic_name[FRONTEND_DATA_LENGTH];
            bzero(topic_name, FRONTEND_DATA_LENGTH + 1);

            read(connFd, topic_name, FRONTEND_DATA_LENGTH);

            string topic_name_s(topic_name);
            topic_name_s = utils.trim_copy(topic_name_s, " \f\n\r\t\v");
            /*if (streamsState->find(topic_name_s) != streamsState->end()) {
                auto steamState = streamsState->find(topic_name_s);
                steamState->second = true;
            }*/

        } else if (line.compare(RMGR) == 0) {
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            } else {
                frontend_logger.log("Graph does not exist or cannot be deleted with the current hosts setting",
                                    "error");
                result_wr = write(connFd, ERROR.c_str(), ERROR.size());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
                result_wr = write(connFd, "\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                    loop = true;
                    continue;
                }
            }

        } else if (line.compare(PROCESS_DATASET) == 0) {
            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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
            int uniqueId = JasmineGraphFrontEnd::getUid();
            int result_wr = write(connFd, GRAPHID_SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id, sqlite)) {
                string error_message = "The specified graph id does not exist";
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);

                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }

                result_wr = write(connFd, "\r\n", 2);

                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            } else {
                int result_wr = write(connFd, PRIORITY.c_str(), PRIORITY.length());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }

                // We get the name and the path to graph as a pair separated by |.
                char priority_data[300];
                bzero(priority_data, 301);

                read(connFd, priority_data, FRONTEND_DATA_LENGTH);

                string priority(priority_data);

                Utils utils;
                priority = utils.trim_copy(priority, " \f\n\r\t\v");

                if (!(std::find_if(priority.begin(),
                                        priority.end(), [](unsigned char c) { return !std::isdigit(c); }) == priority.end())) {
                    string error_message = "Priority should be numeric and > 1 or empty";
                    result_wr = write(connFd, error_message.c_str(), error_message.length());

                    if (result_wr < 0) {
                        frontend_logger.log("Error writing to socket", "error");
                    }

                    result_wr = write(connFd, "\r\n", 2);

                    if (result_wr < 0) {
                        frontend_logger.log("Error writing to socket", "error");
                    }
                    break;
                }

                int threadPriority = std::atoi(priority.c_str());

                //All high priority threads will be set the same high priority level
                if (threadPriority > Conts::DEFAULT_THREAD_PRIORITY) {
                    threadPriority = Conts::HIGH_PRIORITY_DEFAULT_VALUE;

                    if (highPriorityTaskCount > 0) {
                        bool rejectJob = false;
                        PerformanceUtil performanceUtil;
                        performanceUtil.init();

                        bool resourceSufficient = performanceUtil.isResourcesSufficient(graph_id,TRIANGLES,Conts::SLA_CATEGORY::LATENCY,masterIP);
                        int runningHPTaskCount = JasmineGraphFrontEnd::getRunningHighPriorityTaskCount();

                        if (!resourceSufficient && runningHPTaskCount == highPriorityTaskCount) {
                            bool queueTimeAcceptable = JasmineGraphFrontEnd::isQueueTimeAcceptable(sqlite,perfSqlite,
                                    graph_id,TRIANGLES,Conts::SLA_CATEGORY::LATENCY);

                            if (!queueTimeAcceptable) {
                                rejectJob = true;
                            }
                        }

                        if (rejectJob) {
                            string error_message = "System has reached the maximum high priority tasks allowed "
                                                   "at a given time. Please try again later.";
                            result_wr = write(connFd, error_message.c_str(), error_message.length());

                            if (result_wr < 0) {
                                frontend_logger.log("Error writing to socket", "error");
                            }

                            result_wr = write(connFd, "\r\n", 2);

                            if (result_wr < 0) {
                                frontend_logger.log("Error writing to socket", "error");
                            }
                            break;
                        }
                    } else {
                        highPriorityTaskCount++;
                    }
                }

                auto begin = chrono::high_resolution_clock::now();
                JobRequest jobDetails;
                jobDetails.setJobId(std::to_string(uniqueId));
                jobDetails.setJobType(TRIANGLES);
                jobDetails.setPriority(threadPriority);
                jobDetails.setMasterIP(masterIP);
                jobDetails.addParameter(Conts::PARAM_KEYS::GRAPH_ID, graph_id);
                jobDetails.addParameter(Conts::PARAM_KEYS::CATEGORY, Conts::SLA_CATEGORY::LATENCY);
                if (canCalibrate) {
                    jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "true");
                } else {
                    jobDetails.addParameter(Conts::PARAM_KEYS::CAN_CALIBRATE, "false");
                }

                jobScheduler.pushJob(jobDetails);
                JobResponse jobResponse = jobScheduler.getResult(jobDetails);
                std::string triangleCount = jobResponse.getParameter(Conts::PARAM_KEYS::TRIANGLE_COUNT);

                if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
                    highPriorityTaskCount--;
                }

                auto end = chrono::high_resolution_clock::now();
                auto dur = end - begin;
                auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
                frontend_logger.log(
                        "Triangle Count: " + triangleCount + " Time Taken: " + to_string(msDuration) +
                        " milliseconds", "info");
                result_wr = write(connFd, triangleCount.c_str(), triangleCount.length());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            }
        } else if (line.compare(VCOUNT) == 0) {
            int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id, sqlite)) {
                string error_message = "The specified graph id does not exist";
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            } else {
                string sqlStatement = "SELECT vertexcount from graph where idgraph=" + graph_id;

                std::vector < vector < pair < string, string>>> output = sqlite.runSelect(sqlStatement);

                int vertexCount = std::stoi(output[0][0].second);
                frontend_logger.log("Vertex Count: " + to_string(vertexCount), "info");
                result_wr = write(connFd, to_string(vertexCount).c_str(), to_string(vertexCount).length());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            }
        } else if (line.compare(ECOUNT) == 0) {
            int result_wr = write(connFd, GRAPHID_SEND.c_str(), GRAPHID_SEND.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
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

            if (!JasmineGraphFrontEnd::graphExistsByID(graph_id, sqlite)) {
                string error_message = "The specified graph id does not exist";
                result_wr = write(connFd, error_message.c_str(), FRONTEND_COMMAND_LENGTH);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            } else {
                string sqlStatement = "SELECT edgecount from graph where idgraph=" + graph_id;

                std::vector < vector < pair < string, string>>> output = sqlite.runSelect(sqlStatement);

                int edgeCount = std::stoi(output[0][0].second);
                frontend_logger.log("Edge Count: " + to_string(edgeCount), "info");
                result_wr = write(connFd, to_string(edgeCount).c_str(), to_string(edgeCount).length());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
            }
        } else if (line.compare(TRAIN) == 0) {
            string message = "Available main flags:\n";
            int result_wr = write(connFd, message.c_str(), message.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            string flags =
                    Conts::FLAGS::GRAPH_ID + " " + Conts::FLAGS::LEARNING_RATE + " " + Conts::FLAGS::BATCH_SIZE + " " +
                    Conts::FLAGS::VALIDATE_ITER + " " + Conts::FLAGS::EPOCHS;
            result_wr = write(connFd, flags.c_str(), flags.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            message = "Send --<flag1> <value1> --<flag2> <value2> .. \n";
            result_wr = write(connFd, message.c_str(), message.size());
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            char train_data[300];
            bzero(train_data, 301);

            read(connFd, train_data, 300);

            string trainData(train_data);

            Utils utils;
            trainData = utils.trim_copy(trainData, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + trainData, "info");

            std::vector <std::string> trainargs = Utils::split(trainData, ' ');
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
                result_wr = write(connFd, error_message.c_str(), error_message.length());
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                result_wr = write(connFd, "\r\n", 2);
                if (result_wr < 0) {
                    frontend_logger.log("Error writing to socket", "error");
                }
                continue;
            }

            JasminGraphTrainingInitiator *jasminGraphTrainingInitiator = new JasminGraphTrainingInitiator();
            jasminGraphTrainingInitiator->initiateTrainingLocally(graphID, trainData);
        } else if (line.compare(IN_DEGREE) == 0) {
            frontend_logger.log("Calculating In Degree Distribution", "info");

            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            char graph_id[FRONTEND_DATA_LENGTH];
            bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

            read(connFd, graph_id, FRONTEND_DATA_LENGTH);

            string graphID(graph_id);

            Utils utils;
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            frontend_logger.log("Graph ID received: " + graphID, "info");

            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            jasmineServer->inDegreeDistribution(graphID);

            int result_wr_done = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr_done = write(connFd, "\r\n", 2);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
        } else if (line.compare(PAGE_RANK) == 0) {
            frontend_logger.log("Calculating Page Rank", "info");

            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            char page_rank_command[FRONTEND_DATA_LENGTH];
            bzero(page_rank_command, FRONTEND_DATA_LENGTH + 1);
            string name = "";
            string path = "";

            read(connFd, page_rank_command, FRONTEND_DATA_LENGTH);
            std::vector<std::string> strArr = Utils::split(page_rank_command, '|');

            string graphID;
            graphID = strArr[0];
            double alpha = PAGE_RANK_ALPHA;
            if (strArr.size() > 1) {
                alpha  = std::stod(strArr[1]);
                if (alpha < 0 || alpha >= 1) {
                        frontend_logger.log("Invalid value for alpha", "error");
                        loop = true;
                        continue;
                }
            }

            Utils utils;
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            frontend_logger.log("Graph ID received: " + graphID, "info");
            frontend_logger.log("Alpha value: " + to_string(alpha), "info");

            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            jasmineServer->pageRank(graphID, alpha);

            int result_wr_done = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr_done = write(connFd, "\r\n", 2);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
        } else if (line.compare(EGONET) == 0) {
            frontend_logger.log("Calculating EgoNet", "info");

            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            char graph_id[FRONTEND_DATA_LENGTH];
            bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

            read(connFd, graph_id, FRONTEND_DATA_LENGTH);

            string graphID(graph_id);

            Utils utils;
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            frontend_logger.log("Graph ID received: " + graphID, "info");

            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            jasmineServer->egoNet(graphID);

            int result_wr_done = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr_done = write(connFd, "\r\n", 2);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
        } else if (line.compare(DPCNTRL) == 0) {
            frontend_logger.log("Duplicate Centralstore", "info");

            int result_wr = write(connFd, SEND.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr = write(connFd, "\r\n", 2);
            if (result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }

            char graph_id[FRONTEND_DATA_LENGTH];
            bzero(graph_id, FRONTEND_DATA_LENGTH + 1);

            read(connFd, graph_id, FRONTEND_DATA_LENGTH);

            string graphID(graph_id);

            Utils utils;
            graphID = utils.trim_copy(graphID, " \f\n\r\t\v");
            frontend_logger.log("Graph ID received: " + graphID, "info");

            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            jasmineServer->duplicateCentralStore(graphID);

            int result_wr_done = write(connFd, DONE.c_str(), FRONTEND_COMMAND_LENGTH);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
            result_wr_done = write(connFd, "\r\n", 2);
            if (result_wr_done < 0) {
                frontend_logger.log("Error writing to socket", "error");
                loop = true;
                continue;
            }
        } else if (line.compare(PREDICT) == 0) {
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
        } else if (line.compare(START_REMOTE_WORKER) == 0) {
            int result_wr = write(connFd, REMOTE_WORKER_ARGS.c_str(), REMOTE_WORKER_ARGS.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            char worker_data[300];
            bzero(worker_data, 301);

            read(connFd, worker_data, 300);

            string remote_worker_data(worker_data);

            Utils utils;
            remote_worker_data = utils.trim_copy(remote_worker_data, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + remote_worker_data, "info");
            string host = "";
            string port = "";
            string dataPort = "";
            string profile = "";
            string masterHost = "";
            string enableNmon = "";

            std::vector<std::string> strArr = Utils::split(remote_worker_data, '|');

            if (strArr.size() < 6) {
                frontend_logger.log("Message format not recognized", "error");
                continue;
            }

            host = strArr[0];
            port = strArr[1];
            dataPort = strArr[2];
            profile = strArr[3];
            masterHost = strArr[4];
            enableNmon = strArr[5];

            JasmineGraphServer *jasmineServer = new JasmineGraphServer();
            bool isSpawned = jasmineServer->spawnNewWorker(host,port,dataPort,profile,masterHost,enableNmon);

        } else if (line.compare(SLA) == 0) {
            int result_wr = write(connFd, COMMAND.c_str(), COMMAND.size());
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }
            result_wr = write(connFd, "\r\n", 2);
            if(result_wr < 0) {
                frontend_logger.log("Error writing to socket", "error");
            }

            char category[FRONTEND_DATA_LENGTH];
            bzero(category, FRONTEND_DATA_LENGTH + 1);

            read(connFd, category, FRONTEND_DATA_LENGTH);

            string command_info(category);

            Utils utils;
            command_info = utils.trim_copy(command_info, " \f\n\r\t\v");
            frontend_logger.log("Data received: " + command_info, "info");

            std::vector<vector<pair<string, string>>> categoryResults = perfSqlite.runSelect(
                    "SELECT id FROM sla_category where command='" + command_info + "';");

            string slaCategoryIds;

            for (std::vector<vector<pair<string, string>>>::iterator i = categoryResults.begin(); i != categoryResults.end(); ++i) {
                for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
                    slaCategoryIds = slaCategoryIds + "'" + j->second + "',";
                }
            }

            string adjustedIdList = slaCategoryIds.substr(0, slaCategoryIds.size() - 1);

            std::stringstream ss;
            std::vector<vector<pair<string, string>>> v = perfSqlite.runSelect(
                    "SELECT graph_id, partition_count, sla_value FROM graph_sla where id_sla_category in (" + adjustedIdList + ");");
            for (std::vector<vector<pair<string, string>>>::iterator i = v.begin(); i != v.end(); ++i) {
                std::stringstream slass;
                slass << "|";
                int counter = 0;
                for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
                    if (counter == 0) {
                        std::string graphId = j->second;
                        std::string graphQuery = "SELECT name FROM graph where idgraph='" + graphId + "';";
                        std::vector<vector<pair<string, string>>> graphData = sqlite.runSelect(graphQuery);
                        if (graphData.size() == 0) {
                            slass.str(std::string());
                            break;
                        }
                        std::string graphName = graphData[0][0].second;
                        slass << graphName << "|";
                    } else {
                        slass << j->second << "|";
                    }
                    counter++;
                }
                std::string entryString = slass.str();
                if (entryString.size() > 0) {
                    ss << entryString << "\n";
                }
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
        } else {
            frontend_logger.log("Message format not recognized " + line, "error");
        }
    }
    frontend_logger.log("Closing thread " + to_string(pthread_self()) + " and connection", "info");
    close(connFd);
}

JasmineGraphFrontEnd::JasmineGraphFrontEnd(SQLiteDBInterface db, PerformanceSQLiteDBInterface perfDb, std::string masterIP,
        JobScheduler jobScheduler) {
    this->sqlite = db;
    this->masterIP = masterIP;
    this->perfSqlite = perfDb;
    this->jobScheduler = jobScheduler;
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
    std::vector<std::thread> threadVector;
    len = sizeof(clntAdd);

    int noThread = 0;

    while (true) {
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

        //TODO(miyurud):Temporarily commenting this line to enable building the project. Asked tmkasun to provide a
        // permanent fix later when he is available.
        threadVector.push_back(std::thread(frontendservicesesion, masterIP, connFd, this->sqlite, this->perfSqlite, this->jobScheduler));

        std::thread();

        currentFESession++;
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
    for (vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin(); i != hostPartitionResults.end(); ++i) {
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

int JasmineGraphFrontEnd::getUid() {
    static std::atomic<std::uint32_t> uid { 0 };
    return ++uid;
}


bool JasmineGraphFrontEnd::isQueueTimeAcceptable(SQLiteDBInterface sqlite, PerformanceSQLiteDBInterface perfSqlite, std::string graphId,
        std::string command, std::string category) {

    bool queueTimeAcceptable = true;
    std::chrono::milliseconds currentTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    long currentTimestamp = currentTime.count();
    long maxTimeRemaining = 0;
    int count = 0;

    string sqlStatement = "SELECT worker_idworker, name,ip,user,server_port,server_data_port,partition_idpartition "
                          "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
                          "WHERE partition_graph_idgraph=" + graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite.runSelect(sqlStatement);

    int partitionCount = results.size();

    string graphSlaQuery = "select graph_sla.sla_value from graph_sla,sla_category where graph_sla.id_sla_category=sla_category.id "
                           "and sla_category.command='" + command + "' and sla_category.category='" + category + "' and "
                           "graph_sla.graph_id='" + graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) + "';";

    std::vector<vector<pair<string, string>>> slaResults = perfSqlite.runSelect(graphSlaQuery);

    if (slaResults.size() > 0) {
        string currentSlaString = slaResults[0][0].second;
        long currentSla = atol(currentSlaString.c_str());

        std::set<ProcessInfo>::iterator processQueryIterator;
        for (processQueryIterator = processData.begin(); processQueryIterator != processData.end(); ++processQueryIterator) {
            ProcessInfo processInformation = *processQueryIterator;

            if (processInformation.priority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
                string highPriorityGraphId = processInformation.graphId;
                long currentProcessStart = processInformation.startTimestamp;
                long elapsedTime = currentTimestamp - currentProcessStart;

                string highPrioritySlaQuery = "select graph_sla.sla_value from graph_sla,sla_category where "
                                              "graph_sla.id_sla_category=sla_category.id and sla_category.command='" + command +
                                              "' and sla_category.category='" + category + "' and graph_sla.graph_id='" +
                                              highPriorityGraphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) + "';";

                std::vector<vector<pair<string, string>>> currentSlaResults = perfSqlite.runSelect(graphSlaQuery);

                string currentProcessSlaString = currentSlaResults[0][0].second;
                long currentProcessSla = atol(currentProcessSlaString.c_str());

                long remainingTime = currentProcessSla - elapsedTime;

                if (count = 0) {
                    maxTimeRemaining = remainingTime;
                } else if (maxTimeRemaining < remainingTime) {
                    maxTimeRemaining = remainingTime;
                }
                count++;
            }
        }

        if (maxTimeRemaining > 0 && maxTimeRemaining > currentSla * 0.2) {
            queueTimeAcceptable = false;
        }

    }

    return queueTimeAcceptable;

}

int JasmineGraphFrontEnd::getRunningHighPriorityTaskCount() {
    int taskCount = 0;

    std::set<ProcessInfo>::iterator processQueryIterator;
    for (processQueryIterator = processData.begin(); processQueryIterator != processData.end(); ++processQueryIterator) {
        ProcessInfo processInformation = *processQueryIterator;

        if (processInformation.priority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
            taskCount++;
        }
    }

    return taskCount;
}

void JasmineGraphServer::pageRank(std::string graphID, double alpha) {

    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
            JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dataPort;
    std::string workerList;
    Utils utils;

    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = utils.split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();
    frontend_logger.log("Worker list " + workerList, "error");

    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = utils.split(host, '@')[1];
        }

        int sockfd;
        char data[300];
        bool loop = false;
        socklen_t len;
        struct sockaddr_in serv_addr;
        struct hostent *server;

        sockfd = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cout << "Cannot accept connection" << std::endl;
        }
        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cout << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *) &serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *) server->h_addr,
              (char *) &serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(port);
        if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            std::cout << "ERROR connecting" << std::endl;
            //TODO::exit
        }

        bzero(data, 301);
        int result_wr = write(sockfd, JasmineGraphInstanceProtocol::PAGE_RANK.c_str(),
                              JasmineGraphInstanceProtocol::PAGE_RANK.size());
        if (result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::PAGE_RANK, "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        string response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            frontend_logger.log("Error reading from socket", "error");
        }

        result_wr = write(sockfd, graphID.c_str(), graphID.size());

        if (result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }
        frontend_logger.log("Sent : Graph ID " + graphID, "info");

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            frontend_logger.log("Error reading from socket", "error");
        }

        int partitionID = stoi(partition);

        result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

        if (result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : Partition ID " + std::to_string(partitionID), "info");

        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            frontend_logger.log("Error reading from socket", "error");
        }

        result_wr = write(sockfd, workerList.c_str(), workerList.size());

        if (result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("Sent : Host List ", "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            frontend_logger.log("Error reading from socket", "error");
        }

        long graphVertexCount = JasmineGraphServer::getGraphVertexCount(graphID);
        result_wr = write(sockfd, std::to_string(graphVertexCount).c_str(), std::to_string(graphVertexCount).size());

        if (result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("graph vertex count: " + std::to_string(graphVertexCount), "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            frontend_logger.log("Error reading from socket", "error");
        }

        result_wr = write(sockfd, std::to_string(alpha).c_str(), std::to_string(alpha).size());

        if (result_wr < 0) {
            frontend_logger.log("Error writing to socket", "error");
        }

        frontend_logger.log("page rank alpha value sent : " + std::to_string(alpha), "info");
        bzero(data, 301);
        read(sockfd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
            frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
        } else {
            frontend_logger.log("Error reading from socket", "error");
        }
    }
}

void JasmineGraphServer::egoNet(std::string graphID) {

    std::map<std::string, JasmineGraphServer::workerPartition> graphPartitionedHosts =
            JasmineGraphServer::getWorkerPartitions(graphID);
    int partition_count = 0;
    string partition;
    string host;
    int port;
    int dataPort;
    std::string workerList;
    Utils utils;

    std::map<std::string, JasmineGraphServer::workerPartition>::iterator workerit;
    for (workerit = graphPartitionedHosts.begin(); workerit != graphPartitionedHosts.end(); workerit++) {
        JasmineGraphServer::workerPartition workerPartition = workerit->second;
        partition = workerPartition.partitionID;
        host = workerPartition.hostname;
        port = workerPartition.port;
        dataPort = workerPartition.dataPort;

        if (host.find('@') != std::string::npos) {
            host = utils.split(host, '@')[1];
        }

        workerList.append(host + ":" + std::to_string(port) + ":" + partition + ",");
    }

    workerList.pop_back();

    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cout << "Cannot accept connection" << std::endl;
    }
    server = gethostbyname(host.c_str());
    if (server == NULL) {
        std::cout << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cout << "ERROR connecting" << std::endl;
        //TODO::exit
    }

    bzero(data, 301);
    int result_wr = write(sockfd, JasmineGraphInstanceProtocol::EGONET.c_str(),
                          JasmineGraphInstanceProtocol::EGONET.size());
    if(result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }

    frontend_logger.log("Sent : " + JasmineGraphInstanceProtocol::EGONET, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
    } else {
        frontend_logger.log("Error reading from socket", "error");
    }

    result_wr = write(sockfd, graphID.c_str(), graphID.size());

    if (result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }
    frontend_logger.log("Sent : Graph ID " + graphID, "info");

    bzero(data, 301);
    read(sockfd, data, 300);
    response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
    } else {
        frontend_logger.log("Error reading from socket", "error");
    }

    int partitionID = stoi(partition);

    result_wr = write(sockfd, std::to_string(partitionID).c_str(), std::to_string(partitionID).size());

    if (result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }

    frontend_logger.log("Sent : Partition ID " + std::to_string(partitionID), "info");

    bzero(data, 301);
    read(sockfd, data, 300);
    response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
    } else {
        frontend_logger.log("Error reading from socket", "error");
    }

    result_wr = write(sockfd, workerList.c_str(), workerList.size());

    if (result_wr < 0) {
        frontend_logger.log("Error writing to socket", "error");
    }

    frontend_logger.log("Sent : Host List ", "info");

    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        frontend_logger.log("Received : " + JasmineGraphInstanceProtocol::OK, "info");
    } else {
        frontend_logger.log("Error reading from socket", "error");
    }
}

