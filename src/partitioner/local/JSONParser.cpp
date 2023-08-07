/**
Copyright 2019 JasmineGraph Team
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

#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"
#include <boost/foreach.hpp>
#include <boost/optional/optional.hpp>

#include "JSONParser.h"

#include <sstream>
#include <string>
#include <jsoncpp/json/json.h>
#include <ctime>
#include <chrono>
#include <thread>
#include "../../util/logger/Logger.h"

Logger jsonparser_logger;


class Value;

namespace pt = boost::property_tree;

using namespace std;


JSONParser::JSONParser() {


}

void JSONParser::jsonParse(string filePath) {

    this->inputFilePath = filePath;
    this->outputFilePath = utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser/output";
    const clock_t begin = clock();
    readFile();
    float time = float(clock() - begin) / CLOCKS_PER_SEC;
    jsonparser_logger.log("FieldMap size : "+to_string(fieldsMap.size()), "info");
    jsonparser_logger.log("Time for 1st read : "+to_string(time), "info");
    const clock_t begin2 = clock();
    attributeFileCreate();
    float time2 = float(clock() - begin2) / CLOCKS_PER_SEC;
    jsonparser_logger.log("Time for 2nd read : "+to_string(time2), "info");
}


void JSONParser::attributeFileCreate() {
    ofstream attrFile;
    attrFile.open(outputFilePath + "/attributeList.txt");

    std::ifstream infile(this->inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    std::vector<int> vectorOfzeros(fieldsMap.size(), 0);

    while (std::getline(infile, line)) {
        std::vector<int> tempVectorOfZeros = vectorOfzeros;
        if (!reader.parse(line, root)) {
            string message = reader.getFormattedErrorMessages();
            jsonparser_logger.log("Error : " + message, "error");
            exit(1);
        } else {
            string id = root["id"].asString();
            auto idFound = vertexToIDMap.find(stol(id));
            if (idFound != vertexToIDMap.end()) {
                int mapped_id = vertexToIDMap.find(stol(id))->second;
                const Json::Value fos = root["fos"];
                for (int i = 0; i < fos.size(); i++) {
                    string field = fos[i]["name"].asString();
                    double weight = fos[i]["w"].asDouble();
                    if (weight > 0.5) {
                        auto search = fieldsMap.find(field);
                        if (search != fieldsMap.end()) {
                            tempVectorOfZeros.at(search->second) = 1;
                        }
                    }
                }
                attrFile << mapped_id << "\t";
                for (auto it = tempVectorOfZeros.begin(); it != tempVectorOfZeros.end(); ++it) {
                    attrFile << *it << "\t ";
                }
                attrFile << endl;
            }
        }
    }
    attrFile.close();
}

void JSONParser::readFile() {
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser/output");

    std::thread *workerThreads = new std::thread[2];
    workerThreads[0] = std::thread(&JSONParser::createEdgeList, this);
    workerThreads[1] = std::thread(&JSONParser::countFileds, this);

    for (int threadCount = 0; threadCount < 2; threadCount++) {
        workerThreads[threadCount].join();
    }
}

void JSONParser::createEdgeList() {
    ofstream file;
    file.open(this->outputFilePath + "/edgelist.txt");

    std::ifstream infile(this->inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    int idCounter = 0;
    int mapped_id = 0;

    while (std::getline(infile, line)) {

        if (!reader.parse(line, root)) {
            jsonparser_logger.log("File format mismatch", "error");
            exit(1);
        } else {

            string id = root["id"].asString();
            const Json::Value references = root["references"];
            if (references.empty()){
                continue;
            }
            long idValue = stol(id);
            if (vertexToIDMap.insert(make_pair(idValue, idCounter)).second) {
                mapped_id = idCounter;
                idCounter++;
            } else {
                mapped_id = vertexToIDMap.find(idValue)->second;
            }

            int mapped_ref_id;
            for (int index = 0; index < references.size(); ++index) {
                string ref_id = references[index].asString();
                long ref_idValue = stol(ref_id);
                if (vertexToIDMap.insert(make_pair(ref_idValue, idCounter)).second) {
                    mapped_ref_id = idCounter;
                    idCounter++;
                } else {
                    mapped_ref_id = vertexToIDMap.find(ref_idValue)->second;
                }

                file << mapped_id << " " << mapped_ref_id << endl;

            }
        }
    }
    file.close();
}

void JSONParser::countFileds(){
    std::ifstream infile(this->inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    int field_counter = 0;

    while (std::getline(infile, line)) {

        if (!reader.parse(line, root)) {
            jsonparser_logger.log("File format mismatch", "error");
            exit(1);
        } else {
            const Json::Value fos = root["fos"];

            for (int i = 0; i < fos.size(); i++) {
                string field = fos[i]["name"].asString();
                double weight = fos[i]["w"].asDouble();
                if (weight > 0.5) {
                    if (fieldCounts.find(field) == fieldCounts.end()) {
                        fieldCounts.insert(make_pair(field, 1));
                    } else {
                        fieldCounts[field]++;
                    }
                }
            }
        }
    }

    jsonparser_logger.log("Done counting fields", "info");

    for (auto it = fieldCounts.begin(); it != fieldCounts.end(); ++it) {
        std::string field = it->first;
        if (it->second > 821){
            if (fieldsMap.find(field) == fieldsMap.end()) {
                fieldsMap.insert(make_pair(field, field_counter));
                field_counter++;
            }
        }
    }
}
