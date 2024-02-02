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

#include "JSONParser.h"

#include <jsoncpp/json/json.h>

#include <boost/foreach.hpp>
#include <boost/optional/optional.hpp>
#include <boost/property_tree/ptree.hpp>
#include <chrono>
#include <ctime>
#include <future>
#include <sstream>
#include <string>

#include "../../util/logger/Logger.h"

Logger jsonparser_logger;

class Value;

namespace pt = boost::property_tree;

using namespace std;

static void attributeFileCreate(std::map<long, int> &vertexToIDMap, std::map<std::string, int> &fieldsMap,
                                std::string inputFilePath, string outputFilePath);
static void readFile(std::map<long, int> &vertexToIDMap, std::map<std::string, int> &fieldsMap,
                     std::string inputFilePath, string outputFilePath);
static std::map<long, int> createEdgeList(std::string inputFilePath, string outputFilePath);
static std::map<std::string, int> countFileds(std::string inputFilePath);

void JSONParser::jsonParse(string &filePath) {
    std::string inputFilePath = filePath;
    std::string outputFilePath = Utils::getHomeDir() + "/.jasminegraph/tmp/JSONParser/output";
    const clock_t begin = clock();
    std::map<long, int> vertexToIDMap;
    std::map<std::string, int> fieldsMap;
    readFile(vertexToIDMap, fieldsMap, inputFilePath, outputFilePath);
    float time = float(clock() - begin) / CLOCKS_PER_SEC;
    jsonparser_logger.log("FieldMap size : " + to_string(fieldsMap.size()), "info");
    jsonparser_logger.log("Time for 1st read : " + to_string(time), "info");
    const clock_t begin2 = clock();
    attributeFileCreate(vertexToIDMap, fieldsMap, inputFilePath, outputFilePath);
    float time2 = float(clock() - begin2) / CLOCKS_PER_SEC;
    jsonparser_logger.log("Time for 2nd read : " + to_string(time2), "info");
}

static void attributeFileCreate(std::map<long, int> &vertexToIDMap, std::map<std::string, int> &fieldsMap,
                                std::string inputFilePath, string outputFilePath) {
    ofstream attrFile;
    attrFile.open(outputFilePath + "/attributeList.txt");

    std::ifstream infile(inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    std::vector<int> vectorOfzeros(fieldsMap.size(), 0);

    while (std::getline(infile, line)) {
        std::vector<int> tempVectorOfZeros = vectorOfzeros;
        if (!reader.parse(line, root)) {
            string message = reader.getFormattedErrorMessages();
            jsonparser_logger.log("Error : " + message, "error");
            break;
        }
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
    infile.close();
    attrFile.close();
}

static void readFile(std::map<long, int> &vertexToIDMap, std::map<std::string, int> &fieldsMap,
                     std::string inputFilePath, string outputFilePath) {
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/");
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp");
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/JSONParser");
    Utils::createDirectory(Utils::getHomeDir() + "/.jasminegraph/tmp/JSONParser/output");

    auto edgeListFuture = std::async(createEdgeList, inputFilePath, outputFilePath);
    auto filedsFuture = std::async(countFileds, inputFilePath);

    vertexToIDMap = edgeListFuture.get();
    fieldsMap = filedsFuture.get();
}

static std::map<long, int> createEdgeList(std::string inputFilePath, string outputFilePath) {
    ofstream file;
    file.open(outputFilePath + "/edgelist.txt");

    std::ifstream infile(inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    int idCounter = 0;
    int mapped_id = 0;
    std::map<long, int> vertexToIDMap;

    while (std::getline(infile, line)) {
        if (!reader.parse(line, root)) {
            jsonparser_logger.log("File format mismatch", "error");
            file.close();
            infile.close();
            return vertexToIDMap;
        }
        string id = root["id"].asString();
        const Json::Value references = root["references"];
        if (references.empty()) {
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
    file.close();
    infile.close();
    return vertexToIDMap;
}

static std::map<std::string, int> countFileds(std::string inputFilePath) {
    std::ifstream infile(inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    std::map<std::string, int> fieldCounts;
    int field_counter = 0;

    while (std::getline(infile, line)) {
        if (!reader.parse(line, root)) {
            jsonparser_logger.log("File format mismatch", "error");
            return fieldCounts;
        }
        const Json::Value fos = root["fos"];

        for (int i = 0; i < fos.size(); i++) {
            string field = fos[i]["name"].asString();
            double weight = fos[i]["w"].asDouble();
            if (weight <= 0.5) {
                continue;
            }
            if (fieldCounts.find(field) == fieldCounts.end()) {
                fieldCounts.insert(make_pair(field, 1));
            } else {
                fieldCounts[field]++;
            }
        }
    }
    infile.close();

    jsonparser_logger.log("Done counting fields", "info");
    std::map<std::string, int> fieldsMap;
    for (auto it = fieldCounts.begin(); it != fieldCounts.end(); ++it) {
        std::string field = it->first;
        if (it->second > 821) {
            if (fieldsMap.find(field) == fieldsMap.end()) {
                fieldsMap.insert(make_pair(field, field_counter));
                field_counter++;
            }
        }
    }
    return fieldsMap;
}
