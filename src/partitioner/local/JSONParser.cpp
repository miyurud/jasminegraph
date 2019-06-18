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
#include "json/json.h"
#include <ctime>
#include <chrono>
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

    readFile();

    attributeFileCreate();

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
            std::cout << reader.getFormattedErrorMessages();
            exit(1);
        } else {
            string id = root["id"].asString();
            int mapped_id = vertexToIDMap.find(stol(id))->second;
            const Json::Value fos = root["fos"];
            for (int i = 0; i < fos.size(); i++) {
                string field = fos[i]["name"].asString();

                double weight = fos[i]["w"].asDouble();
                auto search = fieldsMap.find(field);
                if (search != fieldsMap.end()) {
                    int index = search->second;
                    if (weight < 0.5 && weight > 0.0) {
                        tempVectorOfZeros.at(index) = 1;

                    } else if (weight > 0.5) {
                        tempVectorOfZeros.at(index) = 2;

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
    attrFile.close();


}


void JSONParser::readFile() {
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser/output");

    ofstream file;
    file.open(this->outputFilePath + "/edgelist.txt");

    std::ifstream infile(this->inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    int count = 0;
    int idCounter = 0;
    int field_counter = 0;
    int mapped_id = 0;

    while (std::getline(infile, line)) {
        count++;

        if (!reader.parse(line, root)) {
            jsonparser_logger.log("File format mismatch", "error");
            exit(1);
        } else {

            string id = root["id"].asString();
            long idValue = stol(id);
            if (vertexToIDMap.find(idValue) == vertexToIDMap.end()) {
                vertexToIDMap.insert(make_pair(idValue, idCounter));
                mapped_id = idCounter;
                idCounter++;
            }

            const Json::Value references = root["references"];
            int mapped_ref_id;
            for (int index = 0; index < references.size(); ++index) {
                string ref_id = references[index].asString();
                long ref_idValue = stol(ref_id);
                if (vertexToIDMap.find(ref_idValue) == vertexToIDMap.end()) {
                    vertexToIDMap.insert(make_pair(idValue, idCounter));
                    mapped_ref_id = idCounter;
                    idCounter++;
                } else {
                    mapped_ref_id = vertexToIDMap.find(ref_idValue)->second;
                }

                file << mapped_id << " " << mapped_ref_id << endl;

            }

            const Json::Value fos = root["fos"];

            for (int i = 0; i < fos.size(); i++) {
                string field = fos[i]["name"].asString();
                if (fieldsMap.find(field) == fieldsMap.end()) {
                    fieldsMap.insert(make_pair(field, field_counter));
                    field_counter++;
                }
            }
        }
    }

    file.close();
}



