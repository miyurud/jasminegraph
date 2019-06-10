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


long JSONParser::addToVenues(string name) {
    long id;
    id = venueMap.size();

    auto search = venueMap.find(name);
    if (search != venueMap.end()) {
        return search->second;
    }


    venueMap.insert({name, id});
    return id;
}

void JSONParser::attributeFileCreate() {
    int count = 0;
    ofstream attrFile;
    attrFile.open(outputFilePath + "/attributeList.txt");

    std::ifstream infile(this->inputFilePath);
    std::string line;
    Json::Reader reader;
    Json::Value root;
    while (std::getline(infile, line)) {
        count++;
        std::vector<int> vectorOfzeros(venueMap.size(), 0);


        if (!reader.parse(line, root)) {
            std::cout << reader.getFormattedErrorMessages();
            exit(1);
        } else {
            string id = root["id"].asString();


            string venue = root["venue"].toStyledString();


            auto search = venueMap.find(venue);
            if (search != venueMap.end()) {
                int index = search->second;
                vectorOfzeros.at(index) = 1;

            }

            attrFile << id << "\t";


            for (auto it = vectorOfzeros.begin(); it != vectorOfzeros.end(); ++it) {
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

    while (std::getline(infile, line)) {

        if (!reader.parse(line, root)) {
            std::cout << reader.getFormattedErrorMessages();
            exit(1);
        } else {
            string id = root["id"].asString();
            const Json::Value references = root["references"];
            for (int index = 0; index < references.size(); ++index) {
                file << id << " " << references[index].asString() << endl;

            }

            string venue = root["venue"].toStyledString();
            addToVenues(venue);

        }
    }


    file.close();
}





