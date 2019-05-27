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


namespace pt = boost::property_tree;

using namespace std;


JSONParser::JSONParser() {


}

void JSONParser::readFile(string filePath) {
    this->inputFilePath = filePath;
    this->outputFilePath = utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser/output";
    formatFile();

    ofstream file;
    file.open(this->outputFilePath + "/edgelist.txt");

    boost::property_tree::ptree root;
    boost::property_tree::read_json(this->outputFilePath + "/reformatted.txt", root);


    for (auto &v : root.get_child("")) {
        auto &node = v.second;

        string id = node.get("id", "");


        if (node.count("references") != 0) {
            for (auto &param :node.get_child("references")) {
                file << id << " " << param.second.get_value("") << endl;
            }

        }

        if (node.count("venue") != 0) {

            for (pt::ptree::value_type &venue : node.get_child("venue")) {

                std::string key = venue.first;
                if (key == "raw") {
                    std::string name = venue.second.data();
                    addToVenues(name);

                }
            }
        }

    }

    file.close();

    ofstream attrFile;
    attrFile.open(outputFilePath + "/attributeList.txt");


    for (auto &v : root.get_child("")) {
        auto &node = v.second;
        std::vector<int> vectorOfzeros(venueMap.size(), 0);

        string id = node.get("id", "");


        string venue_name;
        if (node.count("venue") != 0) {

            for (pt::ptree::value_type &venue : node.get_child("venue")) {

                std::string key = venue.first;
                if (key == "raw") {
                    venue_name = venue.second.data();

                }


                auto search = venueMap.find(venue_name);
                if (search != venueMap.end()) {
                    int index = search->second;
                    vectorOfzeros.at(index) = 1;

                }
            }
        }

        attrFile << id << "\t";


        for (auto it = vectorOfzeros.begin(); it != vectorOfzeros.end(); ++it) {
            attrFile << *it << "\t ";

        }
        attrFile << endl;


    }


    attrFile.close();
    utils.deleteDirectory(this->outputFilePath + "/reformatted.txt");

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


void JSONParser::formatFile() {
    int count = 0;
    ofstream formattedFile;

    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser");
    this->utils.createDirectory(utils.getHomeDir() + "/.jasminegraph/tmp/JSONParser/output");

    formattedFile.open(this->outputFilePath + "/reformatted.txt");

    std::ifstream infile(this->inputFilePath);
    std::string line;
    formattedFile << "[";
    while (std::getline(infile, line)) {
        if (count != 0) {
            formattedFile << ',';

        }
        formattedFile << line << endl;
        count++;
        if (count == 250000) {
            break;
        }
    }
    
    formattedFile << "]";

    formattedFile.close();
}






