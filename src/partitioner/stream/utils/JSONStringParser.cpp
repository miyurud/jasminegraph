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

#include "JSONStringParser.h"

#include <boost/foreach.hpp>
#include <boost/optional/optional.hpp>
#include <chrono>
#include <ctime>
#include <sstream>
#include <string>
#include <thread>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

class Value;

namespace pt = boost::property_tree;

using namespace std;

Json::Value JSONStringParser::parse(string content) {
    Json::Reader reader;
    Json::Value edgePayload;

    if (!reader.parse(content, edgePayload)) {
        std::cout << "Error while parsing!!" << std::endl;
        exit(1);
    } else {
        return edgePayload;
    }
}
