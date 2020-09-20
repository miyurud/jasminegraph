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

#ifndef JASMINEGRAPH_JSON_STRING_PARSER_H
#define JASMINEGRAPH_JSON_STRING_PARSER_H

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string.h>
#include <map>
#include <vector>
#include "json/json.h"


using std::string;
using namespace std;


class JSONStringParser {
public:

    JSONStringParser();

    static Json::Value parse(string content);

};

#endif //JASMINEGRAPH_JSON_STRING_PARSER_H