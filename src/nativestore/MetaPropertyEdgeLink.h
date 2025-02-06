/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#ifndef JASMINEGRAPH_METAPROPERTYEDGELINK_H
#define JASMINEGRAPH_METAPROPERTYEDGELINK_H
#include <cstring>
#include <fstream>
#include <set>
#include <string>

class MetaPropertyEdgeLink {
 public:
    static const unsigned long MAX_NAME_SIZE = 12;    // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE = 180;  // Size of a property value in bytes
    static thread_local unsigned int nextPropertyIndex;            // Next available property block index
    static const unsigned long META_PROPERTY_BLOCK_SIZE = MAX_NAME_SIZE + MAX_VALUE_SIZE + sizeof(unsigned int);
    std::string name;
    char value[MetaPropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    unsigned int blockAddress;
    unsigned int nextPropAddress;
    static std::string DB_PATH;
    static thread_local std::fstream* metaEdgePropertiesDB;
    MetaPropertyEdgeLink(unsigned int);
    MetaPropertyEdgeLink(unsigned int, std::string, char*, unsigned int);
    static MetaPropertyEdgeLink* get(unsigned int);
    static MetaPropertyEdgeLink* create(std::string, char[]);
    unsigned int insert(std::string, char[]);
    MetaPropertyEdgeLink* next();
};


#endif  // JASMINEGRAPH_METAPROPERTYEDGELINK_H
