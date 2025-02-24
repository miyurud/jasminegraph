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

#ifndef JASMINEGRAPH_METAPROPERTYLINK_H
#define JASMINEGRAPH_METAPROPERTYLINK_H
#include <cstring>
#include <fstream>
#include <set>
#include <string>

class MetaPropertyLink {
 public:
    static const unsigned long MAX_NAME_SIZE = 12;    // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE = 180;  // Size of a property value in bytes
    static inline const std::string PARTITION_ID = "pid";
    static thread_local unsigned int nextPropertyIndex;            // Next available property block index
    static const unsigned long META_PROPERTY_BLOCK_SIZE = MAX_NAME_SIZE + MAX_VALUE_SIZE + sizeof(unsigned int);

    std::string name;
    char value[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
    unsigned int blockAddress;
    unsigned int nextPropAddress;

    static thread_local std::string DB_PATH;
    static thread_local std::fstream* metaPropertiesDB;

    MetaPropertyLink(unsigned int);
    MetaPropertyLink(unsigned int, std::string, const char*, unsigned int);
    static MetaPropertyLink* get(unsigned int);
    static MetaPropertyLink* create(std::string, const char*);

    unsigned int insert(std::string, const char*);
    MetaPropertyLink* next();
};

#endif  //  JASMINEGRAPH_METAPROPERTYLINK_H
