/**
Copyright 2023 JasminGraph Team
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

#include <cstring>
#include <fstream>
#include <set>
#include <string>

#ifndef JASMINEGRAPH_PROPERTYEDGELINK_H
#define JASMINEGRAPH_PROPERTYEDGELINK_H

class PropertyEdgeLink {
 public:
    static const unsigned long MAX_NAME_SIZE = 30;    // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE = 400;  // Size of a property value in bytes
    static thread_local unsigned int nextPropertyIndex;   // Next available property block index
    // unless open in wipe data
    // mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    static const unsigned long PROPERTY_BLOCK_SIZE = MAX_NAME_SIZE + MAX_VALUE_SIZE + sizeof(unsigned int);

    std::string name;
    char value[PropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    unsigned int blockAddress;  // contains the address of the first element in the list
    unsigned int nextPropAddress;

    static std::string DB_PATH;
    static thread_local std::fstream* edgePropertiesDB;

    PropertyEdgeLink(unsigned int);
    PropertyEdgeLink(unsigned int, std::string, char*, unsigned int);
    bool isEmpty();
    static PropertyEdgeLink* get(unsigned int);
    static PropertyEdgeLink* create(std::string, char[]);

    unsigned int insert(std::string, char[]);
    PropertyEdgeLink* next();
};

#endif  // JASMINEGRAPH_PROPERTYEDGELINK_H
