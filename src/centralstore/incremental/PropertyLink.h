/**
Copyright 2020 JasminGraph Team
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

#include <cstring>
#include <fstream>
#include <set>
#include <string>

#ifndef PROPERTY_LINK
#define PROPERTY_LINK

class PropertyLink {
   private:
    unsigned int blockAddress; // contains the address of the first element in the list
    unsigned int nextPropAddress;
    
   public:
    std::string name;
    char *value = NULL;

    static const unsigned long MAX_NAME_SIZE;  // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE;  // Size of a property value in bytes
    static const unsigned long PROPERTY_BLOCK_SIZE;
    static unsigned int nextPropertyIndex; // Next available property block index // unless open in wipe data mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    static std::string DB_PATH;
    static std::fstream *propertiesDB;

    PropertyLink(unsigned int);
    PropertyLink();
    bool isEmpty();
    
    unsigned int insert(std::string, char[]);
    PropertyLink* next();

};

#endif