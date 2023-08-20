//
// Created by sandaruwan on 5/3/23.
//

#include <cstring>
#include <fstream>
#include <set>
#include <string>


#ifndef JASMINEGRAPH_PROPERTYEDGELINK_H
#define JASMINEGRAPH_PROPERTYEDGELINK_H

class PropertyEdgeLink {
public:
    static const unsigned long MAX_NAME_SIZE = 12;    // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE = 180;  // Size of a property value in bytes
    static unsigned int nextPropertyIndex;            // Next available property block index
    // unless open in wipe data
    // mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    static const unsigned long PROPERTY_BLOCK_SIZE = MAX_NAME_SIZE + MAX_VALUE_SIZE + sizeof(unsigned int);

    std::string name;
    char value[PropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    unsigned int blockAddress;  // contains the address of the first element in the list
    unsigned int nextPropAddress;

    static std::string DB_PATH;
    static std::fstream* edgePropertiesDB;



    PropertyEdgeLink(unsigned int);
    PropertyEdgeLink(unsigned int, std::string, char*, unsigned int);
    bool isEmpty();
    static PropertyEdgeLink* get(unsigned int);
    static PropertyEdgeLink* create(std::string, char[]);

    unsigned int insert(std::string, char[]);
    PropertyEdgeLink* next();
};


#endif //JASMINEGRAPH_PROPERTYEDGELINK_H
