//
// Created by sandaruwan on 5/3/23.
//

#include <cstring>
#include <fstream>
#include <set>
#include <string>


#ifndef JASMINEGRAPH_CENTRALPROPERTYEDGELINK_H
#define JASMINEGRAPH_CENTRALPROPERTYEDGELINK_H

class CentralPropertyEdgeLink {
public:
    static const unsigned long MAX_NAME_SIZE = 12;    // Size of a property name in bytes
    static const unsigned long MAX_VALUE_SIZE = 180;  // Size of a property value in bytes
    static unsigned int nextPropertyIndex;            // Next available property block index
    // unless open in wipe data
    // mode(trunc) need to set this value to property db seekp()/BLOCK_SIZE
    static const unsigned long PROPERTY_BLOCK_SIZE = MAX_NAME_SIZE + MAX_VALUE_SIZE + sizeof(unsigned int);

    std::string name;
    char value[CentralPropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    unsigned int blockAddress;  // contains the address of the first element in the list
    unsigned int nextPropAddress;

    static std::string DB_PATH;
    static std::fstream* centralEdgePropertiesDB;



    CentralPropertyEdgeLink(unsigned int);
    CentralPropertyEdgeLink(unsigned int, std::string, char*, unsigned int);
    bool isEmpty();
    static CentralPropertyEdgeLink* get(unsigned int);
    static CentralPropertyEdgeLink* create(std::string, char[]);

    unsigned int insert(std::string, char[]);
    CentralPropertyEdgeLink* next();
};




#endif //JASMINEGRAPH_CENTRALPROPERTYEDGELINK_H
