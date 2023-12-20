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

#include "CentralPropertyEdgeLink.h"
#include <sstream>
#include <vector>
#include <iostream>

#include "../util/logger/Logger.h"

#include "CentralPropertyEdgeLink.h"

Logger central_property_edge_link_logger;
unsigned int CentralPropertyEdgeLink::nextPropertyIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue
std::string CentralPropertyEdgeLink::DB_PATH = "/home/ubuntu/software/jasminegraph/streamStore/properties.db";
std::fstream* CentralPropertyEdgeLink::centralEdgePropertiesDB = NULL;
pthread_mutex_t lockCentralPropertyEdgeLink;
pthread_mutex_t lockCreateCentralPropertyEdgeLink;
pthread_mutex_t lockInsertCentralPropertyEdgeLink;
pthread_mutex_t lockGetCentralPropertyEdgeLink;





CentralPropertyEdgeLink::CentralPropertyEdgeLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    pthread_mutex_lock(&lockCentralPropertyEdgeLink);
    if (propertyBlockAddress > 0) {
        this->centralEdgePropertiesDB->seekg(propertyBlockAddress);
        char rawName[CentralPropertyEdgeLink::MAX_NAME_SIZE] = {0};
        central_property_edge_link_logger.info("Traverse state  = " + std::to_string(CentralPropertyEdgeLink::centralEdgePropertiesDB->rdstate()));

        if (!this->centralEdgePropertiesDB->read(reinterpret_cast<char*>(&rawName), CentralPropertyEdgeLink::MAX_NAME_SIZE)) {
            central_property_edge_link_logger.error("Error while reading property name from block " + std::to_string(blockAddress));
        }
        central_property_edge_link_logger.debug(
                "Current file descriptor curser position = " + std::to_string(this->centralEdgePropertiesDB->tellg()) +
                " when reading = " + std::to_string(blockAddress));
        if (!this->centralEdgePropertiesDB->read(reinterpret_cast<char*>(&this->value), CentralPropertyEdgeLink::MAX_VALUE_SIZE)) {
            central_property_edge_link_logger.error("Error while reading property value from block " + std::to_string(blockAddress));
        }

        if (!this->centralEdgePropertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            central_property_edge_link_logger.error("Error while reading property next address from block " +
                                            std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
    pthread_mutex_unlock(&lockCentralPropertyEdgeLink);
};

CentralPropertyEdgeLink::CentralPropertyEdgeLink(unsigned int blockAddress, std::string name, char* rvalue, unsigned int nextAddress)
        : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    // Can't use just string copyer here because of binary data formats
    for (size_t i = 0; i < CentralPropertyEdgeLink::MAX_VALUE_SIZE; i++) {
        this->value[i] = rvalue[i];
    }
};

/**
 * pseudo code for inserting an element to a property (single) link list
 * ### Note: This link list implementation adds a new link to the tail of the link, unlike in normal case relocating the
 * head link
 *
 * propertyAddress; If no property address means no property has been stored if(!propertyAddress) { // Means
 * this is the very first link in the chain yet to be filled store property in this link it self } else if ( //Check
 * this property name is available in this link) {
 *      // If so update the current link
 *  } else {
 *      // Check if this->next() exist
 *      if(this->next()) {
 *          // Try to insert property set to next link
 *          this->next().insert(name,value);
 *      } else { // If no next, That means this is leaf or at the end of the link, So add a new link and update the
 * current link's next property address
 *          // attach new link
 *      }
 *  }
 * if propertyAddress was 0 before inserting you will get head address of the link in return value
 *  else either updated link address or last appended link address will be returned
 * **/
unsigned int CentralPropertyEdgeLink::insert(std::string name, char* value) {

    char dataName[CentralPropertyEdgeLink::MAX_NAME_SIZE] = {0};
    char dataValue[CentralPropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value,
                CentralPropertyEdgeLink::MAX_VALUE_SIZE);  // strcpy or strncpy get terminated at null-character hence using memcpy

    central_property_edge_link_logger.debug("Received name = " + name);
    central_property_edge_link_logger.debug("Received value = " + std::string(value));
    unsigned int nextAddress = 0;

    central_property_edge_link_logger.info("current property name  = " + (this->name));
    central_property_edge_link_logger.info("new property name  = " + (name));


    if (this->name == name) {
        // TODO[tmkasun]: update existing property value
        central_property_edge_link_logger.warn("Property key/name already exist key = " + std::string(name));
        return this->blockAddress;
    } else if (this->next()) {  // Traverse to the edge/end of the link list
        return this->next()->insert(name, value);
    } else {  // No next link means end of the link, Now add the new link
        central_property_edge_link_logger.debug("Next prop index = " + std::to_string(CentralPropertyEdgeLink::nextPropertyIndex));

        pthread_mutex_lock(&lockInsertCentralPropertyEdgeLink);
        unsigned int newAddress = CentralPropertyEdgeLink::nextPropertyIndex * CentralPropertyEdgeLink::PROPERTY_BLOCK_SIZE;
        this->centralEdgePropertiesDB->seekp(newAddress);
        this->centralEdgePropertiesDB->write(dataName, CentralPropertyEdgeLink::MAX_NAME_SIZE);
        this->centralEdgePropertiesDB->write(reinterpret_cast<char*>(dataValue), CentralPropertyEdgeLink::MAX_VALUE_SIZE);
        if (!this->centralEdgePropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            central_property_edge_link_logger.error("Error while inserting a property " + name + " into block address " +
                                            std::to_string(newAddress));
            return -1;
        }

        this->centralEdgePropertiesDB->flush();

        this->nextPropAddress = newAddress;
        this->centralEdgePropertiesDB->seekp(this->blockAddress + CentralPropertyEdgeLink::MAX_NAME_SIZE +
                                      CentralPropertyEdgeLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->centralEdgePropertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            central_property_edge_link_logger.error("Error while updating  property next address for " + name +
                                            " into block address " + std::to_string(this->blockAddress));
            return -1;
        }
        central_property_edge_link_logger.info("nextPropertyIndex = " + std::to_string(CentralPropertyEdgeLink::nextPropertyIndex));
        CentralPropertyEdgeLink::nextPropertyIndex++;  // Increment the shared property index value
        pthread_mutex_unlock(&lockInsertCentralPropertyEdgeLink);
        return this->blockAddress;

    }

}

/**
 * Create a brand new property link to an empty node block
 *
 * */
CentralPropertyEdgeLink* CentralPropertyEdgeLink::create(std::string name, char value[]) {

    pthread_mutex_lock(&lockCreateCentralPropertyEdgeLink);
    unsigned int nextAddress = 0;
    char dataName[CentralPropertyEdgeLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = CentralPropertyEdgeLink::nextPropertyIndex * CentralPropertyEdgeLink::PROPERTY_BLOCK_SIZE;
    CentralPropertyEdgeLink::centralEdgePropertiesDB->seekp(newAddress);
    CentralPropertyEdgeLink::centralEdgePropertiesDB->write(dataName, CentralPropertyEdgeLink::MAX_NAME_SIZE);
    CentralPropertyEdgeLink::centralEdgePropertiesDB->write(reinterpret_cast<char*>(value), CentralPropertyEdgeLink::MAX_VALUE_SIZE);
    if (!CentralPropertyEdgeLink::centralEdgePropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
        central_property_edge_link_logger.error("Error while inserting the property = " + name +
                                        " into block a new address = " + std::to_string(newAddress));
        return NULL;
    }
    CentralPropertyEdgeLink::centralEdgePropertiesDB->flush();
    central_property_edge_link_logger.info("nextPropertyIndex = " + std::to_string(CentralPropertyEdgeLink::nextPropertyIndex));
    central_property_edge_link_logger.info("newAddress = " + std::to_string(newAddress));
    CentralPropertyEdgeLink::nextPropertyIndex++;  // Increment the shared property index value
    pthread_mutex_unlock(&lockCreateCentralPropertyEdgeLink);
    return new CentralPropertyEdgeLink(newAddress, name, value, nextAddress);
}

/**
 * If a nextPropAddress available return CentralPropertyEdgeLink with that else return null pointer
 * */
CentralPropertyEdgeLink* CentralPropertyEdgeLink::next() {
    if (this->nextPropAddress) {
        return new CentralPropertyEdgeLink(this->nextPropAddress);
    }
    return NULL;
}

bool CentralPropertyEdgeLink::isEmpty() { return !(this->blockAddress); }

CentralPropertyEdgeLink* CentralPropertyEdgeLink::get(unsigned int propertyBlockAddress) {
    CentralPropertyEdgeLink* pl = NULL;

    pthread_mutex_lock(&lockGetCentralPropertyEdgeLink);
    if (propertyBlockAddress > 0) {
        char propertyName[CentralPropertyEdgeLink::MAX_NAME_SIZE] = {0};
        char propertyValue[CentralPropertyEdgeLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        CentralPropertyEdgeLink::centralEdgePropertiesDB->seekg(propertyBlockAddress);

        central_property_edge_link_logger.info("Searching propertyHead state  = " + std::to_string(CentralPropertyEdgeLink::centralEdgePropertiesDB->rdstate()));
        std::cout << "Stream state: " << CentralPropertyEdgeLink::centralEdgePropertiesDB->rdstate() << std::endl;
//        std::string  CentralPropertyEdgeLink::centralEdgePropertiesDB->rdstate();
        if (!CentralPropertyEdgeLink::centralEdgePropertiesDB->read(reinterpret_cast<char*>(&propertyName), CentralPropertyEdgeLink::MAX_NAME_SIZE)) {
            central_property_edge_link_logger.error("Error  = " +
                                            std::to_string(CentralPropertyEdgeLink::centralEdgePropertiesDB->rdstate()));
            central_property_edge_link_logger.error("Error while reading edge property name from block = " +
                                            std::to_string(propertyBlockAddress));
        }
        if (!CentralPropertyEdgeLink::centralEdgePropertiesDB->read(reinterpret_cast<char*>(&propertyValue), CentralPropertyEdgeLink::MAX_VALUE_SIZE)) {
            central_property_edge_link_logger.error("Error while reading edge property value from block = " +
                                            std::to_string(propertyBlockAddress));
        }

        if (!CentralPropertyEdgeLink::centralEdgePropertiesDB->read(reinterpret_cast<char*>(&(nextAddress)), sizeof(unsigned int))) {
            central_property_edge_link_logger.error("Error while reading edge property next address from block = " +
                                            std::to_string(propertyBlockAddress));
        }
        central_property_edge_link_logger.info("Property head propertyBlockAddress  = " + std::to_string(propertyBlockAddress));
        central_property_edge_link_logger.info("Property head property name  = " + std::string(propertyName));
        central_property_edge_link_logger.info("Property head nextAddress   = " + std::to_string(nextAddress));

        pl = new CentralPropertyEdgeLink(propertyBlockAddress, std::string(propertyName), propertyValue, nextAddress);
    }
    pthread_mutex_unlock(&lockGetCentralPropertyEdgeLink);
    return pl;
}
