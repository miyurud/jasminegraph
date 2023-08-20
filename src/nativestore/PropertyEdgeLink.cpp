//
// Created by sandaruwan on 5/3/23.
//

#include "PropertyEdgeLink.h"
#include <sstream>
#include <vector>
#include <iostream>

#include "../util/logger/Logger.h"
Logger property_edge_link_logger;
unsigned int PropertyEdgeLink::nextPropertyIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue
std::string PropertyEdgeLink::DB_PATH = "/home/sandaruwan/ubuntu/software/jasminegraph/streamStore/properties.db";
std::fstream* PropertyEdgeLink::edgePropertiesDB = NULL;
pthread_mutex_t lockPropertyEdgeLink;
pthread_mutex_t lockCreatePropertyEdgeLink;
pthread_mutex_t lockInsertPropertyEdgeLink;
pthread_mutex_t lockGetPropertyEdgeLink;





PropertyEdgeLink::PropertyEdgeLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    pthread_mutex_lock(&lockPropertyEdgeLink);
    if (propertyBlockAddress > 0) {
        this->edgePropertiesDB->seekg(propertyBlockAddress);
        char rawName[PropertyEdgeLink::MAX_NAME_SIZE] = {0};
//        property_edge_link_logger.info("Traverse state  = " + std::to_string(PropertyEdgeLink::edgePropertiesDB->rdstate()));

        if (!this->edgePropertiesDB->read(reinterpret_cast<char*>(&rawName), PropertyEdgeLink::MAX_NAME_SIZE)) {
            property_edge_link_logger.error("Error while reading property name from block " + std::to_string(blockAddress));
        }
        property_edge_link_logger.debug(
                "Current file descriptor curser position = " + std::to_string(this->edgePropertiesDB->tellg()) +
                " when reading = " + std::to_string(blockAddress));
        if (!this->edgePropertiesDB->read(reinterpret_cast<char*>(&this->value), PropertyEdgeLink::MAX_VALUE_SIZE)) {
            property_edge_link_logger.error("Error while reading property value from block " + std::to_string(blockAddress));
        }

        if (!this->edgePropertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            property_edge_link_logger.error("Error while reading property next address from block " +
                                       std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
    pthread_mutex_unlock(&lockPropertyEdgeLink);
};

PropertyEdgeLink::PropertyEdgeLink(unsigned int blockAddress, std::string name, char* rvalue, unsigned int nextAddress)
        : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    // Can't use just string copyer here because of binary data formats
    for (size_t i = 0; i < PropertyEdgeLink::MAX_VALUE_SIZE; i++) {
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
unsigned int PropertyEdgeLink::insert(std::string name, char* value) {

    char dataName[PropertyEdgeLink::MAX_NAME_SIZE] = {0};
    char dataValue[PropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value,
                PropertyEdgeLink::MAX_VALUE_SIZE);  // strcpy or strncpy get terminated at null-character hence using memcpy

//    property_edge_link_logger.debug("Received name = " + name);
//    property_edge_link_logger.debug("Received value = " + std::string(value));
    unsigned int nextAddress = 0;

//    property_edge_link_logger.info("current property name  = " + (this->name));
//    property_edge_link_logger.info("new property name  = " + (name));


    if (this->name == name) {
        // TODO[tmkasun]: update existing property value
        property_edge_link_logger.warn("Property key/name already exist key = " + std::string(name));
        return this->blockAddress;
    } else if (this->next()) {  // Traverse to the edge/end of the link list
        return this->next()->insert(name, value);
    } else {  // No next link means end of the link, Now add the new link
//        property_edge_link_logger.debug("Next prop index = " + std::to_string(PropertyEdgeLink::nextPropertyIndex));

        pthread_mutex_lock(&lockInsertPropertyEdgeLink);
        unsigned int newAddress = PropertyEdgeLink::nextPropertyIndex * PropertyEdgeLink::PROPERTY_BLOCK_SIZE;
        this->edgePropertiesDB->seekp(newAddress);
        this->edgePropertiesDB->write(dataName, PropertyEdgeLink::MAX_NAME_SIZE);
        this->edgePropertiesDB->write(reinterpret_cast<char*>(dataValue), PropertyEdgeLink::MAX_VALUE_SIZE);
        if (!this->edgePropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            property_edge_link_logger.error("Error while inserting a property " + name + " into block address " +
                                       std::to_string(newAddress));
            return -1;
        }

        this->edgePropertiesDB->flush();

        this->nextPropAddress = newAddress;
        this->edgePropertiesDB->seekp(this->blockAddress + PropertyEdgeLink::MAX_NAME_SIZE +
                                  PropertyEdgeLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->edgePropertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            property_edge_link_logger.error("Error while updating  property next address for " + name +
                                       " into block address " + std::to_string(this->blockAddress));
            return -1;
        }
//        property_edge_link_logger.info("nextPropertyIndex = " + std::to_string(PropertyEdgeLink::nextPropertyIndex));
        PropertyEdgeLink::nextPropertyIndex++;  // Increment the shared property index value
        pthread_mutex_unlock(&lockInsertPropertyEdgeLink);
        return this->blockAddress;

    }

}

/**
 * Create a brand new property link to an empty node block
 *
 * */
PropertyEdgeLink* PropertyEdgeLink::create(std::string name, char value[]) {

    pthread_mutex_lock(&lockCreatePropertyEdgeLink);
    unsigned int nextAddress = 0;
    char dataName[PropertyEdgeLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = PropertyEdgeLink::nextPropertyIndex * PropertyEdgeLink::PROPERTY_BLOCK_SIZE;
    PropertyEdgeLink::edgePropertiesDB->seekp(newAddress);
    PropertyEdgeLink::edgePropertiesDB->write(dataName, PropertyEdgeLink::MAX_NAME_SIZE);
    PropertyEdgeLink::edgePropertiesDB->write(reinterpret_cast<char*>(value), PropertyEdgeLink::MAX_VALUE_SIZE);
    if (!PropertyEdgeLink::edgePropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
        property_edge_link_logger.error("Error while inserting the property = " + name +
                                   " into block a new address = " + std::to_string(newAddress));
        return NULL;
    }
    PropertyEdgeLink::edgePropertiesDB->flush();
//    property_edge_link_logger.info("nextPropertyIndex = " + std::to_string(PropertyEdgeLink::nextPropertyIndex));
//    property_edge_link_logger.info("newAddress = " + std::to_string(newAddress));
    PropertyEdgeLink::nextPropertyIndex++;  // Increment the shared property index value
    pthread_mutex_unlock(&lockCreatePropertyEdgeLink);
    return new PropertyEdgeLink(newAddress, name, value, nextAddress);
}

/**
 * If a nextPropAddress available return PropertyEdgeLink with that else return null pointer
 * */
PropertyEdgeLink* PropertyEdgeLink::next() {
    if (this->nextPropAddress) {
        return new PropertyEdgeLink(this->nextPropAddress);
    }
    return NULL;
}

bool PropertyEdgeLink::isEmpty() { return !(this->blockAddress); }

PropertyEdgeLink* PropertyEdgeLink::get(unsigned int propertyBlockAddress) {
    PropertyEdgeLink* pl = NULL;

    pthread_mutex_lock(&lockGetPropertyEdgeLink);
    if (propertyBlockAddress > 0) {
        char propertyName[PropertyEdgeLink::MAX_NAME_SIZE] = {0};
        char propertyValue[PropertyEdgeLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        PropertyEdgeLink::edgePropertiesDB->seekg(propertyBlockAddress);

//        property_edge_link_logger.info("Searching propertyHead state  = " + std::to_string(PropertyEdgeLink::edgePropertiesDB->rdstate()));
//        std::cout << "Stream state: " << PropertyEdgeLink::edgePropertiesDB->rdstate() << std::endl;
//        std::string  PropertyEdgeLink::edgePropertiesDB->rdstate();
        if (!PropertyEdgeLink::edgePropertiesDB->read(reinterpret_cast<char*>(&propertyName), PropertyEdgeLink::MAX_NAME_SIZE)) {
//            property_edge_link_logger.error("Error  = " +
//                                       std::to_string(PropertyEdgeLink::edgePropertiesDB->rdstate()));
            property_edge_link_logger.error("Error while reading edge property name from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        if (!PropertyEdgeLink::edgePropertiesDB->read(reinterpret_cast<char*>(&propertyValue), PropertyEdgeLink::MAX_VALUE_SIZE)) {
            property_edge_link_logger.error("Error while reading edge property value from block = " +
                                       std::to_string(propertyBlockAddress));
        }

        if (!PropertyEdgeLink::edgePropertiesDB->read(reinterpret_cast<char*>(&(nextAddress)), sizeof(unsigned int))) {
            property_edge_link_logger.error("Error while reading edge property next address from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        property_edge_link_logger.info("Property head propertyBlockAddress  = " + std::to_string(propertyBlockAddress));
        property_edge_link_logger.info("Property head property name  = " + std::string(propertyName));
        property_edge_link_logger.info("Property head nextAddress   = " + std::to_string(nextAddress));

        pl = new PropertyEdgeLink(propertyBlockAddress, std::string(propertyName), propertyValue, nextAddress);
    }
    pthread_mutex_unlock(&lockGetPropertyEdgeLink);
    return pl;
}

