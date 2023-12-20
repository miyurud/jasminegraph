//
// Created by sandaruwan on 5/3/23.
//

#include "CentralPropertyLink.h"

#include <sstream>
#include <vector>
#include <iostream>

#include "../util/logger/Logger.h"

Logger central_property_link_logger;
unsigned int CentralPropertyLink::nextPropertyIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue
std::string CentralPropertyLink::DB_PATH = "/home/ubuntu/software/jasminegraph/streamStore/properties.db";
std::fstream* CentralPropertyLink::centralPropertiesDB = NULL;
pthread_mutex_t lockCentralPropertyLink;
pthread_mutex_t lockCreateCentralPropertyLink;
pthread_mutex_t lockInsertCentralPropertyLink;
pthread_mutex_t lockGetCentralPropertyLink;





CentralPropertyLink::CentralPropertyLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    pthread_mutex_lock(&lockCentralPropertyLink);
    if (propertyBlockAddress > 0) {
        this->centralPropertiesDB->seekg(propertyBlockAddress);
        char rawName[CentralPropertyLink::MAX_NAME_SIZE] = {0};
        central_property_link_logger.info("Traverse state  = " + std::to_string(CentralPropertyLink::centralPropertiesDB->rdstate()));

        if (!this->centralPropertiesDB->read(reinterpret_cast<char*>(&rawName), CentralPropertyLink::MAX_NAME_SIZE)) {
            central_property_link_logger.error("Error while reading node property name from block " + std::to_string(blockAddress));
        }
        central_property_link_logger.debug(
                "Current file descriptor curser position = " + std::to_string(this->centralPropertiesDB->tellg()) +
                " when reading = " + std::to_string(blockAddress));
        if (!this->centralPropertiesDB->read(reinterpret_cast<char*>(&this->value), CentralPropertyLink::MAX_VALUE_SIZE)) {
            central_property_link_logger.error("Error while reading node property value from block " + std::to_string(blockAddress));
        }

        if (!this->centralPropertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            central_property_link_logger.error("Error while reading node property next address from block " +
                                       std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
    pthread_mutex_unlock(&lockCentralPropertyLink);
};

CentralPropertyLink::CentralPropertyLink(unsigned int blockAddress, std::string name, char* rvalue, unsigned int nextAddress)
        : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    // Can't use just string copyer here because of binary data formats
    for (size_t i = 0; i < CentralPropertyLink::MAX_VALUE_SIZE; i++) {
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
unsigned int CentralPropertyLink::insert(std::string name, char* value) {

    char dataName[CentralPropertyLink::MAX_NAME_SIZE] = {0};
    char dataValue[CentralPropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value,
                CentralPropertyLink::MAX_VALUE_SIZE);  // strcpy or strncpy get terminated at null-character hence using memcpy

    central_property_link_logger.debug("Received name = " + name);
    central_property_link_logger.debug("Received value = " + std::string(value));
    unsigned int nextAddress = 0;

    central_property_link_logger.info("current property name  = " + (this->name));
    central_property_link_logger.info("new property name  = " + (name));


    if (this->name == name) {
        // TODO[tmkasun]: update existing property value
        central_property_link_logger.warn("Property key/name already exist key = " + std::string(name));
        return this->blockAddress;
    } else if (this->next()) {  // Traverse to the edge/end of the link list
        return this->next()->insert(name, value);
    } else {  // No next link means end of the link, Now add the new link
        central_property_link_logger.debug("Next prop index = " + std::to_string(CentralPropertyLink::nextPropertyIndex));

        pthread_mutex_lock(&lockInsertCentralPropertyLink);
        unsigned int newAddress = CentralPropertyLink::nextPropertyIndex * CentralPropertyLink::PROPERTY_BLOCK_SIZE;
        this->centralPropertiesDB->seekp(newAddress);
        this->centralPropertiesDB->write(dataName, CentralPropertyLink::MAX_NAME_SIZE);
        this->centralPropertiesDB->write(reinterpret_cast<char*>(dataValue), CentralPropertyLink::MAX_VALUE_SIZE);
        if (!this->centralPropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            central_property_link_logger.error("Error while inserting a property " + name + " into block address " +
                                       std::to_string(newAddress));
            return -1;
        }

        this->centralPropertiesDB->flush();

        this->nextPropAddress = newAddress;
        this->centralPropertiesDB->seekp(this->blockAddress + CentralPropertyLink::MAX_NAME_SIZE +
                                  CentralPropertyLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->centralPropertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            central_property_link_logger.error("Error while updating  property next address for " + name +
                                       " into block address " + std::to_string(this->blockAddress));
            return -1;
        }
        central_property_link_logger.info("nextPropertyIndex = " + std::to_string(CentralPropertyLink::nextPropertyIndex));
        CentralPropertyLink::nextPropertyIndex++;  // Increment the shared property index value
        pthread_mutex_unlock(&lockInsertCentralPropertyLink);
        return this->blockAddress;

    }

}

/**
 * Create a brand new property link to an empty node block
 *
 * */
CentralPropertyLink* CentralPropertyLink::create(std::string name, char value[]) {

    pthread_mutex_lock(&lockCreateCentralPropertyLink);
    unsigned int nextAddress = 0;
    char dataName[CentralPropertyLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = CentralPropertyLink::nextPropertyIndex * CentralPropertyLink::PROPERTY_BLOCK_SIZE;
    CentralPropertyLink::centralPropertiesDB->seekp(newAddress);
    CentralPropertyLink::centralPropertiesDB->write(dataName, CentralPropertyLink::MAX_NAME_SIZE);
    CentralPropertyLink::centralPropertiesDB->write(reinterpret_cast<char*>(value), CentralPropertyLink::MAX_VALUE_SIZE);
    if (!CentralPropertyLink::centralPropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
        central_property_link_logger.error("Error while inserting the property = " + name +
                                   " into block a new address = " + std::to_string(newAddress));
        return NULL;
    }
    CentralPropertyLink::centralPropertiesDB->flush();
    central_property_link_logger.info("nextPropertyIndex = " + std::to_string(CentralPropertyLink::nextPropertyIndex));
    central_property_link_logger.info("newAddress = " + std::to_string(newAddress));
    CentralPropertyLink::nextPropertyIndex++;  // Increment the shared property index value
    pthread_mutex_unlock(&lockCreateCentralPropertyLink);
    return new CentralPropertyLink(newAddress, name, value, nextAddress);
}

/**
 * If a nextPropAddress available return CentralPropertyLink with that else return null pointer
 * */
CentralPropertyLink* CentralPropertyLink::next() {
    if (this->nextPropAddress) {
        return new CentralPropertyLink(this->nextPropAddress);
    }
    return NULL;
}

bool CentralPropertyLink::isEmpty() { return !(this->blockAddress); }

CentralPropertyLink* CentralPropertyLink::get(unsigned int propertyBlockAddress) {
    CentralPropertyLink* pl = NULL;

    pthread_mutex_lock(&lockGetCentralPropertyLink);
    if (propertyBlockAddress > 0) {
        char propertyName[CentralPropertyLink::MAX_NAME_SIZE] = {0};
        char propertyValue[CentralPropertyLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        CentralPropertyLink::centralPropertiesDB->seekg(propertyBlockAddress);

        central_property_link_logger.info("Searching propertyHead state  = " + std::to_string(CentralPropertyLink::centralPropertiesDB->rdstate()));
        std::cout << "Stream state: " << CentralPropertyLink::centralPropertiesDB->rdstate() << std::endl;
//        std::string  CentralPropertyLink::centralPropertiesDB->rdstate();
        if (!CentralPropertyLink::centralPropertiesDB->read(reinterpret_cast<char*>(&propertyName), CentralPropertyLink::MAX_NAME_SIZE)) {
            central_property_link_logger.error("Error  = " +
                                       std::to_string(CentralPropertyLink::centralPropertiesDB->rdstate()));
            central_property_link_logger.error("Error while reading node property name from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        if (!CentralPropertyLink::centralPropertiesDB->read(reinterpret_cast<char*>(&propertyValue), CentralPropertyLink::MAX_VALUE_SIZE)) {
            central_property_link_logger.error("Error while reading node property value from block = " +
                                       std::to_string(propertyBlockAddress));
        }

        if (!CentralPropertyLink::centralPropertiesDB->read(reinterpret_cast<char*>(&(nextAddress)), sizeof(unsigned int))) {
            central_property_link_logger.error("Error while reading node property next address from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        central_property_link_logger.info("Property head propertyBlockAddress  = " + std::to_string(propertyBlockAddress));
        central_property_link_logger.info("Property head property name  = " + std::string(propertyName));
        central_property_link_logger.info("Property head nextAddress   = " + std::to_string(nextAddress));

        pl = new CentralPropertyLink(propertyBlockAddress, std::string(propertyName), propertyValue, nextAddress);
    }
    pthread_mutex_unlock(&lockGetCentralPropertyLink);
    return pl;
}


