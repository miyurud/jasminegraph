/**
Copyright 2020 JasmineGraph Team
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

#include "PropertyLink.h"

#include <iostream>
#include <sstream>
#include <vector>

#include "../util/logger/Logger.h"

Logger property_link_logger;
thread_local unsigned int PropertyLink::nextPropertyIndex = 1;
thread_local std::fstream* PropertyLink::propertiesDB = NULL;
pthread_mutex_t lockPropertyLink;
pthread_mutex_t lockCreatePropertyLink;
pthread_mutex_t lockInsertPropertyLink;
pthread_mutex_t lockGetPropertyLink;

PropertyLink::PropertyLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    // The problem is when we create the PropertyLink.
    pthread_mutex_lock(&lockPropertyLink);
    if (propertyBlockAddress > 0) {
        PropertyLink::propertiesDB->seekg(propertyBlockAddress * PropertyLink::PROPERTY_BLOCK_SIZE);
        char rawName[PropertyLink::MAX_NAME_SIZE] = {0};
        //        property_link_logger.info("Traverse state  = " +
        //        std::to_string(PropertyLink::propertiesDB->rdstate()));

        if (!this->propertiesDB->read(reinterpret_cast<char*>(&rawName), PropertyLink::MAX_NAME_SIZE)) {
            property_link_logger.error("Error while reading node property name from block " +
                                       std::to_string(blockAddress));
        }
        property_link_logger.debug(
            "Current file descriptor curser position = " + std::to_string(this->propertiesDB->tellg()) +
            " when reading = " + std::to_string(blockAddress));
        if (!this->propertiesDB->read(reinterpret_cast<char*>(&this->value), PropertyLink::MAX_VALUE_SIZE)) {
            property_link_logger.error("Error while reading node property value from block " +
                                       std::to_string(blockAddress));
        }

        if (!this->propertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            property_link_logger.error("Error while reading node property next address from block " +
                                       std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
    pthread_mutex_unlock(&lockPropertyLink);
};

PropertyLink::PropertyLink(unsigned int blockAddress, std::string name, const char* rvalue, unsigned int nextAddress)
    : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    memcpy(this->value, rvalue, PropertyLink::MAX_VALUE_SIZE);
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
unsigned int PropertyLink::insert(std::string name, const char* value) {
    // TODO(thevindu-w): Temporarily commented to resolve conflict
    /*
    if (this->nextPropAddress) {  // for
        PropertyLink *last;
        // If any element has same property key/name, return its blockAddress
        for (PropertyLink *link = this; link != nullptr; link = link->next()) {
            if (link->name == name) {
                // TODO[tmkasun]: update existing property value
                property_link_logger.warn("Property key/name already exist key = " + name);
                return link->blockAddress;
            }
            last = link;
        }
        return last->insert(name, value);
    }
    if (this->name == name) {
        // TODO[tmkasun]: update existing property value
        property_link_logger.warn("Property key/name already exist key = " + name);
        return this->blockAddress;
    }
    // Following code is only executed in the last element
    */
    char dataName[PropertyLink::MAX_NAME_SIZE] = {0};
    char dataValue[PropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value,
                PropertyLink::MAX_VALUE_SIZE);  // strcpy or strncpy get terminated at null-character hence using memcpy

    //    property_link_logger.debug("Received name = " + name);
    //    property_link_logger.debug("Received value = " + std::string(value));
    unsigned int nextAddress = 0;

    //    property_link_logger.info("current property name  = " + (this->name));
    //    property_link_logger.info("new property name  = " + (name));

    if (this->name == name) {
        // TODO[tmkasun]: update existing property value
        property_link_logger.warn("Property key/name already exist key = " + std::string(name));
        return this->blockAddress;
    } else if (this->nextPropAddress) {  // Traverse to the edge/end of the link list
        property_link_logger.info("Next Called");
        return this->next()->insert(name, value);
    } else {  // No next link means end of the link, Now add the new link
        //        property_link_logger.debug("Next prop index = " + std::to_string(PropertyLink::nextPropertyIndex));

        pthread_mutex_lock(&lockInsertPropertyLink);
        unsigned int newAddress = PropertyLink::nextPropertyIndex * PropertyLink::PROPERTY_BLOCK_SIZE;
        this->propertiesDB->seekp(newAddress);
        this->propertiesDB->write(dataName, PropertyLink::MAX_NAME_SIZE);
        this->propertiesDB->write(reinterpret_cast<char*>(dataValue), PropertyLink::MAX_VALUE_SIZE);
        if (!this->propertiesDB->write(reinterpret_cast<char*>(nextAddress), sizeof(nextAddress))) {
            property_link_logger.error("Error while inserting a property " + name + " into block address " +
                                       std::to_string(newAddress));
            return -1;
        }

        this->propertiesDB->flush();

        this->nextPropAddress = newAddress;
        this->propertiesDB->seekp(this->blockAddress + PropertyLink::MAX_NAME_SIZE +
                                  PropertyLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->propertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            property_link_logger.error("Error while updating  property next address for " + name +
                                       " into block address " + std::to_string(this->blockAddress));
            return -1;
        }
        //        property_link_logger.info("nextPropertyIndex = " + std::to_string(PropertyLink::nextPropertyIndex));
        PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
        pthread_mutex_unlock(&lockInsertPropertyLink);
        return this->blockAddress;
    }

    // TODO(thevindu-w): Temporarily commented to resolve conflict
    // PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
    // return newAddress;
}

/**
 * Create a brand new property link to an empty node block
 *
 * */
PropertyLink* PropertyLink::create(std::string name, const char* value) {
    pthread_mutex_lock(&lockCreatePropertyLink);
    unsigned int nextAddress = 0;
    char dataName[PropertyLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = PropertyLink::nextPropertyIndex * PropertyLink::PROPERTY_BLOCK_SIZE;
    PropertyLink::propertiesDB->seekp(newAddress);
    PropertyLink::propertiesDB->write(dataName, PropertyLink::MAX_NAME_SIZE);
    PropertyLink::propertiesDB->write(reinterpret_cast<const char*>(value), PropertyLink::MAX_VALUE_SIZE);
    if (!PropertyLink::propertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
        property_link_logger.error("Error while inserting the property = " + name +
                                   " into block a new address = " + std::to_string(newAddress));
        return NULL;
    }
    PropertyLink::propertiesDB->flush();
    //    property_link_logger.info("nextPropertyIndex = " + std::to_string(PropertyLink::nextPropertyIndex));
    //    property_link_logger.info("newAddress = " + std::to_string(newAddress));
    PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
    pthread_mutex_unlock(&lockCreatePropertyLink);
    return new PropertyLink(newAddress, name, value, nextAddress);
}

/**
 * If a nextPropAddress available return PropertyLink with that else return null pointer
 * */
PropertyLink* PropertyLink::next() {
    if (this->nextPropAddress) {
        return new PropertyLink(this->nextPropAddress);
    }
    return NULL;
}

bool PropertyLink::isEmpty() { return !(this->blockAddress); }

PropertyLink* PropertyLink::get(unsigned int propertyBlockAddress) {
    PropertyLink* pl = NULL;

    pthread_mutex_lock(&lockGetPropertyLink);
    if (propertyBlockAddress > 0) {
        char propertyName[PropertyLink::MAX_NAME_SIZE] = {0};
        char propertyValue[PropertyLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        PropertyLink::propertiesDB->seekg(propertyBlockAddress * PropertyLink::PROPERTY_BLOCK_SIZE);

        //        property_link_logger.info("Searching propertyHead state  = " +
        //        std::to_string(PropertyLink::propertiesDB->rdstate())); std::cout << "Stream state: " <<
        //        PropertyLink::propertiesDB->rdstate() << std::endl; std::string PropertyLink::propertiesDB->rdstate();
        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&propertyName), PropertyLink::MAX_NAME_SIZE)) {
            //            property_link_logger.error("Error  = " +
            //                                       std::to_string(PropertyLink::propertiesDB->rdstate()));
            property_link_logger.error("Error while reading node property name from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&propertyValue), PropertyLink::MAX_VALUE_SIZE)) {
            property_link_logger.error("Error while reading node property value from block = " +
                                       std::to_string(propertyBlockAddress));
        }

        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&(nextAddress)), sizeof(unsigned int))) {
            property_link_logger.error("Error while reading node property next address from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        //        property_link_logger.info("Property head propertyBlockAddress  = " +
        //        std::to_string(propertyBlockAddress)); property_link_logger.info("Property head property name  = " +
        //        std::string(propertyName)); property_link_logger.info("Property head nextAddress   = " +
        //        std::to_string(nextAddress));

        pl = new PropertyLink(propertyBlockAddress, std::string(propertyName), propertyValue, nextAddress);
    }
    pthread_mutex_unlock(&lockGetPropertyLink);
    return pl;
}
