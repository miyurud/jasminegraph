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

#include <sstream>
#include <vector>

#include "../../util/logger/Logger.h"

Logger property_link_logger;

PropertyLink::PropertyLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    if (propertyBlockAddress > 0) {
        this->propertiesDB->seekg(propertyBlockAddress);
        char rawName[PropertyLink::MAX_NAME_SIZE] = {0};

        if (!this->propertiesDB->read(reinterpret_cast<char*>(&rawName), PropertyLink::MAX_NAME_SIZE)) {
            property_link_logger.error("Error while reading property name from block " + std::to_string(blockAddress));
        }
        property_link_logger.debug(
            "Current file descriptor curser position = " + std::to_string(this->propertiesDB->tellg()) +
            " when reading = " + std::to_string(blockAddress));
        if (!this->propertiesDB->read(reinterpret_cast<char*>(&this->value), PropertyLink::MAX_VALUE_SIZE)) {
            property_link_logger.error("Error while reading property value from block " + std::to_string(blockAddress));
        }

        if (!this->propertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            property_link_logger.error("Error while reading property next address from block " +
                                       std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
};

PropertyLink::PropertyLink(unsigned int blockAddress, std::string name, char* rvalue, unsigned int nextAddress)
    : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    // Can't use just string copyer here because of binary data formats
    for (size_t i = 0; i < PropertyLink::MAX_VALUE_SIZE; i++) {
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
unsigned int PropertyLink::insert(std::string name, char* value) {
    char dataName[PropertyLink::MAX_NAME_SIZE] = {0};
    char dataValue[PropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value,
                PropertyLink::MAX_VALUE_SIZE);  // strcpy or strncpy get terminated at null-character hence using memcpy

    property_link_logger.debug("Received name = " + name);
    property_link_logger.debug("Received value = " + std::string(value));
    unsigned int nextAddress = 0;

    if (this->name == name) {
        // TODO[tmkasun]: update existing property value
        property_link_logger.warn("Property key/name already exist key = " + std::string(name));
        return this->blockAddress;
    } else if (this->next()) {  // Traverse to the edge/end of the link list
        return this->next()->insert(name, value);
    } else {  // No next link means end of the link, Now add the new link
        property_link_logger.debug("Next prop index = " + std::to_string(PropertyLink::nextPropertyIndex));

        unsigned int newAddress = PropertyLink::nextPropertyIndex * PropertyLink::PROPERTY_BLOCK_SIZE;
        this->propertiesDB->seekp(newAddress);
        this->propertiesDB->write(dataName, PropertyLink::MAX_NAME_SIZE);
        this->propertiesDB->write(reinterpret_cast<char*>(dataValue), PropertyLink::MAX_VALUE_SIZE);
        if (!this->propertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
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

        PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
        return newAddress;
    }
}

/**
 * Create a brand new property link to an empty node block
 *
 * */
PropertyLink* PropertyLink::create(std::string name, char value[]) {
    unsigned int nextAddress = 0;
    char dataName[PropertyLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = PropertyLink::nextPropertyIndex * PropertyLink::PROPERTY_BLOCK_SIZE;
    PropertyLink::propertiesDB->seekp(newAddress);
    PropertyLink::propertiesDB->write(dataName, PropertyLink::MAX_NAME_SIZE);
    PropertyLink::propertiesDB->write(reinterpret_cast<char*>(value), PropertyLink::MAX_VALUE_SIZE);
    if (!PropertyLink::propertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
        property_link_logger.error("Error while inserting the property = " + name +
                                   " into block a new address = " + std::to_string(newAddress));
        return NULL;
    }
    PropertyLink::propertiesDB->flush();
    PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
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
    if (propertyBlockAddress > 0) {
        char propertyName[PropertyLink::MAX_NAME_SIZE] = {0};
        char propertyValue[PropertyLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        PropertyLink::propertiesDB->seekg(propertyBlockAddress);

        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&propertyName), PropertyLink::MAX_NAME_SIZE)) {
            property_link_logger.error("Error while reading property name from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&propertyValue), PropertyLink::MAX_VALUE_SIZE)) {
            property_link_logger.error("Error while reading property value from block = " +
                                       std::to_string(propertyBlockAddress));
        }

        if (!PropertyLink::propertiesDB->read(reinterpret_cast<char*>(&(nextAddress)), sizeof(unsigned int))) {
            property_link_logger.error("Error while reading property next address from block = " +
                                       std::to_string(propertyBlockAddress));
        }
        pl = new PropertyLink(propertyBlockAddress, std::string(propertyName), propertyValue, nextAddress);
    }
    return pl;
}
unsigned int PropertyLink::nextPropertyIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue
std::string PropertyLink::DB_PATH = "streamStore/properties.db";
std::fstream* PropertyLink::propertiesDB = NULL;
