#include "PropertyLink.h"

#include <iostream>
#include <sstream>
#include <vector>

PropertyLink::PropertyLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    if (propertyBlockAddress > 0) {
        bool brk = propertyBlockAddress == 51940;
        this->propertiesDB->seekg(propertyBlockAddress);
        char rawName[PropertyLink::MAX_NAME_SIZE] = {0};
        char rawValue[PropertyLink::MAX_VALUE_SIZE] = {0};

        if (!this->propertiesDB->read(reinterpret_cast<char*>(&rawName), PropertyLink::MAX_NAME_SIZE)) {
            std::cout << "ERROR: Error while reading property name from block " << blockAddress << std::endl;
        }
        std::cout << "DEBUG: " << this->propertiesDB->tellg() << std::endl;
        if (!this->propertiesDB->read(reinterpret_cast<char*>(&rawValue), PropertyLink::MAX_VALUE_SIZE)) {
            std::cout << "ERROR: Error while reading property value from block " << blockAddress << std::endl;
        }
        std::cout << "DEBUG: " << this->propertiesDB->tellg() << std::endl;

        if (!this->propertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            std::cout << "ERROR: Error while reading property next address from block " << blockAddress << std::endl;
        }
        std::cout << "DEBUG: " << this->propertiesDB->tellg() << std::endl;

        this->name = std::string(rawName);
        this->value = &rawValue[0];
    }
};
PropertyLink::PropertyLink() : blockAddress(0), nextPropAddress(0){};

/**
 * propertyAddress; If no property address means no property has been stored
 *  if(!propertyAddress) { // Means this is the very first link in the chain yet to be filled
 *      store property in this link it self
 *  } else if ( //Check this property name is available in this link) {
 *      // If so Update the current link
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

    std::cout << "DEBUG: Received name = " << name << std::endl;
    std::cout << "DEBUG: Received value = " << value << std::endl;
    unsigned int nextAddress = 0;

    if (!this->blockAddress) {  // When no links in the chain
        std::cout << "Insert new property link" << std::endl;
        std::cout << "Next prop index = " << PropertyLink::nextPropertyIndex << std::endl;

        unsigned int headAddress = PropertyLink::nextPropertyIndex * PropertyLink::PROPERTY_BLOCK_SIZE;

        this->propertiesDB->seekp(headAddress);
        this->propertiesDB->write(dataName, PropertyLink::MAX_NAME_SIZE);
        this->propertiesDB->write(reinterpret_cast<char*>(dataValue), PropertyLink::MAX_VALUE_SIZE);
        if (!this->propertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            std::cout << "ERROR: Error while inserting a property " << name << " into block address " << blockAddress
                      << std::endl;
            return -1;
        }
        this->propertiesDB->flush();
        this->name = name;
        this->value = value;
        this->blockAddress = headAddress;

        PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
        return headAddress;

    } else if (this->name == name) {
        // TODO: update existing property value
    } else if (this->next()) {
        return this->next()->insert(name, value);
    } else {
        std::cout << "Insert new property link" << std::endl;
        std::cout << "Next prop index = " << PropertyLink::nextPropertyIndex << std::endl;

        unsigned int newAddress = PropertyLink::nextPropertyIndex * PropertyLink::PROPERTY_BLOCK_SIZE;
        bool brk = newAddress == 51940;
        this->propertiesDB->seekp(newAddress);
        this->propertiesDB->write(dataName, PropertyLink::MAX_NAME_SIZE);
        this->propertiesDB->write(reinterpret_cast<char*>(dataValue), PropertyLink::MAX_VALUE_SIZE);
        if (!this->propertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            std::cout << "ERROR: Error while inserting a property " << name << " into block address " << newAddress
                      << std::endl;
            return -1;
        }
        this->propertiesDB->flush();

        this->nextPropAddress = newAddress;
        this->propertiesDB->seekp(this->blockAddress + PropertyLink::MAX_NAME_SIZE +
                                  PropertyLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->propertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            std::cout << "ERROR: Error while updating property next address" << name << " into block address "
                      << this->blockAddress << std::endl;
            return -1;
        }

        PropertyLink::nextPropertyIndex++;  // Increment the shared property index value
        return newAddress;
    }
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

bool PropertyLink::isEmpty(){
    return !(this->blockAddress);
}

const unsigned long PropertyLink::MAX_NAME_SIZE = 12;
const unsigned long PropertyLink::MAX_VALUE_SIZE = 180;
const unsigned long PropertyLink::PROPERTY_BLOCK_SIZE =
    PropertyLink::MAX_NAME_SIZE + PropertyLink::MAX_VALUE_SIZE + sizeof(unsigned int);
unsigned int PropertyLink::nextPropertyIndex = 1;  // Starting with 1 because of the 0 and '\0' differentiation issue
std::string PropertyLink::DB_PATH = "streamStore/properties.db";
std::fstream* PropertyLink::propertiesDB = NULL;
