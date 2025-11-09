/**
Copyright 2025 JasmineGraph Team
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

#include "MetaPropertyLink.h"
#include "../util/logger/Logger.h"

Logger meta_property_link_logger;
thread_local unsigned int MetaPropertyLink::nextPropertyIndex = 1;
thread_local std::fstream* MetaPropertyLink::metaPropertiesDB = NULL;
pthread_mutex_t lockMetaPropertyLink;
pthread_mutex_t lockCreateMetaPropertyLink;
pthread_mutex_t lockInsertMetaPropertyLink;
pthread_mutex_t lockGetMetaPropertyLink;

MetaPropertyLink::MetaPropertyLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    pthread_mutex_lock(&lockMetaPropertyLink);
    if (propertyBlockAddress >= 0) {
        MetaPropertyLink::metaPropertiesDB->seekg(propertyBlockAddress);
        char rawName[MetaPropertyLink::MAX_NAME_SIZE] = {0};

        if (!this->metaPropertiesDB->read(reinterpret_cast<char*>(&rawName), MetaPropertyLink::MAX_NAME_SIZE)) {
            meta_property_link_logger.error("Error while reading node property name from block " +
                                       std::to_string(blockAddress));
        }
        meta_property_link_logger.debug(
                "Current file descriptor curser position = " + std::to_string(this->metaPropertiesDB->tellg()) +
                " when reading = " + std::to_string(blockAddress));
        if (!this->metaPropertiesDB->read(reinterpret_cast<char*>(&this->value), MetaPropertyLink::MAX_VALUE_SIZE)) {
            meta_property_link_logger.error("Error while reading node property value from block " +
                                       std::to_string(blockAddress));
        }

        if (!this->metaPropertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)), sizeof(unsigned int))) {
            meta_property_link_logger.error("Error while reading node property next address from block " +
                                       std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
    pthread_mutex_unlock(&lockMetaPropertyLink);
};

MetaPropertyLink::MetaPropertyLink(unsigned int blockAddress, std::string name,
                                   const char* rvalue, unsigned int nextAddress)
        : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    memcpy(this->value, rvalue, MetaPropertyLink::MAX_VALUE_SIZE);
};

unsigned int MetaPropertyLink::insert(std::string name, const char* value) {
    char dataName[MetaPropertyLink::MAX_NAME_SIZE] = {0};
    char dataValue[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value,
                MetaPropertyLink::MAX_VALUE_SIZE);

    unsigned int nextAddress = 0;

    if (this->name == name) {
        pthread_mutex_lock(&lockInsertMetaPropertyLink);
        this->metaPropertiesDB->seekp(this->blockAddress + MetaPropertyLink::MAX_NAME_SIZE);
        this->metaPropertiesDB->write(reinterpret_cast<char*>(dataValue), MetaPropertyLink::MAX_VALUE_SIZE);
        this->metaPropertiesDB->flush();
        pthread_mutex_unlock(&lockInsertMetaPropertyLink);
        meta_property_link_logger.debug("Updating already existing property key = " + std::string(name));
        return this->blockAddress;
    } else if (this->nextPropAddress) {  // Traverse to the edge/end of the link list
        return this->next()->insert(name, value);
    } else {
        pthread_mutex_lock(&lockInsertMetaPropertyLink);
        unsigned int newAddress = MetaPropertyLink::nextPropertyIndex * MetaPropertyLink::META_PROPERTY_BLOCK_SIZE;
        this->metaPropertiesDB->seekp(newAddress);
        this->metaPropertiesDB->write(dataName, MetaPropertyLink::MAX_NAME_SIZE);
        this->metaPropertiesDB->write(reinterpret_cast<char*>(dataValue), MetaPropertyLink::MAX_VALUE_SIZE);
        if (!this->metaPropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            meta_property_link_logger.error("Error while inserting a property " + name + " into block address " +
                                       std::to_string(newAddress));
            return -1;
        }

        this->metaPropertiesDB->flush();

        this->nextPropAddress = newAddress;
        this->metaPropertiesDB->seekp(this->blockAddress + MetaPropertyLink::MAX_NAME_SIZE +
                                  MetaPropertyLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->metaPropertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            meta_property_link_logger.error("Error while updating  property next address for " + name +
                                       " into block address " + std::to_string(this->blockAddress));
            return -1;
        }

        MetaPropertyLink::nextPropertyIndex++;  // Increment the shared property index value
        pthread_mutex_unlock(&lockInsertMetaPropertyLink);
        return this->blockAddress;
    }
}

MetaPropertyLink* MetaPropertyLink::create(std::string name, const char* value) {
    pthread_mutex_lock(&lockCreateMetaPropertyLink);
    unsigned int nextAddress = 0;
    char dataName[MetaPropertyLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = MetaPropertyLink::nextPropertyIndex * MetaPropertyLink::META_PROPERTY_BLOCK_SIZE;
    MetaPropertyLink::metaPropertiesDB->seekp(newAddress);
    MetaPropertyLink::metaPropertiesDB->write(dataName, MetaPropertyLink::MAX_NAME_SIZE);
    MetaPropertyLink::metaPropertiesDB->write(reinterpret_cast<const char*>(value), MetaPropertyLink::MAX_VALUE_SIZE);
    if (!MetaPropertyLink::metaPropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
        meta_property_link_logger.error("Error while inserting the property = " + name +
                                   " into block a new address = " + std::to_string(newAddress));
        return NULL;
    }
    MetaPropertyLink::metaPropertiesDB->flush();
    MetaPropertyLink::nextPropertyIndex++;  // Increment the shared property index value
    pthread_mutex_unlock(&lockCreateMetaPropertyLink);
    return new MetaPropertyLink(newAddress, name, value, nextAddress);
}

MetaPropertyLink* MetaPropertyLink::next() {
    if (this->nextPropAddress) {
        return new MetaPropertyLink(this->nextPropAddress);
    }
    return nullptr;
}

MetaPropertyLink* MetaPropertyLink::get(unsigned int propertyBlockAddress) {
    MetaPropertyLink* pl = nullptr;

    meta_property_link_logger.debug("Entering MetaPropertyLink::get with propertyBlockAddress = "
        + std::to_string(propertyBlockAddress));
    pthread_mutex_lock(&lockGetMetaPropertyLink);
    if (propertyBlockAddress >= 0) {
        char propertyName[MetaPropertyLink::MAX_NAME_SIZE] = {0};
        char propertyValue[MetaPropertyLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        meta_property_link_logger.debug("Seeking to propertyBlockAddress = " +
            std::to_string(propertyBlockAddress));
        MetaPropertyLink::metaPropertiesDB->seekg(propertyBlockAddress);

        if (!MetaPropertyLink::metaPropertiesDB->read(reinterpret_cast<char*>(&propertyName),
                                                      MetaPropertyLink::MAX_NAME_SIZE)) {
            meta_property_link_logger.error("Error while reading node property name from block = " +
                                       std::to_string(propertyBlockAddress));
        } else {
            meta_property_link_logger.debug("Read property name: " + std::string(propertyName));
        }
        if (!MetaPropertyLink::metaPropertiesDB->read(reinterpret_cast<char*>(&propertyValue),
                                                      MetaPropertyLink::MAX_VALUE_SIZE)) {
            meta_property_link_logger.error("Error while reading node property value from block = " +
                                       std::to_string(propertyBlockAddress));
        } else {
            meta_property_link_logger.debug("Read property value for property name: " +
                std::string(propertyName));
        }

        if (!MetaPropertyLink::metaPropertiesDB->read(reinterpret_cast<char*>(&(nextAddress)),
                                                      sizeof(unsigned int))) {
            meta_property_link_logger.error("Error while reading node property next address from block = " +
                                       std::to_string(propertyBlockAddress));
        } else {
            meta_property_link_logger.debug("Read next address: " + std::to_string(nextAddress));
        }

        pl = new MetaPropertyLink(propertyBlockAddress, std::string(propertyName),
                                  propertyValue, nextAddress);
        meta_property_link_logger.debug("Created MetaPropertyLink object at address: " +
            std::to_string(propertyBlockAddress));
    }
    pthread_mutex_unlock(&lockGetMetaPropertyLink);
    meta_property_link_logger.debug("Exiting MetaPropertyLink::get with propertyBlockAddress = " +
        std::to_string(propertyBlockAddress));
    return pl;
}