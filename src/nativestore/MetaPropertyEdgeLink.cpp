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

#include <sstream>
#include <vector>
#include <memory>
#include "MetaPropertyEdgeLink.h"
#include "../util/logger/Logger.h"

Logger metaPropertyEdgeLinkLogger;
thread_local unsigned int MetaPropertyEdgeLink::nextPropertyIndex = 1;
thread_local std::fstream* MetaPropertyEdgeLink::metaEdgePropertiesDB = nullptr;
pthread_mutex_t lockMetaPropertyEdgeLink;
pthread_mutex_t lockCreateMetaPropertyEdgeLink;
pthread_mutex_t lockInsertMetaPropertyEdgeLink;
pthread_mutex_t lockGetMetaPropertyEdgeLink;

MetaPropertyEdgeLink::MetaPropertyEdgeLink(unsigned int propertyBlockAddress) : blockAddress(propertyBlockAddress) {
    pthread_mutex_lock(&lockMetaPropertyEdgeLink);
    if (propertyBlockAddress > 0) {
        this->metaEdgePropertiesDB->seekg(propertyBlockAddress);
        char rawName[MetaPropertyEdgeLink::MAX_NAME_SIZE] = {0};
        if (!this->metaEdgePropertiesDB->read(reinterpret_cast<char*>(&rawName),
                                              MetaPropertyEdgeLink::MAX_NAME_SIZE)) {
            metaPropertyEdgeLinkLogger.error("Error while reading property name from block " +
                                            std::to_string(blockAddress));
        }
        metaPropertyEdgeLinkLogger.debug(
                "Current file descriptor cursor position = " +
                std::to_string(this->metaEdgePropertiesDB->tellg()) +
                " when reading = " + std::to_string(blockAddress));
        if (!this->metaEdgePropertiesDB->read(reinterpret_cast<char*>(&this->value),
                                              MetaPropertyEdgeLink::MAX_VALUE_SIZE)) {
            metaPropertyEdgeLinkLogger.error("Error while reading property value from block " +
                                            std::to_string(blockAddress));
        }

        if (!this->metaEdgePropertiesDB->read(reinterpret_cast<char*>(&(this->nextPropAddress)),
                                              sizeof(unsigned int))) {
            metaPropertyEdgeLinkLogger.error("Error while reading property next address from block " +
                                            std::to_string(blockAddress));
        }

        this->name = std::string(rawName);
    }
    pthread_mutex_unlock(&lockMetaPropertyEdgeLink);
};

MetaPropertyEdgeLink::MetaPropertyEdgeLink(unsigned int blockAddress, std::string name,
                                           char* rvalue, unsigned int nextAddress)
        : blockAddress(blockAddress), nextPropAddress(nextAddress), name(name) {
    memcpy(this->value, rvalue, MetaPropertyEdgeLink::MAX_VALUE_SIZE);
};

unsigned int MetaPropertyEdgeLink::insert(std::string name, char* value) {
    char dataName[MetaPropertyEdgeLink::MAX_NAME_SIZE] = {0};
    char dataValue[MetaPropertyEdgeLink::MAX_VALUE_SIZE] = {0};
    std::strcpy(dataName, name.c_str());
    std::memcpy(dataValue, value, MetaPropertyEdgeLink::MAX_VALUE_SIZE);
    unsigned int nextAddress = 0;
    if (this->name == name) {
        pthread_mutex_lock(&lockInsertMetaPropertyEdgeLink);
        this->metaEdgePropertiesDB->seekp(this->blockAddress + MetaPropertyEdgeLink::MAX_NAME_SIZE);
        this->metaEdgePropertiesDB->write(reinterpret_cast<char*>(dataValue), MetaPropertyEdgeLink::MAX_VALUE_SIZE);
        this->metaEdgePropertiesDB->flush();
        pthread_mutex_unlock(&lockInsertMetaPropertyEdgeLink);
        metaPropertyEdgeLinkLogger.debug("Updating already existing property key = " + std::string(name));
        return this->blockAddress;
    } else if (this->nextPropAddress) {  // Traverse to the edge/end of the link list
        std::unique_ptr<MetaPropertyEdgeLink> pel(new MetaPropertyEdgeLink(this->nextPropAddress));
        return pel->insert(name, value);
    } else {  // No next link means end of the link, Now add the new link
        pthread_mutex_lock(&lockInsertMetaPropertyEdgeLink);
        unsigned int newAddress = MetaPropertyEdgeLink::nextPropertyIndex *
                MetaPropertyEdgeLink::META_PROPERTY_BLOCK_SIZE;
        this->metaEdgePropertiesDB->seekp(newAddress);
        this->metaEdgePropertiesDB->write(dataName, MetaPropertyEdgeLink::MAX_NAME_SIZE);
        this->metaEdgePropertiesDB->write(reinterpret_cast<char*>(dataValue), MetaPropertyEdgeLink::MAX_VALUE_SIZE);
        if (!this->metaEdgePropertiesDB->write(reinterpret_cast<char*>(&nextAddress), sizeof(nextAddress))) {
            metaPropertyEdgeLinkLogger.error("Error while inserting a property " + name
            + " into block address " + std::to_string(newAddress));
            return -1;
        }

        this->metaEdgePropertiesDB->flush();
        this->nextPropAddress = newAddress;
        this->metaEdgePropertiesDB->seekp(this->blockAddress + MetaPropertyEdgeLink::MAX_NAME_SIZE +
                                      MetaPropertyEdgeLink::MAX_VALUE_SIZE);  // seek to current property next address
        if (!this->metaEdgePropertiesDB->write(reinterpret_cast<char*>(&newAddress), sizeof(newAddress))) {
            metaPropertyEdgeLinkLogger.error("Error while updating  property next address for " + name +
                                            " into block address " + std::to_string(this->blockAddress));
            return -1;
        }

        MetaPropertyEdgeLink::nextPropertyIndex++;  // Increment the shared property index value
        pthread_mutex_unlock(&lockInsertMetaPropertyEdgeLink);
        return this->blockAddress;
    }
}

MetaPropertyEdgeLink* MetaPropertyEdgeLink::create(std::string name, char value[]) {
    pthread_mutex_lock(&lockCreateMetaPropertyEdgeLink);
    unsigned int nextAddress = 0;
    char dataName[MetaPropertyEdgeLink::MAX_NAME_SIZE] = {0};
    strcpy(dataName, name.c_str());
    unsigned int newAddress = MetaPropertyEdgeLink::nextPropertyIndex * MetaPropertyEdgeLink::META_PROPERTY_BLOCK_SIZE;
    MetaPropertyEdgeLink::metaEdgePropertiesDB->seekp(newAddress);
    MetaPropertyEdgeLink::metaEdgePropertiesDB->write(dataName, MetaPropertyEdgeLink::MAX_NAME_SIZE);
    MetaPropertyEdgeLink::metaEdgePropertiesDB->write(reinterpret_cast<char*>(value),
                                                      MetaPropertyEdgeLink::MAX_VALUE_SIZE);
    if (!MetaPropertyEdgeLink::metaEdgePropertiesDB->write(reinterpret_cast<char*>(&nextAddress),
                                                           sizeof(nextAddress))) {
        metaPropertyEdgeLinkLogger.error("Error while inserting the property = " + name +
                                        " into block a new address = " + std::to_string(newAddress));
        return nullptr;
    }
    MetaPropertyEdgeLink::metaEdgePropertiesDB->flush();
    MetaPropertyEdgeLink::nextPropertyIndex++;  // Increment the shared property index value
    pthread_mutex_unlock(&lockCreateMetaPropertyEdgeLink);
    return new MetaPropertyEdgeLink(newAddress, name, value, nextAddress);
}

MetaPropertyEdgeLink* MetaPropertyEdgeLink::next() {
    if (this->nextPropAddress) {
        return new MetaPropertyEdgeLink(this->nextPropAddress);
    }
    return nullptr;
}

MetaPropertyEdgeLink* MetaPropertyEdgeLink::get(unsigned int propertyBlockAddress) {
    MetaPropertyEdgeLink* pl = nullptr;

    pthread_mutex_lock(&lockGetMetaPropertyEdgeLink);
    if (propertyBlockAddress > 0) {
        char propertyName[MetaPropertyEdgeLink::MAX_NAME_SIZE] = {0};
        char propertyValue[MetaPropertyEdgeLink::MAX_VALUE_SIZE] = {0};
        unsigned int nextAddress;
        MetaPropertyEdgeLink::metaEdgePropertiesDB->seekg(propertyBlockAddress);

        if (!MetaPropertyEdgeLink::metaEdgePropertiesDB->read(reinterpret_cast<char*>(&propertyName),
                                                      MetaPropertyEdgeLink::MAX_NAME_SIZE)) {
            metaPropertyEdgeLinkLogger.error("Error while reading edge property name from block = " +
                                            std::to_string(propertyBlockAddress));
        }
        if (!MetaPropertyEdgeLink::metaEdgePropertiesDB->read(reinterpret_cast<char*>(&propertyValue),
                                                      MetaPropertyEdgeLink::MAX_VALUE_SIZE)) {
            metaPropertyEdgeLinkLogger.error("Error while reading edge property value from block = " +
                                            std::to_string(propertyBlockAddress));
        }

        if (!MetaPropertyEdgeLink::metaEdgePropertiesDB->read(reinterpret_cast<char*>(&(nextAddress)),
                                                              sizeof(unsigned int))) {
            metaPropertyEdgeLinkLogger.error("Error while reading edge property next address from block = " +
                                            std::to_string(propertyBlockAddress));
        }
        metaPropertyEdgeLinkLogger.debug("Property head propertyBlockAddress  = " +
                                        std::to_string(propertyBlockAddress));
        metaPropertyEdgeLinkLogger.debug("Property head property name  = " + std::string(propertyName));
        metaPropertyEdgeLinkLogger.debug("Property head nextAddress   = " + std::to_string(nextAddress));

        pl = new MetaPropertyEdgeLink(propertyBlockAddress, std::string(propertyName),
                                      propertyValue, nextAddress);
    }
    pthread_mutex_unlock(&lockGetMetaPropertyEdgeLink);
    return pl;
}