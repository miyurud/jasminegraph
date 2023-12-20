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
#include "CtypesLibrary.h"
#include "NodeBlock.h"
#include "NodeManager.h"
#include <unordered_map>
#include <stdio.h>
#include <iostream>
#include <fstream>


void display() {
    printf("Hello Python, I from C server...");
}

static const unsigned long BLOCK_SIZE = 20;
extern "C" {
void CtypesLibrary::get_node_data() {
    auto it = nodeManagerIndex.find("1_1");
    NodeManager *nm = it->second;

//    if (it != nodeManagerIndex.end()) {
//        NodeManager *nm = it->second;
//    }


    std::string nodesDBPath = "/var/tmp/jasminegraph-localstore/g1_p0_nodes.db";
    std::string propertiesDBPath = "/var/tmp/jasminegraph-localstore/g1_p0_properties.db";
    std::ios_base::openmode openMode = std::ios::app;
    std::fstream
            nodesDB(nodesDBPath, std::ios::in | std::ios::out | openMode | std::ios::binary);

    if (nodesDB.is_open()) {
        std::cout << "File opened successfully." << std::endl;

        for (int i = 0; i < 10; i++) {
            unsigned int nodeIndex = i;
            const unsigned int blockAddress = nodeIndex * BLOCK_SIZE;
            nodesDB.seekg(blockAddress);

            unsigned int edgeRef;
            unsigned int centralEdgeRef;
            unsigned char edgeRefPID;
            unsigned int propRef;
            char usageBlock;

            if (!nodesDB.get(usageBlock)) {
                std::cout << "Error while reading usage data from block " << std::endl;
            }

            if (!nodesDB.read(reinterpret_cast<char *>(&edgeRef), sizeof(unsigned int))) {
                std::cout << "Error while reading usage data from block " << std::endl;
            }
            if (!nodesDB.read(reinterpret_cast<char *>(&centralEdgeRef), sizeof(unsigned int))) {
                std::cout << "Error while reading usage data from block " << std::endl;
            }

            if (!nodesDB.read(reinterpret_cast<char *>(&edgeRefPID), sizeof(unsigned char))) {
                std::cout << "Error while reading usage data from block " << std::endl;
            }

            if (!nodesDB.read(reinterpret_cast<char *>(&propRef), sizeof(unsigned int))) {
                std::cout << "Error while reading usage data from block " << std::endl;
            }
            else {
                std::cout << edgeRef << std::endl;
                std::cout << propRef << std::endl;

            }

        }


        // Do something with the file stream

        nodesDB.close();
    } else {
        std::cout << "Failed to open file." << std::endl;
    }

}
}


int our_function(int num_numbers, int *numbers) {
    int i;
    int sum = 0;
    for (i = 0; i < num_numbers; i++) {
        sum += numbers[i];
    }
    return sum;
}


