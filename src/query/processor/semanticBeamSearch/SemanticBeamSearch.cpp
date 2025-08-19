//
// Created by sajeenthiran on 2025-08-18.
//

#include "SemanticBeamSearch.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "../../../nativestore/NodeManager.h"
#include "../../../vectorStore/FaissStore.h"
#include "../cypher/runtime/Helpers.h"


SemanticBeamSearch::SemanticBeamSearch( FaissStore* faissStore,
                                        std::vector<float> emb, int k ,GraphConfig gc )
    : faissStore(faissStore), emb(std::move(emb)), k(k) , gc(gc)
{
    // Constructor implementation
}
std::vector<int> SemanticBeamSearch::getSeedNodes()
{
    std::cout<<"getSeedNodes"<<std::endl;
    NodeManager nodeManager(gc);
    // std::cout<< "Searching for top "  << " nodes using FAISS...\n";
    std::cout<<"28"<<std::endl;
    // check the emb


    try
    {
        auto results = faissStore->search(emb, 5);
        std::cout << "Top " << results.size() << " nodes found:\n";
        for (auto& [id, dist] : results) {
        std::cout << "ID: " << id << ", Distance: " << dist << "\n";
        //     NodeBlock* node = nodeManager.get( std::to_string(id));
        //
        //     json nodeData;
        //
        //     std::string value(node->getMetaPropertyHead()->value);
        //     if (value == to_string(gc.partitionID)) {
        //         nodeData["partitionID"] = value;
        //         std::map<std::string, char*> properties = node->getAllProperties();
        //         for (auto property : properties) {
        //             nodeData[property.first] = property.second;
        //         }
        //         for (auto& [key, value] : properties) {
        //             delete[] value;  // Free each allocated char* array
        //         }
        //         properties.clear();
        //
        //         std::cout << "Node ID: " << node->id << ", Properties: " << nodeData.dump() << "\n";
        //
        //     }
        //
        }
    } catch ( std::exception& e )
    {
        std::cout << e.what() << "\n";
    }

}
