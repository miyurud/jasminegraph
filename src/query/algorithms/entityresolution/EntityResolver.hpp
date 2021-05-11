//
// Created by root on 4/16/21.
//

#ifndef JASMINEGRAPH_ENTITYRESOLVER_HPP
#define JASMINEGRAPH_ENTITYRESOLVER_HPP

#include "armadillo"
#include "BloomFilter.hpp"
#include "set"
#include "vector"

class EntityResolver {

public:

    std::map<int, std::string>
    createFilters(std::map<int, std::vector<std::string>> entityData, int filterSize, int numHashes);

    std::vector<std::map<int, std::string>>
    createFilters(std::map<int, std::vector<std::string>> entityData, std::map<int, std::vector<int>> neighborhoodData,
                  int filterSize, int numHashes);

    void clusterFilters(std::string graphID, int partitionCount, int noClusters);

    void seperateClusters(arma::Mat<float> &data, arma::Mat<short> pred, int clusterCount, std::string outfilePrefix);

    std::map<unsigned long, std::vector<std::string>> bucketLocalClusters(int filterSize, std::vector<int> clusters);

    std::map<unsigned long, std::set<std::string>> combineLocalBuckets(std::vector<std::map<unsigned long, std::vector<std::string>>> totalWorkerBuckets);

    /**
     * Method call for party coordinator to combine buckets of clusters given by each party to compute the similar clusters
     * Clusters that fall under the same bucket will be considered similar
     * Buckets that have clusters from all the parties will be kept since only they correspond to the possible common entities
     * across all parties
     * @param allBuckets Map of party IDs to mapping of bucket IDs to sets of clusters of that party
     * @return Map of bucket ID to clusters across all parties
     */
    std::map<unsigned long, std::map<std::string, std::set<std::string>>> getSimilarClusters(std::map<std::string, std::map<unsigned long, std::set<std::string>>> allBuckets);

    /**
     * Compare filters against each other and get the most similar.
     * Classify as similar or not using a similarity threshold
     * @param selfFilters Filters coming from the party doing the computation
     * @param otherFilters Filters from the other party
     * @param similarityThreshold Similarity threshold for classification
     * @return Vector of two maps
     */
    std::vector<std::map<std::string, std::string>> compareFilters(arma::Mat<short> &selfFilters, arma::Mat<short> &otherFilters, float similarityThreshold = 0.9);

    /**
     * @param results A vector of maps
     */
    void combineFilterwiseResults(std::vector<std::map<std::string, std::string>> results);

    /**
     * Compute the common entities accross all parties given a chainable pairwise common entity information
     * @param pairwiseCommonEntities Map of party-ids to the mappings of common entities between other parties
     * @return A map of party ID to a vector of cluster indices that correspond to the common entities
     */
    std::map<std::string, std::vector<std::string>> synchronizeCommonEntities(std::map<std::string, std::map<std::string, std::map<std::string, std::string>>> pairwiseCommonEntities);

    arma::Mat<short> generateCRVs(int minhashSize, int noClusters);


};



#endif //JASMINEGRAPH_ENTITYRESOLVER_HPP
