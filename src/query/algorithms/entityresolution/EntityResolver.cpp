//
// Created by root on 4/16/21.
//

#include "EntityResolver.hpp"
#include "armadillo"
#include "BloomFilter.hpp"
#include "Kmeans.hpp"
#include "MinHash.hpp"
#include "MurmurHash3.h"
#include "../../../util/Utils.h"

using namespace std;
using namespace arma;

/**
 * Create attribute filters for entities given the feature infomation
 * @param entityData A map of node IDs to a vector of string values pertaining to the weak identifiers of that node
 * @param filterSize The length of bloom filters
 * @param numHashes  no of hash functions to use when creating the bloom filter
 * @return Map of node IDs to bloom filter string
 */
map<int, string> EntityResolver::createFilters(map<int, vector<string>> entityData, int filterSize = 256,
                                               int numHashes = 4) {
    //Create bloom filters
    cout << "Creating filters" << endl;
    Utils utils;
    map<int, string> attrFilters;
    //For each entity create attr and structural bloom filters
    for (const auto &entity: entityData) {
        //Create attr bloom filter
        BloomFilter attrFilter(filterSize, numHashes);
        vector<string> attributes = entity.second;
        //Add node attributes to bloom filter
        for (auto attr: attributes) {
            cout << attr << endl;
            attrFilter.insert(attr);
        }
        //Convert bloom filter to appropriate string
        string filterStr = attrFilter.m_bits.to_string();
        cout << filterStr << endl;
        filterStr = utils.replace(filterStr, "0", ",0");
        filterStr = utils.replace(filterStr, "1", ",1");
        attrFilters[entity.first] = filterStr;
    }

    return attrFilters;
}

/**
 * Create attribute filters and structural filters for entities given the feature infomation and who its neighbors are
 * @param entityData A map of node IDs to a vector of string values pertaining to the weak identifiers of that node
 * @param neighborhoodData A map of node IDs to a vector its neighbors' node IDs
 * @param filterSize The length of bloom filters
 * @param numHashes  no of hash functions to use when creating the bloom filter
 * @return Map of node IDs to bloom filter string
 */
vector<map<int, string>>
EntityResolver::createFilters(map<int, vector<string>> entityData, map<int, vector<int>> neighborhoodData,
                              int filterSize = 256,
                              int numHashes = 4) {
//Create bloom filters
    cout << "Creating filters" << endl;
    Utils utils;
    map<int, string> attrFilters;
    map<int, string> structFilters;
//For each entity create attr and structural bloom filters
    for (const auto &entity: entityData) {
//Create attr bloom filter
        BloomFilter attrFilter(filterSize, numHashes);
        vector<string> attributes = entity.second;
//Add node attributes to bloom filter
        for (auto attr: attributes) {
            cout << attr << endl;
            attrFilter.insert(attr);
        }
//Convert bloom filter to appropriate string
        string filterStr = attrFilter.m_bits.to_string();
        cout << filterStr << endl;
        filterStr = utils.replace(filterStr, "0", ",0");
        filterStr = utils.replace(filterStr, "1", ",1");
        attrFilters[entity.first] = filterStr;

//Create structural filter
        BloomFilter structFilter(256, 4);
//For each neighbour add selected attribute to bloom filter
        for (auto neighbour: neighborhoodData[entity.first]) {
            string selectedAttr = entityData[neighbour][0];
            structFilter.insert(selectedAttr);
        }
//Convert bloom filter to appropriate string
        filterStr = structFilter.m_bits.to_string();
        filterStr = utils.replace(filterStr, "0", ",0");
        filterStr = utils.replace(filterStr, "1", ",1");
        structFilters[entity.first] = filterStr;
    }

    return {attrFilters, structFilters};
}

/**
 * Cluster created and collected Bloom filters of a given graph ID into a desired number of clusters
 * @param graphID Graph ID of corresponding graph
 * @param partitionCount The count of the (collected) partitions for the graph
 * @param noClusters Desired clustering amount
 */
void EntityResolver::clusterFilters(string graphID, int partitionCount, int noClusters) {

    Utils utils;
    int noWorkers = stoi(utils.getJasmineGraphProperty("org.jasminegraph.server.nworkers"));
    string partitionID = "0"; // use first worker related partitons
    std::string attrFilterPath = "";
    //std::string structFilterPath = "";

    attrFilterPath =
            utils.getJasmineGraphProperty("org.jasminegraph.server.instance.entityresolutionfolder") + "AttrFilters_" +
            graphID + "_" + partitionID + ".txt";
    //structFilterPath = utils.getJasmineGraphProperty("org.jasminegraph.entity.location")+"structfilters_" + graphID + "_" + partitionID + ".txt";

    Mat<float> data;
    data.load(attrFilterPath);

    for (int i = 1; i < partitionCount; i++) {

        string filepath = "attrfilters_" + graphID + "_" + to_string(i) + ".txt";
        Mat<float> workerData;
        workerData.load(filepath);
        data = join_cols(data, workerData);
    }

    Mat<float> ids = data.col(0);
    data.shed_col(0);
    inplace_trans(data, "lowmem");

    Kmeans<float> model(noClusters);
    model.fit(data, 10);

    //Apply clustering to bloom filters
    Mat<short> pred = model.apply(data);

    //Re-transpose data for saving
    inplace_trans(data, "lowmem");
    //Join with graph ids again
    data = join_rows(ids, data);
    //For each cluster write bloom filters of said cluster into a separate file
    int clusterCount = model.getMeans().n_cols;
    //Separate attr filters into clusters
    seperateClusters(data, pred, clusterCount, "AttrFiltersCluster_");
    data.clear();
    //Separate struct filters into clusters
    //data.load(structFilterPath, arma::csv_ascii);
    //seperateClusters(data, pred, clusterCount, "structfilterscluster");

}

/**
 * Seperate Bloom filters into cluster files given the prediction for corresponding data point
 * @param data Matrix of bloom filters
 * @param pred Column matrix of cluster prediction for associated data point
 * @param clusterCount No of clusters
 * @param outfilePrefix Save file name prefix (Without extension)
 */
void seperateClusters(Mat<float> &data, Mat<short> pred, int clusterCount, string outfilePrefix) {
    Utils utils;
    //Iterate through clusters and filter Bloom filters of each cluster, then write to file
    for (int i = 0; i < clusterCount; i++) {
        //Filter indices of filters belonging to cluster
        Col<uword> indices = find(pred == i);
        //Filter out cluster data
        Mat<short> clusterData = conv_to<Mat<short>>::from(data.rows(indices));
        //Write to file
        string outfile = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.entityresolutionfolder") +
                         outfilePrefix + to_string(i) + ".txt";
        clusterData.save(outfile, arma::csv_ascii);
    }
}

/**
 * Generate worker-local candidate sets by generating cluster representative vectors and bucket each cluster
 * @param filterSize Size of the bloom filter
 * @param clusters Vector of worker-local clusters
 */
map<unsigned long, vector<string>> EntityResolver::bucketLocalClusters(int filterSize, vector<int> clusters) {

    Utils utils;

    int clusterCount = clusters.size();
    Mat<short> CRVs(filterSize, clusterCount);
    for (int i = 0; i < clusterCount; i++) {
        //Load cluster file into memory
        string filename = utils.getJasmineGraphProperty("org.jasminegraph.entity.location") + "AttrFiltersCluster_" +
                          to_string(clusters.at(i)) + ".txt";
        Mat<float> clusterData;
        clusterData.load(filename, arma::csv_ascii);
        //Remove node id column
        clusterData.shed_col(0);
        inplace_trans(clusterData, "lowmem");
        //Create minhash signature of cluster
        MinHash minHash(100, 256);
        Col<short> crv = minHash.generateCRV(clusterData, 50);
        //Store in matrix
        CRVs.col(i) = crv;
    }

    //Generate local candidate sets
    int bandLength = 10;
    std::ostringstream s;
    hash<string> stdhash;
    map<unsigned long, vector<string>> lshBuckets;
    for (int i = 0; i < clusterCount; i++) {
        Col<short> crv = CRVs.col(i);
        crv.st().raw_print(s);
        string crvStr = s.str();
        crvStr = crvStr.substr(0, (crvStr.size() > 0) ? (crvStr.size() - 1) : 0);
        crvStr.erase(remove(crvStr.begin(), crvStr.end(), ' '), crvStr.end());

        for (int j = 0; j < bandLength; j++) {
            //Select appropriate band
            string crvband = crvStr.substr(j * bandLength, (j + 1) * bandLength);
            unsigned long bucket = stdhash(crvband);
            string name = "A" + to_string(i); //Party name + cluster id
            lshBuckets[bucket].emplace_back(name);
        }
    }

    return lshBuckets;
}


map<unsigned long, set<string>> combineLocalBuckets(vector<map<unsigned long, vector<string>>> totalWorkerBuckets) {
    map<unsigned long, set<string>> combinedBuckets;
    for (auto workerBuckets: totalWorkerBuckets) {
        for (auto bucket: workerBuckets) {
            unsigned long bucketID = bucket.first;
            vector<string> clusters = bucket.second;
            set<string> existingBucket = combinedBuckets[bucketID];
            copy(clusters.begin(), clusters.end(), std::inserter(existingBucket, existingBucket.end()));
        }
    }

    return combinedBuckets;
}

/**
 * Method call for party coordinator to combine buckets of clusters given by each party to compute the similar clusters
 * Clusters that fall under the same bucket will be considered similar
 * Buckets that have clusters from all the parties will be kept since only they correspond to the possible common entities
 * across all parties
 * @param allBuckets Map of party IDs to mapping of bucket IDs to sets of clusters of that party
 * @return Map of bucket ID to clusters across all parties
 */
map<unsigned long, map<string, set<string>>>
getSimilarClusters(map<string, map<unsigned long, set<string>>> allBuckets) {
    map<unsigned long, map<string, set<string>>> combinedBuckets; //Map of bucket to organisation-cluster
//Combine all organization buckets
    for (auto orgBuckets: allBuckets) {
        string partyID = orgBuckets.first;

        for (auto bucket: orgBuckets.second) {
            unsigned long bucketID = bucket.first;
            set<string> clusters = bucket.second;
            combinedBuckets[bucketID][partyID].insert(clusters.begin(), clusters.end());
        }
    }

//Filter buckets
    map<unsigned long, map<string, set<string>>> filteredBuckets;

    for (auto bucket: combinedBuckets) {
        if (bucket.second.size() >= 3) {
            filteredBuckets[bucket.first] = bucket.second;
        }
    }

    return filteredBuckets;
}

/**
 * Compare filters against each other and get the most similar.
 * Classify as similar or not using a similarity threshold
 * @param selfFilters Filters coming from the party doing the computation
 * @param otherFilters Filters from the other party
 * @param similarityThreshold Similarity threshold for classification
 * @return Vector of two maps
 */
vector<map<string, string>>
compareFilters(Mat<short> &selfFilters, Mat<short> &otherFilters, float similarityThreshold = 0.9) {
    map<string, string> commonEntityMapSelf;
    map<string, string> commonEntityMapOther;

    Row<short> denominator = arma::sum(selfFilters, 0) + arma::sum(otherFilters, 0); //denominator of dice coeff

    //For each filter in self cluster, compare against filters from other clusters and determine most similar filter
    for (int i = 0; i < selfFilters.n_cols; i++) {
        //Compute dice coefficient values
        Col<short> selfFilter = selfFilters.col(i);
        Row<short> numerator =
                2 * arma::sum(otherFilters.each_col() % selfFilter, 0); //Numerator to compute the dice coeff
        Row<float> diceCoeff = (conv_to<Mat<float>>::from(numerator) / conv_to<Mat<float>>::from(denominator));

        //Get the most similar filter and check if it's meets the similarity threshold
        uword maxIndex = arma::index_max(diceCoeff); //arg max
        if (diceCoeff(maxIndex) > similarityThreshold) {
            //Assign the two filters as the same common entity
            commonEntityMapSelf[to_string(i)] = to_string(maxIndex);
            commonEntityMapOther[to_string(maxIndex)] = to_string(i);
        }

    }

    return {commonEntityMapSelf, commonEntityMapOther};
}

/**
 * @param results A vector of maps
 */
void combineFilterwiseResults(vector<map<string, string>> results) {
    map<string, string> combinedEntityMap;
    for (auto filterEntityMap: results) {
        for (auto entity: filterEntityMap) {
            combinedEntityMap[entity.first] = entity.second;
        }
    }
}

/**
 * Compute the common entities accross all parties given a chainable pairwise common entity information
 * @param pairwiseCommonEntities Map of party-ids to the mappings of common entities between other parties
 * @return A map of party ID to a vector of cluster indices that correspond to the common entities
 */
map<string, vector<string>>
synchronizeCommonEntities(map<string, map<string, map<string, string>>> pairwiseCommonEntities) {
    map<string, vector<string>> partyCommonEntityMap;
    string firstPassEndParty;

    //Get common entitiy ids of self
    string currentParty = "A"; //Self party ID
    string nextParty = pairwiseCommonEntities[currentParty].begin()->first;
    vector<string> currentPartyIds;
    for (auto idPairs: pairwiseCommonEntities[currentParty][nextParty]) {
        currentPartyIds.emplace_back(idPairs.first);
    }

    //First pass iteration through intermediate pairwise results
    for (int i = 0; i < pairwiseCommonEntities.size(); i++) {
        auto commonEntityMap = pairwiseCommonEntities[currentParty][nextParty];
        //Iterate through common entity ids of currentParty with nextParty
        vector<string> nextPartyIds;
        for (string id: currentPartyIds) {
            partyCommonEntityMap[currentParty].emplace_back(id);
            //If id is present in the next party common entities, mark it to check in the next iterationa
            if (commonEntityMap.find(id) != commonEntityMap.end()) {
                nextPartyIds.emplace_back(pairwiseCommonEntities[currentParty][nextParty][id]);
            }
        }
        //Party to iterate
        currentParty = nextParty;
        //Filtered next set of ids, when the loop terminates we will have entities of self which are common for all parties
        currentPartyIds = nextPartyIds;
        //Change pointer to next paty in the chain
        nextParty = pairwiseCommonEntities[currentParty].begin()->first;
    }

    //Second pass to compute common entities across all entities
    for (int i = 0; i < pairwiseCommonEntities.size(); i++) {
        //Replace with the entities of current party which are common for all parties
        partyCommonEntityMap[currentParty] = currentPartyIds;

        nextParty = pairwiseCommonEntities[currentParty].begin()->first;
        vector<string> nextPartyIds;
        //For only the entities common for all, get mapping entities of next party
        for (string id: currentPartyIds) {
            nextPartyIds.emplace_back(pairwiseCommonEntities[currentParty][nextParty][id]);
        }
        //Party to iterate
        currentParty = nextParty;
        //Filtered next set of ids
        currentPartyIds = nextPartyIds;
        //Change pointer to next paty in the chain
        nextParty = pairwiseCommonEntities[currentParty].begin()->first;
    }

    return partyCommonEntityMap;
}

void EntityResolver::entityRes(string trainData) {

    Utils utils;
    map<int, vector<string>> entityData;
    map<int, vector<int>> neighborhoodData;

    std::string filePath = "";
    std::string edgefilePath = "";
    std::string attrfilePath = "";
    std::string attrFilterPath = "";
    std::string structFilterPath = "";

    std::vector<std::string> trainargs = Utils::split(trainData, ' ');
    string graphID = trainargs[1];
    string partitionID = trainargs[2];

    //Read attributes from file
    std::ifstream dataFile;
    cout << "reading file" << endl;

    filePath = utils.getJasmineGraphProperty("org.jasminegraph.entity.location");
    dataFile.open("/home/damitha/ubuntu/entity_data/entityData.txt", std::ios::binary | std::ios::in);

    char splitter;
    string line;

    std::getline(dataFile, line);
    splitter = ' ';

    cout << "Getting data" << endl;
    while (!line.empty()) {

        int strpos = line.find(splitter);
        string nodeID = line.substr(0, strpos);
        string weakIDStr = line.substr(strpos + 1, -1);
        entityData[stoi(nodeID)] = utils.split(weakIDStr, splitter);
        cout << line << endl;


        std::getline(dataFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(dataFile, line);
        }
    }
    dataFile.close();

    //Read edgelist from file
    std::ifstream edgeFile;
    std::cout << "reading file" << endl;

    edgefilePath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.trainedmodelfolder")
                   + graphID + "_" + partitionID;
    edgeFile.open(edgefilePath);
    std::getline(edgeFile, line);

    splitter = ' ';

    cout << "Getting neighbourhood data" << endl;
    while (!line.empty()) {

        int strpos = line.find(splitter);
        string vertex1 = line.substr(0, strpos);
        string vertex2 = line.substr(strpos + 1, -1);
        if (neighborhoodData.size() <= 5) {
            neighborhoodData[stoi(vertex1)].emplace_back(stoi(vertex2));
        }


        std::getline(edgeFile, line);
        while (!line.empty() && line.find_first_not_of(splitter) == std::string::npos) {
            std::getline(edgeFile, line);
        }
    }
    edgeFile.close();

    map<int, string> attrFilters = createFilters(entityData, 256, 4);

    attrFilterPath =
            utils.getJasmineGraphProperty("org.jasminegraph.entity.location") + "attrfilters_" + graphID + "_" +
            partitionID + ".txt";
    structFilterPath =
            utils.getJasmineGraphProperty("org.jasminegraph.entity.location") + "structfilters_" + graphID + "_" +
            partitionID + ".txt";

    //Write attr and struct filters separately into files
    writeToFile(attrFilterPath, attrFilters);
//    writeBloomFiltersToFile(structFilterPath, structFilters);

}

Mat<short> EntityResolver::generateCRVs(int minhashSize, int noClusters) { //minHash def 100
    Mat<short> CRVs(minhashSize, noClusters);
    for (int i = 0; i < noClusters; i++) {
        //Load cluster file into memory
        string filename = "/root/CLionProjects/EntityResolution/cluster" + to_string(i) + "filters.txt";
        Mat<float> clusterData;
        clusterData.load(filename, arma::csv_ascii);
        //Remove node id column
        clusterData.shed_col(0);
        inplace_trans(clusterData, "lowmem");
        //Create minhash signature of cluster
        MinHash minHash(minhashSize, 256);
        Col<short> crv = minHash.generateCRV(clusterData, 50);
        //Store in matrix
        CRVs.col(i) = crv;
    }
    return CRVs;
}
