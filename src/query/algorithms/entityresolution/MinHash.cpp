//
// Created by root on 4/16/21.
//

#include "MinHash.hpp"

arma::Col<short> MinHash::generateCRV(arma::Mat<float> &data, uint8_t d, bool quietPrint) {
    display("Creating CRV", quietPrint);
    //Calculate density vector
    display("Create density vector", quietPrint);
    arma::Col<float> denVec = getDensity(data);
//        denVec.print();
    //Discretize vector based on given threshold
    display("Discretize density vector", quietPrint);
    arma::Col<short> catVec = discretize(denVec, d);
//        catVec.print();

    //For the determined cluster representative vector length, create minhash signature
    std::cout << "Creating minhash signature" << std::endl;
    arma::Col<short> crv(minhashSize);
    for (int i = 0; i < minhashSize; i++) {
        //Get permutation of discretized vector
        arma::Col<short> permutedVec = catVec.elem(permutations.col(i));
        //Assign minhash signature value
        crv(i) = permutedVec.index_max();
    }

    return crv;
}

std::array<uint64_t, 2> MinHash::hash(const char *data, std::size_t len) {
    std::array<uint64_t, 2> hashValue;
    MurmurHash3_x64_128(data, len, 0, hashValue.data());

    return hashValue;
}

inline short MinHash::nthHash(uint8_t n, uint64_t hashA, uint64_t hashB, int size) {
    return (hashA + n * hashB) % size;
}

inline arma::Col<float> MinHash::getDensity(arma::Mat<float> &data) {
    return arma::mean(data, 1);
}

inline void MinHash::display(std::string mes, bool b) {
    if (!b)
        std::cout << mes << std::endl;
}

arma::Col<short> MinHash::discretize(arma::Col<float> &densityVec, uint8_t d) {
    arma::Col<float> sorted = arma::sort(densityVec);
    float threshold = sorted(d);
    densityVec.elem(find(densityVec > threshold)).ones();
    densityVec.elem(find(densityVec <= threshold)).zeros();

    return arma::conv_to<arma::Col<short>>::from(densityVec);
}