//
// Created by root on 1/29/21.
//

#ifndef JASMINEGRAPH_MINHASH_HPP
#define JASMINEGRAPH_MINHASH_HPP
#include <stdint.h>
#include <array>
#include <armadillo>
#include "MurmurHash3.h"

class MinHash {
public:
    MinHash(uint8_t l, uint16_t filterLen): permutations(filterLen, l) {
        minhashSize = l;
        arma::Col<arma::uword> order = arma::conv_to<arma::Col<arma::uword>>::from(
                arma::linspace(0, filterLen, filterLen));

        //For the decided minhash length, create permutations and store
        for (int i = 0; i < l; i++) {
            for (uint16_t n = 0; n < filterLen; n++) {
                std::string num = std::to_string(n);
                const char *numchar = num.c_str();
                std::array<uint64_t, 2> hashValues = hash(numchar, num.size());
                arma::uword val = nthHash(i, hashValues[0], hashValues[1], filterLen);
                order(n) = val;
            }
            permutations.col(i) = order;
        }
        permutations.col(0).print();
    }

    arma::Col<short> generateCRV(arma::Mat<float> &data, uint8_t d, bool quietPrint=true);

    std::array<uint64_t, 2> hash(const char *data, std::size_t len);

    inline short nthHash(uint8_t n, uint64_t hashA, uint64_t hashB, int size);

    inline arma::Col<float> getDensity(arma::Mat<float> &data);

    inline void display(std::string mes, bool b);

    arma::Col<short> discretize(arma::Col<float> &densityVec, uint8_t d);



private:
    uint8_t minhashSize;
    arma::Mat<arma::uword> permutations;
};

#endif //JASMINEGRAPH_MINHASH_HPP
