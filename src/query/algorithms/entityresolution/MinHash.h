/**
Copyright 2021 JasmineGraph Team
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

#ifndef JASMINEGRAPH_MINHASH_H
#define JASMINEGRAPH_MINHASH_H
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

#endif //JASMINEGRAPH_MINHASH_H
