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

#ifndef JASMINEGRAPH_KMEANS_H
#define JASMINEGRAPH_KMEANS_H
#include <armadillo>

template <typename T>
class Kmeans {
public:
    Kmeans(uint8_t k) {
        this->k = k;
    }

    arma::Mat<T> getMeans();

    void fit(arma::Mat<T> &data, uint8_t noOfIterations, bool printMode = true);

    void fit(arma::Mat<T> &data, arma::Mat<T> &means, uint8_t noOfIterations, bool printMode = true);

    arma::Mat<short> apply(arma::Mat<T> &data);

private:
    uint8_t k;
    arma::Mat<T> means;

};

#endif //JASMINEGRAPH_KMEANS_H
