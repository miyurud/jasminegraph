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

#include "Kmeans.h"
#include "../../../util/logger/Logger.h"

template <typename T>

Logger logger;

template <typename T>
arma::Mat<T> Kmeans::getMeans() {
    return means;
}

void Kmeans::fit(arma::Mat<T> &data, uint8_t noOfIterations, bool printMode = true) {
    bool status = kmeans(means, data, k, arma::random_spread, noOfIterations, true);

    if(status == false) {
        logger.error("clustering failed");
    }
}

void Kmeans::fit(arma::Mat<T> &data, arma::Mat<T> &means, uint8_t noOfIterations, bool printMode = true) {
    bool status = kmeans(means, data, k, arma::keep_existing, noOfIterations, true);

    if(status == false) {
        logger.error("clustering failed");
    }
}

arma::Mat<short> Kmeans::apply(arma::Mat<T> &data) {
    arma::Mat<short> predictions(1, data.n_cols);

    for (int i = 0; i < data.n_cols; i++) {
        arma::Col<float> datapoint = data.col(i);

        double best_dist = arma::Datum<double>::inf;
        uint8_t best_g = 0;
        for(uint8_t g=0; g < means.n_cols; ++g)
        {
            const double tmp_dist = sum(square(datapoint - means.col(g)));
            if(tmp_dist <= best_dist) {
                best_dist = tmp_dist;
                best_g = g;
            }
        }

        predictions[i] = best_g;
    }

    return predictions;
}