//
// Created by root on 1/29/21.
//

#ifndef JASMINEGRAPH_KMEANS_HPP
#define JASMINEGRAPH_KMEANS_HPP
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

#endif //JASMINEGRAPH_KMEANS_HPP
