#!/bin/bash

apt-get update
apt-get install --no-install-recommends -y apt-transport-https
apt-get update
apt-get install --no-install-recommends -y curl gnupg2 ca-certificates software-properties-common nlohmann-json3-dev
apt-get install --no-install-recommends -y git cmake build-essential sqlite3 libsqlite3-dev libssl-dev librdkafka-dev libboost-all-dev libtool libxerces-c-dev libflatbuffers-dev libjsoncpp-dev libspdlog-dev pigz
add-apt-repository ppa:deadsnakes/ppa
apt-get install --no-install-recommends -y python3.8-dev python3-pip python3.8-distutils
#python3.8 -m pip install stellargraph
#python3.8 -m pip install chardet scikit-learn joblib threadpoolctl pandas
#python3.8 -m pip cache purge
mkdir prerequisites && cd prerequisites
git clone --single-branch --depth 1 https://github.com/mfontanini/cppkafka.git
git clone --single-branch --depth 1 --branch v5.1.1-DistDGL-v0.5 https://github.com/KarypisLab/METIS.git
cd METIS
git submodule update --init
find . -type f -print0 | xargs -0 sed -i '/-march=native/d'
make config shared=1 cc=gcc prefix=/usr/local
make install

mkdir ../cppkafka/build && cd ../cppkafka/build
cmake ..
make -j4
make install
cd ../../.. && rm -r prerequisites
