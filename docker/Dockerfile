FROM ubuntu:bionic
WORKDIR /home/ubuntu
RUN mkdir software
WORKDIR /home/ubuntu/software
RUN apt-get update && apt-get install -y git
RUN apt-get install -y cmake
RUN git clone https://github.com/google/flatbuffers.git
WORKDIR /home/ubuntu/software/flatbuffers
RUN git checkout tags/v1.10.0
RUN apt-get install -y clang-6.0
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-6.0 100
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-6.0 100
RUN CC=/usr/bin/clang CXX=/usr/bin/clang++ cmake -G "Unix Makefiles" .
RUN make
WORKDIR /home/ubuntu/software
RUN apt-get install -y wget
#RUN wget http://glaros.dtc.umn.edu/gkhome/fetch/sw/metis/metis-5.1.0.tar.gz
RUN git clone https://github.com/chinthakarukshan/metis.git
WORKDIR /home/ubuntu/software/metis
RUN tar -xvf metis-5.1.0.tar.gz
WORKDIR /home/ubuntu/software/metis/metis-5.1.0
RUN sed -i '/#define IDXTYPEWIDTH 32/c\#define IDXTYPEWIDTH 64' include/metis.h
RUN make config shared=1 cc=gcc-7
RUN make install
WORKDIR /home/ubuntu/software
RUN git clone https://github.com/gabime/spdlog.git
RUN apt-get install -y sqlite3
RUN apt-get install -y libsqlite3-dev
RUN apt install -y librdkafka-dev
RUN apt-get update
RUN apt-get install -y libboost-all-dev
WORKDIR /home/ubuntu/software
RUN git clone https://github.com/mfontanini/cppkafka.git
RUN apt-get install -y libssl-dev
WORKDIR /home/ubuntu/software/cppkafka
RUN mkdir build
WORKDIR /home/ubuntu/software/cppkafka/build
RUN cmake ..
RUN make
RUN make install
WORKDIR /home/ubuntu/software
RUN wget https://www-eu.apache.org/dist//xerces/c/3/sources/xerces-c-3.2.2.tar.gz
RUN tar -xvf xerces-c-3.2.2.tar.gz
WORKDIR /home/ubuntu/software/xerces-c-3.2.2
RUN sh configure --disable-transcoder-icu
RUN make install
WORKDIR /home/ubuntu/software
RUN git clone https://github.com/open-source-parsers/jsoncpp.git
WORKDIR /home/ubuntu/software/jsoncpp
RUN git checkout tags/1.8.4
RUN mkdir -p build/debug
WORKDIR /home/ubuntu/software/jsoncpp/build/debug
RUN cmake -DCMAKE_BUILD_TYPE=debug -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DARCHIVE_INSTALL_DIR=. -G "Unix Makefiles" ../..
RUN make
WORKDIR /home/ubuntu/software
RUN wget http://zlib.net/pigz/pigz-2.4.tar.gz
RUN tar -xvf pigz-2.4.tar.gz
WORKDIR /home/ubuntu/software/pigz-2.4
RUN make
ENV PATH="/home/ubuntu/software/pigz-2.4/pigz:${PATH}"
WORKDIR /home/ubuntu/software
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install -y python3.5-dev
RUN apt-get install -y libtool
RUN git clone https://github.com/miyurud/jasminegraph.git
ENV HOME="/home/ubuntu"
WORKDIR /home/ubuntu/software/jasminegraph
RUN apt-get update
RUN apt-get install -y python-pip
RUN pip install -r ./GraphSAGE/requirements
RUN sed -i '/target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.so)/c\target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.so)' CMakeLists.txt
RUN sed -i '/target_link_libraries(JasmineGraph \/usr\/local\/lib\/libxerces-c-3.2.so)/c\target_link_libraries(JasmineGraph \/usr\/local\/lib\/libxerces-c-3.2.so)' CMakeLists.txt
RUN sed -i '/target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.a)/c\#target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.a)' CMakeLists.txt
RUN sed -i '/target_link_libraries(JasmineGraph $ENV{HOME}\/software\/xerces-c-3.2.2\/lib\/libxerces-c.so)/c\#target_link_libraries(JasmineGraph $ENV{HOME}\/software\/xerces-c-3.2.2\/lib\/libxerces-c.so)' CMakeLists.txt
RUN sed -i '/target_link_libraries(JasmineGraph \/opt\/lib\/libxerces-c.a)/c\#target_link_libraries(JasmineGraph \/opt\/lib\/libxerces-c.a)' CMakeLists.txt
RUN sed -i '/target_link_libraries(JasmineGraph $ENV{HOME}\/software\/cppkafka\/build\/usr\/local\/lib\/libcppkafka.so)/c\target_link_libraries(JasmineGraph \/usr\/local\/lib\/libcppkafka.so)' CMakeLists.txt
RUN sed -i '/namespace JasminGraph.Edgestore;/c\namespace JasmineGraph.Edgestore;' src/util/dbutil/edgestore.fbs
WORKDIR /home/ubuntu/software/flatbuffers
RUN ./flatc --cpp -o /home/ubuntu/software/jasminegraph/src/util/dbutil /home/ubuntu/software/jasminegraph/src/util/dbutil/edgestore.fbs
RUN ./flatc --cpp -o /home/ubuntu/software/jasminegraph/src/util/dbutil /home/ubuntu/software/jasminegraph/src/util/dbutil/attributestore.fbs
RUN ./flatc --cpp -o /home/ubuntu/software/jasminegraph/src/util/dbutil /home/ubuntu/software/jasminegraph/src/util/dbutil/partedgemapstore.fbs
WORKDIR /home/ubuntu/software/jasminegraph
ENV JASMINEGRAPH_HOME="/home/ubuntu/software/jasminegraph"
RUN sh build.sh
CMD ["bash"]