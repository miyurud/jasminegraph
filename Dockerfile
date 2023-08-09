FROM ubuntu:focal
WORKDIR /home/ubuntu
RUN mkdir software
WORKDIR /home/ubuntu/software

RUN apt-get update
RUN apt-get install --no-install-recommends -y apt-transport-https
RUN apt-get update
RUN apt-get install --no-install-recommends -y curl gnupg2 ca-certificates software-properties-common nlohmann-json3-dev

RUN apt-get install --no-install-recommends -y git cmake build-essential sqlite3 libsqlite3-dev libssl-dev librdkafka-dev libboost-all-dev libtool libxerces-c-dev libflatbuffers-dev
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install --no-install-recommends -y python3.11-dev
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN apt-get install --no-install-recommends -y libjsoncpp-dev libspdlog-dev pigz
RUN python3.11 get-pip.py

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
RUN apt-get update
RUN apt-get install --no-install-recommends -y docker-ce-cli

RUN git clone --single-branch --depth 1 https://github.com/mfontanini/cppkafka.git

WORKDIR /home/ubuntu/software
RUN git clone --single-branch --depth 1 --branch v5.1.1-DistDGL-v0.5 https://github.com/KarypisLab/METIS.git
WORKDIR /home/ubuntu/software/METIS
RUN git submodule update --init
RUN make config shared=1 cc=gcc prefix=/usr/local
RUN make install

RUN mkdir /home/ubuntu/software/cppkafka/build
WORKDIR /home/ubuntu/software/cppkafka/build
RUN cmake ..
RUN make -j4
RUN make install

ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="/home/ubuntu/software/jasminegraph"
RUN mkdir /home/ubuntu/software/jasminegraph
WORKDIR /home/ubuntu/software/jasminegraph

COPY ./GraphSAGE ./GraphSAGE
RUN pip install -r ./GraphSAGE/requirements
RUN pip install joblib
RUN pip install threadpoolctl

COPY ./conf ./conf
COPY ./src ./src
COPY ./src_python ./src_python
COPY ./build.sh ./build.sh
COPY ./CMakeLists.txt ./CMakeLists.txt
COPY ./main.cpp ./main.cpp
COPY ./main.h ./main.h
COPY ./run-docker.sh ./run-docker.sh

RUN sh build.sh
ENTRYPOINT ["/home/ubuntu/software/jasminegraph/run-docker.sh"]
CMD ["bash"]
