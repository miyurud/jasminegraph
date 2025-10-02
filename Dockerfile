FROM miyurud/jasminegraph-prerequisites:20241231T070657

RUN apt-get update && apt-get install -y libcurl4-openssl-dev sysstat nmon git wget libgflags-dev libopenblas-dev libomp-dev
RUN rm -r /usr/lib/python3.8/distutils
RUN apt-get purge -y libpython3.8-dev python3.8-dev python3.8-distutils libpython3.8 python3.8

ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="${HOME}/software/jasminegraph"

RUN ln -sf /usr/bin/python3.8 /usr/bin/python3

WORKDIR "${JASMINEGRAPH_HOME}"

ARG DEBUG="false"
RUN if [ "$DEBUG" = "true" ]; then apt-get update \
&& apt-get install --no-install-recommends -y gdb gdbserver \
&& apt-get clean; fi

RUN wget https://github.com/Kitware/CMake/releases/download/v3.29.6/cmake-3.29.6.tar.gz \
 && tar -zxvf cmake-3.29.6.tar.gz \
 && cd cmake-3.29.6 \
 && ./bootstrap \
 && make -j$(nproc) \
 && make install \
 && cd .. \
 && rm -rf cmake-3.29.6 cmake-3.29.6.tar.gz

#WORKDIR /usr/local/lib
#RUN git clone --depth=1 https://github.com/ggml-org/llama.cpp.git
#WORKDIR /usr/local/lib/llama.cpp
#RUN cmake -B build -DGGML_NATIVE=ON && cmake --build build -j



WORKDIR /usr/local/lib
RUN git clone --depth=1 https://github.com/facebookresearch/faiss.git
WORKDIR /usr/local/lib/faiss
RUN mkdir build && cd build \
 && cmake -DFAISS_ENABLE_PYTHON=OFF -DFAISS_ENABLE_GPU=OFF .. \
 && make -j$(nproc) \
 && make install

WORKDIR "${JASMINEGRAPH_HOME}"
COPY ./build.sh ./build.sh
COPY ./CMakeLists.txt ./CMakeLists.txt
COPY ./main.h ./main.h
COPY ./main.cpp ./main.cpp
COPY ./globals.h ./globals.h
COPY ./src ./src

#WORKDIR /usr/local/lib
#RUN git clone https://github.com/ggml-org/llama.cpp.git
#WORKDIR /usr/local/lib/llama.cpp
#RUN cmake -B build -DGGML_NATIVE=ON && cmake --build build -j
## Install headers and library into system paths
##RUN mkdir -p /usr/local/include/llama && \
##    cp llama.h /usr/local/include/llama/ && \
##    cp build/libllama.so* /usr/local/lib/ && \
##    ldconfig
#WORKDIR "${JASMINEGRAPH_HOME}"
RUN if [ "$DEBUG" = "true" ]; then echo "building in DEBUG mode" && sh build.sh --debug; else sh build.sh; fi




COPY ./run-docker.sh ./run-docker.sh
COPY ./src_python ./src_python
COPY ./conf ./conf
COPY ./k8s ./k8s
COPY ./ddl ./ddl

ENTRYPOINT ["/home/ubuntu/software/jasminegraph/run-docker.sh"]
CMD ["bash"]
