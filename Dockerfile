FROM miyurud/jasminegraph-prerequisites:20250909T070000

RUN rm -r /usr/lib/python3.8/distutils
RUN apt-get purge -y libpython3.8-dev python3.8-dev python3.8-distutils libpython3.8 python3.8

ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="${HOME}/software/jasminegraph"

RUN ln -sf /usr/bin/python3.8 /usr/bin/python3

# Install build dependencies for OpenTelemetry
RUN apt-get update && apt-get install --no-install-recommends -y \
    git cmake build-essential libcurl4-openssl-dev libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install OpenTelemetry C++ SDK v1.16.1 (stable version with full trace context propagation support)
WORKDIR /tmp
RUN git clone --branch v1.16.1 --recurse-submodules https://github.com/open-telemetry/opentelemetry-cpp.git
WORKDIR /tmp/opentelemetry-cpp
RUN mkdir build && cd build && \
    cmake -DWITH_PROMETHEUS=ON \
          -DWITH_OTLP_GRPC=OFF \
          -DWITH_OTLP_HTTP=ON \
          -DBUILD_TESTING=OFF \
          -DWITH_EXAMPLES=OFF \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
          .. && \
    make -j$(nproc) && \
    make install && \
    ldconfig

WORKDIR "${JASMINEGRAPH_HOME}"

ARG DEBUG="false"
RUN if [ "$DEBUG" = "true" ]; then apt-get update \
&& apt-get install --no-install-recommends -y gdb gdbserver \
&& apt-get clean; fi


WORKDIR "${JASMINEGRAPH_HOME}"
COPY ./build.sh ./build.sh
COPY ./CMakeLists.txt ./CMakeLists.txt
COPY ./main.h ./main.h
COPY ./main.cpp ./main.cpp
COPY ./globals.h ./globals.h
COPY ./src ./src

RUN if [ "$DEBUG" = "true" ]; then echo "building in DEBUG mode" && sh build.sh --debug; else sh build.sh; fi

COPY ./run-docker.sh ./run-docker.sh
COPY ./src_python ./src_python
COPY ./conf ./conf
COPY ./k8s ./k8s
COPY ./ddl ./ddl

ENTRYPOINT ["/home/ubuntu/software/jasminegraph/run-docker.sh"]
CMD ["bash"]
