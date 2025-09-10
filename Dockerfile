FROM miyurud/jasminegraph-prerequisites:20250909T070000

RUN rm -r /usr/lib/python3.8/distutils
RUN apt-get purge -y libpython3.8-dev python3.8-dev python3.8-distutils libpython3.8 python3.8

ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="${HOME}/software/jasminegraph"

RUN ln -sf /usr/bin/python3.8 /usr/bin/python3

# Install OpenTelemetry C++ SDK dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
    git cmake build-essential libcurl4-openssl-dev libssl-dev \
    libprotobuf-dev protobuf-compiler libgrpc++-dev libgrpc-dev \
    pkg-config libc-ares-dev libre2-dev libabsl-dev nlohmann-json3-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Build and install OpenTelemetry C++ SDK with exporters
WORKDIR /tmp
RUN git clone --recurse-submodules https://github.com/open-telemetry/opentelemetry-cpp.git \
    && cd opentelemetry-cpp \
    && mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release \
             -DCMAKE_INSTALL_PREFIX=/usr/local \
             -DWITH_PROMETHEUS=ON \
             -DWITH_OTLP_GRPC=ON \
             -DWITH_OTLP_HTTP=ON \
             -DWITH_ZIPKIN=OFF \
             -DWITH_JAEGER=OFF \
             -DBUILD_TESTING=OFF \
             -DWITH_EXAMPLES=OFF \
             -DWITH_BENCHMARK=OFF \
             -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
             .. \
    && make -j$(nproc) \
    && make install \
    && ldconfig \
    && cd / && rm -rf /tmp/opentelemetry-cpp

# Set environment variables to help CMake find OpenTelemetry
ENV PKG_CONFIG_PATH="/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH"
ENV CMAKE_PREFIX_PATH="/usr/local:$CMAKE_PREFIX_PATH"

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
