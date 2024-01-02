FROM miyurud/jasminegraph-prerequisites:20240101T095619

RUN python3 -m pip install --no-cache-dir gcovr==6.0

ENV TERM=xterm-256color
ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="${HOME}/software/jasminegraph"

WORKDIR "${JASMINEGRAPH_HOME}"

COPY ./cmake_modules ./cmake_modules
COPY ./build.sh ./build.sh
COPY ./CMakeLists.txt ./CMakeLists.txt
COPY ./src_python ./src_python
COPY ./src ./src
COPY ./main.h ./main.h
COPY ./main.cpp ./main.cpp
COPY ./conf ./conf
COPY ./k8s ./k8s
COPY ./metadb ./metadb
RUN mkdir build
RUN mkdir tests
RUN mkdir tests/unit
COPY ./tests/unit ./tests/unit

WORKDIR "${JASMINEGRAPH_HOME}/build"

RUN cmake -DCMAKE_BUILD_TYPE=DEBUG ..
RUN make -j4

CMD ["bash", "-c", "LD_LIBRARY_PATH=/usr/local/lib make -j2 coverage && cp coverage.xml ../coverage/"]