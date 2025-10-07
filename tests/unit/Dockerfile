FROM miyurud/jasminegraph-prerequisites:20250930T172442

RUN apt-get update
RUN apt-get install libcurl4-openssl-dev -y
RUN apt-get install -y sysstat
RUN apt-get install -y nmon
RUN apt-get install -y python3-setuptools

RUN ln -sf /usr/bin/python3.8 /usr/bin/python3

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
COPY ./ddl ./ddl
COPY ./main.h ./main.h
COPY ./main.cpp ./main.cpp
COPY ./globals.h ./globals.h
COPY ./conf ./conf
COPY ./k8s ./k8s

RUN mkdir build
RUN mkdir tests
RUN mkdir tests/unit
COPY ./tests/unit ./tests/unit

WORKDIR "${JASMINEGRAPH_HOME}/build"

RUN cmake -DCMAKE_BUILD_TYPE=DEBUG ..
RUN make -j4

CMD ["bash", "-c", "LD_LIBRARY_PATH=/usr/local/lib make -j2 coverage && cp coverage.xml ../coverage/"]
