FROM miyurud/jasminegraph:20230909T110050

RUN python3 -m pip install gcovr

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
RUN mkdir ./tests
COPY ./tests/unit ./tests/unit
RUN mkdir build

WORKDIR "${JASMINEGRAPH_HOME}/build"

RUN cmake -DCMAKE_BUILD_TYPE=DEBUG ..
RUN make -j4

CMD ["bash", "-c", "make -j2 coverage && cp coverage.xml ../build_mount/"]
