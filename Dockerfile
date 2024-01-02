FROM miyurud/jasminegraph-prerequisites:20240101T095619
ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="${HOME}/software/jasminegraph"

WORKDIR "${JASMINEGRAPH_HOME}"

ARG DEBUG="false"
RUN if [ "$DEBUG" = "true" ]; then apt-get update \
&& apt-get install --no-install-recommends -y gdb gdbserver \
&& apt-get clean; fi

COPY ./build.sh ./build.sh
COPY ./CMakeLists.txt ./CMakeLists.txt
COPY ./main.h ./main.h
COPY ./main.cpp ./main.cpp
COPY ./src ./src

RUN if [ "$DEBUG" = "true" ]; then echo "building in DEBUG mode" && sh build.sh --debug; else sh build.sh; fi

COPY ./run-docker.sh ./run-docker.sh
COPY ./src_python ./src_python
COPY ./conf ./conf
COPY ./k8s ./k8s

ENTRYPOINT ["/home/ubuntu/software/jasminegraph/run-docker.sh"]
CMD ["bash"]
