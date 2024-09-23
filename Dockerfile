FROM pre:latest

RUN apt-get update && apt-get install -y libcurl4-openssl-dev sysstat nmon
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

WORKDIR "${HOME}"/software
RUN mkdir antlr
WORKDIR "${HOME}"/software/antlr
RUN apt-get update && apt-get install --no-install-recommends -y default-jre
RUN curl -O https://s3.amazonaws.com/artifacts.opencypher.org/M23/Cypher.g4
RUN curl -O https://www.antlr.org/download/antlr-4.13.2-complete.jar
RUN java -jar antlr-4.13.2-complete.jar -Dlanguage=Cpp -visitor Cypher.g4
RUN apt-get purge default-jre -y

WORKDIR "${JASMINEGRAPH_HOME}"
RUN mkdir "${JASMINEGRAPH_HOME}"/code_generated/
RUN mkdir "${JASMINEGRAPH_HOME}"/code_generated/antlr
RUN mv /home/ubuntu/software/antlr/*.cpp "${JASMINEGRAPH_HOME}"/code_generated/antlr
RUN mv /home/ubuntu/software/antlr/*.h "${JASMINEGRAPH_HOME}"/code_generated/antlr
RUN rm -f "${HOME}"/software/antlr/*

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
