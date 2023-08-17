FROM miyurud/jasminegraph
ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="/home/ubuntu/software/jasminegraph"

WORKDIR /home/ubuntu/software/jasminegraph

COPY ./GraphSAGE ./GraphSAGE
RUN pip install -r ./GraphSAGE/requirements

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
