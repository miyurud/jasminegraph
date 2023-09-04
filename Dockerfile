FROM miyurud/jasminegraph
ENV HOME="/home/ubuntu"
ENV JASMINEGRAPH_HOME="/home/ubuntu/software/jasminegraph"

WORKDIR /home/ubuntu/software/jasminegraph

RUN apt-get update
RUN apt-get install -y python3.8
RUN apt-get install -y python3-pip
RUN apt-get install -y python3.8-distutils
RUN python3.8 -m pip install stellargraph
RUN python3.8 -m pip install chardet

COPY ./GraphSAGE ./GraphSAGE
RUN pip install -r ./GraphSAGE/requirements
RUN pip install pandas

COPY ./build.sh ./build.sh
COPY ./run-docker.sh ./run-docker.sh
COPY ./CMakeLists.txt ./CMakeLists.txt
COPY ./src_python ./src_python
COPY ./main.h ./main.h
COPY ./main.cpp ./main.cpp
COPY ./src ./src

RUN sh build.sh
COPY ./conf ./conf

ENTRYPOINT ["/home/ubuntu/software/jasminegraph/run-docker.sh"]
CMD ["bash"]
