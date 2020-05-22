FROM damithas/jasminegraph-prerequisites
WORKDIR /home/ubuntu/software
RUN apt-get update
RUN git clone https://github.com/DamithaSenevirathne/jasminegraph.git
ENV HOME="/home/ubuntu"
WORKDIR /home/ubuntu/software/jasminegraph
RUN apt-get update && \
    sed -i '/target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.so)/c\target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.so)' CMakeLists.txt && \
    sed -i '/target_link_libraries(JasmineGraph \/usr\/local\/lib\/libxerces-c-3.2.so)/c\target_link_libraries(JasmineGraph \/usr\/local\/lib\/libxerces-c-3.2.so)' CMakeLists.txt && \
    sed -i '/target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.a)/c\#target_link_libraries(JasmineGraph \/usr\/local\/lib\/libmetis.a)' CMakeLists.txt && \
    sed -i '/target_link_libraries(JasmineGraph $ENV{HOME}\/software\/xerces-c-3.2.2\/lib\/libxerces-c.so)/c\#target_link_libraries(JasmineGraph $ENV{HOME}\/software\/xerces-c-3.2.2\/lib\/libxerces-c.so)' CMakeLists.txt && \
    sed -i '/target_link_libraries(JasmineGraph \/opt\/lib\/libxerces-c.a)/c\#target_link_libraries(JasmineGraph \/opt\/lib\/libxerces-c.a)' CMakeLists.txt && \
    sed -i '/target_link_libraries(JasmineGraph $ENV{HOME}\/software\/cppkafka\/build\/usr\/local\/lib\/libcppkafka.so)/c\target_link_libraries(JasmineGraph \/usr\/local\/lib\/libcppkafka.so)' CMakeLists.txt && \
    sed -i '/namespace JasminGraph.Edgestore;/c\namespace JasmineGraph.Edgestore;' src/util/dbutil/edgestore.fbs
WORKDIR /home/ubuntu/software/flatbuffers
RUN ./flatc --cpp -o /home/ubuntu/software/jasminegraph/src/util/dbutil /home/ubuntu/software/jasminegraph/src/util/dbutil/edgestore.fbs && \
    ./flatc --cpp -o /home/ubuntu/software/jasminegraph/src/util/dbutil /home/ubuntu/software/jasminegraph/src/util/dbutil/attributestore.fbs && \
    ./flatc --cpp -o /home/ubuntu/software/jasminegraph/src/util/dbutil /home/ubuntu/software/jasminegraph/src/util/dbutil/partedgemapstore.fbs
WORKDIR /home/ubuntu/software/jasminegraph
ENV JASMINEGRAPH_HOME="/home/ubuntu/software/jasminegraph"
RUN sh build.sh
ENTRYPOINT ["/home/ubuntu/software/jasminegraph/run-docker.sh"]
CMD ["bash"]
