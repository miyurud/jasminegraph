FROM busybox:stable-glibc

COPY --chmod=755 files.tmp/lib/* /lib/x86_64-linux-gnu/
COPY --chmod=755 files.tmp/bin/* /bin/

RUN mkdir -p /home/ubuntu/software/jasminegraph/conf
RUN mkdir -p /home/ubuntu/software/jasminegraph/k8s
RUN mkdir -p /home/ubuntu/software/jasminegraph/metadb
RUN mkdir -p /home/ubuntu/software/jasminegraph/performancedb
RUN mkdir -p /home/ubuntu/software/jasminegraph/src/metadb
RUN mkdir -p /home/ubuntu/software/jasminegraph/src/performancedb
RUN mkdir -p /home/ubuntu/software/jasminegraph/src/streamingdb

COPY --chmod=755 files.tmp/JasmineGraph /home/ubuntu/software/jasminegraph/
COPY --chmod=755 minimal/run-docker.sh /home/ubuntu/software/jasminegraph/
COPY --chmod=644 conf/jasminegraph-server.properties /home/ubuntu/software/jasminegraph/conf/
COPY --chmod=644 conf/hosts.txt /home/ubuntu/software/jasminegraph/conf/
COPY --chmod=644 k8s/*.yaml /home/ubuntu/software/jasminegraph/k8s/
COPY --chmod=644 src/metadb/ddl.sql /home/ubuntu/software/jasminegraph/src/metadb/
COPY --chmod=644 src/performancedb/ddl.sql /home/ubuntu/software/jasminegraph/src/performancedb/
COPY --chmod=644 src/streamingdb/ddl.sql /home/ubuntu/software/jasminegraph/src/streamingdb/
