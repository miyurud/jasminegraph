![Build](https://github.com/miyurud/jasminegraph/actions/workflows/build.yml/badge.svg)
[![License](https://img.shields.io/github/license/miyurud/jasminegraph?color=blue)](https://opensource.org/licenses/Apache-2.0)
[![GitHub last commit](https://img.shields.io/github/last-commit/miyurud/jasminegraph.svg)](https://github.com/miyurud/jasminegraph/commits/master)

# JasmineGraph

## 1. Introduction
JasmineGraph is a C/C++ based distributed graph database server. It has been developed following the [Acacia](https://github.com/miyurud/Acacia) graph database server architecture. JasmineGraph can be run on single computer as well as on a compute cluster.


## 2. Building and Running JasmineGraph on Docker
JasmineGraph can be run inside a docker image. After cloning the project, build the image as follows:

    docker build -t jasminegraph .


Run the image by providing the appropriate volume mount paths and parameters: 

    docker run -v "/var/run/docker.sock:/var/run/docker.sock:rw" -v "/root/.ssh:/home/user/.ssh" -v "/tmp:/tmp" -v "/var/tmp/jasminegraph-localstore:/var/tmp/jasminegraph-localstore" -v "/var/tmp/jasminegraph-aggregate:/var/tmp/jasminegraph-aggregate" -v "/home/user/Documents/jasminegraph/metadb:/home/ubuntu/software/jasminegraph/metadb" -v "/home/user/Documents/MSc/jasminegraph/performancedb:/home/ubuntu/software/jasminegraph/performancedb" -p 7777:7777 -p 7778:7778 jasminegraph --MODE 1 --MASTERIP <docker0 interface ip> --WORKERS 4 --WORKERIP <docker0 interface ip> --ENABLE_NMON false


Run JasmineGraph in Kubernetes environment by providing appropriate parameters:

    ./start-k8s.sh --META_DB_PATH "$(pwd)/metadb" \
        --PERFORMANCE_DB_PATH "$(pwd)/performancedb" \
        --DATA_PATH "$(pwd)/data" \
        --LOG_PATH "$(pwd)/logs" \
        --AGGREGATE_PATH "$(pwd)/aggregate" \
        --CONFIG_DIRECTORY_PATH "$(pwd)/config" \
        --NO_OF_WORKERS 2 \
        --ENABLE_NMON false \
        --MAX_COUNT 4

>Note: `NO_OF_WORKERS`, `MAX_COUNT`, and `ENABLE_NMON` are optional arguments, and it defaults to `2`, `4` and `false` respectively.


Remove all resources created by JasmineGraph in Kubernetes environment:

    ./start-k8s.sh clean


## 3. Contact Information

Please contact [Miyuru Dayarathna](miyurud at yahoo dot co dot uk) for further information. Please let us know about bug reports or any further improvements you wish to have in JasmineGraph.

## Open Source License
JasmineGraph is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
[Task Scheduler](https://github.com/Bosma/Scheduler) which is integrated in JasmineGraph is using [MIT](https://opensource.org/licenses/MIT) License.

## References
More details of JasmineGraph's approach for managment and mining of large graph data is available from the following list of papers.

- Chinthaka Weerakkody, Miyuru Dayarathna, Sanath Jayasena, and Toyotaro Suzumura. 2022. "[Guaranteeing Service Level Agreements for Triangle Counting via Observation-based Admission Control Algorithm.](https://doi.org/10.1109/CLOUD55607.2022.00050)," in 2022 IEEE 15th International Conference on Cloud Computing (CLOUD), Barcelona, Spain, 2022 pp. 283-288.
- Damitha Senevirathne, Isuru Wijesiri, Suchitha Dehigaspitiya, Miyuru Dayarathna, Sanath Jayasena, and Toyotaro Suzumura. 2020. "[Memory Efficient Graph Convolutional Network based Distributed Link Prediction](https://doi.org/10.1109/BigData50022.2020.9377874),"  2020 IEEE International Conference on Big Data (Big Data), Atlanta, GA, USA, pp. 2977-2986.
- Anuradha Karunarathna, Dinika Senarath, Shalika Madhushanki, Chinthaka Weerakkody, Miyuru Dayarathna, Sanath Jayasena, and Toyotaro Suzumura. 2020. "[Scalable Graph Convolutional Network based Link Prediction on a Distributed Graph Database Server.](https://doi.org/10.1109/CLOUD49709.2020.00028)," IEEE 13th International Conference on Cloud Computing (CLOUD),  Beijing, China, 2020, pp. 107-115.
- Miyuru Dayarathna, Sathya Bandara, Nandula Jayamaha, Mahen Herath, Achala Madhushan, Sanath Jayasena, and Toyotaro Suzumura. 2017. "[An X10-Based Distributed Streaming Graph Database Engine.](https://doi.org/10.1109/HiPC.2017.00036)," 2017 IEEE 24th International Conference on High Performance Computing (HiPC), Jaipur, 2017, pp. 243-252.
- Miyuru Dayarathna, Isuru Herath, Yasima Dewmini, Gayan Mettananda, Sameera Nandasiri, Sanath Jayasena, and Toyotaro Suzumura. 2016 "[Acacia-RDF: An X10-Based Scalable Distributed RDF Graph Database Engine.](https://doi.org/10.1109/CLOUD.2016.0075)," 2016 IEEE 9th International Conference on Cloud Computing (CLOUD), San Francisco, CA, 2016, pp. 521-528.
doi: 10.1109/CLOUD.2016.0075
- Miyuru Dayarathna and Toyotaro Suzumura. 2014. "[Towards scalable distributed graph database engine for hybrid clouds.](http://dx.doi.org/10.1109/DataCloud.2014.9)" *In Proceedings of the 5th International Workshop on Data-Intensive Computing in the Clouds (DataCloud '14).* IEEE Press, Piscataway, NJ, USA, 1-8.
