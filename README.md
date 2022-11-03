![Build](https://github.com/miyurud/jasminegraph/actions/workflows/build.yml/badge.svg)
[![License](https://img.shields.io/github/license/miyurud/jasminegraph?color=blue)](https://opensource.org/licenses/Apache-2.0)
[![GitHub last commit](https://img.shields.io/github/last-commit/miyurud/jasminegraph.svg)](https://github.com/miyurud/jasminegraph/commits/master)

# JasmineGraph

## 1. Introduction
JasmineGraph is a C/C++ based distributed graph database server. It has been developed following the [Acacia](https://github.com/miyurud/Acacia) graph database server architecture. JasmineGraph can be run on single computer as well as on a compute cluster.

## 2. Building JasmineGraph on Single Computer
**Prerequisites**

The following build tools, applications, and libraries need to be installed before building JasmineGraph. The CMakeLists.txt file's entries need to be updated to match with the installation locations of the dependencies. For examole, if you install SpdLog in a directory called "software" in your Linux system you may have to add a line as "include_directories("$ENV{HOME}/software/spdlog/include")" to get the SpdLog's header files associated with you build process.

- GCC 9.1.0 (recommended) or above
- Cmake version 3.10.3 or above
- Google Flat Buffers (https://github.com/google/flatbuffers)
    - Clone the flatbuffers repository to $ENV{HOME}/software
    - Checkout Tag [v1.12.0](https://github.com/google/flatbuffers/archive/v1.12.0.zip)
    - Follow https://google.github.io/flatbuffers/flatbuffers_guide_building.html link to build flatbuffers
- Metis
    - First, clone the GKlib package from the following URL and build it and install it following the [guidelines](https://github.com/KarypisLab/GKlib#readme). [GKlib](https://github.com/KarypisLab/GKlib.git).
    - Download Metis version 5.1.1 sources from this [URL](https://github.com/KarypisLab/METIS/archive/refs/tags/v5.1.1-DistDGL-v0.5.zip).
    - Build and install the Metis following the [guidelines](https://github.com/KarypisLab/METIS#building-and-installing-metis).
    - Path for `metis.h` header could be set by updating the environment variables as follows,
      `export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/home/username/local/include`
- SpdLog (https://github.com/gabime/spdlog)
    - Clone or download the repository from the above link
    - Issue "cmake ."
    - Issue "make" followed by "sudo make install"
- SQLite3 (https://www.sqlite.org/download.html)
    - Download the `sqlite-autoconf-3390200` from this [URL](https://www.sqlite.org/2022/sqlite-autoconf-3390200.tar.gz). 
    - Extract `sqlite-autoconf-3390200.tar.gz` to some location. E.g., `/media/user/software/sqlite-autoconf-3390200-install`
    - Change to that location and run `./configure --prefix=/media/user/software/sqlite-autoconf-3390200`
    - Issue "make" followed by "sudo make install"
    - Once installed specify the `target_link_libraries` path to `libsqlite3`.
      E.g., `target_link_libraries(JasmineGraph /media/user/software/sqlite-autoconf-3390200/lib/libsqlite3.so)`
- cppkafka (https://github.com/mfontanini/cppkafka)
    - Install librdkafka - Follow the Readme in (https://github.com/edenhill/librdkafka) - use `sudo apt install librdkafka-dev`
    - Install boost library - use `sudo apt-get install libboost-all-dev`
    - Follow the guidelines in (https://github.com/mfontanini/cppkafka#compiling) - use cppkafka release [v0.3.1](https://github.com/mfontanini/cppkafka/archive/refs/tags/v0.3.1.zip)
    - When doing the above step for `cmake <OPTIONS> ..` use of `cmake ..` should be sufficient enough
    - Once cppkafka is built install it by running `sudo make install` from the build directory
- Xerces-c-3.2.2 (https://xerces.apache.org/xerces-c/)
    - Intallation guide is available at http://xerces.apache.org/xerces-c/build-3.html
    - [Download](https://xerces.apache.org/xerces-c/download.cgi) the [archive](https://dlcdn.apache.org//xerces/c/3/sources/xerces-c-3.2.3.zip) from the xerces.apache.org website
    - Unzip the archive xerces-c-3.2.3.zip to a location like `/media/user/software/xerces-c-3.2.3-install`
    - When configuring Xerces skip the use of transcoder ICU by using the flag `--disable-transcoder-icu`

        E.g., `./configure --prefix=/media/user/software/xerces-c-3.2.3 --disable-transcoder-icu`

    - Run `cmake .` & `sudo make install`
    - Once installed specify the `target_link_libraries` path to xerces.
- Jsoncpp(https://github.com/open-source-parsers/jsoncpp)
    - Install the release https://github.com/open-source-parsers/jsoncpp/releases/tag/1.8.4
    - Issue "cmake ."
    - Issue "make" followed by "sudo make install"
<!-- - pigz (optional)
    - pigz, which stands for Parallel Implementation of GZip, is a fully functional
      replacement for gzip that exploits multiple processors and multiple cores to
      the hilt when compressing data.
    - JasmineGraph by default uses pigz for file compression and decompression.
    - However, if pigz is not found, gzip is used instead.
    - Download pigz from (http://zlib.net/pigz/) or (https://github.com/madler/pigz.git).
    - Type "make" in pigz directory to build the "pigz" executable. 
    - Add pigz executable's path to ~/.bashrc file and refresh.
- nlohmann_json (https://github.com/nlohmann/json)
    - Download the nolhmann latest release sources i:e [v3.9.1.zip](https://github.com/nlohmann/json/archive/refs/tags/v3.9.1.zip) 
    - Create a directory in your software directory named `nlohmann_json`
    - Extract the `include.zip` to  `nlohmann_json` directory
    - Issue "cmake ."
    - Issue "make" followed by "sudo make install" -->
- python3.5 (https://www.python.org/downloads/release/python-350/)
    - Read this [blog](https://passingcuriosity.com/2015/installing-python-from-source/) for more details on setting up python3.5
- Recent versions of TensorFlow, numpy, scipy, sklearn, and networkx are required (but networkx must be <=1.11). You can install all the required packages using the following command:

   pip install -r ./GraphSAGE/requirements

First, this repository should be cloned into one of your computer's local directory. Then change directory to jasminegraph and run ./build.sh to build the JasmineGraph executable.

## 3. Running JasmineGraph
JasmineGraph can be run by executing the run.sh script. This will start master on your local computer while workers are created in the list of the hosts mentioned in the conf/hosts.txt file.

## 4. Building and Running JasmineGraph on Docker
JasmineGraph can be run inside a docker image. After cloning the project, build the image as follows:

    cd docker
    docker build -t jasminegraph .


Run the image by providing the appropriate volume mount paths and parameters: 

    docker run -v "/var/run/docker.sock:/var/run/docker.sock:rw" -v "/root/.ssh:/home/user/.ssh" -v "/tmp:/tmp" -v "/var/tmp/jasminegraph-localstore:/var/tmp/jasminegraph-localstore" -v "/var/tmp/jasminegraph-aggregate:/var/tmp/jasminegraph-aggregate" -v "/home/user/Documents/jasminegraph/metadb:/home/ubuntu/software/jasminegraph/metadb" -v "/home/user/Documents/MSc/jasminegraph/performancedb:/home/ubuntu/software/jasminegraph/performancedb" -p 7777:7777 -p 7778:7778 jasminegraph --MODE 1 --MASTERIP <docker0 interface ip> --WORKERS 4 --WORKERIP <docker0 interface ip> --ENABLE_NMON false


## 5. Contact Information

Please contact [Miyuru Dayarathna](miyurud at yahoo dot co dot uk) for further information. Please let us know about bug reports or any further improvements you wish to have in JasmineGraph.

## Open Source License
JasmineGraph is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
[Task Scheduler](https://github.com/Bosma/Scheduler) which is integrated in JasmineGraph is using [MIT](https://opensource.org/licenses/MIT) License.
[GraphSAGE](https://github.com/williamleif/GraphSAGE/)  which is integrated in JasmineGraph to generate node embeddings is using MIT License.

## References
More details of JasmineGraph's approach for managment and mining of large graph data is available from the following list of papers.

- Chinthaka Weerakkody, Miyuru Dayarathna, Sanath Jayasena, and Toyotaro Suzumura. 2022. "[Guaranteeing Service Level Agreements for Triangle Counting via Observation-based Admission Control Algorithm.](https://doi.org/10.1109/CLOUD55607.2022.00050)," in 2022 IEEE 15th International Conference on Cloud Computing (CLOUD), Barcelona, Spain, 2022 pp. 283-288.
- Damitha Senevirathne, Isuru Wijesiri, Suchitha Dehigaspitiya, Miyuru Dayarathna, Sanath Jayasena, and Toyotaro Suzumura. 2020. "[Memory Efficient Graph Convolutional Network based Distributed Link Prediction](https://doi.org/10.1109/BigData50022.2020.9377874),"  2020 IEEE International Conference on Big Data (Big Data), Atlanta, GA, USA, pp. 2977-2986.
- Anuradha Karunarathna, Dinika Senarath, Shalika Madhushanki, Chinthaka Weerakkody, Miyuru Dayarathna, Sanath Jayasena, and Toyotaro Suzumura. 2020. "[Scalable Graph Convolutional Network based Link Prediction on a Distributed Graph Database Server.](https://doi.org/10.1109/CLOUD49709.2020.00028)," IEEE 13th International Conference on Cloud Computing (CLOUD),  Beijing, China, 2020, pp. 107-115.
- Miyuru Dayarathna, Sathya Bandara, Nandula Jayamaha, Mahen Herath, Achala Madhushan, Sanath Jayasena, and Toyotaro Suzumura. 2017. "[An X10-Based Distributed Streaming Graph Database Engine.](https://doi.org/10.1109/HiPC.2017.00036)," 2017 IEEE 24th International Conference on High Performance Computing (HiPC), Jaipur, 2017, pp. 243-252.
- Miyuru Dayarathna, Isuru Herath, Yasima Dewmini, Gayan Mettananda, Sameera Nandasiri, Sanath Jayasena, and Toyotaro Suzumura. 2016 "[Acacia-RDF: An X10-Based Scalable Distributed RDF Graph Database Engine.](https://doi.org/10.1109/CLOUD.2016.0075)," 2016 IEEE 9th International Conference on Cloud Computing (CLOUD), San Francisco, CA, 2016, pp. 521-528.
doi: 10.1109/CLOUD.2016.0075
- Miyuru Dayarathna and Toyotaro Suzumura. 2014. "[Towards scalable distributed graph database engine for hybrid clouds.](http://dx.doi.org/10.1109/DataCloud.2014.9)" *In Proceedings of the 5th International Workshop on Data-Intensive Computing in the Clouds (DataCloud '14).* IEEE Press, Piscataway, NJ, USA, 1-8.
