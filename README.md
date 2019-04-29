# Jasminegraph

## 1. Introduction
Jasminegraph is a C/C++ based distributed graph database server. It has been developed following the [Acacia](https://github.com/miyurud/Acacia) graph database server architecture.

## 2. Building Jasminegraph on Single Computer
Prerequisites
The following build tools, applications, and libraries need to be installed before building Jasminegraph.
- GCC 5.4.0 or above
- Cmake version 3.10.3 or above
- Google Flat Buffers (https://github.com/google/flatbuffers)
- Metis (http://glaros.dtc.umn.edu/gkhome/metis/metis/download)
- SdpLog (Ubuntu: apt-get install libspdlog-dev) (https://github.com/gabime/spdlog)
- SQLite3 (https://www.sqlite.org/download.html)
- cppkafka (https://github.com/mfontanini/cppkafka)
    - Install librdkafka - Follow the Readme in (https://github.com/mfontanini/cppkafka)
    - Install boost library - use 'sudo apt-get install libboost-all-dev'
    - Once cppkafka is built install it by running 'sudo make install' from the build directory
- Xerces-c-3.2.2 (https://xerces.apache.org/xerces-c/)
    - Intallation guide is available at http://xerces.apache.org/xerces-c/build-3.html
    - Once installed specify the target_link_libraries path to xerces.


First, this repository should be cloned into one of your computer's local directory. Then change directory to jasminegraph and run ./build.sh to build the Jasminegraph executable.

## 3. Running Jasminegraph
Jasminegraph can be run by executing the run.sh script. This will start master on your local computer while workers are created in the list of the hosts mentioned in the conf/hosts.txt file.

## 4. Contact Information

Please contact [Miyuru Dayarathna](miyurud at yahoo dot co dot uk) for further information. Please let us know about bug reports or any further improvements you wish to have in Jasminegraph.

## Open Source License
Jasminegraph is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## References
More details of Jasminegraph/Acacia architecture is available from the following list of papers.

- Miyuru Dayarathna, Sathya Bandara, Nandula Jayamaha, Mahen Herath, Achala Madhushan, Sanath Jayasena, Toyotaro Suzumura. 2017. "[An X10-Based Distributed Streaming Graph Database Engine.](https://doi.org/10.1109/HiPC.2017.00036)," 2017 IEEE 24th International Conference on High Performance Computing (HiPC), Jaipur, 2017, pp. 243-252.
- Miyuru Dayarathna, Isuru Herath, Yasima Dewmini, Gayan Mettananda, Sameera Nandasiri, Sanath Jayasena, Toyotaro Suzumura. 2016 "[Acacia-RDF: An X10-Based Scalable Distributed RDF Graph Database Engine.](https://doi.org/10.1109/CLOUD.2016.0075)," 2016 IEEE 9th International Conference on Cloud Computing (CLOUD), San Francisco, CA, 2016, pp. 521-528.
doi: 10.1109/CLOUD.2016.0075
- Miyuru Dayarathna and Toyotaro Suzumura. 2014. "[Towards scalable distributed graph database engine for hybrid clouds.](http://dx.doi.org/10.1109/DataCloud.2014.9)" *In Proceedings of the 5th International Workshop on Data-Intensive Computing in the Clouds (DataCloud '14).* IEEE Press, Piscataway, NJ, USA, 1-8.
