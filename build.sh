#!/usr/bin/env bash
mkdir -p /home/$USER/git/jasminegraph/cmake-build-debug
/home/$USER/software/clion-2018.1.3/bin/cmake/bin/cmake --build /home/$USER/git/jasminegraph/cmake-build-debug --target JasmineGraph -- -j 2
