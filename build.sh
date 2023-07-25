#!/usr/bin/env bash
if [ "$1" == "--debug" ]; then
    cmake -DCMAKE_ENABLE_DEBUG=1 clean .
else
    cmake clean .
fi
cmake --build . --target JasmineGraph -- -j 4
