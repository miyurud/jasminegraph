#!/usr/bin/env bash
cmake clean .
cmake --build . --target JasmineGraph -- -j 2
