#!/bin/bash

set -e

libs=(
    /lib/x86_64-linux-gnu/libcurl.so.4
    /lib/x86_64-linux-gnu/libsqlite3.so.0
    /lib/x86_64-linux-gnu/libfmt.so.8
    /lib/x86_64-linux-gnu/libxerces-c-3.2.so
    /lib/x86_64-linux-gnu/libjsoncpp.so.25
    /usr/local/lib/libcppkafka.so.0.4.0
    /usr/local/lib/libkubernetes.so
    /lib/x86_64-linux-gnu/libyaml-cpp.so.0.7
    /lib/x86_64-linux-gnu/libstdc++.so.6
    /lib/x86_64-linux-gnu/libgcc_s.so.1
    /lib/x86_64-linux-gnu/libnghttp2.so.14
    /lib/x86_64-linux-gnu/libidn2.so.0
    /lib/x86_64-linux-gnu/librtmp.so.1
    /lib/x86_64-linux-gnu/libssh.so.4
    /lib/x86_64-linux-gnu/libpsl.so.5
    /lib/x86_64-linux-gnu/libssl.so.3
    /lib/x86_64-linux-gnu/libcrypto.so.3
    /lib/x86_64-linux-gnu/libgssapi_krb5.so.2
    /lib/x86_64-linux-gnu/libldap-2.5.so.0
    /lib/x86_64-linux-gnu/liblber-2.5.so.0
    /lib/x86_64-linux-gnu/libzstd.so.1
    /lib/x86_64-linux-gnu/libbrotlidec.so.1
    /lib/x86_64-linux-gnu/libz.so.1
    /lib/x86_64-linux-gnu/libcurl-gnutls.so.4
    /lib/x86_64-linux-gnu/libicuuc.so.70
    /lib/x86_64-linux-gnu/librdkafka.so.1
    /usr/local/lib/libyaml.so
    /usr/local/lib/libwebsockets.so.18
    /lib/x86_64-linux-gnu/libunistring.so.2
    /lib/x86_64-linux-gnu/libgnutls.so.30
    /lib/x86_64-linux-gnu/libhogweed.so.6
    /lib/x86_64-linux-gnu/libnettle.so.8
    /lib/x86_64-linux-gnu/libgmp.so.10
    /lib/x86_64-linux-gnu/libkrb5.so.3
    /lib/x86_64-linux-gnu/libk5crypto.so.3
    /lib/x86_64-linux-gnu/libcom_err.so.2
    /lib/x86_64-linux-gnu/libkrb5support.so.0
    /lib/x86_64-linux-gnu/libsasl2.so.2
    /lib/x86_64-linux-gnu/libbrotlicommon.so.1
    /lib/x86_64-linux-gnu/libicudata.so.70
    /lib/x86_64-linux-gnu/liblz4.so.1
    /lib/x86_64-linux-gnu/libp11-kit.so.0
    /lib/x86_64-linux-gnu/libtasn1.so.6
    /lib/x86_64-linux-gnu/libkeyutils.so.1
    /lib/x86_64-linux-gnu/libresolv.so.2
    /lib/x86_64-linux-gnu/libffi.so.8
    /lib/x86_64-linux-gnu/libncurses.so.6
    /usr/local/lib/libmetis.so
)

bins=(
    /usr/bin/nmon
    /usr/local/bin/gpmetis
)

if [ ! -f Dockerfile ]; then
    echo 'Run the script from JasmineGraph root'
    exit 1
fi

docker build -t jasminegraph .

TEMP_DIR="files.tmp"
mkdir -p "$TEMP_DIR"
cd "$TEMP_DIR"
rm -r * || true
mkdir -p bin
mkdir -p lib

cont_jasmine="$(docker create jasminegraph)"
for file in "${libs[@]}"; do
    echo "Copying $file"
    docker cp -L "$cont_jasmine":"$file" lib/ | sed '/Copying from container/d'
done
for file in "${bins[@]}"; do
    echo "Copying $file"
    docker cp -L "$cont_jasmine":"$file" bin/ | sed '/Copying from container/d'
done
echo 'Copying JasmineGraph'
docker cp "$cont_jasmine":/home/ubuntu/software/jasminegraph/JasmineGraph ./ | sed '/Copying from container/d'
docker rm "$cont_jasmine"

docker build -t jasminegraph:fs -f ../minimal/Dockerfile.fs ..
cont_fs="$(docker create jasminegraph:fs)"
docker export "$cont_fs" | docker import - jasminegraph:squash
docker rm "$cont_fs"
docker rmi jasminegraph:fs

docker build -t jasminegraph:minimal -f ../minimal/Dockerfile.minimal .

docker rmi jasminegraph:squash
cd ..
rm -r "$TEMP_DIR"

echo 'Created docker image jasminegraph:minimal'
