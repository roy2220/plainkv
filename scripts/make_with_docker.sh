#!/usr/bin/env bash

set -euo pipefail

tee<<EOF | docker run -i --rm -v${PWD}:/plainkv golang:1.13.8-alpine3.11 sh -euxo pipefail -
apk add -q --no-progress make \
                         protoc \
                         protobuf-dev \
                         graphviz
CGO_ENABLED=0 make -C /plainkv ${*}
EOF
