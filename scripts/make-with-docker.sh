#!/usr/bin/env bash

set -euxo pipefail

tar -C scripts/builder -czh . | docker build -t plainkv-builder:latest --cache-from=plainkv-builder:latest -

tee<<EOF | docker run -i --rm -v${PWD}:/plainkv plainkv-builder:latest /usr/bin/env bash -euxo pipefail -
CGO_ENABLED=0 GOCACHE=/plainkv/build/go-cache make -C /plainkv ${*}
EOF
