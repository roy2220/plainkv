#!/usr/bin/env bash

set -euo pipefail

COMMANDS=()

COMMANDS+=(store)
store() {
    local id=$(docker images -q plainkv-builder:latest)

    if [[ ! -z ${id} && ( ! -f builder-image-id || $(<builder-image-id) != ${id} ) ]]; then
        docker save plainkv-builder:latest > builder-image.tar
        echo ${id} > builder-image-id
    fi
}

COMMANDS+=(load)
load() {
    if [[ -f builder-image-id ]]; then
        docker load < builder-image.tar
    fi
}

if [[ $# == 0 ]]; then
    echo 'command required'
    exit 1
fi

for command in "${COMMANDS[@]}"; do
    if [[ ${1} == ${command} ]]; then
        set -x
        mkdir -p build/cache/docker
        cd build/cache/docker
        "${1}" "${@:2}"
        exit 0
    fi
done

echo "unknown command: $(printf '%q' ${1})"
exit 1
