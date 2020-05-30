#!/usr/bin/env bash

set -euo pipefail

COMMANDS=()

COMMANDS+=(trysave)
trysave() {
    local id=$(docker images -q "${1}")

    if [[ -z ${id} ]]; then
        return
    fi

    local file=${1}.tar.${id}

    if [[ -f ${file} ]]; then
        return
    fi

    docker save "${1}" -o "${file}"
    find . -maxdepth 1 -name "${1}.tar.*" ! -name "${file}" -type f -exec rm {} \;
}

COMMANDS+=(tryload)
tryload() {
    find . -maxdepth 1 -name "${1}.tar.*" -type f -exec docker load -i {} \; -quit
}

case $# in
    0)
        echo 'command required'
        exit 1
        ;;
    1)
        echo 'image required'
        exit 1
        ;;
esac

for command in "${COMMANDS[@]}"; do
    if [[ ${1} == ${command} ]]; then
        set -x
        mkdir -p build/cache/docker-images
        cd build/cache/docker-images
        "${1}" "${@:2}"
        exit 0
    fi
done

echo "unknown command: $(printf '%q' ${1})"
exit 1
