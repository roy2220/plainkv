#!/usr/bin/env bash

set -euxo pipefail

cp go.* scripts/builder
export COMPOSE_FILE=scripts/builder.docker-compose.yml
export COMPOSE_PROJECT_NAME=plainkv
trap 'docker-compose down --remove-orphans' EXIT
docker-compose build
docker-compose run --rm builder make "${@}"
