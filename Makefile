override SHELL := /usr/bin/env bash -euo pipefail
override .DEFAULT_GOAL := all

ifdef USE_DOCKER

%: force
	@scripts/make-with-docker.sh $@

else # ifdef USE_DOCKER

all: force vet lint test docs

include scripts/pbgofiles.mk
vet: force $(pbgofiles)
	go vet ./...

lint: force
	go run golang.org/x/lint/golint -set_exit_status ./...

test: force
	go test -coverprofile=coverage.txt -covermode=count ./...

include scripts/svgfiles.mk
docs: force $(svgfiles)

endif # ifdef USE_DOCKER

.PHONY: force
force:
