override SHELL := /usr/bin/env bash -euxo pipefail
override .DEFAULT_GOAL := all

ifdef USE_DOCKER

%: force
	@scripts/make-with-docker.sh $@ $(MAKEFLAGS) USE_DOCKER=

Makefile:
	# a dummy target to suppress bugs of GNU Make

else # ifdef USE_DOCKER

all: force vet lint test docs

include scripts/pbgofiles.mk
vet: force $(pbgofiles)
	@go vet $(ARGS) ./...

lint: force
	@go run golang.org/x/lint/golint -set_exit_status $(ARGS) ./...

test: force
	@go test -coverprofile=coverage.txt -covermode=count $(ARGS) ./...

include scripts/svgfiles.mk
docs: force $(svgfiles)

endif # ifdef USE_DOCKER

.PHONY: force
force:
