.PHONY: all
all: vet lint test docs

.PHONY: vet
include scripts/pbgofiles.mk
vet: $(pbgofiles)
	go vet ./...

.PHONY: lint
lint:
	go run golang.org/x/lint/golint -set_exit_status ./...

.PHONY: test
test:
	go test -coverprofile=coverage.txt -covermode=count ./...

.PHONY: docs
include scripts/svgfiles.mk
docs: $(svgfiles)
