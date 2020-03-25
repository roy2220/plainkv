override pbgofiles := $(patsubst %.proto,%.pb.go,$(wildcard hashmap/internal/protocol/*.proto))
override protoc-gen-gogo := build/gogoprotobuf/bin/protoc-gen-gogofaster
override include := build/include
override gopackage := $(shell go list -m)

%.pb.go: %.proto $(protoc-gen-gogo) $(include)
	@protoc --plugin=protoc-gen-gogo=$(protoc-gen-gogo) -I$(include) --gogo_out=$(include) $(gopackage)/$<

$(protoc-gen-gogo):
	@go build -o $@ github.com/gogo/protobuf/protoc-gen-gogofaster

$(include): $(include)/$(gopackage) \
            $(include)/github.com/gogo/protobuf

$(include)/$(gopackage):
	@mkdir -p $(dir $@)
	@ln -s "$${PWD}" $@

$(include)/%:
	@mkdir -p $(dir $@)
	@ln -s "$$(go list -m -f '{{.Dir}}' $*)" $@
