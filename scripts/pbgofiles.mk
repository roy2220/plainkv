override pbgofiles := $(patsubst %.proto,%.pb.go,$(wildcard internal/protocol/*.proto))
override protoc := build/protoc/bin/protoc
override protoc-gen-gogo := build/gogoprotobuf/bin/protoc-gen-gogofaster
override include := build/include
override gopackage := $(shell go list -m)

%.pb.go: %.proto $(protoc) $(protoc-gen-gogo) $(include)
	$(protoc) --plugin=protoc-gen-gogo=$(protoc-gen-gogo) -Ibuild/protoc/include -I$(include) --gogo_out=$(include) $(gopackage)/$<

$(protoc): build/protoc

build/protoc: build/protoc.zip
	unzip $< -d $@

build/protoc.zip:
	curl -L https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip -o $@

$(protoc-gen-gogo): build/protoc
	go build -o $@ github.com/gogo/protobuf/protoc-gen-gogofaster

$(include): $(include)/$(gopackage) \
            $(include)/github.com/gogo/protobuf

$(include)/$(gopackage):
	mkdir -p $(dir $@)
	ln -s $(CURDIR) $@

$(include)/%:
	mkdir -p $(dir $@)
	ln -s $(shell go list -m -f '{{.Dir}}' $*) $@
