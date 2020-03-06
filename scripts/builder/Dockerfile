FROM golang:1.13.8-alpine3.11
RUN apk add -q --no-progress --no-cache bash \
                                        make \
                                        protoc \
                                        protobuf-dev \
                                        graphviz
ADD go.mod go.sum /plainkv/
RUN cd /plainkv && go mod download
