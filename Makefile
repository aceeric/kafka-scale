ROOT   := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
#GOROOT := $(shell go env GOROOT)

.PHONY : all
all: build

.PHONY : build
build:
	go mod tidy
	CGO_ENABLED=0 GO111MODULE=auto go build -a -o $(ROOT)/kafka-scale github.com/aceeric/kafka-scale
