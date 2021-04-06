ROOT           := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
APP_VERSION    := 1.0.0
IMAGE_REGISTRY := quay.io
IMAGE_ORG      := appzygy
IMAGE_NAME     := kafka-scale
IMAGE          := $(IMAGE_REGISTRY)/$(IMAGE_ORG)/$(IMAGE_NAME):$(APP_VERSION)

# containerized build and push for in-cluster deployment
.PHONY : quay
quay: podman-build podman-push

# desktop build for desktop testing
.PHONY : local-build
local-build:
	go mod tidy
	CGO_ENABLED=0 GO111MODULE=auto go build -ldflags "-X 'main.APP_VERSION=${APP_VERSION}'"\
    -a -o $(ROOT)/kafka-scale github.com/aceeric/kafka-scale

# containerized build
.PHONY : podman-build
podman-build:
	podman build . -t $(IMAGE) --build-arg APP_VERSION=$(APP_VERSION) --file ./Dockerfile

# push to quay
.PHONY : podman-push
podman-push:
	podman push $(IMAGE)
