FROM golang:1.16 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go sources
COPY cmdline.go cmdline.go
COPY compute.go compute.go
COPY kafka.go   kafka.go
COPY main.go    main.go
COPY metrics.go metrics.go
COPY read.go    read.go
COPY results.go results.go

# Build
ARG APP_VERSION
RUN	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build  -ldflags "-X 'main.version=${APP_VERSION}'"\
    -a -o kafka-scale github.com/aceeric/kafka-scale

# Use distroless as minimal base image to package the binary
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/kafka-scale .
USER nonroot:nonroot

ENTRYPOINT ["/kafka-scale"]
