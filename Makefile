VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

.PHONY: all build test test-cover bench lint fmt vet docker-build clean

all: build

build:
	go build -ldflags "-X main.Version=$(VERSION)" -o kache .

test:
	go test ./... -v -race

test-cover:
	go test ./... -coverprofile=coverage.out && go tool cover -func=coverage.out

bench:
	go test -bench=. -benchtime=3s ./tests/

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

docker-build:
	docker build -t kache .

clean:
	rm -f kache coverage.out
