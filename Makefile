.PHONY: all lint fmt vet test test-race test-cover build build-examples test-docker test-docker-tc3 test-docker-tc2 clean

all: fmt lint vet test

lint:
	golangci-lint run

fmt:
	gofumpt -w .
	golangci-lint fmt

vet:
	go vet ./...

test:
	go test -v -timeout 120s ./...

test-race:
	go test -v -race -timeout 120s ./...

test-cover:
	go test -v -coverprofile=coverage.out -covermode=atomic -timeout 120s ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

build:
	go build ./...

build-examples:
	cd examples/simple && go build ./...

test-docker-tc3:
	docker/run-tests.sh .env.integration.224

test-docker-tc2:
	docker/run-tests.sh .env.integration.70

test-docker: test-docker-tc3 test-docker-tc2

clean:
	rm -f coverage.out coverage.html
	rm -f examples/simple/simple examples/simple/go-ads-cli
