.PHONY: all all-log lint fmt vet test test-race test-cover build build-examples hardware hardware-log test-docker test-docker-tc3 test-docker-tc2 clean

# Software tests (CI-safe, no PLC needed)
all: fmt lint vet test

# Software tests with log file
all-log:
	@mkdir -p logs
	@LOGFILE="logs/all-$$(date +%Y%m%d-%H%M%S).log"; \
	echo "Log: $$LOGFILE"; \
	$(MAKE) all 2>&1 | tee "$$LOGFILE"

# Hardware integration tests (requires PLC access)
hardware: test-docker

# Hardware tests with log file (clean terminal + raw log file)
hardware-log:
	@mkdir -p logs
	@LOGFILE="logs/hardware-$$(date +%Y%m%d-%H%M%S).log"; \
	echo "Log: $$LOGFILE"; \
	$(MAKE) hardware 2>"$$LOGFILE"

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
	rm -rf logs/
