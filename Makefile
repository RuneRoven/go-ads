.PHONY: all all-log lint fmt vet test test-race test-cover build build-examples hardware hardware-log hardware-parallel test-docker test-docker-tc3 test-docker-tc3-118 test-docker-tc2 test-native-tc3 test-native-tc2 clean

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

# Second pass compiles the integration-tagged files, which the normal
# build never touches. Without it a broken integration file is only
# found hours later on hardware.
vet:
	go vet ./...
	go vet -tags integration ./...

test:
	go test -v -timeout 600s ./...

test-race:
	go test -v -race -timeout 600s ./...

test-cover:
	go test -v -coverprofile=coverage.out -covermode=atomic -timeout 600s ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

build:
	go build ./...

build-examples:
	cd examples/simple && go build ./...

test-docker-tc3:
	docker/run-tests.sh .env.integration.224

test-docker-tc3-118:
	docker/run-tests.sh .env.integration.118

test-docker-tc2:
	docker/run-tests.sh .env.integration.70

test-docker: test-docker-tc3 test-docker-tc2

# All three PLCs concurrently (separate devices, no PLC-side contention).
# Per-PLC log under logs/, exits non-zero if any run fails.
hardware-parallel:
	docker/run-tests-parallel.sh 224 118 70

# Native hardware integration tests (run on host, not Docker)
# Requires PLC reachable from host network. macOS may prompt for firewall on first run.
test-native-tc3:
	set -a && . ./.env.integration.224 && set +a && go test -v -tags integration -timeout 10m -run 'TestIntegration' .

test-native-tc2:
	set -a && . ./.env.integration.70 && set +a && go test -v -tags integration -timeout 10m -run 'TestIntegration' .

clean:
	rm -f coverage.out coverage.html
	rm -f examples/simple/simple examples/simple/go-ads-cli
	rm -rf logs/
