.PHONY: test bench cover run run-01 run-02 lint all

GOLANGCI_LINT_CMD ?= $(shell command -v golangci-lint 2>/dev/null || echo "go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest")

test:
	go test ./cache -v -race -count=1

bench:
	go test ./cache -bench=. -benchmem -benchtime=2s

cover:
	go test ./cache -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

run:
	go run ./example/01-memory-rt-cache
	@echo ""
	go run ./example/02-weighted-lru-cache

run-01:
	go run ./example/01-memory-rt-cache

run-02:
	go run ./example/02-weighted-lru-cache
lint:
	$(GOLANGCI_LINT_CMD) run ./...

all: test bench lint
