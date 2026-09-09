.PHONY: test bench cover run lint all

test:
	go test ./cache -v -race -count=1

bench:
	go test ./cache -bench=. -benchmem -benchtime=2s

cover:
	go test ./cache -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

run:
	go run ./example

lint:
	@command -v golangci-lint >/dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run ./...

all: test bench lint
