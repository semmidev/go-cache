test:
	go test ./cache -v -race -count=1

bench:
	go test ./cache -bench=. -benchmem -benchtime=2s

cover:
	go test ./cache -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

run:
	go run ./example

all: test bench
