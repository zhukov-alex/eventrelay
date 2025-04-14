MOCK_DIR := internal/mocks
BIN_DIR := bin

.PHONY: all test acceptance mocks fmt lint clean build docker-build

all: test

build:
	@echo "Building clean..."
	go build -o $(BIN_DIR)/cleaner ./cmd/clean
	@echo "Building relay..."
	go build -o $(BIN_DIR)/relay ./cmd/relay

docker-build:
	docker build -t relay:latest .

test:
	go test ./... -v -race

acceptance:
	go test -v ./test/acceptance

mocks:
	mockgen -source=internal/output/output.go -destination=$(MOCK_DIR)/mock_output.go -package=mocks
	mockgen -source=internal/relay/service.go -destination=$(MOCK_DIR)/mock_relay.go -package=mocks
	mockgen -source=internal/ingest/ingest.go -destination=$(MOCK_DIR)/mock_ingest.go -package=mocks
	mockgen -source=internal/wal/wal_writer.go -destination=$(MOCK_DIR)/mock_wal.go -package=mocks

fmt:
	go fmt ./...

lint:
	go vet ./...

clean:
	go clean
	rm -rf $(BIN_DIR)
