# EventRelay

A high-performance event relay service designed for reliably receiving, logging, and asynchronously forwarding events. It ensures at-least-once delivery guarantees by utilizing a local Write-Ahead Log (WAL), with immediate acknowledgment after disk persistence, and subsequent batch forwarding to downstream systems such as Kafka or other sinks. In case of a service restart, events stored in the WAL are replayed and reliably delivered. This ensures data durability, crash resilience, and scalability, making it suitable for audit logging, event sourcing, and high-load data ingestion services.


## Features

- Durable write-ahead logging (WAL) with immediate disk sync
- At-least-once delivery guarantees
- Batch-oriented asynchronous event forwarding
- Support for pluggable ingestion protocols (currently TCP, designed to support gRPC, etc.)
- Modular output support (Kafka implemented, extendable to others)
- WAL replay capability for crash recovery
- Graceful shutdown and configurable flush intervals
- Prometheus-compatible metrics
- Structured logging


## Project Structure

```
cmd/
  relay/             - Command-line entrypoint
  clean/             - CLI command for WAL cleanup

internal/
  app/               - Application bootstrap and coordination
  cleaner/           - Periodic WAL segment cleanup
  config/            - Configuration
  ingest/            - Event ingestion (TCP, extendable)
  logger/            - Structured logging and rotating files
  metrics/           - Metrics exposure
  mocks/             - Generated test mocks
  output/            - Output sinks (Kafka, extendable)
  relay/             - Relay logic (event handling, WAL replay, batching)
  wal/               - Durable writer implementations
```


## Dev

### Build binaries

```bash
make build
```

This will compile:

- `bin/relay` — the main event relay service
- `bin/cleaner` — a CLI utility for cleaning old WAL segments

### Run locally

```bash
./bin/relay --config ./config/config.yaml
```

Or use Docker:

```bash
docker build -t relay:latest .
docker run -p 9000:9000 -v $(pwd)/wal:/wal relay:latest
```

### Run tests

```bash
make test          # Unit & integration tests
make acceptance    # End-to-end acceptance tests
```

### Generate mocks

```bash
make mocks
```

### Format and lint

```bash
make fmt
make lint
```

## License

This project is licensed under the MIT License.
