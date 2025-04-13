FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o relay ./cmd/relay

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /app/relay .
COPY config/config.yaml ./config.yaml

ENTRYPOINT ["./relay"]
