BINARY := bin/majordomo-steward
CMD    := ./cmd/majordomo-steward
IMAGE  := majordomo-steward

.PHONY: help build run test test-cover lint fmt vet tidy migrate docker-build

help:
	@echo "Available targets:"
	@echo "  build        Build the binary"
	@echo "  run          Build and run the server"
	@echo "  test         Run all tests"
	@echo "  test-cover   Run tests with coverage report"
	@echo "  lint         Run golangci-lint"
	@echo "  fmt          Format code"
	@echo "  vet          Run go vet"
	@echo "  tidy         Run go mod tidy"
	@echo "  docker-build Build Docker image"

build:
	go build -o $(BINARY) $(CMD)

run: build
	$(BINARY)

test:
	go test ./...

test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

migrate: build ## Run database migrations
	$(BINARY) migrate

docker-build:
	docker build -t $(IMAGE) .
