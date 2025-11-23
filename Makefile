.PHONY: all build test clean docker run-scheduler run-worker proto

# Variables
BINARY_SCHEDULER=scheduler
BINARY_WORKER=worker
MAIN_SCHEDULER=./cmd/scheduler
MAIN_WORKER=./cmd/worker

# Build all binaries
all: build

# Build binaries
build:
	@echo "Building scheduler..."
	go build -o bin/$(BINARY_SCHEDULER) $(MAIN_SCHEDULER)
	@echo "Building worker..."
	go build -o bin/$(BINARY_WORKER) $(MAIN_WORKER)

# Generate protobuf files
proto:
	@echo "Generating protobuf files..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pkg/proto/scheduler.proto

# Run tests
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...

# Run tests with coverage report
test-coverage: test
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	go test -v -tags=integration ./tests/...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	rm -rf data/
	rm -f coverage.out coverage.html

# Docker operations
docker-build:
	@echo "Building Docker images..."
	docker build -t distributed-scheduler:latest -f Dockerfile.scheduler .
	docker build -t distributed-worker:latest -f Dockerfile.worker .

docker-up:
	@echo "Starting Docker containers..."
	docker-compose up -d

docker-down:
	@echo "Stopping Docker containers..."
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
	docker rmi distributed-scheduler:latest distributed-worker:latest

# Run scheduler locally
run-scheduler:
	@echo "Starting scheduler node..."
	go run $(MAIN_SCHEDULER) \
		--node-id=scheduler1 \
		--http-addr=:8001 \
		--raft-addr=:9001 \
		--grpc-addr=:7001 \
		--bootstrap=true \
		--log-format=console

# Run worker locally
run-worker:
	@echo "Starting worker node..."
	go run $(MAIN_WORKER) \
		--worker-id=worker1 \
		--scheduler=localhost:7001 \
		--log-format=console

# Development helpers
dev-cluster:
	@echo "Starting development cluster..."
	@make -j3 run-scheduler1 run-scheduler2 run-scheduler3

run-scheduler1:
	go run $(MAIN_SCHEDULER) --node-id=scheduler1 --http-addr=:8001 --raft-addr=:9001 --grpc-addr=:7001 --bootstrap=true --log-format=console

run-scheduler2:
	sleep 2 && go run $(MAIN_SCHEDULER) --node-id=scheduler2 --http-addr=:8002 --raft-addr=:9002 --grpc-addr=:7002 --join=localhost:9001 --log-format=console

run-scheduler3:
	sleep 3 && go run $(MAIN_SCHEDULER) --node-id=scheduler3 --http-addr=:8003 --raft-addr=:9003 --grpc-addr=:7003 --join=localhost:9001 --log-format=console

# Linting
lint:
	@echo "Running linters..."
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	goimports -w .

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	go mod tidy

# Install development tools
install-tools:
	@echo "Installing development tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest

# Help
help:
	@echo "Available targets:"
	@echo "  all              - Build all binaries"
	@echo "  build            - Build scheduler and worker binaries"
	@echo "  test             - Run unit tests"
	@echo "  test-coverage    - Run tests with coverage report"
	@echo "  test-integration - Run integration tests"
	@echo "  bench            - Run benchmarks"
	@echo "  clean            - Clean build artifacts"
	@echo "  docker-build     - Build Docker images"
	@echo "  docker-up        - Start Docker containers"
	@echo "  docker-down      - Stop Docker containers"
	@echo "  docker-logs      - View Docker logs"
	@echo "  docker-clean     - Clean Docker resources"
	@echo "  run-scheduler    - Run scheduler locally"
	@echo "  run-worker       - Run worker locally"
	@echo "  dev-cluster      - Start 3-node development cluster"
	@echo "  lint             - Run linters"
	@echo "  fmt              - Format code"
	@echo "  tidy             - Tidy dependencies"
	@echo "  install-tools    - Install development tools"
