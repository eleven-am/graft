.PHONY: proto proto-clean proto-gen test build clean mocks mock-clean bump-patch bump-minor bump-major current-version debug-basic

PROTO_DIR := internal/proto
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

proto: proto-clean proto-gen

proto-clean:
	@rm -rf $(PROTO_DIR)/*.pb.go

proto-gen:
	@protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

# Mock generation - managed by .mockery.yaml
mocks: mock-clean
	@echo "Generating mocks using .mockery.yaml configuration..."
	@mockery
	@echo "All mocks generated successfully"

mock-clean:
	@echo "Cleaning existing mocks..."
	@find . -type d -name "mocks" -exec rm -rf {} + 2>/dev/null || true

test:
	@go test -v ./...

test-coverage:
	@go test -v -cover ./...

build:
	@go build -v ./...

clean:
	@go clean -v ./...
	@rm -rf $(PROTO_DIR)/*.pb.go
	@find . -type d -name "mocks" -exec rm -rf {} + 2>/dev/null || true

deps:
	@go mod download
	@go mod tidy

deps-install:
	@which protoc > /dev/null || (echo "protoc is required but not installed. Please install Protocol Buffers compiler." && exit 1)
	@protoc --version
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@go install github.com/vektra/mockery/v2@latest

# Version management using git tags
current-version:
	@git tag -l | sort -V | tail -1 2>/dev/null || echo "v0.0.0"

bump-patch:
	@current=$$(git tag -l | sort -V | tail -1 2>/dev/null || echo "v0.0.0"); \
	if [ "$$current" = "v0.0.0" ]; then \
		new="v0.0.1"; \
	else \
		new=$$(echo $$current | awk -F. '{$$NF = $$NF + 1;} 1' | sed 's/ /./g'); \
	fi; \
	echo "Bumping version from $$current to $$new"; \
	git tag $$new; \
	git push origin $$new; \
	echo "Tagged and pushed $$new"

bump-minor:
	@current=$$(git tag -l | sort -V | tail -1 2>/dev/null || echo "v0.0.0"); \
	if [ "$$current" = "v0.0.0" ]; then \
		new="v0.1.0"; \
	else \
		new=$$(echo $$current | awk -F. '{$$(NF-1) = $$(NF-1) + 1; $$NF = 0;} 1' | sed 's/ /./g'); \
	fi; \
	echo "Bumping version from $$current to $$new"; \
	git tag $$new; \
	git push origin $$new; \
	echo "Tagged and pushed $$new"

# Debug basic example - cleanup and run with logging
debug-basic:
	@echo "Cleaning up any existing processes..."
	@lsof -ti:7000 | xargs kill -9 2>/dev/null || true
	@pkill -f "go run main.go" 2>/dev/null || true
	@sleep 1
	@echo "Starting basic example with debug logging..."
	@cd examples/basic && rm -rf data/ && go run main.go

bump-major:
	@current=$$(git tag -l | sort -V | tail -1 2>/dev/null || echo "v0.0.0"); \
	if [ "$$current" = "v0.0.0" ]; then \
		new="v1.0.0"; \
	else \
		new=$$(echo $$current | awk -F. '{$$(NF-2) = $$(NF-2) + 1; $$(NF-1) = 0; $$NF = 0;} 1' | sed 's/ /./g'); \
	fi; \
	echo "Bumping version from $$current to $$new"; \
	git tag $$new; \
	git push origin $$new; \
	echo "Tagged and pushed $$new"