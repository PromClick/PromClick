.PHONY: build ui run test

# Build React UI
ui:
	cd ui && npm install && npm run build

# Build all binaries (requires ui/dist/)
build: ui
	go build -o promclick-proxy ./cmd/proxy/
	go build -o promclick-writer ./cmd/writer/
	go build -o promclick-downsampler ./cmd/downsampler/

# Run proxy locally
run:
	go run ./cmd/proxy/ --config ../deploy/proxy.yaml

# Run tests
test:
	go test ./...
