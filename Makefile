.PHONY: build test docker up down check

# ── Build ────────────────────────────────────────────────────

build:
	cd proxy && go build -o ../promclick-proxy ./cmd/proxy/
	cd proxy && go build -o ../promclick-writer ./cmd/writer/
	cd proxy && go build -o ../promclick-downsampler ./cmd/downsampler/

test:
	go test ./eval/... ./translator/... ./fingerprint/...
	cd proxy && go test ./...

# ── Docker ───────────────────────────────────────────────────

docker:
	docker build -t promclick .

up:
	docker compose up -d

down:
	docker compose down -v

# ── Health checks ────────────────────────────────────────────

check:
	@echo "ClickHouse:" && curl -sf 'http://localhost:8123/?query=SELECT+1' && echo " OK" || echo " FAIL"
	@echo "Prometheus:" && curl -sf http://localhost:9090/-/healthy | head -c 30
	@echo ""
	@echo "PromClick Proxy:" && curl -sf http://localhost:9099/-/healthy
	@echo ""
	@echo "PromClick Writer:" && curl -sf http://localhost:9091/-/healthy
	@echo ""
	@echo "Grafana:" && curl -sf http://localhost:3000/api/health | head -c 30
	@echo ""
	@echo ""
	@echo "Samples in CH:"
	@curl -sf 'http://localhost:8123/?database=metrics' --data-binary "SELECT count() FROM samples" || echo "0"
	@echo "Series in CH:"
	@curl -sf 'http://localhost:8123/?database=metrics' --data-binary "SELECT count(DISTINCT fingerprint) FROM time_series" || echo "0"
