package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	nativech "github.com/hinskii/promclick/proxy/clickhouse"
	"github.com/hinskii/promclick/proxy/config"
)

func main() {
	configPath := flag.String("config", "writer.yaml", "path to config file")
	listen := flag.String("listen", "", "listen address (overrides config)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	cfg, err := config.LoadWriter(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if *listen != "" {
		cfg.ListenAddr = *listen
	}

	// Create native TCP pool
	schema := nativech.SchemaInfo{
		Database:        cfg.ClickHouse.Database,
		SamplesTable:    cfg.Schema.SamplesTable,
		TimeSeriesTable: cfg.Schema.TimeSeriesTable,
		FingerprintCol:  cfg.Schema.Columns.Fingerprint,
		TimestampCol:    cfg.Schema.Columns.Timestamp,
		ValueCol:        cfg.Schema.Columns.Value,
		MetricNameCol:   cfg.Schema.Columns.MetricName,
	}
	pool, err := nativech.NewPool(cfg.ClickHouse.NativeAddr, cfg.ClickHouse.Database, cfg.ClickHouse.User, cfg.ClickHouse.Password, cfg.ClickHouse.HTTPAddr, schema)
	if err != nil {
		logger.Error("failed to create pool", "error", err)
		os.Exit(1)
	}

	// Warmup
	for i := 0; i < 3; i++ {
		if err := pool.Ping(context.Background()); err != nil {
			logger.Warn("warmup ping failed", "error", err)
		}
	}
	logger.Info("ch-go connections warmed up")

	// Create writer
	writer := nativech.NewWriter(pool, nativech.WriterConfig{
		Database:      cfg.ClickHouse.Database,
		BatchSize:     cfg.Write.BatchSize,
		QueueSize:     cfg.Write.QueueSize,
		FlushInterval: cfg.Write.FlushInterval,
	})

	writerCtx, writerCancel := context.WithCancel(context.Background())
	defer writerCancel()
	writer.Start(writerCtx)

	logger.Info("remote_write writer started",
		"batch_size", cfg.Write.BatchSize,
		"flush_interval", cfg.Write.FlushInterval,
	)

	// HTTP routes
	var ready atomic.Bool
	ready.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/write", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := req.Unmarshal(reqBuf); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := writer.Write(r.Context(), &req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /-/healthy", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("Healthy\n"))
	})
	mux.HandleFunc("GET /-/ready", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			_, _ = w.Write([]byte("Ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Not Ready\n"))
	})

	httpSrv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting writer", "addr", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
	ready.Store(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}

	writerCancel()
	logger.Info("writer stopped")
}
