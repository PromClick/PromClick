package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	nativech "github.com/hinskii/promclick/proxy/clickhouse"
	"github.com/hinskii/promclick/proxy/config"
)

func main() {
	configPath := flag.String("config", "downsampler.yaml", "path to config file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	once := flag.Bool("once", false, "run once and exit (overrides daemon config)")
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

	cfg, err := config.LoadDownsampler(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if !cfg.Downsampling.Enabled {
		logger.Info("downsampling not enabled, nothing to do")
		return
	}

	// Create pool (needs both native TCP + HTTP for DDL)
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

	// Apply downsampling config
	if err := pool.ApplyDownsamplingConfig(context.Background(), &cfg.Downsampling); err != nil {
		logger.Error("downsampling config failed", "error", err)
		os.Exit(1)
	}
	logger.Info("downsampling config applied successfully")

	// One-shot mode
	if *once || !cfg.Daemon {
		return
	}

	// Daemon mode: re-apply periodically (idempotent)
	logger.Info("running in daemon mode", "interval", cfg.Interval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("downsampler stopped")
			return
		case <-ticker.C:
			if err := pool.ApplyDownsamplingConfig(ctx, &cfg.Downsampling); err != nil {
				logger.Error("periodic downsampling check failed", "error", err)
			} else {
				logger.Info("periodic downsampling check completed")
			}
		}
	}
}
