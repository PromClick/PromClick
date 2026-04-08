package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hinskii/promclick/clickhouse"
	"github.com/hinskii/promclick/eval"

	"github.com/hinskii/promclick/proxy/config"
	nativech "github.com/hinskii/promclick/proxy/clickhouse"
	"github.com/hinskii/promclick/proxy/server"
	"github.com/hinskii/promclick/proxy/server/handlers"
)

func main() {
	configPath := flag.String("config", "proxy.yaml", "path to config file")
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

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if *listen != "" {
		cfg.ListenAddr = *listen
	}

	// Create promql2chsql config
	promCfg := cfg.ToPromqlConfig()

	// Create evaluator — use native TCP if configured, fallback to HTTP
	var evaluator *eval.Evaluator
	var queryPool *nativech.Pool // shared pool for downsampled queries
	if cfg.ClickHouse.NativeAddr != "" {
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
			logger.Warn("native TCP failed, using HTTP", "error", err)
			evaluator = eval.NewWithClient(promCfg, clickhouse.NewClient(promCfg.ClickHouse))
		} else {
			logger.Info("using ch-go native TCP", "addr", cfg.ClickHouse.NativeAddr)

			// Warmup connections
			for i := 0; i < 3; i++ {
				if err := pool.Ping(context.Background()); err != nil {
					logger.Warn("warmup ping failed", "error", err)
				}
			}
			logger.Info("ch-go connections warmed up")

			// Label cache — eliminates JOIN per query
			if cfg.Labels.CacheEnabled {
				lc := nativech.NewLabelCache(cfg.Labels.CacheTTL, cfg.Labels.CacheMaxSeries,
					pool.HTTPClient,
					cfg.ClickHouse.HTTPAddr, cfg.ClickHouse.Database,
					cfg.ClickHouse.User, cfg.ClickHouse.Password,
					cfg.Schema.TimeSeriesTable,
					cfg.Schema.Columns.Fingerprint,
					cfg.Schema.Columns.MetricName,
					cfg.Schema.Columns.Labels,
				)
				if err := lc.Refresh(context.Background()); err != nil {
					logger.Warn("label cache initial load failed", "error", err)
				} else {
					logger.Info("label cache loaded", "series", lc.Size())
				}
				lc.StartBackgroundRefresh(context.Background())
				pool.LabelCache = lc
			}

			evaluator = eval.NewWithFetcher(promCfg, pool)
			queryPool = pool
		}
	} else {
		evaluator = eval.NewWithClient(promCfg, clickhouse.NewClient(promCfg.ClickHouse))
	}

	// Create meta querier
	// Create shared HTTP client for meta queries
	var metaHTTPClient *http.Client
	if queryPool != nil {
		metaHTTPClient = queryPool.HTTPClient
	}
	meta := &handlers.MetaQuerier{
		Addr:       cfg.ClickHouse.HTTPAddr,
		Database:   cfg.ClickHouse.Database,
		User:       cfg.ClickHouse.User,
		Password:   cfg.ClickHouse.Password,
		HTTPClient: metaHTTPClient,
	}

	// Create handler
	h := &handlers.Handler{
		Cfg:       cfg,
		PromCfg:   promCfg,
		Evaluator: evaluator,
		Meta:      meta,
		Pool:      queryPool,
	}

	// Create server
	srv := server.New(cfg, logger, h)
	srv.SetReady(true)

	httpSrv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      srv.Routes(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: cfg.QueryTimeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting proxy", "addr", cfg.ListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down...")
	srv.SetReady(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
	logger.Info("server stopped")
}
