package server

import (
	"log/slog"
	"net/http"
	_ "net/http/pprof"

	"github.com/hinskii/promclick/proxy/config"
	"github.com/hinskii/promclick/proxy/server/handlers"
	"github.com/hinskii/promclick/proxy/server/middleware"
	"github.com/hinskii/promclick/proxy/ui"
)

// Server is the HTTP proxy server.
type Server struct {
	cfg     *config.Config
	logger  *slog.Logger
	handler *handlers.Handler
	status  *handlers.StatusHandler
}

// New creates a new Server.
func New(cfg *config.Config, logger *slog.Logger, handler *handlers.Handler) *Server {
	return &Server{
		cfg:     cfg,
		logger:  logger,
		handler: handler,
		status:  handlers.NewStatusHandler(),
	}
}

// SetReady marks the server as ready.
func (s *Server) SetReady(ready bool) {
	s.status.IsReady.Store(ready)
}

// Routes returns the http.Handler with all routes and middleware.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Instant query
	mux.HandleFunc("GET /api/v1/query", s.handler.Query)
	mux.HandleFunc("POST /api/v1/query", s.handler.Query)

	// Range query
	mux.HandleFunc("GET /api/v1/query_range", s.handler.QueryRange)
	mux.HandleFunc("POST /api/v1/query_range", s.handler.QueryRange)

	// Labels
	mux.HandleFunc("GET /api/v1/labels", s.handler.Labels)
	mux.HandleFunc("POST /api/v1/labels", s.handler.Labels)

	// Label values
	mux.HandleFunc("GET /api/v1/label/{name}/values", s.handler.LabelValues)

	// Series
	mux.HandleFunc("GET /api/v1/series", s.handler.Series)
	mux.HandleFunc("POST /api/v1/series", s.handler.Series)

	// Remote write
	mux.HandleFunc("POST /api/v1/write", s.handler.Write)

	// Metadata
	mux.HandleFunc("GET /api/v1/metadata", s.handler.Metadata)

	// Build info
	mux.HandleFunc("GET /api/v1/status/buildinfo", handlers.BuildInfo)

	// Status endpoints
	mux.HandleFunc("GET /api/v1/status/config", s.handler.StatusConfig)
	mux.HandleFunc("GET /api/v1/status/flags", handlers.StatusFlags)
	mux.HandleFunc("GET /api/v1/status/runtimeinfo", s.status.RuntimeInfo)
	mux.HandleFunc("GET /api/v1/status/tsdb", s.handler.TSDBStatus)

	// Health
	mux.HandleFunc("GET /-/healthy", s.status.Healthy)
	mux.HandleFunc("GET /-/ready", s.status.Ready)

	// pprof
	mux.HandleFunc("/debug/pprof/", http.DefaultServeMux.ServeHTTP)

	// UI (catch-all, must be last)
	mux.Handle("/", ui.Handler())

	// Apply middleware chain (outermost first):
	// gzip -> requestid -> cors -> logging -> timeout
	return middleware.Chain(
		mux,
		middleware.Gzip,
		middleware.RequestID,
		middleware.CORS(s.cfg.CORS.AllowOrigin),
		middleware.Logging(s.logger),
		middleware.Timeout(s.cfg.QueryTimeout),
	)
}
