package handlers

import (
	"io"
	"net/http"

	"github.com/golang/snappy"

	"github.com/prometheus/prometheus/prompb"

	nativech "github.com/hinskii/promclick/proxy/clickhouse"
)

// Write handles POST /api/v1/write (Prometheus remote_write receiver).
func (h *Handler) Write(w http.ResponseWriter, r *http.Request) {
	if h.Writer == nil {
		writeError(w, http.StatusNotImplemented, "not_implemented", "remote_write not enabled")
		return
	}

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	var req prompb.WriteRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	if err := h.Writer.Write(r.Context(), &req); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// SetWriter sets the Writer on the handler (optional, only if write is enabled).
func (h *Handler) SetWriter(writer *nativech.Writer) {
	h.Writer = writer
}
