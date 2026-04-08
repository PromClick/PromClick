package handlers

import (
	"net/http"
)

// Metadata handles /api/v1/metadata. Returns an empty object stub.
func (h *Handler) Metadata(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, APIResponse{
		Status: "success",
		Data:   map[string]interface{}{},
	})
}
