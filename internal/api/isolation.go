package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (a *App) isolationProgress(w http.ResponseWriter, r *http.Request) {
	zoneID := chi.URLParam(r, "zoneID")
	if _, ok := a.Network.Get(zoneID); !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("zone %s not found", zoneID))
		return
	}
	writeJSON(w, http.StatusOK, a.IsolationSupervisor.Current().Progress(zoneID))
}

func (a *App) resumeIsolation(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if input.Actor == "" {
		input.Actor = "operator"
	}
	progress, err := a.IsolationSupervisor.Current().ResumeZone(r.Context(), chi.URLParam(r, "zoneID"), input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, progress)
}
