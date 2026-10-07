package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/telemetry"
)

func (a *App) recordTelemetry(w http.ResponseWriter, r *http.Request) {
	var sample telemetry.Sample
	if err := decodeJSON(r, &sample); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.Telemetry.Add(sample); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, sample)
}

func (a *App) evaluateHydraulic(w http.ResponseWriter, r *http.Request) {
	stationID := chi.URLParam(r, "stationID")
	snapshot, err := a.SnapshotsBuilder.Build(stationID)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	balance, err := a.Balancer.Evaluate(snapshot)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	incident, err := a.LeakProtection.Apply(balance)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	var protectionIncident any
	var isolationProgress any
	if incident != nil && incident.Confirmed {
		raised, raiseErr := a.Protection.Raise(stationID, "hydraulic_leak", "critical", "confirmed mass imbalance")
		if raiseErr != nil {
			writeError(w, http.StatusConflict, raiseErr)
			return
		}
		protectionIncident = raised
		stationState, ok := a.Stations.Get(stationID)
		if ok {
			sections := a.Topology.SectionsForZone(stationState.ZoneID)
			if len(sections) > 0 {
				progress, isolateErr := a.IsolationSupervisor.Current().IsolateZone(r.Context(), *incident, stationState.ZoneID, "leak-protection")
				if isolateErr != nil {
					writeError(w, http.StatusConflict, isolateErr)
					return
				}
				isolationProgress = progress
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot": snapshot, "balance": balance, "incident": incident,
		"protection_incident": protectionIncident, "isolation": isolationProgress,
	})
}
