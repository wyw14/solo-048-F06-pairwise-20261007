package protection

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Incident struct {
	ID        string    `json:"id"`
	StationID string    `json:"station_id"`
	Kind      string    `json:"kind"`
	Severity  string    `json:"severity"`
	Message   string    `json:"message"`
	Active    bool      `json:"active"`
	At        time.Time `json:"at"`
}

type Service struct {
	mu         sync.RWMutex
	incidents  map[string]Incident
	generation uint64
}

func NewService() *Service {
	return &Service{incidents: make(map[string]Incident), generation: 1}
}

// Generation is the protection generation （保护代次）. It increases whenever
// the protection posture is reset, fencing off coordinators bound to the
// previous generation.
func (s *Service) Generation() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.generation
}

func (s *Service) BumpGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	return s.generation
}

func (s *Service) Raise(stationID, kind, severity, message string) (Incident, error) {
	if stationID == "" || kind == "" {
		return Incident{}, fmt.Errorf("incident station and kind are required")
	}
	incident := Incident{
		ID: uuid.NewString(), StationID: stationID, Kind: kind, Severity: severity,
		Message: message, Active: true, At: time.Now().UTC(),
	}
	s.mu.Lock()
	s.incidents[incident.ID] = incident
	s.mu.Unlock()
	return incident, nil
}

func (s *Service) Resolve(incidentID string) (Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	incident, ok := s.incidents[incidentID]
	if !ok {
		return Incident{}, fmt.Errorf("incident %s not found", incidentID)
	}
	incident.Active = false
	s.incidents[incidentID] = incident
	return incident, nil
}

func (s *Service) List() []Incident {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Incident, 0, len(s.incidents))
	for _, incident := range s.incidents {
		result = append(result, incident)
	}
	return result
}
