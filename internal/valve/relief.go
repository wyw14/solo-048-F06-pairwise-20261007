package valve

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ReliefProof struct {
	ValveID   string    `json:"valve_id"`
	SessionID string    `json:"session_id"`
	Open      bool      `json:"open"`
	At        time.Time `json:"at"`
}

type ReliefService struct {
	mu       sync.RWMutex
	position map[string]bool
	delay    time.Duration
}

func NewReliefService(delay time.Duration) *ReliefService {
	return &ReliefService{position: make(map[string]bool), delay: delay}
}

func (s *ReliefService) Open(ctx context.Context, valveID, sessionID string) (ReliefProof, error) {
	s.mu.RLock()
	delay := s.delay
	s.mu.RUnlock()
	if delay < 0 {
		return ReliefProof{}, fmt.Errorf("relief valve %s driver unavailable", valveID)
	}
	select {
	case <-ctx.Done():
		return ReliefProof{}, ctx.Err()
	case <-time.After(delay):
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.position[valveID] = true
	return ReliefProof{ValveID: valveID, SessionID: sessionID, Open: true, At: time.Now().UTC()}, nil
}

// SetDelay adjusts the simulated driver latency. A negative delay marks the
// driver unavailable, which simulates a relief fault during drills.
func (s *ReliefService) SetDelay(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = delay
}

func (s *ReliefService) IsOpen(valveID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position[valveID]
}
