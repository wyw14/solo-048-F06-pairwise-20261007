package isolation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/network"
	"heatpulse/internal/valve"
)

type Status string

const (
	StatusOpeningRelief      Status = "opening_relief"
	StatusClosingBoundary    Status = "closing_boundaries"
	StatusConfirmingReceipts Status = "confirming_receipts"
	StatusFinalizing         Status = "finalizing"
	StatusCompleted          Status = "completed"
	StatusFailed             Status = "failed"
)

type Session struct {
	ID          string                `json:"id"`
	SectionID   string                `json:"section_id"`
	ZoneID      string                `json:"zone_id"`
	Status      Status                `json:"status"`
	Stage       Stage                 `json:"stage"`
	Actor       string                `json:"actor"`
	Relief      valve.ReliefProof     `json:"relief"`
	Boundaries  []valve.BoundaryProof `json:"boundaries"`
	Error       string                `json:"error,omitempty"`
	StartedAt   time.Time             `json:"started_at"`
	CompletedAt time.Time             `json:"completed_at,omitempty"`
}

type Service struct {
	topology   *network.Topology
	state      *network.State
	relief     *valve.ReliefService
	boundaries *valve.BoundaryService
	audit      *audit.Recorder
	mu         sync.RWMutex
	sessions   map[string]Session
}

func NewService(topology *network.Topology, state *network.State, relief *valve.ReliefService, boundaries *valve.BoundaryService, recorder *audit.Recorder) *Service {
	return &Service{topology: topology, state: state, relief: relief, boundaries: boundaries, audit: recorder, sessions: make(map[string]Session)}
}

// Begin opens a new isolation session for the section and moves the zone into
// the isolating phase. The session then advances one stage at a time via the
// Step methods so that progress stays observable and resumable.
func (s *Service) Begin(sectionID, actor string) (Session, error) {
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Session{}, err
	}
	session := Session{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID,
		Status: StatusOpeningRelief, Stage: StageRelief, Actor: actor,
		StartedAt: time.Now().UTC(), Boundaries: make([]valve.BoundaryProof, 0, len(section.BoundaryValves)),
	}
	s.save(session)
	if _, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	return session, nil
}

// ResumeSession puts a failed or interrupted session back into an active
// state at its first incomplete stage, keeping every receipt collected so
// far. Completed sessions are returned unchanged.
func (s *Service) ResumeSession(sessionID string) (Session, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Session{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	if session.Status == StatusCompleted {
		return session, nil
	}
	section, err := s.topology.Section(session.SectionID)
	if err != nil {
		return Session{}, err
	}
	session.Error = ""
	session.CompletedAt = time.Time{}
	switch {
	case !reliefProven(session):
		session.Stage, session.Status = StageRelief, StatusOpeningRelief
	case !boundariesProven(session, section):
		session.Stage, session.Status = StageBoundary, StatusClosingBoundary
	default:
		session.Stage, session.Status = StageReceipt, StatusConfirmingReceipts
	}
	s.save(session)
	if _, err := s.state.Update(session.ZoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	}); err != nil {
		return s.fail(session, err)
	}
	return session, nil
}

// StepRelief opens the section relief valve （泄压）. If the session already
// holds a valid relief receipt the step is skipped.
func (s *Service) StepRelief(ctx context.Context, sessionID string) (Session, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Session{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	if session.Status == StatusCompleted {
		return session, nil
	}
	section, err := s.topology.Section(session.SectionID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Stage, session.Status = StageRelief, StatusOpeningRelief
	s.save(session)
	if reliefProven(session) {
		return session, nil
	}
	proof, err := s.relief.Open(ctx, section.ReliefValve, session.ID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Relief = proof
	s.save(session)
	return session, nil
}

// StepBoundary closes every boundary valve （边界阀） that does not yet have a
// receipt in this session, so a resumed run only repeats the valves that
// never finished.
func (s *Service) StepBoundary(ctx context.Context, sessionID string) (Session, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Session{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	if session.Status == StatusCompleted {
		return session, nil
	}
	section, err := s.topology.Section(session.SectionID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Stage, session.Status = StageBoundary, StatusClosingBoundary
	s.save(session)
	for _, valveID := range section.BoundaryValves {
		if boundaryProven(session, valveID) {
			continue
		}
		proof, err := s.boundaries.Close(ctx, valveID, session.ID, session.Relief)
		if err != nil {
			return s.fail(session, err)
		}
		session.Boundaries = append(session.Boundaries, proof)
		s.save(session)
	}
	return session, nil
}

// StepReceipts confirms that every receipt （回执） collected by the session
// still matches the physical valve positions before isolation is finalized.
func (s *Service) StepReceipts(_ context.Context, sessionID string) (Session, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Session{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	if session.Status == StatusCompleted {
		return session, nil
	}
	section, err := s.topology.Section(session.SectionID)
	if err != nil {
		return s.fail(session, err)
	}
	session.Stage, session.Status = StageReceipt, StatusConfirmingReceipts
	s.save(session)
	if !reliefProven(session) {
		return s.fail(session, fmt.Errorf("relief receipt missing for valve %s", section.ReliefValve))
	}
	if !s.relief.IsOpen(section.ReliefValve) {
		return s.fail(session, fmt.Errorf("relief valve %s is not open", section.ReliefValve))
	}
	for _, valveID := range section.BoundaryValves {
		if !boundaryProven(session, valveID) {
			return s.fail(session, fmt.Errorf("boundary receipt missing for valve %s", valveID))
		}
		if !s.boundaries.IsClosed(valveID) {
			return s.fail(session, fmt.Errorf("boundary valve %s is not closed", valveID))
		}
	}
	return session, nil
}

// StepFinal completes the session once all receipts are confirmed. The zone
// itself is sealed separately via SealZone, after every branch of the zone
// has completed.
func (s *Service) StepFinal(_ context.Context, sessionID string) (Session, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Session{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	if session.Status == StatusCompleted {
		return session, nil
	}
	section, err := s.topology.Section(session.SectionID)
	if err != nil {
		return s.fail(session, err)
	}
	if !reliefProven(session) || !boundariesProven(session, section) {
		return s.fail(session, fmt.Errorf("cannot finalize isolation %s: receipts incomplete", session.ID))
	}
	session.Stage, session.Status = StageFinal, StatusFinalizing
	s.save(session)
	session.Status = StatusCompleted
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	if err := s.audit.Accepted("isolation.complete", session.SectionID, session.Actor, "section safely isolated", map[string]any{"session_id": session.ID}); err != nil {
		return session, err
	}
	return session, nil
}

// Run advances the session through every remaining stage.
func (s *Service) Run(ctx context.Context, sessionID string) (Session, error) {
	steps := []func(context.Context, string) (Session, error){s.StepRelief, s.StepBoundary, s.StepReceipts, s.StepFinal}
	for _, step := range steps {
		if _, err := step(ctx, sessionID); err != nil {
			session, _ := s.Get(sessionID)
			return session, err
		}
	}
	session, _ := s.Get(sessionID)
	return session, nil
}

// SealZone marks the zone as fully isolated （最终隔离）. Callers must ensure
// every branch of the zone has completed before sealing.
func (s *Service) SealZone(zoneID, actor string) error {
	if _, err := s.state.Update(zoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolated
		return nil
	}); err != nil {
		return err
	}
	return s.audit.Accepted("isolation.zone_sealed", zoneID, actor, "zone fully isolated", nil)
}

// Isolate keeps the original one-call behaviour: begin a session, run every
// stage, then seal the zone.
func (s *Service) Isolate(ctx context.Context, sectionID, actor string) (Session, error) {
	session, err := s.Begin(sectionID, actor)
	if err != nil {
		return session, err
	}
	session, err = s.Run(ctx, session.ID)
	if err != nil {
		return session, err
	}
	if err := s.SealZone(session.ZoneID, actor); err != nil {
		return session, err
	}
	session, _ = s.Get(session.ID)
	return session, nil
}

func (s *Service) SectionsForZone(zoneID string) []network.Section {
	return s.topology.SectionsForZone(zoneID)
}

func (s *Service) ZonePhase(zoneID string) (network.Phase, bool) {
	zone, ok := s.state.Get(zoneID)
	if !ok {
		return "", false
	}
	return zone.Phase, true
}

// LatestForSection returns the most recently started session for a section,
// regardless of which coordinator created it.
func (s *Service) LatestForSection(sectionID string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest Session
	found := false
	for _, session := range s.sessions {
		if session.SectionID != sectionID {
			continue
		}
		if !found || session.StartedAt.After(latest.StartedAt) {
			latest = session
			found = true
		}
	}
	if found {
		latest.Boundaries = append([]valve.BoundaryProof(nil), latest.Boundaries...)
	}
	return latest, found
}

func (s *Service) fail(session Session, cause error) (Session, error) {
	session.Status = StatusFailed
	session.Error = cause.Error()
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	_ = s.audit.Rejected("isolation.failed", session.SectionID, "system", cause.Error(), map[string]any{"session_id": session.ID})
	return session, fmt.Errorf("isolation %s: %w", session.ID, cause)
}

func (s *Service) save(session Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session.ID] = session
}

func (s *Service) Get(sessionID string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[sessionID]
	session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
	return session, ok
}

func (s *Service) List() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
		result = append(result, session)
	}
	return result
}

func reliefProven(session Session) bool {
	return session.Relief.Open && session.Relief.SessionID == session.ID
}

func boundaryProven(session Session, valveID string) bool {
	for _, proof := range session.Boundaries {
		if proof.ValveID == valveID && proof.Closed && proof.SessionID == session.ID {
			return true
		}
	}
	return false
}

func boundariesProven(session Session, section network.Section) bool {
	for _, valveID := range section.BoundaryValves {
		if !boundaryProven(session, valveID) {
			return false
		}
	}
	return true
}
