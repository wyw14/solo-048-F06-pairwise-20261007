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
	StatusCollectingReceipts Status = "collecting_receipts"
	StatusFinalizing         Status = "finalizing"
	StatusCompleted          Status = "completed"
	StatusFailed             Status = "failed"
)

// Stage identifies one phase of the isolation sequence. Every plan step
// belongs to exactly one stage, and the console groups them by branch.
type Stage string

const (
	StageRelief    Stage = "relief"    // 泄压
	StageBoundary  Stage = "boundary"  // 边界阀
	StageReceipt   Stage = "receipt"   // 回执
	StageIsolation Stage = "isolation" // 最终隔离
)

// sectionBranch labels the section-level steps (relief, receipts, finalize)
// that do not belong to a single topology branch.
const sectionBranch = "section"

type StepState string

const (
	StepPending   StepState = "pending"
	StepRunning   StepState = "running"
	StepCompleted StepState = "completed"
	StepFailed    StepState = "failed"
)

type Step struct {
	ID          string    `json:"id"`
	Stage       Stage     `json:"stage"`
	BranchID    string    `json:"branch_id"`
	ValveID     string    `json:"valve_id,omitempty"`
	State       StepState `json:"state"`
	Error       string    `json:"error,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

type Session struct {
	ID          string                `json:"id"`
	SectionID   string                `json:"section_id"`
	ZoneID      string                `json:"zone_id"`
	Status      Status                `json:"status"`
	Steps       []Step                `json:"steps"`
	Relief      valve.ReliefProof     `json:"relief"`
	Boundaries  []valve.BoundaryProof `json:"boundaries"`
	Error       string                `json:"error,omitempty"`
	StartedAt   time.Time             `json:"started_at"`
	CompletedAt time.Time             `json:"completed_at,omitempty"`
}

// BranchProgress is the per-topology-branch view of a session plan.
type BranchProgress struct {
	BranchID string `json:"branch_id"`
	Steps    []Step `json:"steps"`
}

type Progress struct {
	SessionID string           `json:"session_id"`
	SectionID string           `json:"section_id"`
	Status    Status           `json:"status"`
	Branches  []BranchProgress `json:"branches"`
}

// planSteps lays out the isolation sequence for a section: relief first,
// then every branch's boundary valves, then receipt verification, then the
// final isolation marker.
func planSteps(section network.Section) []Step {
	steps := []Step{
		{ID: "relief", Stage: StageRelief, BranchID: sectionBranch, ValveID: section.ReliefValve, State: StepPending},
	}
	for _, branch := range section.Branches {
		for _, valveID := range branch.BoundaryValves {
			steps = append(steps, Step{
				ID: "boundary:" + valveID, Stage: StageBoundary, BranchID: branch.ID, ValveID: valveID, State: StepPending,
			})
		}
	}
	return append(steps,
		Step{ID: "receipts", Stage: StageReceipt, BranchID: sectionBranch, State: StepPending},
		Step{ID: "finalize", Stage: StageIsolation, BranchID: sectionBranch, State: StepPending},
	)
}

func statusFor(stage Stage) Status {
	switch stage {
	case StageRelief:
		return StatusOpeningRelief
	case StageBoundary:
		return StatusClosingBoundary
	case StageReceipt:
		return StatusCollectingReceipts
	default:
		return StatusFinalizing
	}
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

func (s *Service) Isolate(ctx context.Context, sectionID, actor string) (Session, error) {
	section, err := s.topology.Section(sectionID)
	if err != nil {
		return Session{}, err
	}
	session := Session{
		ID: uuid.NewString(), SectionID: sectionID, ZoneID: section.ZoneID,
		Status: StatusOpeningRelief, StartedAt: time.Now().UTC(),
		Steps: planSteps(section), Boundaries: make([]valve.BoundaryProof, 0, len(section.BoundaryValves())),
	}
	s.save(session)
	if err := s.markIsolating(section.ZoneID); err != nil {
		return s.fail(session, err)
	}
	return s.advance(ctx, session, actor)
}

// Resume continues a failed session from its first incomplete step. Steps
// that already completed (relief opened, a boundary valve closed) are not
// re-executed; failed steps are retried.
func (s *Service) Resume(ctx context.Context, sessionID, actor string) (Session, error) {
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
	if len(session.Steps) == 0 {
		session.Steps = planSteps(section)
	}
	for i := range session.Steps {
		if session.Steps[i].State == StepFailed {
			session.Steps[i].State = StepPending
			session.Steps[i].Error = ""
		}
	}
	session.Error = ""
	session.CompletedAt = time.Time{}
	if err := s.markIsolating(section.ZoneID); err != nil {
		return s.fail(session, err)
	}
	s.save(session)
	return s.advance(ctx, session, actor)
}

func (s *Service) markIsolating(zoneID string) error {
	_, err := s.state.Update(zoneID, func(zone *network.Zone) error {
		zone.Phase = network.PhaseIsolating
		return nil
	})
	return err
}

func (s *Service) advance(ctx context.Context, session Session, actor string) (Session, error) {
	section, err := s.topology.Section(session.SectionID)
	if err != nil {
		return s.fail(session, err)
	}
	for i := range session.Steps {
		step := &session.Steps[i]
		if step.State == StepCompleted {
			continue
		}
		step.State = StepRunning
		step.StartedAt = time.Now().UTC()
		session.Status = statusFor(step.Stage)
		s.save(session)
		if err := s.runStep(ctx, section, &session, step); err != nil {
			step.State = StepFailed
			step.Error = err.Error()
			return s.fail(session, err)
		}
		step.State = StepCompleted
		step.CompletedAt = time.Now().UTC()
		s.save(session)
	}
	session.Status = StatusCompleted
	session.CompletedAt = time.Now().UTC()
	s.save(session)
	if err := s.audit.Accepted("isolation.complete", session.SectionID, actor, "section safely isolated", map[string]any{"session_id": session.ID}); err != nil {
		return session, err
	}
	return session, nil
}

func (s *Service) runStep(ctx context.Context, section network.Section, session *Session, step *Step) error {
	switch step.Stage {
	case StageRelief:
		if session.Relief.Open && session.Relief.SessionID == session.ID {
			return nil
		}
		proof, err := s.relief.Open(ctx, section.ReliefValve, session.ID)
		if err != nil {
			return err
		}
		session.Relief = proof
		return nil
	case StageBoundary:
		for _, proof := range session.Boundaries {
			if proof.ValveID == step.ValveID && proof.Closed {
				return nil
			}
		}
		boundary, err := s.boundaries.Close(ctx, step.ValveID, session.ID, session.Relief)
		if err != nil {
			return err
		}
		session.Boundaries = append(session.Boundaries, boundary)
		return nil
	case StageReceipt:
		return s.verifyReceipts(section, session)
	case StageIsolation:
		_, err := s.state.Update(section.ZoneID, func(zone *network.Zone) error {
			zone.Phase = network.PhaseIsolated
			return nil
		})
		return err
	}
	return fmt.Errorf("unknown isolation stage %s", step.Stage)
}

// verifyReceipts confirms every valve acknowledgement (回执) is on record:
// the relief valve reports open and every branch boundary valve reports
// closed for this session.
func (s *Service) verifyReceipts(section network.Section, session *Session) error {
	if !session.Relief.Open || session.Relief.SessionID != session.ID || !s.relief.IsOpen(section.ReliefValve) {
		return fmt.Errorf("relief valve %s receipt missing", section.ReliefValve)
	}
	closed := make(map[string]bool, len(session.Boundaries))
	for _, proof := range session.Boundaries {
		if proof.Closed && proof.SessionID == session.ID {
			closed[proof.ValveID] = true
		}
	}
	for _, valveID := range section.BoundaryValves() {
		if !closed[valveID] || !s.boundaries.IsClosed(valveID) {
			return fmt.Errorf("boundary valve %s receipt missing", valveID)
		}
	}
	return nil
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
	session.Steps = append([]Step(nil), session.Steps...)
	session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
	s.sessions[session.ID] = session
}

func (s *Service) Get(sessionID string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[sessionID]
	session.Steps = append([]Step(nil), session.Steps...)
	session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
	return session, ok
}

func (s *Service) List() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		session.Steps = append([]Step(nil), session.Steps...)
		session.Boundaries = append([]valve.BoundaryProof(nil), session.Boundaries...)
		result = append(result, session)
	}
	return result
}

// Progress groups a session's steps by topology branch for display.
func (s *Service) Progress(sessionID string) (Progress, error) {
	session, ok := s.Get(sessionID)
	if !ok {
		return Progress{}, fmt.Errorf("isolation session %s not found", sessionID)
	}
	progress := Progress{SessionID: session.ID, SectionID: session.SectionID, Status: session.Status, Branches: make([]BranchProgress, 0)}
	byBranch := make(map[string]int)
	for _, step := range session.Steps {
		index, ok := byBranch[step.BranchID]
		if !ok {
			index = len(progress.Branches)
			byBranch[step.BranchID] = index
			progress.Branches = append(progress.Branches, BranchProgress{BranchID: step.BranchID, Steps: make([]Step, 0)})
		}
		progress.Branches[index].Steps = append(progress.Branches[index].Steps, step)
	}
	return progress, nil
}
