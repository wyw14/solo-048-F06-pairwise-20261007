package isolation

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"heatpulse/internal/hydraulic"
)

// ErrCoordinatorFrozen is returned by every mutating call on a coordinator
// whose partition or protection generation has been superseded.
var ErrCoordinatorFrozen = errors.New("isolation coordinator frozen")

// Epoch identifies the conditions an isolation plan was computed under: the
// network partition (topology fingerprint) and the protection generation.
type Epoch struct {
	Partition  string `json:"partition"`
	Generation uint64 `json:"generation"`
}

// EpochSource reports the current partition/protection epoch.
type EpochSource interface {
	Current() Epoch
}

// EpochSourceFunc adapts a plain function to EpochSource.
type EpochSourceFunc func() Epoch

func (f EpochSourceFunc) Current() Epoch { return f() }

type CoordinatorInfo struct {
	Epoch  Epoch  `json:"epoch"`
	Frozen bool   `json:"frozen"`
	Reason string `json:"reason,omitempty"`
}

// Coordinator drives isolation sessions for one epoch. It is bound to the
// (partition, protection generation) pair captured at creation; once the
// source reports a different epoch the coordinator freezes and only serves
// read-only progress queries.
type Coordinator struct {
	service *Service
	source  EpochSource
	epoch   Epoch
	mu      sync.Mutex
	active  map[string]string
	owned   map[string]bool
	frozen  bool
	reason  string
}

func NewCoordinator(service *Service, source EpochSource) *Coordinator {
	return &Coordinator{
		service: service, source: source, epoch: source.Current(),
		active: make(map[string]string), owned: make(map[string]bool),
	}
}

// Freeze permanently stops the coordinator: mutating calls fail with
// ErrCoordinatorFrozen while progress reads keep working.
func (c *Coordinator) Freeze(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.freezeLocked(reason)
}

func (c *Coordinator) freezeLocked(reason string) {
	if !c.frozen {
		c.frozen = true
		c.reason = reason
	}
}

// checkAliveLocked freezes the coordinator if the epoch has moved on, and
// reports whether mutating operations are still allowed.
func (c *Coordinator) checkAliveLocked() error {
	if c.frozen {
		return fmt.Errorf("%w: %s", ErrCoordinatorFrozen, c.reason)
	}
	if current := c.source.Current(); current != c.epoch {
		reason := fmt.Sprintf("epoch moved from partition %s generation %d to partition %s generation %d",
			c.epoch.Partition, c.epoch.Generation, current.Partition, current.Generation)
		c.freezeLocked(reason)
		return fmt.Errorf("%w: %s", ErrCoordinatorFrozen, c.reason)
	}
	return nil
}

func (c *Coordinator) Info() CoordinatorInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CoordinatorInfo{Epoch: c.epoch, Frozen: c.frozen, Reason: c.reason}
}

func (c *Coordinator) FromIncident(ctx context.Context, incident hydraulic.LeakIncident, sectionID string) (Session, error) {
	if !incident.Confirmed {
		return Session{}, fmt.Errorf("leak incident %s is not confirmed", incident.ID)
	}
	return c.begin(ctx, sectionID, "leak-protection")
}

// Begin starts a manual isolation under the coordinator's epoch.
func (c *Coordinator) Begin(ctx context.Context, sectionID, actor string) (Session, error) {
	return c.begin(ctx, sectionID, actor)
}

func (c *Coordinator) begin(ctx context.Context, sectionID, actor string) (Session, error) {
	c.mu.Lock()
	if err := c.checkAliveLocked(); err != nil {
		c.mu.Unlock()
		return Session{}, err
	}
	if sessionID := c.active[sectionID]; sessionID != "" {
		c.mu.Unlock()
		if session, ok := c.service.Get(sessionID); ok {
			return session, fmt.Errorf("section %s already has an isolation session", sectionID)
		}
	} else {
		c.active[sectionID] = "starting"
	}
	c.mu.Unlock()
	session, err := c.service.Isolate(ctx, sectionID, actor)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		delete(c.active, sectionID)
		if session.ID != "" {
			// Failed sessions stay resumable by this coordinator.
			c.owned[session.ID] = true
		}
		return session, err
	}
	c.active[sectionID] = session.ID
	c.owned[session.ID] = true
	return session, nil
}

// Resume continues a failed session from its first incomplete step. Only
// sessions started by this coordinator (hence under its epoch) may be
// resumed; sessions from a frozen epoch must be restarted as fresh
// isolations on the current coordinator.
func (c *Coordinator) Resume(ctx context.Context, sessionID, actor string) (Session, error) {
	c.mu.Lock()
	if err := c.checkAliveLocked(); err != nil {
		c.mu.Unlock()
		return Session{}, err
	}
	if !c.owned[sessionID] {
		c.mu.Unlock()
		return Session{}, fmt.Errorf("session %s belongs to another coordinator epoch; start a fresh isolation", sessionID)
	}
	c.mu.Unlock()
	session, err := c.service.Resume(ctx, sessionID, actor)
	if err != nil {
		return session, err
	}
	c.mu.Lock()
	c.active[session.SectionID] = session.ID
	c.mu.Unlock()
	return session, nil
}

func (c *Coordinator) Active(sectionID string) (Session, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sessionID := c.active[sectionID]
	if sessionID == "" || sessionID == "starting" {
		return Session{}, false
	}
	return c.service.Get(sessionID)
}

// Supervisor hands out the coordinator for the current epoch. When the
// partition or protection generation changes it freezes the old coordinator
// and builds a new one bound to the new epoch.
type Supervisor struct {
	service *Service
	source  EpochSource
	mu      sync.Mutex
	current *Coordinator
}

func NewSupervisor(service *Service, source EpochSource) *Supervisor {
	return &Supervisor{service: service, source: source}
}

func (s *Supervisor) Current() *Coordinator {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		s.current = NewCoordinator(s.service, s.source)
		return s.current
	}
	if epoch := s.source.Current(); epoch != s.current.epoch {
		s.current.Freeze(fmt.Sprintf("superseded by partition %s protection generation %d", epoch.Partition, epoch.Generation))
		s.current = NewCoordinator(s.service, s.source)
	}
	return s.current
}
