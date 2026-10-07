package isolation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/hydraulic"
)

// Fence pins a coordinator to the network partition and protection
// generation （保护代次） it was created for. When either moves on, the
// coordinator is frozen and a new one takes over.
type Fence struct {
	Partition  uint64 `json:"partition"`
	Generation uint64 `json:"generation"`
}

type Coordinator struct {
	service      *Service
	fence        Fence
	mu           sync.Mutex
	active       map[string]string
	frozen       bool
	freezeReason string
	frozenAt     time.Time
}

func NewCoordinator(service *Service, fence Fence) *Coordinator {
	return &Coordinator{service: service, fence: fence, active: make(map[string]string)}
}

func (c *Coordinator) Fence() Fence {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fence
}

// Freeze permanently stops the coordinator: it rejects new isolations and
// resumes, and any run still in flight halts before its next stage. Sessions
// it already started stay resumable through the new coordinator.
func (c *Coordinator) Freeze(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.frozen = true
	c.freezeReason = reason
	c.frozenAt = time.Now().UTC()
}

func (c *Coordinator) Frozen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frozen
}

func (c *Coordinator) frozenErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return fmt.Errorf("isolation coordinator frozen: %s", c.freezeReason)
	}
	return nil
}

// FromIncident isolates a single section, resuming its previous session from
// the first incomplete step when an earlier attempt failed.
func (c *Coordinator) FromIncident(ctx context.Context, incident hydraulic.LeakIncident, sectionID string) (Session, error) {
	if !incident.Confirmed {
		return Session{}, fmt.Errorf("leak incident %s is not confirmed", incident.ID)
	}
	session, needsRun, err := c.ensureSession(sectionID, "leak-protection")
	if err != nil {
		return Session{}, err
	}
	if !needsRun {
		return session, fmt.Errorf("section %s already has an isolation session", sectionID)
	}
	return c.runSession(ctx, session.ID)
}

// IsolateZone isolates every topology branch of the zone in turn and seals
// the zone once all of them complete. Branches whose latest session already
// completed are skipped, so a partially isolated zone only runs the branches
// that are still open.
func (c *Coordinator) IsolateZone(ctx context.Context, incident hydraulic.LeakIncident, zoneID, actor string) (Progress, error) {
	if !incident.Confirmed {
		return Progress{}, fmt.Errorf("leak incident %s is not confirmed", incident.ID)
	}
	if err := c.frozenErr(); err != nil {
		return c.Progress(zoneID), err
	}
	sections := c.service.SectionsForZone(zoneID)
	if len(sections) == 0 {
		return Progress{}, fmt.Errorf("zone %s has no isolation branches", zoneID)
	}
	for _, section := range sections {
		session, needsRun, err := c.ensureSession(section.ID, actor)
		if err != nil {
			return c.Progress(zoneID), err
		}
		if !needsRun {
			continue
		}
		if _, err := c.runSession(ctx, session.ID); err != nil {
			return c.Progress(zoneID), err
		}
	}
	if err := c.service.SealZone(zoneID, actor); err != nil {
		return c.Progress(zoneID), err
	}
	return c.Progress(zoneID), nil
}

// Resume continues the section's incomplete session from its first
// unfinished step. Completed sessions are returned unchanged.
func (c *Coordinator) Resume(ctx context.Context, sectionID, actor string) (Session, error) {
	session, needsRun, err := c.ensureSession(sectionID, actor)
	if err != nil {
		return Session{}, err
	}
	if !needsRun {
		return session, nil
	}
	return c.runSession(ctx, session.ID)
}

// ResumeZone resumes every incomplete branch of the zone and seals it once
// all branches have completed.
func (c *Coordinator) ResumeZone(ctx context.Context, zoneID, actor string) (Progress, error) {
	if err := c.frozenErr(); err != nil {
		return c.Progress(zoneID), err
	}
	sections := c.service.SectionsForZone(zoneID)
	if len(sections) == 0 {
		return Progress{}, fmt.Errorf("zone %s has no isolation branches", zoneID)
	}
	for _, section := range sections {
		session, needsRun, err := c.ensureSession(section.ID, actor)
		if err != nil {
			return c.Progress(zoneID), err
		}
		if !needsRun {
			continue
		}
		if _, err := c.runSession(ctx, session.ID); err != nil {
			return c.Progress(zoneID), err
		}
	}
	if err := c.service.SealZone(zoneID, actor); err != nil {
		return c.Progress(zoneID), err
	}
	return c.Progress(zoneID), nil
}

// Progress reports every branch of the zone with its stage-by-stage state.
func (c *Coordinator) Progress(zoneID string) Progress {
	progress := Progress{
		ZoneID:   zoneID,
		Fence:    c.Fence(),
		Frozen:   c.Frozen(),
		Branches: make([]BranchProgress, 0),
	}
	if phase, ok := c.service.ZonePhase(zoneID); ok {
		progress.ZonePhase = phase
	}
	for _, section := range c.service.SectionsForZone(zoneID) {
		var session *Session
		if latest, ok := c.service.LatestForSection(section.ID); ok {
			session = &latest
		}
		progress.Branches = append(progress.Branches, branchProgress(section, session))
	}
	return progress
}

func (c *Coordinator) Active(sectionID string) (Session, bool) {
	c.mu.Lock()
	sessionID := c.active[sectionID]
	c.mu.Unlock()
	if sessionID == "" || sessionID == "starting" {
		return Session{}, false
	}
	return c.service.Get(sessionID)
}

// ensureSession returns the session to run for the section: the resumed
// previous session when one is still incomplete, a fresh one otherwise.
// needsRun is false when the section's latest session already completed.
func (c *Coordinator) ensureSession(sectionID, actor string) (Session, bool, error) {
	if err := c.frozenErr(); err != nil {
		return Session{}, false, err
	}
	c.mu.Lock()
	if c.active[sectionID] == "starting" {
		c.mu.Unlock()
		return Session{}, false, fmt.Errorf("section %s isolation is already starting", sectionID)
	}
	c.active[sectionID] = "starting"
	c.mu.Unlock()

	session, needsRun, err := c.prepareSession(sectionID, actor)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		delete(c.active, sectionID)
		return Session{}, false, err
	}
	c.active[sectionID] = session.ID
	return session, needsRun, nil
}

func (c *Coordinator) prepareSession(sectionID, actor string) (Session, bool, error) {
	if latest, ok := c.service.LatestForSection(sectionID); ok {
		if latest.Status == StatusCompleted {
			return latest, false, nil
		}
		session, err := c.service.ResumeSession(latest.ID)
		return session, true, err
	}
	session, err := c.service.Begin(sectionID, actor)
	return session, true, err
}

// runSession advances the session stage by stage, re-checking the freeze
// before every step so a frozen coordinator stops at the next stage boundary.
func (c *Coordinator) runSession(ctx context.Context, sessionID string) (Session, error) {
	steps := []func(context.Context, string) (Session, error){
		c.service.StepRelief, c.service.StepBoundary, c.service.StepReceipts, c.service.StepFinal,
	}
	for _, step := range steps {
		if err := c.frozenErr(); err != nil {
			session, _ := c.service.Get(sessionID)
			return session, err
		}
		if _, err := step(ctx, sessionID); err != nil {
			session, _ := c.service.Get(sessionID)
			return session, err
		}
	}
	session, _ := c.service.Get(sessionID)
	return session, nil
}
