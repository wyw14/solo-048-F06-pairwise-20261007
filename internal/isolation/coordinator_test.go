package isolation

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/store"
	"heatpulse/internal/valve"
)

func newTestService(t *testing.T) (*Service, *valve.ReliefService, *valve.BoundaryService, *network.State, network.Topology) {
	t.Helper()
	topology := network.NewTopology([]network.Section{
		{ID: "east-branch", ZoneID: "east", Upstream: "east-supply", Downstream: "east-return", ReliefValve: "east-rv", Branches: []network.Branch{
			{ID: "east-upstream", BoundaryValves: []string{"east-bv-a"}},
			{ID: "east-downstream", BoundaryValves: []string{"east-bv-b"}},
		}},
	})
	if err := topology.Validate(); err != nil {
		t.Fatalf("validate topology: %v", err)
	}
	state := network.NewState([]network.Zone{{ID: "east", Name: "东支线", Phase: network.PhaseActive}})
	relief := valve.NewReliefService(0)
	boundaries := valve.NewBoundaryService()
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl")))
	service := NewService(topology, state, relief, boundaries, recorder)
	return service, relief, boundaries, state, *topology
}

func confirmedIncident() hydraulic.LeakIncident {
	return hydraulic.LeakIncident{ID: "inc-1", StationID: "binhe", Confirmed: true}
}

func stepByID(t *testing.T, session Session, id string) Step {
	t.Helper()
	for _, step := range session.Steps {
		if step.ID == id {
			return step
		}
	}
	t.Fatalf("step %s not found in %+v", id, session.Steps)
	return Step{}
}

func TestIsolatePlansPerBranchStages(t *testing.T) {
	service, _, _, state, _ := newTestService(t)
	session, err := service.Isolate(context.Background(), "east-branch", "tester")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if session.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", session.Status)
	}
	wantOrder := []string{"relief", "boundary:east-bv-a", "boundary:east-bv-b", "receipts", "finalize"}
	if len(session.Steps) != len(wantOrder) {
		t.Fatalf("steps = %d, want %d", len(session.Steps), len(wantOrder))
	}
	for i, id := range wantOrder {
		step := session.Steps[i]
		if step.ID != id || step.State != StepCompleted {
			t.Fatalf("step %d = %+v, want id %s completed", i, step, id)
		}
	}
	progress, err := service.Progress(session.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	branches := map[string][]Step{}
	for _, branch := range progress.Branches {
		branches[branch.BranchID] = branch.Steps
	}
	if len(branches["east-upstream"]) != 1 || branches["east-upstream"][0].Stage != StageBoundary {
		t.Fatalf("upstream branch progress = %+v", branches["east-upstream"])
	}
	if len(branches["east-downstream"]) != 1 || branches["east-downstream"][0].Stage != StageBoundary {
		t.Fatalf("downstream branch progress = %+v", branches["east-downstream"])
	}
	sectionStages := []Stage{}
	for _, step := range branches["section"] {
		sectionStages = append(sectionStages, step.Stage)
	}
	if len(sectionStages) != 3 || sectionStages[0] != StageRelief || sectionStages[1] != StageReceipt || sectionStages[2] != StageIsolation {
		t.Fatalf("section stages = %v", sectionStages)
	}
	zone, _ := state.Get("east")
	if zone.Phase != network.PhaseIsolated {
		t.Fatalf("zone phase = %s, want isolated", zone.Phase)
	}
}

func TestResumeFromFailedReliefStep(t *testing.T) {
	service, relief, boundaries, _, _ := newTestService(t)
	relief.SetDelay(-1)
	failed, err := service.Isolate(context.Background(), "east-branch", "tester")
	if err == nil {
		t.Fatal("isolate should fail while the relief driver is unavailable")
	}
	if failed.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", failed.Status)
	}
	if step := stepByID(t, failed, "relief"); step.State != StepFailed {
		t.Fatalf("relief step = %+v, want failed", step)
	}
	if step := stepByID(t, failed, "receipts"); step.State != StepPending {
		t.Fatalf("receipts step = %+v, want pending", step)
	}
	relief.SetDelay(0)
	resumed, err := service.Resume(context.Background(), failed.ID, "tester")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", resumed.Status)
	}
	if got := len(boundaries.Commands()); got != 2 {
		t.Fatalf("boundary commands = %d, want 2 (no re-execution)", got)
	}
}

func TestResumeSkipsCompletedSteps(t *testing.T) {
	service, relief, boundaries, _, _ := newTestService(t)
	ctx := context.Background()
	// Seed a session that failed after the relief valve and the upstream
	// boundary valve had already completed.
	section, _ := service.topology.Section("east-branch")
	session := Session{
		ID: "seeded", SectionID: section.ID, ZoneID: section.ZoneID,
		Status: StatusFailed, Steps: planSteps(section), Error: "boundary driver lost",
	}
	reliefProof, err := relief.Open(ctx, section.ReliefValve, session.ID)
	if err != nil {
		t.Fatalf("open relief: %v", err)
	}
	session.Relief = reliefProof
	boundaryProof, err := boundaries.Close(ctx, "east-bv-a", session.ID, reliefProof)
	if err != nil {
		t.Fatalf("close boundary: %v", err)
	}
	session.Boundaries = append(session.Boundaries, boundaryProof)
	session.Steps[0].State = StepCompleted // relief
	session.Steps[1].State = StepCompleted // boundary:east-bv-a
	session.Steps[2].State = StepFailed    // boundary:east-bv-b
	service.save(session)

	resumed, err := service.Resume(ctx, session.ID, "tester")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", resumed.Status)
	}
	if got := len(boundaries.Commands()); got != 2 {
		t.Fatalf("boundary commands = %d, want 2: upstream valve must not be closed twice", got)
	}
	if len(resumed.Boundaries) != 2 {
		t.Fatalf("session boundaries = %d, want 2", len(resumed.Boundaries))
	}
	for _, step := range resumed.Steps {
		if step.State != StepCompleted {
			t.Fatalf("step %s state = %s, want completed", step.ID, step.State)
		}
	}
}

func TestCoordinatorFreezesOnEpochChange(t *testing.T) {
	service, _, _, _, topology := newTestService(t)
	generation := uint64(1)
	source := EpochSourceFunc(func() Epoch {
		return Epoch{Partition: topology.Fingerprint(), Generation: generation}
	})
	supervisor := NewSupervisor(service, source)
	first := supervisor.Current()
	if _, err := first.FromIncident(context.Background(), confirmedIncident(), "east-branch"); err != nil {
		t.Fatalf("from incident: %v", err)
	}
	generation++
	second := supervisor.Current()
	if second == first {
		t.Fatal("supervisor must hand out a new coordinator after the epoch changes")
	}
	if info := first.Info(); !info.Frozen {
		t.Fatalf("old coordinator not frozen: %+v", info)
	}
	if _, err := first.Begin(context.Background(), "east-branch", "tester"); !errors.Is(err, ErrCoordinatorFrozen) {
		t.Fatalf("begin on frozen coordinator = %v, want ErrCoordinatorFrozen", err)
	}
	if _, err := first.Resume(context.Background(), "whatever", "tester"); !errors.Is(err, ErrCoordinatorFrozen) {
		t.Fatalf("resume on frozen coordinator = %v, want ErrCoordinatorFrozen", err)
	}
	// The new coordinator refuses to resume sessions owned by the frozen one.
	var failedSessionID string
	service.mu.RLock()
	for id := range service.sessions {
		failedSessionID = id
	}
	service.mu.RUnlock()
	if _, err := second.Resume(context.Background(), failedSessionID, "tester"); err == nil {
		t.Fatal("new coordinator must not resume a session from a frozen epoch")
	}
	// Starting a fresh isolation on the new coordinator works.
	if _, err := second.Begin(context.Background(), "east-branch", "tester"); err != nil {
		t.Fatalf("begin on current coordinator: %v", err)
	}
}

func TestCoordinatorResumeAfterFailure(t *testing.T) {
	service, relief, _, _, topology := newTestService(t)
	source := EpochSourceFunc(func() Epoch {
		return Epoch{Partition: topology.Fingerprint(), Generation: 1}
	})
	coordinator := NewCoordinator(service, source)
	relief.SetDelay(-1)
	failed, err := coordinator.FromIncident(context.Background(), confirmedIncident(), "east-branch")
	if err == nil {
		t.Fatal("expected isolation to fail while the relief driver is unavailable")
	}
	relief.SetDelay(0)
	resumed, err := coordinator.Resume(context.Background(), failed.ID, "tester")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", resumed.Status)
	}
	active, ok := coordinator.Active("east-branch")
	if !ok || active.ID != resumed.ID {
		t.Fatalf("active session = %+v, %v; want resumed session", active, ok)
	}
}

func TestFromIncidentRequiresConfirmation(t *testing.T) {
	service, _, _, _, topology := newTestService(t)
	source := EpochSourceFunc(func() Epoch {
		return Epoch{Partition: topology.Fingerprint(), Generation: 1}
	})
	coordinator := NewCoordinator(service, source)
	incident := confirmedIncident()
	incident.Confirmed = false
	if _, err := coordinator.FromIncident(context.Background(), incident, "east-branch"); err == nil {
		t.Fatal("unconfirmed incident must not trigger isolation")
	}
}
