package isolation

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/hydraulic"
	"heatpulse/internal/network"
	"heatpulse/internal/store"
	"heatpulse/internal/valve"
)

func newTestService(t *testing.T, reliefDelay time.Duration) (*Service, *network.State, *valve.BoundaryService) {
	t.Helper()
	topology := network.NewTopology([]network.Section{
		{ID: "east-branch", ZoneID: "east", Upstream: "east-supply", Downstream: "east-return", BoundaryValves: []string{"east-bv-a", "east-bv-b"}, ReliefValve: "east-rv"},
		{ID: "east-loop", ZoneID: "east", Upstream: "east-supply-2", Downstream: "east-return-2", BoundaryValves: []string{"east-bv-c", "east-bv-d"}, ReliefValve: "east-rv-2"},
	})
	state := network.NewState([]network.Zone{{ID: "east", Name: "东支线", Phase: network.PhaseActive}})
	relief := valve.NewReliefService(reliefDelay)
	boundaries := valve.NewBoundaryService()
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl")))
	return NewService(topology, state, relief, boundaries, recorder), state, boundaries
}

func confirmedIncident() hydraulic.LeakIncident {
	return hydraulic.LeakIncident{ID: "inc-1", StationID: "binhe", Watermark: 7, Confirmed: true}
}

func stageState(t *testing.T, branch BranchProgress, stage Stage) StageState {
	t.Helper()
	for _, state := range branch.Stages {
		if state.Stage == stage {
			return state
		}
	}
	t.Fatalf("branch %s has no stage %s", branch.SectionID, stage)
	return StageState{}
}

func branch(t *testing.T, progress Progress, sectionID string) BranchProgress {
	t.Helper()
	for _, branch := range progress.Branches {
		if branch.SectionID == sectionID {
			return branch
		}
	}
	t.Fatalf("progress for zone %s has no branch %s", progress.ZoneID, sectionID)
	return BranchProgress{}
}

func TestIsolateZoneTracksBranchStages(t *testing.T) {
	service, state, _ := newTestService(t, time.Millisecond)
	supervisor := NewSupervisor(service, state.Partition, func() uint64 { return 1 })
	coordinator := supervisor.Current()

	progress, err := coordinator.IsolateZone(context.Background(), confirmedIncident(), "east", "leak-protection")
	if err != nil {
		t.Fatalf("isolate zone: %v", err)
	}
	if progress.Frozen {
		t.Fatal("coordinator must not be frozen")
	}
	if progress.Fence != (Fence{Partition: 1, Generation: 1}) {
		t.Fatalf("unexpected fence %+v", progress.Fence)
	}
	if progress.ZonePhase != network.PhaseIsolated {
		t.Fatalf("zone phase = %s, want isolated", progress.ZonePhase)
	}
	for _, sectionID := range []string{"east-branch", "east-loop"} {
		branch := branch(t, progress, sectionID)
		if branch.SessionID == "" {
			t.Fatalf("branch %s has no session", sectionID)
		}
		for _, stage := range stageSequence {
			if state := stageState(t, branch, stage); state.Status != StepCompleted {
				t.Fatalf("branch %s stage %s = %s, want completed", sectionID, stage, state.Status)
			}
		}
		if detail := stageState(t, branch, StageBoundary).Detail; detail != "2/2" {
			t.Fatalf("branch %s boundary detail = %q, want 2/2", sectionID, detail)
		}
	}
	zone, _ := state.Get("east")
	if zone.Phase != network.PhaseIsolated {
		t.Fatalf("zone phase = %s, want isolated", zone.Phase)
	}

	// A repeated trigger is idempotent: completed branches are skipped.
	if _, err := coordinator.IsolateZone(context.Background(), confirmedIncident(), "east", "leak-protection"); err != nil {
		t.Fatalf("repeated isolate zone: %v", err)
	}
}

func TestResumeZoneContinuesFromFailure(t *testing.T) {
	service, state, _ := newTestService(t, time.Millisecond)
	coordinator := NewCoordinator(service, Fence{Partition: 1, Generation: 1})

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	progress, err := coordinator.IsolateZone(canceled, confirmedIncident(), "east", "leak-protection")
	if err == nil {
		t.Fatal("expected isolation to fail with canceled context")
	}
	relief := stageState(t, branch(t, progress, "east-branch"), StageRelief)
	if relief.Status != StepFailed {
		t.Fatalf("relief stage = %s, want failed", relief.Status)
	}
	zone, _ := state.Get("east")
	if zone.Phase != network.PhaseIsolating {
		t.Fatalf("zone phase = %s, want isolating", zone.Phase)
	}

	progress, err = coordinator.ResumeZone(context.Background(), "east", "operator")
	if err != nil {
		t.Fatalf("resume zone: %v", err)
	}
	for _, sectionID := range []string{"east-branch", "east-loop"} {
		for _, stage := range stageSequence {
			if state := stageState(t, branch(t, progress, sectionID), stage); state.Status != StepCompleted {
				t.Fatalf("branch %s stage %s = %s after resume, want completed", sectionID, stage, state.Status)
			}
		}
	}
	zone, _ = state.Get("east")
	if zone.Phase != network.PhaseIsolated {
		t.Fatalf("zone phase = %s after resume, want isolated", zone.Phase)
	}
}

func TestResumeSessionSkipsProvenSteps(t *testing.T) {
	service, _, boundaries := newTestService(t, time.Millisecond)
	ctx := context.Background()

	session, err := service.Begin("east-branch", "test")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if session, err = service.StepRelief(ctx, session.ID); err != nil {
		t.Fatalf("relief: %v", err)
	}
	if session, err = service.StepBoundary(ctx, session.ID); err != nil {
		t.Fatalf("boundary: %v", err)
	}
	if len(session.Boundaries) != 2 {
		t.Fatalf("boundaries = %d, want 2", len(session.Boundaries))
	}
	reliefAt := session.Relief.At

	// Simulate a crash after the boundary stage: the session dies with all
	// relief and boundary receipts already collected.
	if _, err := service.fail(session, errors.New("boom")); err == nil {
		t.Fatal("expected fail to return an error")
	}
	resumed, err := service.ResumeSession(session.ID)
	if err != nil {
		t.Fatalf("resume session: %v", err)
	}
	if resumed.Stage != StageReceipt || resumed.Status != StatusConfirmingReceipts {
		t.Fatalf("resumed at %s/%s, want receipt stage", resumed.Stage, resumed.Status)
	}

	final, err := service.Run(ctx, session.ID)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", final.Status)
	}
	if !final.Relief.At.Equal(reliefAt) {
		t.Fatal("relief valve was re-opened instead of reusing the receipt")
	}
	if len(final.Boundaries) != 2 {
		t.Fatalf("boundaries = %d, want 2 (no duplicates)", len(final.Boundaries))
	}
	if commands := boundaries.Commands(); len(commands) != 2 {
		t.Fatalf("boundary close commands = %d, want 2 (no re-close)", len(commands))
	}
}

func TestSupervisorFreezesStaleCoordinator(t *testing.T) {
	service, state, _ := newTestService(t, time.Millisecond)
	generation := uint64(1)
	supervisor := NewSupervisor(service, state.Partition, func() uint64 { return generation })

	first := supervisor.Current()
	if again := supervisor.Current(); again != first {
		t.Fatal("unchanged fence must keep the same coordinator")
	}

	state.Repartition()
	second := supervisor.Current()
	if second == first {
		t.Fatal("partition change must rotate the coordinator")
	}
	if !first.Frozen() {
		t.Fatal("previous coordinator must be frozen after partition change")
	}
	if _, err := first.FromIncident(context.Background(), confirmedIncident(), "east-branch"); err == nil || !strings.Contains(err.Error(), "network partition changed 1 -> 2") {
		t.Fatalf("FromIncident on frozen coordinator = %v, want partition freeze reason", err)
	}
	if _, err := first.IsolateZone(context.Background(), confirmedIncident(), "east", "test"); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("IsolateZone on frozen coordinator = %v, want frozen error", err)
	}
	if _, err := first.Resume(context.Background(), "east-branch", "test"); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("Resume on frozen coordinator = %v, want frozen error", err)
	}

	if _, err := second.IsolateZone(context.Background(), confirmedIncident(), "east", "test"); err != nil {
		t.Fatalf("new coordinator must keep working: %v", err)
	}

	generation = 2
	third := supervisor.Current()
	if third == second || !second.Frozen() {
		t.Fatal("protection generation change must freeze the previous coordinator")
	}
	if _, err := second.ResumeZone(context.Background(), "east", "test"); err == nil || !strings.Contains(err.Error(), "protection generation changed 1 -> 2") {
		t.Fatalf("ResumeZone on frozen coordinator = %v, want generation freeze reason", err)
	}
}

func TestProgressReportsStageStates(t *testing.T) {
	service, _, _ := newTestService(t, time.Millisecond)
	coordinator := NewCoordinator(service, Fence{Partition: 1, Generation: 1})
	ctx := context.Background()

	session, err := service.Begin("east-branch", "test")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := service.StepRelief(ctx, session.ID); err != nil {
		t.Fatalf("relief: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := service.StepBoundary(canceled, session.ID); err == nil {
		t.Fatal("expected boundary step to fail with canceled context")
	}

	progress := coordinator.Progress("east")
	failed := branch(t, progress, "east-branch")
	if state := stageState(t, failed, StageRelief); state.Status != StepCompleted {
		t.Fatalf("relief = %s, want completed", state.Status)
	}
	boundary := stageState(t, failed, StageBoundary)
	if boundary.Status != StepFailed || boundary.Detail == "" {
		t.Fatalf("boundary = %s %q, want failed with error detail", boundary.Status, boundary.Detail)
	}
	if state := stageState(t, failed, StageReceipt); state.Status != StepPending {
		t.Fatalf("receipt = %s, want pending", state.Status)
	}
	if state := stageState(t, failed, StageFinal); state.Status != StepPending {
		t.Fatalf("final = %s, want pending", state.Status)
	}

	untouched := branch(t, progress, "east-loop")
	if untouched.SessionID != "" {
		t.Fatal("untouched branch must not reference a session")
	}
	for _, stage := range stageSequence {
		if state := stageState(t, untouched, stage); state.Status != StepPending {
			t.Fatalf("untouched branch stage %s = %s, want pending", stage, state.Status)
		}
	}
}

func TestIsolateOneCallCompat(t *testing.T) {
	service, state, _ := newTestService(t, time.Millisecond)
	session, err := service.Isolate(context.Background(), "east-branch", "test")
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if session.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", session.Status)
	}
	zone, _ := state.Get("east")
	if zone.Phase != network.PhaseIsolated {
		t.Fatalf("zone phase = %s, want isolated", zone.Phase)
	}
}

func TestFromIncidentRequiresConfirmation(t *testing.T) {
	service, _, _ := newTestService(t, time.Millisecond)
	coordinator := NewCoordinator(service, Fence{Partition: 1, Generation: 1})
	incident := confirmedIncident()
	incident.Confirmed = false
	if _, err := coordinator.FromIncident(context.Background(), incident, "east-branch"); err == nil {
		t.Fatal("unconfirmed incident must be rejected")
	}
}
