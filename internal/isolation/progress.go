package isolation

import (
	"fmt"

	"heatpulse/internal/network"
)

// Stage identifies one phase of a branch isolation: 泄压 (relief),
// 边界阀 (boundary valves), 回执 (receipts) and 最终隔离 (final isolation).
type Stage string

const (
	StageRelief   Stage = "relief"
	StageBoundary Stage = "boundary"
	StageReceipt  Stage = "receipt"
	StageFinal    Stage = "final"
)

var stageSequence = []Stage{StageRelief, StageBoundary, StageReceipt, StageFinal}

type StepStatus string

const (
	StepPending    StepStatus = "pending"
	StepInProgress StepStatus = "in_progress"
	StepCompleted  StepStatus = "completed"
	StepFailed     StepStatus = "failed"
)

type StageState struct {
	Stage  Stage      `json:"stage"`
	Status StepStatus `json:"status"`
	Detail string     `json:"detail,omitempty"`
}

// BranchProgress is the per-topology-branch view: every stage of one section
// of the zone, derived from the section's latest session.
type BranchProgress struct {
	SectionID string       `json:"section_id"`
	SessionID string       `json:"session_id,omitempty"`
	Stages    []StageState `json:"stages"`
}

// Progress is the coordinator's view of one zone: its fence, whether the
// coordinator is frozen, and every branch's stage-by-stage progress.
type Progress struct {
	ZoneID    string           `json:"zone_id"`
	ZonePhase network.Phase    `json:"zone_phase"`
	Fence     Fence            `json:"fence"`
	Frozen    bool             `json:"frozen"`
	Branches  []BranchProgress `json:"branches"`
}

func stageIndex(stage Stage) int {
	for index, candidate := range stageSequence {
		if candidate == stage {
			return index
		}
	}
	return 0
}

func branchProgress(section network.Section, session *Session) BranchProgress {
	branch := BranchProgress{SectionID: section.ID, Stages: make([]StageState, 0, len(stageSequence))}
	if session != nil {
		branch.SessionID = session.ID
	}
	current := -1
	if session != nil {
		current = stageIndex(session.Stage)
	}
	for index, stage := range stageSequence {
		state := StageState{Stage: stage, Status: StepPending}
		switch {
		case session == nil:
		case session.Status == StatusCompleted:
			state.Status = StepCompleted
		case index < current:
			state.Status = StepCompleted
		case index == current:
			if session.Status == StatusFailed {
				state.Status = StepFailed
				state.Detail = session.Error
			} else {
				state.Status = StepInProgress
			}
		}
		if session != nil && stage == StageBoundary && state.Status != StepPending && state.Detail == "" {
			state.Detail = fmt.Sprintf("%d/%d", len(session.Boundaries), len(section.BoundaryValves))
		}
		branch.Stages = append(branch.Stages, state)
	}
	return branch
}
