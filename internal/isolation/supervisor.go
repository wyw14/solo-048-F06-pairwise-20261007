package isolation

import (
	"fmt"
	"strings"
	"sync"
)

// Supervisor hands out the coordinator that matches the current network
// partition and protection generation. When either changes, the previous
// coordinator is frozen and a fresh one takes over; sessions started by the
// old coordinator stay resumable through the new one.
type Supervisor struct {
	service    *Service
	partition  func() uint64
	generation func() uint64
	mu         sync.Mutex
	current    *Coordinator
}

func NewSupervisor(service *Service, partition, generation func() uint64) *Supervisor {
	return &Supervisor{service: service, partition: partition, generation: generation}
}

func (s *Supervisor) Current() *Coordinator {
	s.mu.Lock()
	defer s.mu.Unlock()
	fence := Fence{Partition: s.partition(), Generation: s.generation()}
	if s.current == nil {
		s.current = NewCoordinator(s.service, fence)
		return s.current
	}
	if s.current.fence != fence {
		s.current.Freeze(fenceChangeReason(s.current.fence, fence))
		s.current = NewCoordinator(s.service, fence)
	}
	return s.current
}

func fenceChangeReason(previous, current Fence) string {
	reasons := make([]string, 0, 2)
	if previous.Partition != current.Partition {
		reasons = append(reasons, fmt.Sprintf("network partition changed %d -> %d", previous.Partition, current.Partition))
	}
	if previous.Generation != current.Generation {
		reasons = append(reasons, fmt.Sprintf("protection generation changed %d -> %d", previous.Generation, current.Generation))
	}
	return strings.Join(reasons, "; ")
}
