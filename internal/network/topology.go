package network

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

type Branch struct {
	ID             string   `json:"id"`
	BoundaryValves []string `json:"boundary_valves"`
}

type Section struct {
	ID          string   `json:"id"`
	ZoneID      string   `json:"zone_id"`
	Upstream    string   `json:"upstream"`
	Downstream  string   `json:"downstream"`
	Branches    []Branch `json:"branches"`
	ReliefValve string   `json:"relief_valve"`
}

// BoundaryValves flattens the branch boundary valves in branch order.
func (s Section) BoundaryValves() []string {
	total := 0
	for _, branch := range s.Branches {
		total += len(branch.BoundaryValves)
	}
	valves := make([]string, 0, total)
	for _, branch := range s.Branches {
		valves = append(valves, branch.BoundaryValves...)
	}
	return valves
}

type Topology struct {
	sections map[string]Section
	byZone   map[string][]string
}

func NewTopology(sections []Section) *Topology {
	t := &Topology{sections: make(map[string]Section), byZone: make(map[string][]string)}
	for _, section := range sections {
		section.Branches = append([]Branch(nil), section.Branches...)
		for i := range section.Branches {
			section.Branches[i].BoundaryValves = append([]string(nil), section.Branches[i].BoundaryValves...)
		}
		t.sections[section.ID] = section
		t.byZone[section.ZoneID] = append(t.byZone[section.ZoneID], section.ID)
	}
	for zoneID := range t.byZone {
		sort.Strings(t.byZone[zoneID])
	}
	return t
}

func (t *Topology) Section(sectionID string) (Section, error) {
	section, ok := t.sections[sectionID]
	if !ok {
		return Section{}, fmt.Errorf("section %s not found", sectionID)
	}
	section.Branches = append([]Branch(nil), section.Branches...)
	for i := range section.Branches {
		section.Branches[i].BoundaryValves = append([]string(nil), section.Branches[i].BoundaryValves...)
	}
	return section, nil
}

func (t *Topology) SectionsForZone(zoneID string) []Section {
	ids := t.byZone[zoneID]
	result := make([]Section, 0, len(ids))
	for _, id := range ids {
		section, err := t.Section(id)
		if err == nil {
			result = append(result, section)
		}
	}
	return result
}

// Fingerprint identifies the current network partition: which sections
// exist, which zone they belong to, and how their branches and valves are
// laid out. Any repartition produces a different fingerprint.
func (t *Topology) Fingerprint() string {
	ids := make([]string, 0, len(t.sections))
	for id := range t.sections {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	hash := fnv.New64a()
	write := func(value string) {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	for _, id := range ids {
		section := t.sections[id]
		write(section.ID)
		write(section.ZoneID)
		write(section.Upstream)
		write(section.Downstream)
		write(section.ReliefValve)
		for _, branch := range section.Branches {
			write(branch.ID)
			for _, valveID := range branch.BoundaryValves {
				write(valveID)
			}
		}
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}

func (t *Topology) Validate() error {
	for _, section := range t.sections {
		if section.ZoneID == "" || section.Upstream == "" || section.Downstream == "" {
			return fmt.Errorf("section %s has incomplete topology", section.ID)
		}
		if len(section.Branches) == 0 || section.ReliefValve == "" {
			return fmt.Errorf("section %s lacks safe isolation devices", section.ID)
		}
		seen := make(map[string]bool)
		for _, branch := range section.Branches {
			if branch.ID == "" || len(branch.BoundaryValves) == 0 {
				return fmt.Errorf("section %s has a branch without boundary valves", section.ID)
			}
			for _, valveID := range branch.BoundaryValves {
				if seen[valveID] {
					return fmt.Errorf("section %s repeats boundary valve %s across branches", section.ID, valveID)
				}
				seen[valveID] = true
			}
		}
		if len(seen) < 2 {
			return fmt.Errorf("section %s lacks safe isolation devices", section.ID)
		}
	}
	return nil
}
