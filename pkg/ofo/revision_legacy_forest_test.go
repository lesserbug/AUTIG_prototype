package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
)

// Exact original verifier retained only as a timing comparator.
func revisionLegacyVerifyBlockForest(state *EvidenceState, active, included map[types.TxID]bool, records []types.BlockRecord, threshold int) error {
	recordByID := make(map[types.TxID]types.BlockRecord, len(records))
	for _, record := range records {
		if !active[record.Vertex] || included[record.Vertex] {
			return fmt.Errorf("block forest contains a vertex outside A\\F")
		}
		if _, duplicate := recordByID[record.Vertex]; duplicate {
			return fmt.Errorf("duplicate block forest record")
		}
		if !record.HasParent {
			if record.Depth != 0 || state.states[record.Vertex] != types.StateShaded {
				return fmt.Errorf("block forest root is not Shaded at depth zero")
			}
		} else if record.Depth == 0 {
			return fmt.Errorf("non-root block record has zero depth")
		}
		recordByID[record.Vertex] = record
	}
	for _, record := range records {
		if !record.HasParent {
			continue
		}
		parent, ok := recordByID[record.Parent]
		if !ok || record.Depth != parent.Depth+1 || !localEdgePred(state, record.Parent, record.Vertex, threshold) {
			return fmt.Errorf("invalid block forest parent edge or depth")
		}
	}
	used := make(map[types.TxID]bool)
	for id := range active {
		if included[id] || state.states[id] != types.StateSolid {
			continue
		}
		current := id
		for {
			record, ok := recordByID[current]
			if !ok {
				return fmt.Errorf("excluded Solid transaction has no blocker path")
			}
			used[current] = true
			if !record.HasParent {
				break
			}
			current = record.Parent
		}
	}
	if len(used) != len(records) {
		return fmt.Errorf("block forest contains records outside certified paths")
	}
	return nil
}
