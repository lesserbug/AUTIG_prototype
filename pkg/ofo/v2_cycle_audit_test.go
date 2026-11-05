package ofo

import "SpeedFair_simplify/pkg/types"

type v2CycleAuditResult struct {
	Threshold, Components, MixedContinuationContexts int
	CrossComponentPairs, PhysicalInversions          int64
	FirstInversion                                   []int `json:",omitempty"`
}

// Independent Kosaraju over original correct receipt profiles. This oracle
// uses no cached weight, SCC implementation, witness, or claimed batch graph.
func auditV2InputCycles(s *OFOService, txs []types.Transaction, positions [][]int, correct, qh int) *v2CycleAuditResult {
	count := len(txs)
	result := &v2CycleAuditResult{Threshold: correct - qh + 1}
	adj, reverse := make([][]int, count), make([][]int, count)
	for x := 0; x < count; x++ {
		for y := 0; y < count; y++ {
			if x == y {
				continue
			}
			support := 0
			for r := 0; r < correct; r++ {
				if positions[r][x] < positions[r][y] {
					support++
				}
			}
			if support >= result.Threshold {
				adj[x] = append(adj[x], y)
				reverse[y] = append(reverse[y], x)
			}
		}
	}
	seen := make([]bool, count)
	order := make([]int, 0, count)
	var first func(int)
	first = func(u int) {
		seen[u] = true
		for _, v := range adj[u] {
			if !seen[v] {
				first(v)
			}
		}
		order = append(order, u)
	}
	for u := 0; u < count; u++ {
		if !seen[u] {
			first(u)
		}
	}
	comp := make([]int, count)
	for i := range comp {
		comp[i] = -1
	}
	var second func(int, int)
	second = func(u, c int) {
		comp[u] = c
		for _, v := range reverse[u] {
			if comp[v] < 0 {
				second(v, c)
			}
		}
	}
	for i := len(order) - 1; i >= 0; i-- {
		if comp[order[i]] < 0 {
			second(order[i], result.Components)
			result.Components++
		}
	}
	records := make([]DoneRecord, count)
	contexts := make(map[uint64]map[int]bool)
	for i, tx := range txs {
		r, ok := s.committed.doneLookup(tx.ID)
		if !ok {
			panic("cycle audit requires full settlement")
		}
		records[i] = r
		if contexts[r.BatchIndex] == nil {
			contexts[r.BatchIndex] = make(map[int]bool)
		}
		contexts[r.BatchIndex][comp[i]] = true
	}
	for _, components := range contexts {
		if len(components) > 1 {
			result.MixedContinuationContexts++
		}
	}
	for x := 0; x < count; x++ {
		for _, y := range adj[x] {
			if comp[x] == comp[y] {
				continue
			}
			result.CrossComponentPairs++
			a, b := records[x], records[y]
			if a.ExecutionSeq > b.ExecutionSeq || (a.ExecutionSeq == b.ExecutionSeq && a.ExecutionOrdinal >= b.ExecutionOrdinal) {
				result.PhysicalInversions++
				if result.FirstInversion == nil {
					result.FirstInversion = []int{x, y}
				}
			}
		}
	}
	return result
}
