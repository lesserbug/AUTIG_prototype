package ofo

import "SpeedFair_simplify/pkg/types"

type savedNode struct {
	id          types.TxID
	present     bool
	kind        types.TxState
	kindPresent bool
}
type savedPair struct {
	pair                                             pairKey
	forward, reverse                                 int
	hasForward, hasReverse, edgeForward, edgeReverse bool
}

// Refresh a candidate using only a touched-entry undo log. The service lock
// excludes concurrent readers until rollback; a pending candidate never leaves
// the committed graph modified. Commit reapplies the retained update once.
func beginGraphTrial(dm *DependencyManager, state *EvidenceState, nodes map[types.TxID]bool, pairs map[pairKey]bool) func() {
	for id := range nodes {
		if state.states[id] != types.StateBlank && !dm.nodes[id] {
			for other := range state.Live {
				if other != id {
					pairs[makePair(id, other)] = true
				}
			}
		}
	}
	savedNodes := make([]savedNode, 0, len(nodes))
	for id := range nodes {
		kind, has := dm.txStates[id]
		savedNodes = append(savedNodes, savedNode{id: id, present: dm.nodes[id], kind: kind, kindPresent: has})
	}
	savedPairs := make([]savedPair, 0, len(pairs))
	for p := range pairs {
		uv, hasUV := dm.weights[edgeKey{p.a, p.b}]
		vu, hasVU := dm.weights[edgeKey{p.b, p.a}]
		savedPairs = append(savedPairs, savedPair{pair: p, forward: uv, reverse: vu, hasForward: hasUV, hasReverse: hasVU, edgeForward: dm.edges[p.a][p.b], edgeReverse: dm.edges[p.b][p.a]})
	}
	dm.refresh(state, nodes, pairs)
	return func() {
		for _, s := range savedPairs {
			u, v := s.pair.a, s.pair.b
			if s.hasForward {
				dm.weights[edgeKey{u, v}] = s.forward
			} else {
				delete(dm.weights, edgeKey{u, v})
			}
			if s.hasReverse {
				dm.weights[edgeKey{v, u}] = s.reverse
			} else {
				delete(dm.weights, edgeKey{v, u})
			}
			if dm.edges[u] != nil {
				if s.edgeForward {
					dm.edges[u][v] = true
				} else {
					delete(dm.edges[u], v)
				}
			}
			if dm.inverseEdges[v] != nil {
				if s.edgeForward {
					dm.inverseEdges[v][u] = true
				} else {
					delete(dm.inverseEdges[v], u)
				}
			}
			if dm.edges[v] != nil {
				if s.edgeReverse {
					dm.edges[v][u] = true
				} else {
					delete(dm.edges[v], u)
				}
			}
			if dm.inverseEdges[u] != nil {
				if s.edgeReverse {
					dm.inverseEdges[u][v] = true
				} else {
					delete(dm.inverseEdges[u], v)
				}
			}
		}
		for _, s := range savedNodes {
			if s.kindPresent {
				dm.txStates[s.id] = s.kind
			} else {
				delete(dm.txStates, s.id)
			}
			if !s.present {
				delete(dm.nodes, s.id)
				delete(dm.edges, s.id)
				delete(dm.inverseEdges, s.id)
			}
		}
	}
}

// Tarjan and condensation directly over a read-only adjacency view: no complete
// graph clone or adjacency conversion is hidden in the reference extractor.
func decomposeView(nodes map[types.TxID]bool, edges map[types.TxID]map[types.TxID]bool, states map[types.TxID]types.TxState) (map[int][]int, map[types.TxID]int, map[int]types.SCCInfo) {
	index := 0
	indices, low := make(map[types.TxID]int, len(nodes)), make(map[types.TxID]int, len(nodes))
	onStack := make(map[types.TxID]bool, len(nodes))
	stack := make([]types.TxID, 0, len(nodes))
	components := make([][]types.TxID, 0)
	var visit func(types.TxID)
	visit = func(u types.TxID) {
		index++
		indices[u], low[u] = index, index
		stack = append(stack, u)
		onStack[u] = true
		for v := range edges[u] {
			if !nodes[v] {
				continue
			}
			if indices[v] == 0 {
				visit(v)
				if low[v] < low[u] {
					low[u] = low[v]
				}
			} else if onStack[v] && indices[v] < low[u] {
				low[u] = indices[v]
			}
		}
		if low[u] == indices[u] {
			var c []types.TxID
			for {
				v := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[v] = false
				c = append(c, v)
				if v == u {
					break
				}
			}
			types.SortTxIDs(c)
			components = append(components, c)
		}
	}
	for u := range nodes {
		if indices[u] == 0 {
			visit(u)
		}
	}
	componentOf := make(map[types.TxID]int, len(nodes))
	infos := make(map[int]types.SCCInfo, len(components))
	cond := make(map[int][]int, len(components))
	for i, c := range components {
		solid := true
		for _, u := range c {
			componentOf[u] = i
			solid = solid && states[u] == types.StateSolid
		}
		infos[i] = types.SCCInfo{ID: i, Txs: c, IsSolid: solid}
		cond[i] = nil
	}
	seen := make(map[[2]int]bool)
	for u := range nodes {
		for v := range edges[u] {
			if !nodes[v] {
				continue
			}
			a, b := componentOf[u], componentOf[v]
			key := [2]int{a, b}
			if a != b && !seen[key] {
				seen[key] = true
				cond[a] = append(cond[a], b)
			}
		}
	}
	return cond, componentOf, infos
}

func rebuildMaterialization(state *EvidenceState, n, f uint64, gamma float64) *DependencyManager {
	dm := NewDependencyManager(n, f, gamma)
	for id, kind := range state.states {
		if !state.Live[id] {
			continue
		}
		dm.txStates[id] = kind
		if kind != types.StateBlank {
			dm.nodes[id] = true
			dm.edges[id] = make(map[types.TxID]bool)
			dm.inverseEdges[id] = make(map[types.TxID]bool)
		}
	}
	for key, weight := range state.weights {
		if weight != 0 {
			dm.weights[key] = weight
		}
	}
	ids := sortedSet(dm.nodes)
	for i, u := range ids {
		for _, v := range ids[i+1:] {
			if state.QueryCounts != nil {
				state.QueryCounts.RebuiltPairs++
			}
			uv, vu := state.weights[edgeKey{u, v}], state.weights[edgeKey{v, u}]
			if edgePred(u, v, uv, vu, dm.edgeThreshold) {
				dm.edges[u][v] = true
				dm.inverseEdges[v][u] = true
			} else if edgePred(v, u, vu, uv, dm.edgeThreshold) {
				dm.edges[v][u] = true
				dm.inverseEdges[u][v] = true
			}
		}
	}
	return dm
}
