package ofo

import (
	"themisreference/pkg/types"
	"context"
	"testing"
)

func TestPaperAuditMissingReceiptChangesEdge(t *testing.T) {
	// Author's dissertation p.138: appearing alone counts as appearing first.
	a, b := testTx(2), testTx(1) // current ID tie-break deliberately favors B
	var orders []*types.LocalOrder
	for i := uint64(0); i < 9; i++ {
		ids := [][32]byte{a, b}
		if i >= 4 && i < 8 {
			ids = [][32]byte{b, a}
		}
		if i == 8 {
			ids = [][32]byte{a}
		}
		orders = append(orders, &types.LocalOrder{ReplicaID: i, OrderedTxs: ids})
	}
	dm := NewDependencyManager()
	dm.BuildGraphAndClassifyTxs(orders, 10, 1, .95, nil)
	if dm.txStates[a] != types.StateSolid || dm.txStates[b] != types.StateSolid {
		t.Fatal("bad fixture")
	}
	t.Logf("current weights A->B=%d B->A=%d; paper weights=5,4; edge A->B=%v B->A=%v", dm.weights[a][b], dm.weights[b][a], graphHasEdge(dm.graph, a, b), graphHasEdge(dm.graph, b, a))
	if !graphHasEdge(dm.graph, a, b) || graphHasEdge(dm.graph, b, a) {
		t.Fatal("paper expects A->B but implementation chooses B->A")
	}
}

func TestPaperAuditSolidVerticesMustHaveEdge(t *testing.T) {
	a, b := testTx(1), testTx(2)
	orders := []*types.LocalOrder{{ReplicaID: 0, OrderedTxs: [][32]byte{a, b}}, {ReplicaID: 1, OrderedTxs: [][32]byte{b, a}}, {ReplicaID: 2, OrderedTxs: [][32]byte{a}}, {ReplicaID: 3, OrderedTxs: [][32]byte{b}}}
	dm := NewDependencyManager()
	dm.BuildGraphAndClassifyTxs(orders, 5, 1, 1, nil)
	dm.CutShadedTail()
	t.Logf("solid A=%v solid B=%v nodes=%d tournament=%v", dm.txStates[a] == types.StateSolid, dm.txStates[b] == types.StateSolid, len(dm.graph.Nodes), IsTournament(dm.graph))
	if !IsTournament(dm.graph) {
		t.Fatal("two solid vertices have no edge; contradicts dependency graph lemma")
	}
}

func TestPaperAuditUpdateMissingReceiptChangesEdge(t *testing.T) {
	a, b := testTx(2), testTx(1)
	graph := &types.DependencyGraph{Nodes: map[[32]byte]bool{a: true, b: true}, Edges: map[[32]byte][][32]byte{}}
	var orders []*types.UpdateOrder
	for i := uint64(0); i < 9; i++ {
		ids := [][32]byte{a, b}
		if i >= 4 && i < 8 {
			ids = [][32]byte{b, a}
		}
		if i == 8 {
			ids = [][32]byte{a}
		}
		orders = append(orders, &types.UpdateOrder{ReplicaID: i, OrderedTxs: ids})
	}
	updates := FairUpdate(context.Background(), orders, graph, 10, 1, .95)
	t.Logf("current update A->B=%v B->A=%v", containsTx(updates[a], b), containsTx(updates[b], a))
	if !containsTx(updates[a], b) || containsTx(updates[b], a) {
		t.Fatal("FairUpdate also ignores the absent-second vote")
	}
}

func TestPaperPairWeightConservation(t *testing.T) {
	// Every replica that received at least one of a pair contributes exactly one
	// directional vote. Enumerate all four-replica receipt patterns, including
	// empty lists, for the paper's valid n=5, f=1, gamma=1 setting.
	a, b := testTx(1), testTx(2)
	patterns := [][][32]byte{nil, {a}, {b}, {a, b}, {b, a}}
	for sample := 0; sample < 625; sample++ {
		code, union := sample, 0
		var orders []*types.LocalOrder
		for replica := uint64(0); replica < 4; replica++ {
			ids := patterns[code%5]
			code /= 5
			if len(ids) > 0 {
				union++
			}
			orders = append(orders, localOrder(replica, ids...))
		}
		dm := NewDependencyManager()
		dm.BuildGraphAndClassifyTxs(orders, 5, 1, 1, nil)
		if dm.graph.Nodes[a] && dm.graph.Nodes[b] {
			if got := dm.weights[a][b] + dm.weights[b][a]; got != union {
				t.Fatalf("pattern %d: directional votes sum to %d, receipt union is %d", sample, got, union)
			}
		}
		if dm.txStates[a] == types.StateSolid && dm.txStates[b] == types.StateSolid && !IsTournament(dm.graph) {
			t.Fatalf("pattern %d: solid vertices must have an edge", sample)
		}
	}
}
