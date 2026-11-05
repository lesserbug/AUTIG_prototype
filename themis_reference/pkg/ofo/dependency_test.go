package ofo

import (
	"themisreference/pkg/types"
	"context"
	"reflect"
	"testing"
)

func testTx(value byte) [32]byte {
	var id [32]byte
	id[31] = value
	return id
}

func localOrder(replica uint64, txs ...[32]byte) *types.LocalOrder {
	return &types.LocalOrder{ReplicaID: replica, OrderedTxs: txs}
}

func TestThemisClassificationThresholdBoundaries(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	solid, shaded, blank := testTx(1), testTx(2), testTx(3)
	orders := []*types.LocalOrder{
		localOrder(0, solid, shaded, blank),
		localOrder(1, solid, shaded, blank),
		localOrder(2, solid, shaded),
		localOrder(3, solid),
		localOrder(4, solid),
		localOrder(5),
	}
	dm := NewDependencyManager()
	if !dm.BuildGraphAndClassifyTxs(orders, n, f, gamma, nil) {
		t.Fatal("expected a non-empty dependency graph")
	}
	if got := edgeThreshold(n, f, gamma); got != 3 {
		t.Fatalf("edge/nonblank threshold: got %d, want 3", got)
	}
	if dm.txStates[solid] != types.StateSolid || dm.txStates[shaded] != types.StateShaded || dm.txStates[blank] != types.StateBlank {
		t.Fatalf("unexpected states: solid=%s shaded=%s blank=%s", dm.txStates[solid], dm.txStates[shaded], dm.txStates[blank])
	}
	if err := ValidateThemisParameters(5, 1, 0.9); err == nil {
		t.Fatal("strict resilience boundary n(2gamma-1)=4f must be rejected")
	}
	if err := ValidateThemisParameters(6, 1, 0.9); err != nil {
		t.Fatalf("valid parameters were rejected: %v", err)
	}
	if got := edgeThreshold(10, 1, 0.76); got != 5 {
		t.Fatalf("fractional threshold must round up: got %d, want 5", got)
	}
	if got := edgeThreshold(20, 1, 0.95); got != 3 {
		t.Fatalf("decimal boundary: got %d, want 3", got)
	}
	if err := ValidateThemisParameters(10, 1, 0.7); err == nil {
		t.Fatal("exact resilience equality must be rejected")
	}
}

func TestDependencyEdgeThresholdDirectionAndTie(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	u, v := testTx(1), testTx(2)
	orders := []*types.LocalOrder{
		localOrder(0, u, v), localOrder(1, u, v), localOrder(2, u, v),
		localOrder(3, v, u), localOrder(4, v, u), localOrder(5, u),
	}
	dm := NewDependencyManager()
	dm.BuildGraphAndClassifyTxs(orders, n, f, gamma, nil)
	if !graphHasEdge(dm.graph, u, v) || graphHasEdge(dm.graph, v, u) {
		t.Fatal("expected threshold-supported edge u -> v")
	}

	tied := []*types.LocalOrder{
		localOrder(0, u, v), localOrder(1, u, v), localOrder(2, u, v),
		localOrder(3, v, u), localOrder(4, v, u), localOrder(5, v, u),
	}
	dm = NewDependencyManager()
	dm.BuildGraphAndClassifyTxs(tied, n, f, gamma, nil)
	if !graphHasEdge(dm.graph, u, v) || graphHasEdge(dm.graph, v, u) {
		t.Fatal("equal weights must use the deterministic transaction-ID direction")
	}
}

func TestCondorcetCycleSCCAndHamiltonianOrder(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	a, b, c := testTx(1), testTx(2), testTx(3)
	orders := []*types.LocalOrder{
		localOrder(0, a, b, c), localOrder(1, a, b, c),
		localOrder(2, b, c, a), localOrder(3, b, c, a),
		localOrder(4, c, a, b), localOrder(5, c, a, b),
	}
	dm := NewDependencyManager()
	dm.BuildGraphAndClassifyTxs(orders, n, f, gamma, nil)
	dm.CutShadedTail()
	sccs := tarjanSCC(context.Background(), dm.graph)
	if len(sccs) != 1 || len(sccs[0]) != 3 {
		t.Fatalf("expected one three-transaction SCC, got %v", sccs)
	}
	order := dm.ComputeFairOrder()
	if len(order) != 3 {
		t.Fatalf("expected a three-transaction order, got %d", len(order))
	}
	for i := range order {
		if !graphHasEdge(dm.graph, order[i], order[(i+1)%len(order)]) {
			t.Fatalf("output is not a Hamiltonian cycle at index %d", i)
		}
	}
}

func TestCondensationOrder(t *testing.T) {
	a, b, c, d := testTx(1), testTx(2), testTx(3), testTx(4)
	graph := &types.DependencyGraph{
		Nodes: map[[32]byte]bool{a: true, b: true, c: true, d: true},
		Edges: map[[32]byte][][32]byte{
			a: {b, d}, b: {c, d}, c: {a, d},
		},
	}
	dm := NewDependencyManager()
	dm.graph = graph
	dm.txStates = map[[32]byte]types.TxState{a: types.StateSolid, b: types.StateSolid, c: types.StateSolid, d: types.StateSolid}
	sccs := tarjanSCC(context.Background(), graph)
	condensation, _, info := dm.buildCondensationAndSCCInfo(sccs)
	topo := topoSortCondensation(condensation)
	if len(topo) != 2 || len(info[topo[0]].Txs) != 3 || len(info[topo[1]].Txs) != 1 || info[topo[1]].Txs[0] != d {
		t.Fatalf("unexpected condensation order: topo=%v info=%v", topo, info)
	}
	order := dm.ComputeFairOrder()
	if len(order) != 4 || order[3] != d {
		t.Fatalf("condensation order was not preserved: %v", order)
	}
}

func TestCutShadedTailUsesPathToSolid(t *testing.T) {
	a, b, solid := testTx(1), testTx(2), testTx(3)
	dm := NewDependencyManager()
	dm.graph = &types.DependencyGraph{
		Nodes: map[[32]byte]bool{a: true, b: true, solid: true},
		Edges: map[[32]byte][][32]byte{a: {solid}},
	}
	dm.txStates = map[[32]byte]types.TxState{
		a: types.StateShaded, b: types.StateShaded, solid: types.StateSolid,
	}
	dm.CutShadedTail()
	if !dm.graph.Nodes[a] || !dm.graph.Nodes[solid] || dm.graph.Nodes[b] {
		t.Fatalf("expected only transactions with a path to solid to remain: %v", dm.graph.Nodes)
	}
}

func TestFairUpdatePresenceThresholdAndDirection(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	u, v := testTx(1), testTx(2)
	graph := &types.DependencyGraph{Nodes: map[[32]byte]bool{u: true, v: true}, Edges: make(map[[32]byte][][32]byte)}
	orders := []*types.UpdateOrder{
		{ReplicaID: 0, OrderedTxs: [][32]byte{u, v}},
		{ReplicaID: 1, OrderedTxs: [][32]byte{u, v}},
		{ReplicaID: 2, OrderedTxs: [][32]byte{u, v}},
		{ReplicaID: 3, OrderedTxs: [][32]byte{v, u}},
		{ReplicaID: 4, OrderedTxs: [][32]byte{v, u}},
		{ReplicaID: 5, OrderedTxs: [][32]byte{u}},
	}
	edges := FairUpdate(context.Background(), orders, graph, n, f, gamma)
	if !containsTx(edges[u], v) || containsTx(edges[v], u) {
		t.Fatal("expected update edge u -> v")
	}

	for i := 3; i < 6; i++ {
		orders[i].OrderedTxs = nil
	}
	if edges = FairUpdate(context.Background(), orders, graph, n, f, gamma); len(edges) != 0 {
		t.Fatalf("a source below n-2f update presence must not create an edge: %v", edges)
	}
}

func TestDuplicateLocalOrderDoesNotAddWeight(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	u, v := testTx(1), testTx(2)
	orders := []*types.LocalOrder{
		localOrder(0, u, v), localOrder(0, u, v), localOrder(1, u, v),
		localOrder(2), localOrder(3), localOrder(4), localOrder(5),
	}
	dm := NewDependencyManager()
	if dm.BuildGraphAndClassifyTxs(orders, n, f, gamma, nil) {
		t.Fatal("duplicate sender must not lift two appearances to the nonblank threshold")
	}
}

func TestDuplicateTransactionDoesNotAddWeight(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	u, v := testTx(1), testTx(2)
	orders := []*types.LocalOrder{
		localOrder(0, u, v, u, v), localOrder(1, u, v),
		localOrder(2), localOrder(3), localOrder(4), localOrder(5),
	}
	dm := NewDependencyManager()
	if dm.BuildGraphAndClassifyTxs(orders, n, f, gamma, nil) {
		t.Fatal("duplicate transactions inside one LocalOrder must not lift presence or edge weight")
	}
}

func TestHamiltonianCycleForStrongTournaments(t *testing.T) {
	const size = 6
	vertices := make([][32]byte, size)
	for i := range vertices {
		vertices[i] = testTx(byte(i + 1))
	}
	pairs := make([][2]int, 0, size*(size-1)/2)
	for i := 0; i < size; i++ {
		for j := i + 1; j < size; j++ {
			pairs = append(pairs, [2]int{i, j})
		}
	}
	for mask := 0; mask < 1<<len(pairs); mask++ {
		graph := &types.DependencyGraph{Nodes: make(map[[32]byte]bool), Edges: make(map[[32]byte][][32]byte)}
		for _, vertex := range vertices {
			graph.Nodes[vertex] = true
		}
		for bit, pair := range pairs {
			if mask&(1<<bit) == 0 {
				addEdge(graph, vertices[pair[0]], vertices[pair[1]])
			} else {
				addEdge(graph, vertices[pair[1]], vertices[pair[0]])
			}
		}
		if sccs := tarjanSCC(context.Background(), graph); len(sccs) != 1 {
			continue
		}
		first := hamiltonianCycle(vertices, graph)
		second := hamiltonianCycle(vertices, graph)
		if len(first) != size || !reflect.DeepEqual(first, second) {
			t.Fatalf("failed deterministic Hamiltonian cycle for tournament mask %d: %v / %v", mask, first, second)
		}
	}

	const largeSize = 40
	largeVertices := make([][32]byte, largeSize)
	for i := range largeVertices {
		largeVertices[i] = testTx(byte(i + 1))
	}
	state := uint64(1)
	for sample := 0; sample < 100; sample++ {
		graph := &types.DependencyGraph{Nodes: make(map[[32]byte]bool), Edges: make(map[[32]byte][][32]byte)}
		for _, vertex := range largeVertices {
			graph.Nodes[vertex] = true
		}
		for i := 0; i < largeSize; i++ {
			for j := i + 1; j < largeSize; j++ {
				state = state*6364136223846793005 + 1
				if state>>63 == 0 {
					addEdge(graph, largeVertices[i], largeVertices[j])
				} else {
					addEdge(graph, largeVertices[j], largeVertices[i])
				}
			}
		}
		if len(tarjanSCC(context.Background(), graph)) != 1 {
			continue
		}
		if cycle := hamiltonianCycle(largeVertices, graph); len(cycle) != largeSize {
			t.Fatalf("failed Hamiltonian cycle for large tournament sample %d", sample)
		}
	}
}
