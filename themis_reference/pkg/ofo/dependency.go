// pkg/ofo/dependency.go
package ofo

import (
	"themisreference/pkg/types"
	"bytes"
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
)

type WeightMatrix map[[32]byte]map[[32]byte]int

// DependencyManager 封装了Themis的依赖图计算逻辑
type DependencyManager struct {
	ctx      context.Context
	graph    *types.DependencyGraph
	txStates map[[32]byte]types.TxState
	weights  WeightMatrix
}

func NewDependencyManager() *DependencyManager {
	return &DependencyManager{
		ctx: context.Background(),
		graph: &types.DependencyGraph{
			Nodes: make(map[[32]byte]bool),
			Edges: make(map[[32]byte][][32]byte),
		},
		txStates: make(map[[32]byte]types.TxState),
		weights:  make(WeightMatrix),
	}
}

func addEdge(g *types.DependencyGraph, u, v [32]byte) {
	for _, existing := range g.Edges[u] {
		if existing == v {
			return
		}
	}
	g.Edges[u] = append(g.Edges[u], v)
}

func ValidateThemisParameters(n, f uint64, gamma float64) error {
	if n == 0 || f >= n {
		return fmt.Errorf("Themis requires 0 <= f < n")
	}
	if math.IsNaN(gamma) || gamma <= 0.5 || gamma > 1 {
		return fmt.Errorf("Themis requires gamma_all in (1/2,1]")
	}
	// Interpret the configured decimal exactly, including resilience boundaries.
	g, _ := new(big.Rat).SetString(strconv.FormatFloat(gamma, 'g', -1, 64))
	lhs := new(big.Rat).Sub(new(big.Rat).Mul(g, big.NewRat(2, 1)), big.NewRat(1, 1))
	lhs.Mul(lhs, new(big.Rat).SetInt(new(big.Int).SetUint64(n)))
	rhs := new(big.Rat).Mul(new(big.Rat).SetInt(new(big.Int).SetUint64(f)), big.NewRat(4, 1))
	if lhs.Cmp(rhs) <= 0 {
		return fmt.Errorf("Themis requires n > 4f/(2*gamma_all-1)")
	}
	return nil
}

func edgeThreshold(n, f uint64, gamma float64) int {
	// ceil(n*(1-gamma)+f+1) = n+f+1-floor(n*gamma).
	g, _ := new(big.Rat).SetString(strconv.FormatFloat(gamma, 'g', -1, 64))
	g.Mul(g, new(big.Rat).SetInt(new(big.Int).SetUint64(n)))
	floor := new(big.Int).Quo(g.Num(), g.Denom()).Uint64()
	return int(n + f + 1 - floor)
}

// BuildGraphAndClassifyTxs (FairPropose) 从本地排序构建新交易的依赖图
func (dm *DependencyManager) BuildGraphAndClassifyTxs(
	orders []*types.LocalOrder,
	n, f uint64,
	gamma float64,
	committedTxIDs map[[32]byte]bool,
) bool {
	if dm.ctx.Err() != nil {
		return false
	}
	uniqueTxsInOrders := make(map[[32]byte]bool)
	filteredOrders := make([]*types.LocalOrder, 0, len(orders))
	seenSenders := make(map[uint64]bool)
	for _, order := range orders {
		if dm.ctx.Err() != nil {
			return false
		}
		if order == nil || seenSenders[order.ReplicaID] {
			continue
		}
		seenSenders[order.ReplicaID] = true
		pendingTxs := make([][32]byte, 0, len(order.OrderedTxs))
		seenTxs := make(map[[32]byte]bool)
		for _, txID := range order.OrderedTxs {
			if !committedTxIDs[txID] && !seenTxs[txID] {
				pendingTxs = append(pendingTxs, txID)
				uniqueTxsInOrders[txID] = true
				seenTxs[txID] = true
			}
		}
		filteredOrders = append(filteredOrders, &types.LocalOrder{ReplicaID: order.ReplicaID, OrderedTxs: pendingTxs})
	}

	if len(uniqueTxsInOrders) == 0 {
		return false
	}

	threshold := edgeThreshold(n, f, gamma)

	solidThreshold := int(n - 2*f)
	txCounts := make(map[[32]byte]int)
	for _, order := range filteredOrders {
		seenInOrder := make(map[[32]byte]bool)
		for _, tx := range order.OrderedTxs {
			if !seenInOrder[tx] {
				txCounts[tx]++
				seenInOrder[tx] = true
			}
		}
	}

	dm.graph.Nodes = make(map[[32]byte]bool)
	dm.txStates = make(map[[32]byte]types.TxState)
	for tx := range uniqueTxsInOrders {
		count := txCounts[tx]
		state := types.StateBlank
		if count >= threshold {
			if count >= solidThreshold {
				state = types.StateSolid
			} else {
				state = types.StateShaded
			}
		}
		dm.txStates[tx] = state
		if state != types.StateBlank {
			dm.graph.Nodes[tx] = true
		}
	}

	if len(dm.graph.Nodes) == 0 {
		return false
	}

	dm.weights = make(WeightMatrix)
	for tx1 := range dm.graph.Nodes {
		dm.weights[tx1] = make(map[[32]byte]int)
	}
	for _, order := range filteredOrders {
		txList := order.OrderedTxs
		present := make(map[[32]byte]bool, len(txList))
		for _, tx := range txList {
			present[tx] = true
		}
		var absent [][32]byte
		for tx := range dm.graph.Nodes {
			if !present[tx] {
				absent = append(absent, tx)
			}
		}
		for i := 0; i < len(txList); i++ {
			if dm.ctx.Err() != nil {
				return false
			}
			tx1 := txList[i]
			if _, ok := dm.graph.Nodes[tx1]; !ok {
				continue
			}
			for j := i + 1; j < len(txList); j++ {
				tx2 := txList[j]
				if _, ok := dm.graph.Nodes[tx2]; !ok {
					continue
				}
				dm.weights[tx1][tx2]++
			}
			// Themis Weight includes a receipt of tx1 when tx2 is absent.
			// See Kelkar's dissertation, Ch. 6, "Replica ordering notation".
			for _, tx2 := range absent {
				dm.weights[tx1][tx2]++
			}
		}
	}

	dm.graph.Edges = make(map[[32]byte][][32]byte)
	sortedNodes := make([][32]byte, 0, len(dm.graph.Nodes))
	for node := range dm.graph.Nodes {
		sortedNodes = append(sortedNodes, node)
	}
	sort.Slice(sortedNodes, func(i, j int) bool {
		return bytes.Compare(sortedNodes[i][:], sortedNodes[j][:]) < 0
	})

	for i := 0; i < len(sortedNodes); i++ {
		u := sortedNodes[i]
		for j := i + 1; j < len(sortedNodes); j++ {
			if dm.ctx.Err() != nil {
				return false
			}
			v := sortedNodes[j]
			wUV := dm.weights[u][v]
			wVU := dm.weights[v][u]

			if wUV >= threshold && wVU < threshold {
				addEdge(dm.graph, u, v)
			} else if wVU >= threshold && wUV < threshold {
				addEdge(dm.graph, v, u)
			} else if wUV >= threshold && wVU >= threshold {
				if wUV > wVU {
					addEdge(dm.graph, u, v)
				} else if wVU > wUV {
					addEdge(dm.graph, v, u)
				} else if bytes.Compare(u[:], v[:]) < 0 {
					addEdge(dm.graph, u, v)
				} else {
					addEdge(dm.graph, v, u)
				}
			}
		}
	}
	return true
}

func (dm *DependencyManager) CutShadedTail() {
	if dm.ctx.Err() != nil || len(dm.graph.Nodes) == 0 {
		return
	}

	// Build the condensation graph and locate every solid SCC.
	sccsData := tarjanSCC(dm.ctx, dm.graph)
	if dm.ctx.Err() != nil {
		return
	}
	condensationGraph, _, sccInfos := dm.buildCondensationAndSCCInfo(sccsData)
	if dm.ctx.Err() != nil {
		return
	}
	reverseCondensation := make(map[int][]int, len(condensationGraph))
	keepSCC := make(map[int]bool)
	queue := make([]int, 0)
	for from, targets := range condensationGraph {
		if dm.ctx.Err() != nil {
			return
		}
		for _, to := range targets {
			reverseCondensation[to] = append(reverseCondensation[to], from)
		}
		if sccInfos[from].IsSolid {
			keepSCC[from] = true
			queue = append(queue, from)
		}
	}

	// FairPropose outputs exactly the shaded transactions with a path to a
	// solid transaction. With no solid transaction, that set is empty.
	if len(queue) == 0 {
		dm.graph.Nodes = make(map[[32]byte]bool)
		dm.graph.Edges = make(map[[32]byte][][32]byte)
		dm.txStates = make(map[[32]byte]types.TxState)
		return
	}

	// Reverse reachability is required because a topological ordering of the
	// condensation graph need not be a total order.
	for len(queue) > 0 {
		if dm.ctx.Err() != nil {
			return
		}
		to := queue[0]
		queue = queue[1:]
		for _, from := range reverseCondensation[to] {
			if !keepSCC[from] {
				keepSCC[from] = true
				queue = append(queue, from)
			}
		}
	}

	nodesToKeep := make(map[[32]byte]bool)
	for sccID := range keepSCC {
		for _, tx := range sccInfos[sccID].Txs {
			nodesToKeep[tx] = true
		}
	}

	// Prune the graph: remove nodes and their associated edges.
	newGraphNodes := make(map[[32]byte]bool)
	newGraphEdges := make(map[[32]byte][][32]byte)
	newTxStates := make(map[[32]byte]types.TxState)

	for node := range dm.graph.Nodes {
		if dm.ctx.Err() != nil {
			return
		}
		if nodesToKeep[node] {
			newGraphNodes[node] = true
			newTxStates[node] = dm.txStates[node]

			// Keep edges where both source and destination are kept
			var newEdgesForNode [][32]byte
			if originalEdges, ok := dm.graph.Edges[node]; ok {
				for _, neighbor := range originalEdges {
					if nodesToKeep[neighbor] {
						newEdgesForNode = append(newEdgesForNode, neighbor)
					}
				}
			}
			if len(newEdgesForNode) > 0 {
				newGraphEdges[node] = newEdgesForNode
			}
		}
	}

	dm.graph.Nodes = newGraphNodes
	dm.graph.Edges = newGraphEdges
	dm.txStates = newTxStates
}

func FairUpdate(
	ctx context.Context,
	updateOrders []*types.UpdateOrder,
	deferredGraph *types.DependencyGraph,
	n, f uint64,
	gamma float64,
) map[[32]byte][][32]byte {
	if ctx.Err() != nil || deferredGraph == nil {
		return nil
	}
	threshold := edgeThreshold(n, f, gamma)
	solidThreshold := int(n - 2*f)

	txsInGraph := make([][32]byte, 0, len(deferredGraph.Nodes))
	for tx := range deferredGraph.Nodes {
		txsInGraph = append(txsInGraph, tx)
	}
	sort.Slice(txsInGraph, func(i, j int) bool { return bytes.Compare(txsInGraph[i][:], txsInGraph[j][:]) < 0 })

	pairsToUpdate := make(map[[32]byte]map[[32]byte]bool)
	for i := 0; i < len(txsInGraph); i++ {
		u := txsInGraph[i]
		for j := i + 1; j < len(txsInGraph); j++ {
			if ctx.Err() != nil {
				return nil
			}
			v := txsInGraph[j]
			uHasV, vHasU := false, false
			if edges, ok := deferredGraph.Edges[u]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], v[:]) {
						uHasV = true
						break
					}
				}
			}
			if edges, ok := deferredGraph.Edges[v]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], u[:]) {
						vHasU = true
						break
					}
				}
			}
			if !(uHasV || vHasU) {
				if pairsToUpdate[u] == nil {
					pairsToUpdate[u] = make(map[[32]byte]bool)
				}
				pairsToUpdate[u][v] = true
			}
		}
	}

	if len(pairsToUpdate) == 0 {
		return nil
	}

	weights := make(WeightMatrix)
	presence := make(map[[32]byte]int)
	seenSenders := make(map[uint64]bool)
	for _, order := range updateOrders {
		if order == nil || seenSenders[order.ReplicaID] {
			continue
		}
		seenSenders[order.ReplicaID] = true
		txList := order.OrderedTxs
		txPos := make(map[[32]byte]int, len(txList))
		for i, tx := range txList {
			if _, exists := txPos[tx]; !exists {
				txPos[tx] = i
				if deferredGraph.Nodes[tx] {
					presence[tx]++
				}
			}
		}
		for u, vMap := range pairsToUpdate {
			if ctx.Err() != nil {
				return nil
			}
			for v := range vMap {
				posU, okU := txPos[u]
				posV, okV := txPos[v]
				// A present transaction precedes an absent one; if both are
				// absent, this replica contributes no vote in either direction.
				if okU && (!okV || posU < posV) {
					if weights[u] == nil {
						weights[u] = make(map[[32]byte]int)
					}
					weights[u][v]++
				} else if okV {
					if weights[v] == nil {
						weights[v] = make(map[[32]byte]int)
					}
					weights[v][u]++
				}
			}
		}
	}

	newEdges := make(map[[32]byte][][32]byte)
	for u, vMap := range pairsToUpdate {
		if ctx.Err() != nil {
			return nil
		}
		for v := range vMap {
			wUV := weights[u][v]
			wVU := weights[v][u]
			uvEligible := presence[u] >= solidThreshold && wUV >= threshold && wUV >= wVU
			vuEligible := presence[v] >= solidThreshold && wVU >= threshold && wVU >= wUV
			if uvEligible && !vuEligible {
				newEdges[u] = append(newEdges[u], v)
			} else if vuEligible && !uvEligible {
				newEdges[v] = append(newEdges[v], u)
			} else if uvEligible && vuEligible {
				if bytes.Compare(u[:], v[:]) < 0 {
					newEdges[u] = append(newEdges[u], v)
				} else {
					newEdges[v] = append(newEdges[v], u)
				}
			}
		}
	}
	for source := range newEdges {
		sort.Slice(newEdges[source], func(i, j int) bool {
			return bytes.Compare(newEdges[source][i][:], newEdges[source][j][:]) < 0
		})
	}
	return newEdges
}

func IsTournament(g *types.DependencyGraph) bool {
	if g == nil || len(g.Nodes) <= 1 {
		return true
	}
	nodes := make([][32]byte, 0, len(g.Nodes))
	for node := range g.Nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return bytes.Compare(nodes[i][:], nodes[j][:]) < 0 })

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			u, v := nodes[i], nodes[j]
			edgeUV, edgeVU := false, false
			if edges, ok := g.Edges[u]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], v[:]) {
						edgeUV = true
						break
					}
				}
			}
			if edges, ok := g.Edges[v]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], u[:]) {
						edgeVU = true
						break
					}
				}
			}
			if !(edgeUV != edgeVU) {
				return false
			}
		}
	}
	return true
}

func (dm *DependencyManager) ComputeFairOrder() [][32]byte {
	if !IsTournament(dm.graph) {
		return nil
	}

	if len(dm.graph.Nodes) == 0 {
		return nil
	}

	sccsData := tarjanSCC(dm.ctx, dm.graph)
	condensationGraph, _, sccInfos := dm.buildCondensationAndSCCInfo(sccsData)
	sortedSCCIndices := topoSortCondensation(condensationGraph)

	var finalOrder [][32]byte
	for position, sccIdx := range sortedSCCIndices {
		sccInfo := sccInfos[sccIdx]
		componentTxs := sccInfo.Txs
		if len(componentTxs) == 0 {
			continue
		}
		sort.Slice(componentTxs, func(i, j int) bool {
			return bytes.Compare(componentTxs[i][:], componentTxs[j][:]) < 0
		})
		cycle := hamiltonianCycle(componentTxs, dm.graph)
		if len(cycle) != len(componentTxs) {
			return nil
		}
		if position == len(sortedSCCIndices)-1 {
			var solid [32]byte
			foundSolid := false
			for _, tx := range componentTxs {
				if dm.txStates[tx] == types.StateSolid && (!foundSolid || bytes.Compare(tx[:], solid[:]) < 0) {
					solid = tx
					foundSolid = true
				}
			}
			if !foundSolid {
				return nil
			}
			cycle = rotateCycleLast(cycle, solid)
		}
		finalOrder = append(finalOrder, cycle...)
	}
	return finalOrder
}

func hamiltonianCycle(vertices [][32]byte, graph *types.DependencyGraph) [][32]byte {
	if len(vertices) <= 1 {
		return append([][32]byte(nil), vertices...)
	}
	if len(vertices) == 2 {
		return nil
	}

	vertices = append([][32]byte(nil), vertices...)
	sort.Slice(vertices, func(i, j int) bool { return bytes.Compare(vertices[i][:], vertices[j][:]) < 0 })

	// Deterministically construct a Hamiltonian path by insertion.  Every
	// consecutive pair in path is an edge in the tournament.
	path := make([][32]byte, 0, len(vertices))
	for _, tx := range vertices {
		insertAt := len(path)
		for i, member := range path {
			if graphHasEdge(graph, tx, member) {
				insertAt = i
				break
			}
		}
		path = append(path, [32]byte{})
		copy(path[insertAt+1:], path[insertAt:])
		path[insertAt] = tx
	}

	// The maximal prefix whose last vertex points back to the first is a
	// directed cycle.  Such a prefix exists in a strongly connected
	// tournament.
	cycleEnd := -1
	for i := 2; i < len(path); i++ {
		if graphHasEdge(graph, path[i], path[0]) {
			cycleEnd = i
		}
	}
	if cycleEnd < 0 {
		return nil
	}
	cycle := append([][32]byte(nil), path[:cycleEnd+1]...)
	remaining := append([][32]byte(nil), path[cycleEnd+1:]...)

	for len(remaining) > 0 {
		x := remaining[0]
		insertAt := -1
		for i := range cycle {
			next := (i + 1) % len(cycle)
			if graphHasEdge(graph, cycle[i], x) && graphHasEdge(graph, x, cycle[next]) {
				insertAt = i + 1
				break
			}
		}
		if insertAt >= 0 {
			cycle = append(cycle, [32]byte{})
			copy(cycle[insertAt+1:], cycle[insertAt:])
			cycle[insertAt] = x
			remaining = remaining[1:]
			continue
		}

		// The first remaining vertex is reached from the cycle (it follows a
		// cycle member in the Hamiltonian path).  If it cannot be inserted at
		// a cycle gap, every cycle member points to it.  Strong connectivity
		// therefore guarantees a later path vertex with an edge back into the
		// cycle; splice that entire path segment into the cycle.
		for _, member := range cycle {
			if !graphHasEdge(graph, member, x) {
				return nil
			}
		}
		pathEnd, cycleTarget := -1, -1
		for j, tx := range remaining {
			for i, member := range cycle {
				if graphHasEdge(graph, tx, member) {
					pathEnd, cycleTarget = j, i
					break
				}
			}
			if pathEnd >= 0 {
				break
			}
		}
		if pathEnd < 0 {
			return nil
		}
		spliced := make([][32]byte, 0, len(cycle)+pathEnd+1)
		spliced = append(spliced, cycle[:cycleTarget]...)
		spliced = append(spliced, remaining[:pathEnd+1]...)
		spliced = append(spliced, cycle[cycleTarget:]...)
		cycle = spliced
		remaining = remaining[pathEnd+1:]
	}

	for i := range cycle {
		if !graphHasEdge(graph, cycle[i], cycle[(i+1)%len(cycle)]) {
			return nil
		}
	}
	return cycle
}

func rotateCycleLast(cycle [][32]byte, target [32]byte) [][32]byte {
	for i, tx := range cycle {
		if tx == target {
			rotated := append([][32]byte(nil), cycle[i+1:]...)
			rotated = append(rotated, cycle[:i+1]...)
			return rotated
		}
	}
	return nil
}

func graphHasEdge(graph *types.DependencyGraph, from, to [32]byte) bool {
	for _, target := range graph.Edges[from] {
		if target == to {
			return true
		}
	}
	return false
}

func (dm *DependencyManager) buildCondensationAndSCCInfo(sccsData [][][32]byte) (map[int][]int, map[[32]byte]int, map[int]types.SCCInfo) {
	nodeToSCCIndex := make(map[[32]byte]int)
	sccInfos := make(map[int]types.SCCInfo)
	for i, compTxs := range sccsData {
		isSolidSCC := false
		for _, node := range compTxs {
			nodeToSCCIndex[node] = i
			if dm.txStates[node] == types.StateSolid {
				isSolidSCC = true
			}
		}
		sccInfos[i] = types.SCCInfo{ID: i, Txs: compTxs, IsSolid: isSolidSCC}
	}

	condensationGraph := make(map[int][]int)
	for i := range sccsData {
		condensationGraph[i] = []int{}
	}

	for u, neighbors := range dm.graph.Edges {
		if dm.ctx.Err() != nil {
			return nil, nil, nil
		}
		sccUIndex := nodeToSCCIndex[u]
		for _, v := range neighbors {
			if dm.ctx.Err() != nil {
				return nil, nil, nil
			}
			sccVIndex := nodeToSCCIndex[v]
			if sccUIndex != sccVIndex {
				exists := false
				for _, existingNeighbor := range condensationGraph[sccUIndex] {
					if existingNeighbor == sccVIndex {
						exists = true
						break
					}
				}
				if !exists {
					condensationGraph[sccUIndex] = append(condensationGraph[sccUIndex], sccVIndex)
				}
			}
		}
	}
	return condensationGraph, nodeToSCCIndex, sccInfos
}

func tarjanSCC(ctx context.Context, graph *types.DependencyGraph) [][][32]byte {
	index, indices, lowlink := 0, make(map[[32]byte]int), make(map[[32]byte]int)
	var stack [][32]byte
	onStack := make(map[[32]byte]bool)
	var sccs [][][32]byte

	var strongConnect func(v [32]byte)
	strongConnect = func(v [32]byte) {
		if ctx.Err() != nil {
			return
		}
		indices[v], lowlink[v] = index, index
		index++
		stack = append(stack, v)
		onStack[v] = true

		var sortedNeighbors [][32]byte
		if neighbors, ok := graph.Edges[v]; ok {
			sortedNeighbors = make([][32]byte, len(neighbors))
			copy(sortedNeighbors, neighbors)
			sort.Slice(sortedNeighbors, func(i, j int) bool { return bytes.Compare(sortedNeighbors[i][:], sortedNeighbors[j][:]) < 0 })
		}

		for _, w := range sortedNeighbors {
			if ctx.Err() != nil {
				return
			}
			if _, visited := indices[w]; !visited {
				strongConnect(w)
				lowlink[v] = min(lowlink[v], lowlink[w])
			} else if onStack[w] {
				lowlink[v] = min(lowlink[v], indices[w])
			}
		}

		if lowlink[v] == indices[v] {
			var currentSCC [][32]byte
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				currentSCC = append(currentSCC, w)
				if bytes.Equal(w[:], v[:]) {
					break
				}
			}
			sort.Slice(currentSCC, func(i, j int) bool { return bytes.Compare(currentSCC[i][:], currentSCC[j][:]) < 0 })
			sccs = append(sccs, currentSCC)
		}
	}

	nodesToVisit := make([][32]byte, 0, len(graph.Nodes))
	for node := range graph.Nodes {
		nodesToVisit = append(nodesToVisit, node)
	}
	sort.Slice(nodesToVisit, func(i, j int) bool { return bytes.Compare(nodesToVisit[i][:], nodesToVisit[j][:]) < 0 })

	for _, v := range nodesToVisit {
		if ctx.Err() != nil {
			return nil
		}
		if _, visited := indices[v]; !visited {
			strongConnect(v)
		}
	}
	sort.Slice(sccs, func(i, j int) bool { return bytes.Compare(sccs[i][0][:], sccs[j][0][:]) < 0 })
	return sccs
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func topoSortCondensation(condensation map[int][]int) []int {
	inDegree, allNodes := make(map[int]int), make(map[int]bool)
	for u, neighbors := range condensation {
		allNodes[u] = true
		for _, v := range neighbors {
			allNodes[v] = true
			inDegree[v]++
		}
	}

	nodeList := make([]int, 0, len(allNodes))
	for node := range allNodes {
		nodeList = append(nodeList, node)
	}
	sort.Ints(nodeList)

	var queue []int
	for _, u := range nodeList {
		if inDegree[u] == 0 {
			queue = append(queue, u)
		}
	}

	var sorted []int
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		sorted = append(sorted, u)

		neighbors := condensation[u]
		sort.Ints(neighbors)
		for _, v := range neighbors {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
		sort.Ints(queue)
	}
	return sorted
}
