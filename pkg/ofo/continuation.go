package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"sort"
)

// ExecutionRecord marks an open-batch member: its batch index and the fragment
// that assigned it. Members execute, and settle, only in a certified round.
type ExecutionRecord struct{ BatchIndex, FragmentSeq, Ordinal uint64 }

// EnableContinuation fixes the public policy before the first transition.
func (s *OFOService) EnableContinuation(delay uint64) error {
	s.rwMu.Lock()
	defer s.rwMu.Unlock()
	if s.committed.FragmentSeq != 0 || s.pending != nil {
		return fmt.Errorf("continuation policy must be fixed at genesis")
	}
	s.continuationEnabled, s.continuationDelay = true, delay
	s.policyID = types.ContinuationPolicyID(s.replicaCount, s.fFaulty, types.CalculateHonestThreshold(s.replicaCount, s.fFaulty, s.gamma), delay)
	return nil
}

func markFirstSolid(state *EvidenceState) {
	for id, kind := range state.states {
		if kind == types.StateSolid {
			if _, known := state.SolidSince[id]; !known {
				state.SolidSince[id] = state.FragmentSeq
			}
		}
	}
}

func continuationRoots(state *EvidenceState, delay uint64) map[types.TxID]bool {
	roots := make(map[types.TxID]bool)
	for id, kind := range state.states {
		if !state.Live[id] || kind != types.StateSolid {
			continue
		}
		_, old := state.OpenMembers[id]
		since, known := state.SolidSince[id]
		if old || (known && state.FragmentSeq >= since && state.FragmentSeq-since >= delay) {
			roots[id] = true
		}
	}
	return roots
}

// continuationParams are the public integers used by both construction and
// validation of a continuation transition.
type continuationParams struct {
	delay       uint64
	n, qh, edge int
}

func (s *OFOService) continuationParams() continuationParams {
	return continuationParams{
		delay: s.continuationDelay,
		n:     int(s.replicaCount),
		qh:    int(types.CalculateHonestThreshold(s.replicaCount, s.fFaulty, s.gamma)),
		edge:  types.CalculateEdgeThreshold(s.replicaCount, s.fFaulty, s.gamma),
	}
}

// potentialPredecessors returns every live transaction v that could still be a
// receipt-order fairness predecessor of the Solid member u. A correct reporter
// of u whose prefix lacks v received u first, so at most W(v,u)+n-c(u) correct
// replicas can have received v before u; v is excluded once this is below q_h.
func potentialPredecessors(state *EvidenceState, u types.TxID, p continuationParams) []types.TxID {
	need := p.qh + state.visibility[u] - p.n
	if need < 1 {
		need = 1
	}
	var out []types.TxID
	for v, kind := range state.states {
		if v == u || !state.Live[v] || kind == types.StateBlank {
			continue
		}
		if state.QueryCounts != nil {
			state.QueryCounts.FrontierPairs++
			state.QueryCounts.WeightReads++
		}
		if state.weights[edgeKey{v, u}] >= need {
			out = append(out, v)
		}
	}
	types.SortTxIDs(out)
	return out
}

// certifiedExecutionRounds is the deterministic execution rule of an open
// batch. Only Solid members execute. A Solid member executes after every
// potential predecessor has executed, unless the two lie in one strongly
// connected component of the support graph (W >= T_e in a direction). Every
// support edge has at least T_e-b = C-q_h+1 correct reporters in that receipt
// order, so a support cycle certifies a weak-precedence cycle among correct
// replicas, the only place where batch-order fairness permits reordering.
func certifiedExecutionRounds(state *EvidenceState, members map[types.TxID]bool, p continuationParams) []types.FairnessBatch {
	preds := make(map[types.TxID][]types.TxID)
	domain := make(map[types.TxID]bool, len(members))
	for id := range members {
		domain[id] = true
	}
	for id := range members {
		if state.states[id] != types.StateSolid {
			continue
		}
		preds[id] = potentialPredecessors(state, id, p)
		for _, v := range preds[id] {
			domain[v] = true
		}
	}
	if len(preds) == 0 {
		return nil
	}
	support := make(map[types.TxID]map[types.TxID]bool, len(domain))
	for a := range domain {
		for b := range domain {
			if a == b {
				continue
			}
			if state.QueryCounts != nil {
				state.QueryCounts.InternalPairs++
				state.QueryCounts.WeightReads++
			}
			if state.weights[edgeKey{a, b}] >= p.edge {
				if support[a] == nil {
					support[a] = make(map[types.TxID]bool)
				}
				support[a][b] = true
			}
		}
	}
	_, component, _ := decomposeView(domain, support, state.states)
	executed := make(map[types.TxID]bool)
	var rounds []types.FairnessBatch
	for {
		var ready []types.TxID
		for u, list := range preds {
			if executed[u] {
				continue
			}
			ok := true
			for _, v := range list {
				if !executed[v] && component[v] != component[u] {
					ok = false
					break
				}
			}
			if ok {
				ready = append(ready, u)
			}
		}
		if len(ready) == 0 {
			return rounds
		}
		types.SortTxIDs(ready)
		for _, u := range ready {
			executed[u] = true
		}
		rounds = append(rounds, types.FairnessBatch{Transactions: ready})
	}
}

func (dm *DependencyManager) buildContinuationMode(state *EvidenceState, params continuationParams, withProof bool) (*types.ContinuationTransition, *types.ContinuationCertificate, error) {
	delay := params.delay
	roots := continuationRoots(state, delay)
	if state.OpenBatch == 0 && len(roots) == 0 {
		return nil, nil, nil
	}
	index := state.OpenBatch
	if index == 0 {
		index = state.FairnessBatchCounter + 1
	}
	c := &types.ContinuationTransition{BatchIndex: index, Opened: state.OpenBatch == 0}
	proof := &types.ContinuationCertificate{}
	members := make(map[types.TxID]bool)
	for id := range state.OpenMembers {
		members[id] = true
	}
	parents := make(map[types.TxID]types.TxID)
	depth := make(map[types.TxID]uint64)
	queue := sortedSet(roots)
	for _, id := range queue {
		members[id] = true
	}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, v := range sortedSet(dm.inverseEdges[p]) {
			if !state.Live[v] || state.states[v] == types.StateBlank {
				continue
			}
			if members[v] {
				continue
			}
			members[v], parents[v], depth[v] = true, p, depth[p]+1
			if state.states[v] == types.StateSolid {
				queue = append(queue, v)
			}
		}
	}
	executed := make(map[types.TxID]bool)
	for _, round := range certifiedExecutionRounds(state, members, params) {
		round.Index = index
		c.Joins = append(c.Joins, round)
		for _, id := range round.Transactions {
			executed[id] = true
			c.Settled = append(c.Settled, id)
		}
	}
	for id := range members {
		_, old := state.OpenMembers[id]
		if !old && !executed[id] {
			c.Assigned = append(c.Assigned, id)
		}
		if withProof && (!old || roots[id]) {
			p, has := parents[id]
			proof.MemberForest = append(proof.MemberForest, types.BlockRecord{Vertex: id, Parent: p, HasParent: has, Depth: depth[id]})
		}
	}
	types.SortTxIDs(c.Assigned)
	types.SortTxIDs(c.Settled)
	// Wire record order is canonical; witness paths themselves may differ.
	sort.Slice(proof.MemberForest, func(i, j int) bool { return types.LessTxID(proof.MemberForest[i].Vertex, proof.MemberForest[j].Vertex) })
	c.Closed = len(executed) == len(members)
	return c, proof, nil
}

func verifyContinuation(state *EvidenceState, c *types.ContinuationTransition, proof *types.ContinuationCertificate, params continuationParams) error {
	threshold := params.edge
	roots := continuationRoots(state, params.delay)
	if state.OpenBatch == 0 && len(roots) == 0 {
		if c != nil || proof != nil {
			return fmt.Errorf("unsolicited continuation")
		}
		return nil
	}
	if c == nil || proof == nil {
		return fmt.Errorf("mandatory continuation omitted")
	}
	index := state.OpenBatch
	if index == 0 {
		index = state.FairnessBatchCounter + 1
	}
	if c.BatchIndex != index || c.Opened != (state.OpenBatch == 0) {
		return fmt.Errorf("continuation index or open event mismatch")
	}
	if len(proof.Part) != 0 {
		return fmt.Errorf("continuation carries closed-component claims")
	}
	fresh := make(map[types.TxID]bool)
	members := make(map[types.TxID]bool)
	for id := range state.OpenMembers {
		members[id] = true
	}
	admissible := func(id types.TxID) error {
		if !state.Live[id] || state.states[id] == types.StateBlank {
			return fmt.Errorf("continuation member is not live and active")
		}
		if _, done := state.doneLookup(id); done {
			return fmt.Errorf("settled replay in continuation")
		}
		return nil
	}
	executed := make(map[types.TxID]bool)
	for _, batch := range c.Joins {
		if batch.Index != index || len(batch.Transactions) == 0 {
			return fmt.Errorf("join index or empty round")
		}
		for i, id := range batch.Transactions {
			if executed[id] {
				return fmt.Errorf("duplicate execution in continuation")
			}
			if err := admissible(id); err != nil {
				return err
			}
			if i > 0 && !types.LessTxID(batch.Transactions[i-1], id) {
				return fmt.Errorf("execution round serialization")
			}
			executed[id] = true
			if !members[id] {
				fresh[id] = true
			}
		}
	}
	for i, id := range c.Assigned {
		if i > 0 && !types.LessTxID(c.Assigned[i-1], id) {
			return fmt.Errorf("assigned serialization")
		}
		if members[id] || executed[id] {
			return fmt.Errorf("assigned member is old or executed")
		}
		if err := admissible(id); err != nil {
			return err
		}
		fresh[id] = true
	}
	for id := range fresh {
		members[id] = true
	}
	for root := range roots {
		if !members[root] {
			return fmt.Errorf("due or open Solid root omitted")
		}
	}
	solidMembers := make(map[types.TxID]bool)
	for id := range members {
		if state.states[id] == types.StateSolid {
			solidMembers[id] = true
		}
	}
	if !sameIDs(c.Settled, sortedSet(executed)) || c.Closed != (len(executed) == len(members)) {
		return fmt.Errorf("settlement or close event mismatch")
	}
	domain := make(map[types.TxID]bool)
	for id := range fresh {
		domain[id] = true
	}
	for id := range roots {
		domain[id] = true
	}
	if len(proof.MemberForest) != len(domain) {
		return fmt.Errorf("membership forest coverage")
	}
	records := make(map[types.TxID]types.BlockRecord)
	for _, r := range proof.MemberForest {
		if !domain[r.Vertex] {
			return fmt.Errorf("membership forest domain")
		}
		if _, duplicate := records[r.Vertex]; duplicate {
			return fmt.Errorf("membership forest duplicate")
		}
		records[r.Vertex] = r
	}
	for id, r := range records {
		if roots[id] {
			if r.HasParent || r.Depth != 0 || r.Parent != (types.TxID{}) {
				return fmt.Errorf("mandatory root is not canonical root")
			}
			continue
		}
		p, exists := records[r.Parent]
		if state.QueryCounts != nil {
			state.QueryCounts.WitnessEdges++
		}
		if !r.HasParent || r.Depth == 0 || !exists || p.Depth+1 != r.Depth || state.states[r.Parent] != types.StateSolid || !localEdgePred(state, id, r.Parent, threshold) {
			return fmt.Errorf("membership parent, depth, or edge")
		}
	}
	// Every parent depth is one smaller and roots are exactly public roots;
	// thus all new members belong to their incoming closure, in linear work.
	for inside := range solidMembers {
		for outside, kind := range state.states {
			if !state.Live[outside] || kind == types.StateBlank || members[outside] {
				continue
			}
			if state.QueryCounts != nil {
				state.QueryCounts.FrontierPairs++
			}
			if localEdgePred(state, outside, inside, threshold) {
				return fmt.Errorf("Solid member has omitted incoming predecessor")
			}
		}
	}
	// The membership is now exact, so the execution rounds are a public
	// function of it; the order leader has no discretion over them.
	want := certifiedExecutionRounds(state, members, params)
	if len(want) != len(c.Joins) {
		return fmt.Errorf("execution rounds differ from certified order")
	}
	for i := range want {
		if !sameIDs(want[i].Transactions, c.Joins[i].Transactions) {
			return fmt.Errorf("execution rounds differ from certified order")
		}
	}
	return nil
}

func applyContinuation(state *EvidenceState, manager *DependencyManager, c *types.ContinuationTransition, closedOutputCount uint64) {
	if c == nil {
		return
	}
	if c.Opened {
		state.OpenBatch = c.BatchIndex
		state.FairnessBatchCounter = c.BatchIndex
	}
	// Executed members settle in the same transition; Assigned members keep
	// their batch index and live evidence until a later certified round.
	ordinal := closedOutputCount
	ordinals := make(map[types.TxID]uint64)
	for _, batch := range c.Joins {
		for _, id := range batch.Transactions {
			ordinal++
			ordinals[id] = ordinal
		}
	}
	retired := make(map[types.TxID]bool, len(c.Settled))
	for _, id := range c.Settled {
		state.addDone(id, DoneRecord{FragmentSeq: state.FragmentSeq, BatchIndex: c.BatchIndex, ExecutionSeq: state.FragmentSeq, ExecutionOrdinal: ordinals[id]})
		delete(state.OpenMembers, id)
		retired[id] = true
	}
	for _, id := range c.Assigned {
		state.OpenMembers[id] = ExecutionRecord{BatchIndex: c.BatchIndex, FragmentSeq: state.FragmentSeq}
	}
	retireEvidence(state, manager, retired)
	if c.Closed {
		state.OpenBatch = 0
	}
}

func (s *OFOService) buildOutput(pre *EvidenceState, manager *DependencyManager) (*EvidenceState, *types.FairOrderFragment, *types.StructuralCertificate, *types.ContinuationTransition, *types.ContinuationCertificate, error) {
	return s.buildOutputMode(pre, manager, true, true)
}
func (s *OFOService) buildOutputMode(pre *EvidenceState, manager *DependencyManager, withProof, mutate bool) (*EvidenceState, *types.FairOrderFragment, *types.StructuralCertificate, *types.ContinuationTransition, *types.ContinuationCertificate, error) {
	order, proof := &types.FairOrderFragment{}, &types.StructuralCertificate{}
	var err error
	if !s.continuationEnabled || pre.OpenBatch == 0 {
		order, proof, err = manager.buildProposalMode(pre, withProof)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}
	post := pre.clone()
	retirer := manager
	if !mutate {
		retirer = NewDependencyManager(s.replicaCount, s.fFaulty, s.gamma)
	}
	finalize(post, retirer, order, proof.Part)
	if !s.continuationEnabled {
		return post, order, proof, nil, nil, nil
	}
	c, cp, err := manager.buildContinuationMode(post, s.continuationParams(), withProof)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	count := uint64(0)
	for _, batch := range order.Batches {
		count += uint64(len(batch.Transactions))
	}
	applyContinuation(post, retirer, c, count)
	post.StateID = types.ContinuationStateIdentifier(post.Epoch, post.FragmentSeq, pre.StateID, s.policyID, order, c, post.DoneRoot)
	return post, order, proof, c, cp, nil
}

func (s *OFOService) verifyOutput(pre *EvidenceState, f *types.VerifiableFairOrderFragment) (*EvidenceState, error) {
	if f.FinalOrder == nil || f.Certificate == nil {
		return nil, fmt.Errorf("missing output certificate")
	}
	if !s.continuationEnabled || pre.OpenBatch == 0 {
		if err := verifyStructuralCertificate(pre, f.FinalOrder, f.Certificate, s.replicaCount, s.fFaulty, s.gamma); err != nil {
			return nil, err
		}
	} else if len(f.FinalOrder.Batches) != 0 || len(f.Certificate.Part) != 0 || len(f.Certificate.BlockForest) != 0 {
		return nil, fmt.Errorf("later closed batch while continuation open")
	}
	post := pre.clone()
	manager := NewDependencyManager(s.replicaCount, s.fFaulty, s.gamma)
	finalize(post, manager, f.FinalOrder, f.Certificate.Part)
	if !s.continuationEnabled {
		return post, nil
	}
	if err := verifyContinuation(post, f.Continuation, f.ContinuationProof, s.continuationParams()); err != nil {
		return nil, err
	}
	count := uint64(0)
	for _, batch := range f.FinalOrder.Batches {
		count += uint64(len(batch.Transactions))
	}
	applyContinuation(post, manager, f.Continuation, count)
	post.StateID = types.ContinuationStateIdentifier(post.Epoch, post.FragmentSeq, pre.StateID, s.policyID, f.FinalOrder, f.Continuation, post.DoneRoot)
	return post, nil
}

type ProgressStats struct {
	Executed, Settled, Live, OpenMembers int
	OpenBatch, Sequence                  uint64
}

func (s *OFOService) Progress() ProgressStats {
	s.rwMu.RLock()
	defer s.rwMu.RUnlock()
	state := s.committed
	return ProgressStats{Executed: state.doneCount(), Settled: state.doneCount(), Live: len(state.Live), OpenMembers: len(state.OpenMembers), OpenBatch: state.OpenBatch, Sequence: state.FragmentSeq}
}

func outputBatches(f *types.VerifiableFairOrderFragment) []types.FairnessBatch {
	b := append([]types.FairnessBatch(nil), f.FinalOrder.Batches...)
	if f.Continuation != nil {
		b = append(b, f.Continuation.Joins...)
	}
	return b
}
