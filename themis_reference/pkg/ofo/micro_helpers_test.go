package ofo

// Test-only orchestration of the native Themis algorithms. No protocol alternative.
import (
	"themisreference/pkg/network"
	"themisreference/pkg/types"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"
)

const microWorkloadVersion = "themis-receipts-v1"

type microConfig struct {
	N, F                        uint64
	Gamma                       float64
	Seed                        int64
	History, ReceiptCap, TxSize int
}

type microFixture struct {
	s           *OFOService
	orders      []*types.ReplicaOrders
	proposal    *types.LeaderProposal
	prepared    microPrepared
	metrics     map[string]int
	traceDigest [32]byte
}

type microPrepared struct {
	local    []*types.LocalOrder
	update   []*types.UpdateOrder
	excluded map[[32]byte]bool
	deferred map[uint64]*types.LeaderProposal
	n, f     uint64
	gamma    float64
}

type microGraphResult struct {
	dm      *DependencyManager
	updates map[uint64]map[[32]byte][][32]byte
}

// prepareMicro is used only by Core: validation and snapshot preparation excluded.
func prepareMicro(s *OFOService, orders []*types.ReplicaOrders) microPrepared {
	p := microPrepared{excluded: make(map[[32]byte]bool), deferred: s.deferredProposals, n: s.replicaCount, f: s.fFaulty, gamma: s.gamma}
	p.local, p.update = s.separateOrderTypes(orders)
	for id := range s.committedTxIDs {
		p.excluded[id] = true
	}
	for _, prop := range s.deferredProposals {
		for id := range prop.Graph.Nodes {
			p.excluded[id] = true
		}
	}
	return p
}

// Native graph maintenance includes coupled presence/weight computation, pruning
// (including its SCC), and FairUpdate's scan of every retained graph.
func microGraphCore(p microPrepared) microGraphResult {
	dm := NewDependencyManager()
	if dm.BuildGraphAndClassifyTxs(p.local, p.n, p.f, p.gamma, p.excluded) {
		dm.CutShadedTail()
	}
	u := make(map[uint64]map[[32]byte][][32]byte)
	for h, prop := range p.deferred {
		if edges := FairUpdate(context.Background(), p.update, prop.Graph, p.n, p.f, p.gamma); len(edges) > 0 {
			u[h] = edges
		}
	}
	return microGraphResult{dm, u}
}

// Candidate checking after common input validation; does not construct a proof,
// hash a new candidate, or run another verifier after recomputation.
func microCandidateCore(p microPrepared, proposal *types.LeaderProposal) bool {
	r := microGraphCore(p)
	return graphEqual(context.Background(), proposal.Graph, r.dm.graph) && txStatesEqual(proposal.TxStates, r.dm.txStates) && updatesEqual(context.Background(), proposal.Updates, r.updates)
}

func microCloneService(s *OFOService) *OFOService {
	r := microRestoreService(s)
	r.deferredProposals = make(map[uint64]*types.LeaderProposal)
	for h, p := range s.deferredProposals {
		r.deferredProposals[h] = microDeepProposal(p)
	}
	return r
}

// Restore fields that native commit mutates. Deferred proposals are immutable
// inputs: commitVerifiedProposal stages its own copies inside the measured call.
// Do not deep-copy all proof histories outside every benchmark iteration.
func microRestoreService(s *OFOService) *OFOService {
	r := &OFOService{ReplicaID: s.ReplicaID, replicaCount: s.replicaCount, fFaulty: s.fFaulty, gamma: s.gamma, auth: s.auth,
		currentBlockHeight: s.currentBlockHeight, localOrderSequence: s.localOrderSequence, lastCommittedDigest: s.lastCommittedDigest,
		totalLatency: s.totalLatency, finalizedCountForLatency: s.finalizedCountForLatency, MeasurementDeadline: s.MeasurementDeadline,
		pipelineCtx: context.Background(), txPool: make(map[[32]byte]time.Time), txSubmissionTimes: make(map[[32]byte]time.Time),
		committedTxIDs: make(map[[32]byte]bool), committedOrderSequences: make(map[uint64]uint64), deferredProposals: s.deferredProposals,
		finalizedOrder: append([][32]byte(nil), s.finalizedOrder...), measuredFinalized: s.measuredFinalized}
	for id, v := range s.txPool {
		r.txPool[id] = v
	}
	for id, v := range s.txSubmissionTimes {
		r.txSubmissionTimes[id] = v
	}
	for id, v := range s.committedTxIDs {
		r.committedTxIDs[id] = v
	}
	for id, v := range s.committedOrderSequences {
		r.committedOrderSequences[id] = v
	}
	return r
}

func microDeepProposal(p *types.LeaderProposal) *types.LeaderProposal {
	if p == nil {
		return nil
	}
	r := cloneProposal(p)
	r.Updates = make(map[uint64]map[[32]byte][][32]byte)
	for h, edges := range p.Updates {
		r.Updates[h] = make(map[[32]byte][][32]byte)
		for u, vs := range edges {
			r.Updates[h][u] = append([][32]byte(nil), vs...)
		}
	}
	if p.Proof != nil {
		r.Proof = &types.LocalOrderFragment{}
		for _, o := range p.Proof.ReplicaOrders {
			if o == nil {
				r.Proof.ReplicaOrders = append(r.Proof.ReplicaOrders, nil)
				continue
			}
			c := *o
			if o.NewTxOrder != nil {
				l := *o.NewTxOrder
				l.OrderedTxs = slices.Clone(l.OrderedTxs)
				l.Signature = append([]byte(nil), l.Signature...)
				c.NewTxOrder = &l
			}
			if o.Update != nil {
				u := *o.Update
				u.OrderedTxs = slices.Clone(u.OrderedTxs)
				u.Signature = append([]byte(nil), u.Signature...)
				c.Update = &u
			}
			r.Proof.ReplicaOrders = append(r.Proof.ReplicaOrders, &c)
		}
	}
	return r
}

// State comparison intentionally checks fields absent from CommittedContext.
func microStateEqual(a, b *OFOService) bool {
	// Wall-clock latency totals differ between two real commits; ordered state,
	// cleanup, and deterministic counters must agree.
	return a.currentBlockHeight == b.currentBlockHeight && a.localOrderSequence == b.localOrderSequence && a.lastCommittedDigest == b.lastCommittedDigest &&
		a.measuredFinalized == b.measuredFinalized && a.finalizedCountForLatency == b.finalizedCountForLatency &&
		reflect.DeepEqual(a.committedTxIDs, b.committedTxIDs) && reflect.DeepEqual(a.finalizedOrder, b.finalizedOrder) &&
		reflect.DeepEqual(a.committedOrderSequences, b.committedOrderSequences) && reflect.DeepEqual(a.deferredProposals, b.deferredProposals) &&
		reflect.DeepEqual(a.txPool, b.txPool) && reflect.DeepEqual(a.txSubmissionTimes, b.txSubmissionTimes)
}

// Independent test-only staging oracle for exact batch boundaries and ordered
// output. It reuses native graphs/ordering and never installs partial state.
func microExpectedFinalize(s *OFOService, p *types.LeaderProposal) ([][][32]byte, map[uint64]*types.LeaderProposal) {
	staged := make(map[uint64]*types.LeaderProposal)
	for h, old := range s.deferredProposals {
		staged[h] = cloneProposal(old)
	}
	for h, edges := range p.Updates {
		for u, vs := range edges {
			for _, v := range vs {
				addEdge(staged[h].Graph, u, v)
			}
		}
	}
	if len(p.Graph.Nodes) > 0 {
		staged[p.BlockHeight] = cloneProposal(p)
	}
	heights := make([]uint64, 0, len(staged))
	for h := range staged {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	var batches [][][32]byte
	for _, h := range heights {
		dm := NewDependencyManager()
		dm.graph = staged[h].Graph
		dm.txStates = staged[h].TxStates
		if !IsTournament(dm.graph) {
			break
		}
		order := dm.ComputeFairOrder()
		if len(order) != len(dm.graph.Nodes) {
			panic("native ordering failed")
		}
		batches = append(batches, order)
		delete(staged, h)
	}
	return batches, staged
}

func makeMicroFixture(tb testing.TB, c microConfig, scenario string) *microFixture {
	tb.Helper()
	if err := ValidateThemisParameters(c.N, c.F, c.Gamma); err != nil {
		tb.Fatal(err)
	}
	if c.ReceiptCap < 3 || c.TxSize < 16 || c.History < 3 {
		tb.Fatal("receipt-cap >= 3, tx-size >= 16, history >= 3 required")
	}
	auth := newTestOrderAuthenticator(c.N)
	var replicas []*OFOService
	var nets []*testNetwork
	for id := uint64(0); id < c.N-c.F; id++ {
		net := &testNetwork{sent: make(chan network.Message, 1)}
		// Follower constructor avoids collector goroutines. Replica zero uses the
		// same synchronous receipt-generation path through a capturing network.
		s, err := NewOFOService(1, c.N, c.F, c.Gamma, net, 0, 100, false, auth)
		if err != nil {
			tb.Fatal(err)
		}
		s.ReplicaID = id
		replicas = append(replicas, s)
		nets = append(nets, net)
	}
	var serial uint64
	newTxs := func(count int) []*types.Transaction {
		txs := make([]*types.Transaction, count)
		for i := range txs {
			serial++
			payload := make([]byte, c.TxSize)
			binary.BigEndian.PutUint64(payload, uint64(c.Seed))
			binary.BigEndian.PutUint64(payload[8:], serial)
			for j := 16; j < len(payload); j++ {
				payload[j] = byte(uint64(j)*31 + serial + uint64(c.Seed))
			}
			txs[i] = &types.Transaction{ID: types.TransactionID(payload), CanonicalBytes: payload, SubmissionTime: time.Unix(1780000000, 0)}
		}
		return txs
	}
	metrics := map[string]int{"receipt_cap": c.ReceiptCap, "warmup_rounds": 0}
	var trace bytes.Buffer
	deliver := func(lists [][]*types.Transaction, measured bool) {
		for i, list := range lists {
			if len(list) > c.ReceiptCap {
				tb.Fatalf("%s requires %d receipt events at replica %d; cap=%d (never truncated)", scenario, len(list), i, c.ReceiptCap)
			}
			key := "warmup_max_receipts"
			if measured {
				key = "measured_max_receipts"
			}
			if len(list) > metrics[key] {
				metrics[key] = len(list)
			}
			for _, tx := range list {
				binary.Write(&trace, binary.BigEndian, uint64(i))
				trace.Write(tx.ID[:])
				_, known := replicas[i].txPool[tx.ID]
				completed := replicas[i].committedTxIDs[tx.ID]
				replicas[i].handleMessage(network.Message{Payload: tx})
				if !known && !completed {
					// Replace wall-clock receipt time with a strictly increasing logical
					// timestamp. Only the native admission path creates pool entries.
					replicas[i].txPool[tx.ID] = time.Unix(0, int64(trace.Len()))
					if measured {
						metrics["effective_new_receipt_events"]++
					}
				}
			}
		}
	}
	all := func(txs []*types.Transaction) [][]*types.Transaction {
		lists := make([][]*types.Transaction, len(replicas))
		for i := range lists {
			lists[i] = txs
		}
		return lists
	}
	collect := func(measured bool) []*types.ReplicaOrders {
		var orders []*types.ReplicaOrders
		for i, s := range replicas {
			s.generateAndSendOrders()
			o := (<-nets[i].sent).Payload.(*types.ReplicaOrders)
			orders = append(orders, o)
			prefix := "warmup"
			if measured {
				prefix = "measured"
			}
			if l := len(o.NewTxOrder.OrderedTxs); l > metrics[prefix+"_max_local_order"] {
				metrics[prefix+"_max_local_order"] = l
			}
			if o.Update != nil {
				if l := len(o.Update.OrderedTxs); l > metrics[prefix+"_max_update_order"] {
					metrics[prefix+"_max_update_order"] = l
				}
			}
		}
		return orders
	}
	warmCommit := func() {
		orders := collect(false)
		p := replicas[0].buildProposal(orders)
		if p == nil || !replicas[0].VerifyProposal(p) {
			tb.Fatal("invalid warmup candidate")
		}
		for _, s := range replicas {
			if !s.CommitVerifiedProposal(p) {
				tb.Fatal("warmup commit failed")
			}
		}
		metrics["warmup_rounds"]++
	}
	// Every sample starts with real nonempty finalization and receipt cleanup.
	completed := newTxs(3)
	deliver(all(completed), false)
	warmCommit()
	history, fresh, release, cycle := 0, 0, false, false
	switch scenario {
	case "mechanism_small":
		fresh = 24
	case "mechanism_cycle":
		fresh, cycle = 192, true
	case "mechanism_update":
		history, fresh, release = c.History, 8, true
	case "low_release":
		history, fresh, release = 12, 96, true
	case "small_fresh":
		history, fresh = 24, 192
	case "history_few":
		history, fresh = c.History, 8
	case "history_many":
		history, fresh = c.History, 128
	case "no_receipts":
		history = c.History
	case "late_completed":
	case "cycle":
		fresh, cycle = 96, true
	default:
		tb.Fatalf("unknown scenario %q", scenario)
	}
	var blocker []*types.Transaction
	if history > 0 {
		// Keep a genuinely unspecified shaded pair: both appear in 2(t-1)
		// lists, split evenly between their two orders. Neither weight reaches t.
		// Increasing-presence bridge vertices give both a path to a solid node.
		// The old disjoint-receipt fixture incorrectly relied on ignoring absence.
		pairPresence := 2 * (edgeThreshold(c.N, c.F, c.Gamma) - 1)
		solidPresence := int(c.N - 2*c.F)
		if pairPresence >= solidPresence {
			tb.Fatal("micro blocker requires 2*(edge threshold-1) < n-2f")
		}
		counts := []int{pairPresence, pairPresence}
		for count := pairPresence; count < solidPresence; {
			count *= 2
			if count > solidPresence {
				count = solidPresence
			}
			counts = append(counts, count)
		}
		if history < len(counts) {
			tb.Fatal("history too small for a paper-valid deferred blocker")
		}
		blocker = newTxs(len(counts))
		// At equal directional weights the smaller transaction ID wins.
		sort.Slice(blocker, func(i, j int) bool { return bytes.Compare(blocker[i].ID[:], blocker[j].ID[:]) < 0 })
		lists := make([][]*types.Transaction, len(replicas))
		for i := range lists {
			for j, count := range counts {
				if i < count {
					lists[i] = append(lists[i], blocker[j])
				}
			}
			if i >= pairPresence/2 && i < pairPresence {
				lists[i][0], lists[i][1] = lists[i][1], lists[i][0]
			}
		}
		deliver(lists, false)
		warmCommit()
		for remaining := history - len(blocker); remaining > 0; {
			count := remaining
			if count > c.ReceiptCap {
				count = c.ReceiptCap
			}
			deliver(all(newTxs(count)), false)
			warmCommit()
			remaining -= count
		}
	}
	retained := 0
	for _, p := range replicas[0].deferredProposals {
		retained += len(p.Graph.Nodes)
	}
	if retained != history || len(replicas[0].committedTxIDs) != 3 {
		tb.Fatalf("structure mismatch: retained=%d target=%d completed=%d", retained, history, len(replicas[0].committedTxIDs))
	}
	lists := all(newTxs(fresh))
	if cycle {
		for i := range lists {
			base := lists[i]
			shift := (i % 3) * (fresh / 3)
			lists[i] = append(append([]*types.Transaction(nil), base[shift:]...), base[:shift]...)
		}
	}
	if release {
		for i := range lists {
			var missing []*types.Transaction
			for _, tx := range blocker {
				if _, known := replicas[i].txPool[tx.ID]; !known {
					missing = append(missing, tx)
				}
			}
			lists[i] = append(missing, lists[i]...)
		}
	}
	if scenario == "late_completed" {
		lists = all(completed)
	}
	deliver(lists, true)
	orders := collect(true)
	s := replicas[0]
	for _, o := range orders {
		if !s.validateReplicaOrders(o) {
			tb.Fatal("generated invalid native evidence")
		}
	}
	p := s.buildProposal(orders)
	fx := &microFixture{s: s, orders: orders, proposal: p, prepared: prepareMicro(s, orders), metrics: metrics, traceDigest: types.TransactionID(trace.Bytes())}
	metrics["retained_txs"] = retained
	metrics["completed_txs"] = len(s.committedTxIDs)
	metrics["fresh_txs"] = fresh
	metrics["pool_entries"] = len(s.txPool)
	metrics["sequence_entries"] = len(s.committedOrderSequences)
	metrics["deferred_batches"] = len(s.deferredProposals)
	microMeasureStructure(fx)
	if (scenario == "no_receipts" || scenario == "late_completed") && p != nil {
		tb.Fatal("expected no candidate for ineffective extension")
	}
	if fresh > 0 && p == nil {
		tb.Fatal("nonempty workload did not produce candidate")
	}
	if cycle && (metrics["new_graph_max_scc"] != fresh || metrics["output_txs"] != fresh) {
		tb.Fatal("cycle must span all fresh transactions and finalize")
	}
	if release && (metrics["output_txs"] != history+fresh || metrics["post_retained_txs"] != 0) {
		tb.Fatal("release did not drain history")
	}
	if history > 0 && !release && (metrics["output_txs"] != 0 || metrics["post_retained_txs"] != history+fresh || metrics["blocked_missing_pairs"] == 0) {
		tb.Fatal("claimed blocking structure absent")
	}
	return fx
}

func microGraphMetrics(m map[string]int, prefix string, g *types.DependencyGraph) {
	m[prefix+"nodes"] += len(g.Nodes)
	for _, vs := range g.Edges {
		m[prefix+"edges"] += len(vs)
	}
	for _, scc := range tarjanSCC(context.Background(), g) {
		m[prefix+"sccs"]++
		if len(scc) > 1 {
			m[prefix+"nontrivial_sccs"]++
		}
		if len(scc) > m[prefix+"max_scc"] {
			m[prefix+"max_scc"] = len(scc)
		}
	}
}

func microMeasureStructure(fx *microFixture) {
	m := fx.metrics
	for _, key := range []string{"warmup_max_receipts", "measured_max_receipts", "warmup_max_local_order", "measured_max_local_order", "warmup_max_update_order", "measured_max_update_order", "effective_new_receipt_events", "cached_state_entries", "cached_proof_entries", "blocked_missing_pairs", "proof_signatures", "proof_signature_bytes", "local_evidence_occurrences", "effective_local_occurrences", "update_evidence_occurrences", "new_weight_entries", "added_update_edges", "output_txs", "output_batches"} {
		if _, ok := m[key]; !ok {
			m[key] = 0
		}
	}
	for _, prefix := range []string{"new_graph_", "retained_graph_"} {
		for _, suffix := range []string{"nodes", "edges", "sccs", "nontrivial_sccs", "max_scc"} {
			m[prefix+suffix] = 0
		}
	}
	s := fx.s
	for _, p := range s.deferredProposals {
		microGraphMetrics(m, "retained_graph_", p.Graph)
		m["cached_state_entries"] += len(p.TxStates)
		for _, o := range p.Proof.ReplicaOrders {
			m["cached_proof_entries"] += len(o.NewTxOrder.OrderedTxs)
			if o.Update != nil {
				m["cached_proof_entries"] += len(o.Update.OrderedTxs)
			}
		}
		ids := make([][32]byte, 0, len(p.Graph.Nodes))
		for id := range p.Graph.Nodes {
			ids = append(ids, id)
		}
		for i, u := range ids {
			for _, v := range ids[i+1:] {
				if !graphHasEdge(p.Graph, u, v) && !graphHasEdge(p.Graph, v, u) {
					m["blocked_missing_pairs"]++
				}
			}
		}
	}
	touched := make(map[[32]byte]bool)
	newIDs := make(map[[32]byte]bool)
	for _, o := range fx.orders {
		m["proof_signatures"]++
		m["proof_signature_bytes"] += len(o.NewTxOrder.Signature)
		m["local_evidence_occurrences"] += len(o.NewTxOrder.OrderedTxs)
		for _, id := range o.NewTxOrder.OrderedTxs {
			if !fx.prepared.excluded[id] {
				touched[id] = true
				newIDs[id] = true
				m["effective_local_occurrences"]++
			}
		}
		if o.Update != nil {
			m["proof_signatures"]++
			m["proof_signature_bytes"] += len(o.Update.Signature)
			m["update_evidence_occurrences"] += len(o.Update.OrderedTxs)
			for _, id := range o.Update.OrderedTxs {
				touched[id] = true
			}
		}
	}
	m["effective_new_txs"] = len(newIDs)
	m["evidence_touched_txs"] = len(touched)
	r := microGraphCore(fx.prepared)
	for _, row := range r.dm.weights {
		for _, w := range row {
			if w != 0 {
				m["new_weight_entries"]++
			}
		}
	}
	for _, edges := range r.updates {
		for _, vs := range edges {
			m["added_update_edges"] += len(vs)
		}
	}
	microGraphMetrics(m, "new_graph_", r.dm.graph)
	m["post_retained_txs"] = m["retained_txs"]
	if fx.proposal != nil {
		batches, remaining := microExpectedFinalize(s, fx.proposal)
		m["output_batches"] = len(batches)
		for _, batch := range batches {
			m["output_txs"] += len(batch)
		}
		m["post_retained_txs"] = 0
		for _, p := range remaining {
			m["post_retained_txs"] += len(p.Graph.Nodes)
		}
	}
}

func microFixtureName(c microConfig, scenario string) string {
	return fmt.Sprintf("n=%d/f=%d/gamma=%g/seed=%d/history=%d/cap=%d/bytes=%d/%s", c.N, c.F, c.Gamma, c.Seed, c.History, c.ReceiptCap, c.TxSize, scenario)
}
