package ofo

// Finite-input ordering experiment. Reuses production receipt handling, LO
// generation (including the malicious branch), evidence, certificates and commit.
// Like the ablations, synchronous orchestration replaces networking/hosting BFT.
import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

var (
	orderingExperiment   = flag.Bool("ordering-experiment", false, "Run the opt-in finite-input ordering experiment")
	orderingNodes        = flag.Uint64("ordering-nodes", 10, "Replica count")
	orderingFaults       = flag.Uint64("ordering-faults", 2, "Fixed protocol fault bound")
	orderingByzantine    = flag.Uint64("ordering-byzantine", 0, "Actual malicious LO producers")
	orderingGamma        = flag.Float64("ordering-gamma", 1, "Protocol gamma")
	orderingTransactions = flag.Int("ordering-transactions", 200, "Finite transaction set size")
	orderingLOSize       = flag.Int("ordering-lo-size", 50, "Production LO size limit")
	orderingRatio        = flag.Float64("ordering-ratio", 1, "Mean propagation delay / mean generation interval")
	orderingSeed         = flag.Int64("ordering-seed", 1, "Paired input seed, independent of malicious count")
	orderingTraceFile    = flag.String("ordering-trace-file", "", "Optional JSON input trace export")
	orderingCost         = flag.Bool("ordering-cost", false, "Time matched full-input leader and one follower; no network timing")
)

type orderingTrace struct {
	GenerationTimes []float64   `json:"generation_times"`
	ArrivalTimes    [][]float64 `json:"arrival_times"`
	ReceiptOrders   [][]int     `json:"receipt_orders"`
	TransactionIDs  []string    `json:"transaction_ids"`
	txs             []types.Transaction
}

func makeOrderingTrace(n uint64, count int, ratio float64, seed int64) orderingTrace {
	trace := orderingTrace{GenerationTimes: make([]float64, count), ArrivalTimes: make([][]float64, n),
		ReceiptOrders: make([][]int, n), TransactionIDs: make([]string, count), txs: make([]types.Transaction, count)}
	rng := rand.New(rand.NewSource(seed))
	clock := 0.0
	for i := range trace.txs {
		clock += rng.ExpFloat64() // Mean generation interval is one, in simulation units.
		trace.GenerationTimes[i] = clock
		content := make([]byte, 16)
		binary.BigEndian.PutUint64(content, uint64(i))
		binary.BigEndian.PutUint64(content[8:], uint64(seed))
		trace.txs[i] = types.Transaction{ID: types.TransactionID(content), CanonicalBytes: content}
		trace.TransactionIDs[i] = fmt.Sprintf("%x", trace.txs[i].ID)
	}
	for r := range trace.ReceiptOrders {
		trace.ArrivalTimes[r] = make([]float64, count)
		trace.ReceiptOrders[r] = make([]int, count)
		for i := 0; i < count; i++ {
			trace.ArrivalTimes[r][i] = trace.GenerationTimes[i] + ratio*rng.ExpFloat64()
			trace.ReceiptOrders[r][i] = i
		}
		sort.Slice(trace.ReceiptOrders[r], func(a, b int) bool {
			i, j := trace.ReceiptOrders[r][a], trace.ReceiptOrders[r][b]
			if trace.ArrivalTimes[r][i] == trace.ArrivalTimes[r][j] {
				return i < j
			}
			return trace.ArrivalTimes[r][i] < trace.ArrivalTimes[r][j]
		})
	}
	return trace
}

type orderingMetrics struct {
	TotalPairs               int `json:"total_pairs"`
	CompletedPairs           int `json:"completed_pairs"`
	CorrectPairs             int `json:"correct_pairs"`
	TiedPairs                int `json:"tied_pairs"`
	ReversedPairs            int `json:"reversed_pairs"`
	UnresolvedPairs          int `json:"unresolved_pairs"`
	CompletedTransactions    int `json:"completed_transactions"`
	ReceiveDisagreementPairs int `json:"receive_disagreement_pairs"`
	FairnessEligiblePairs    int `json:"fairness_eligible_pairs"`
	FairnessCheckedPairs     int `json:"fairness_checked_pairs"`
	FairnessViolations       int `json:"fairness_violations"`
	MissingPredecessors      int `json:"missing_predecessors"`
}

// The oracle uses original honest receipt positions, never claimed LO order or
// the graph being checked. A committed successor with a missing predecessor is
// a violation, not an excluded/uncompleted sample. Batch ties are fair but do
// not count as strict agreement with the generation order.
func measureOrdering(trace orderingTrace, batches []uint64, f, byzantine uint64, gamma float64) orderingMetrics {
	n, count := len(trace.ReceiptOrders), len(batches)
	positions := make([][]int, n)
	for r, order := range trace.ReceiptOrders {
		positions[r] = make([]int, count)
		for p, tx := range order {
			positions[r][tx] = p
		}
	}
	metrics := orderingMetrics{TotalPairs: count * (count - 1) / 2}
	for _, batch := range batches {
		if batch > 0 {
			metrics.CompletedTransactions++
		}
	}
	qH := int(math.Ceil(gamma * float64(uint64(n)-f)))
	check := func(support, predecessor, successor int) {
		if support < qH {
			return
		}
		metrics.FairnessEligiblePairs++
		if batches[successor] == 0 {
			return
		}
		metrics.FairnessCheckedPairs++
		if batches[predecessor] == 0 || batches[predecessor] > batches[successor] {
			metrics.FairnessViolations++
			if batches[predecessor] == 0 {
				metrics.MissingPredecessors++
			}
		}
	}
	for i := 0; i < count; i++ {
		for j := i + 1; j < count; j++ {
			if batches[i] == 0 || batches[j] == 0 {
				metrics.UnresolvedPairs++
			} else {
				metrics.CompletedPairs++
				switch {
				case batches[i] < batches[j]:
					metrics.CorrectPairs++
				case batches[i] == batches[j]:
					metrics.TiedPairs++
				default:
					metrics.ReversedPairs++
				}
			}
			all, honest := 0, 0
			for r := 0; r < n; r++ {
				if positions[r][i] < positions[r][j] {
					all++
					if r < n-int(byzantine) {
						honest++
					}
				}
			}
			if all > 0 && all < n {
				metrics.ReceiveDisagreementPairs++
			}
			check(honest, i, j)
			check(n-int(byzantine)-honest, j, i)
		}
	}
	return metrics
}

type orderingResult struct {
	Nodes                   uint64              `json:"nodes"`
	Faults                  uint64              `json:"faults"`
	Byzantine               uint64              `json:"byzantine"`
	Gamma                   float64             `json:"gamma"`
	Transactions            int                 `json:"transactions"`
	LOSize                  int                 `json:"lo_size"`
	Ratio                   float64             `json:"ratio"`
	Seed                    int64               `json:"seed"`
	TraceDigest             string              `json:"trace_digest"`
	Rounds                  int                 `json:"rounds"`
	Batches                 []uint64            `json:"batch_indices"`
	MaxBatchSize            int                 `json:"max_batch_size"`
	MaliciousFreshOrders    int                 `json:"malicious_fresh_orders"`
	MaliciousReversedOrders int                 `json:"malicious_reversed_orders"`
	RetainedChunkResends    int                 `json:"retained_chunk_resends"`
	ReplicaStatesAgree      bool                `json:"replica_states_agree"`
	SelectedSenders         []uint64            `json:"selected_senders,omitempty"`
	ClaimedLists            [][]int             `json:"claimed_lists,omitempty"`
	Cost                    *orderingCostSample `json:"cost,omitempty"`
	orderingMetrics
}

// Native service-call costs, not network latency. FollowerAccept includes both
// verification and commit, since protocols place final ordering differently.
type orderingCostSample struct {
	LeaderBuildNS    int64 `json:"leader_build_ns"`
	LeaderCommitNS   int64 `json:"leader_commit_ns"`
	FollowerAcceptNS int64 `json:"follower_accept_ns"`
}

// Optional fixed senders select a single full-input proposal for matched-evidence
// comparisons. Existing callers retain the rotating, multi-round experiment.
func runOrdering(t testing.TB, trace orderingTrace, f, byzantine uint64, gamma float64, loSize int, seed int64, fixedSenders ...uint64) orderingResult {
	t.Helper()
	n, count := uint64(len(trace.ReceiptOrders)), len(trace.txs)
	if byzantine > f || count < 2 || loSize < 1 {
		t.Fatal("invalid ordering experiment dimensions")
	}
	if *orderingCost && len(fixedSenders) == 0 {
		t.Fatal("cost mode requires matched full-input evidence")
	}
	if len(fixedSenders) > 0 {
		seen := make(map[uint64]bool)
		for _, r := range fixedSenders {
			if r >= n || seen[r] {
				t.Fatal("invalid or repeated fixed sender")
			}
			seen[r] = true
		}
		if uint64(len(fixedSenders)) != n-f || loSize < count {
			t.Fatal("matched proposal requires n-F senders and full-input LOs")
		}
	}
	auth := newAblationAuthenticator(n, seed)
	net := newTestNetwork()
	sent := make(map[uint64]*types.LocalOrder)
	net.Register(0, func(message network.Message) {
		order, ok := message.Payload.(*types.LocalOrder)
		if !ok {
			t.Fatal("unexpected experiment network message")
		}
		sent[message.From] = cloneLocalOrder(order)
	})
	services := make([]*OFOService, n)
	for r := uint64(0); r < n; r++ {
		s, err := NewOFOService(r, n, f, gamma, net, loSize, nil, r >= n-byzantine, auth,
			newTestAdmission(), types.AuthContext{Epoch: 1, LeaderID: 0}, types.ProtocolGenesis{},
			func(*types.VerifiableFairOrderFragment, [32]byte) { t.Error("unexpected asynchronous candidate") })
		if err != nil {
			t.Fatal(err)
		}
		s.Stop() // Synchronous collection below, as in the existing ablations.
		services[r] = s
		// All contents are available before ordering starts; arrival times define
		// finite input profiles, not a simulated online wall-clock protocol.
		for _, i := range trace.ReceiptOrders[r] {
			s.HandleMessage(network.Message{Type: "Transaction", Payload: &trace.txs[i]})
		}
	}
	encoded, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	result := orderingResult{Nodes: n, Faults: f, Byzantine: byzantine, Gamma: gamma,
		Transactions: count, LOSize: loSize, Seed: seed, TraceDigest: fmt.Sprintf("%x", sha256.Sum256(encoded)),
		Batches: make([]uint64, count), ReplicaStatesAgree: true}
	leader := services[0]
	// Every sender is selected at least once in n rounds. Drain complete signed
	// input profiles, including omitted chunks and late occurrences of Done IDs.
	maxRounds := int(n) * ((count+loSize-1)/loSize + 2)
	for round := 1; round <= maxRounds; round++ {
		result.Rounds = round
		for r, s := range services {
			wasPending := s.localOrderPending != nil
			expected := append([]types.TxID(nil), s.receiptQueue[:min(loSize, len(s.receiptQueue))]...)
			var retained []types.TxID
			if wasPending {
				retained = append(retained, s.localOrderPending.OrderedTxs...)
			}
			s.GenerateAndSendLocalOrder() // Exactly the production malicious logic.
			order := sent[uint64(r)]
			if order == nil {
				t.Fatal("LO generation did not send")
			}
			if wasPending {
				result.RetainedChunkResends++
				if !sameIDs(order.OrderedTxs, retained) {
					t.Fatal("retransmission changed the retained chunk")
				}
			} else {
				if s.isMalicious {
					result.MaliciousFreshOrders++
					if len(expected) > 1 {
						result.MaliciousReversedOrders++
					}
					for i := range expected {
						if order.OrderedTxs[i] != expected[len(expected)-1-i] {
							t.Fatal("production malicious LO did not reverse its chunk")
						}
					}
				} else if !sameIDs(order.OrderedTxs, expected) {
					t.Fatal("honest LO changed receipt order")
				}
			}
		}
		var evidence []*types.LocalOrder
		senders := ablationSenders(n, f, leader.committed.FragmentSeq+1)
		if len(fixedSenders) > 0 {
			senders = fixedSenders
			result.SelectedSenders = append([]uint64(nil), senders...)
			indices := make(map[types.TxID]int, count)
			for i, tx := range trace.txs {
				indices[tx.ID] = i
			}
			for _, r := range senders {
				list := make([]int, len(sent[r].OrderedTxs))
				for pos, id := range sent[r].OrderedTxs {
					var ok bool
					list[pos], ok = indices[id]
					if !ok {
						t.Fatal("LO contains an unknown transaction")
					}
				}
				result.ClaimedLists = append(result.ClaimedLists, list)
			}
		}
		for _, r := range senders {
			evidence = append(evidence, sent[r])
		}
		started := time.Now()
		candidate, err := leader.constructCandidate(evidence)
		buildNS := time.Since(started).Nanoseconds()
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		committing := services
		if *orderingCost {
			// All ten native reports are generated; only leader and one follower
			// execute ordering, matching the Themis cost fixture.
			committing = services[:2]
			result.Cost = &orderingCostSample{LeaderBuildNS: buildNS}
			started = time.Now()
			if !services[1].verifyCandidate(candidate.fragment, 0) {
				t.Fatal("timed follower rejected candidate")
			}
			if _, ok := services[1].CommitPending(candidate.digest); !ok {
				t.Fatal("timed follower commit failed")
			}
			result.Cost.FollowerAcceptNS = time.Since(started).Nanoseconds()
			started = time.Now()
			if _, ok := leader.CommitPending(candidate.digest); !ok {
				t.Fatal("timed leader commit failed")
			}
			result.Cost.LeaderCommitNS = time.Since(started).Nanoseconds()
		} else {
			for _, follower := range services[1:] {
				if !follower.verifyCandidate(candidate.fragment, 0) {
					t.Fatalf("replica %d rejected round %d", follower.ReplicaID, round)
				}
			}
			for _, s := range services {
				if _, ok := s.CommitPending(candidate.digest); !ok {
					t.Fatal("commit failed")
				}
			}
		}
		for _, s := range committing[1:] {
			if s.committed.StateID != leader.committed.StateID || s.latestCommittedFragment != leader.latestCommittedFragment {
				t.Fatal("replicas diverged")
			}
		}
		if len(fixedSenders) > 0 {
			for _, tx := range trace.txs {
				if leader.committed.Done[tx.ID].BatchIndex == 0 {
					t.Fatal("full-input matched proposal did not finalize every transaction")
				}
			}
			break
		}
		drained := true
		for _, s := range services {
			if len(s.receiptQueue) != 0 {
				drained = false
			}
		}
		if drained {
			break
		}
	}
	batchSizes := make(map[uint64]int)
	for i, tx := range trace.txs {
		result.Batches[i] = leader.committed.Done[tx.ID].BatchIndex
		if result.Batches[i] > 0 {
			batchSizes[result.Batches[i]]++
		}
	}
	for _, size := range batchSizes {
		result.MaxBatchSize = max(result.MaxBatchSize, size)
	}
	result.orderingMetrics = measureOrdering(trace, result.Batches, f, byzantine, gamma)
	return result
}

func TestOrderingExperiment(t *testing.T) {
	if !*orderingExperiment {
		t.Skip("opt-in; use benchmark/run_ordering.py")
	}
	if *orderingNodes == 0 || *orderingTransactions < 2 || *orderingLOSize < 1 || *orderingRatio < 0 || math.IsNaN(*orderingRatio) || math.IsInf(*orderingRatio, 0) {
		t.Fatal("invalid ordering experiment input")
	}
	trace := makeOrderingTrace(*orderingNodes, *orderingTransactions, *orderingRatio, *orderingSeed)
	if *orderingTraceFile != "" {
		encoded, err := json.Marshal(trace)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*orderingTraceFile, encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
	result := runOrdering(t, trace, *orderingFaults, *orderingByzantine, *orderingGamma, *orderingLOSize, *orderingSeed)
	result.Ratio = *orderingRatio
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("ORDERING_RESULT %s\n", encoded)
}

func TestOrderingOracleDistinguishesTiesReversalsAndMissingPredecessors(t *testing.T) {
	trace := makeOrderingTrace(5, 3, 0, 1)
	for _, tc := range []struct {
		batches                                                  []uint64
		correct, tied, reversed, unresolved, violations, missing int
	}{
		{[]uint64{1, 2, 3}, 3, 0, 0, 0, 0, 0},
		{[]uint64{1, 1, 2}, 2, 1, 0, 0, 0, 0},
		{[]uint64{3, 2, 1}, 0, 0, 3, 0, 3, 0},
		{[]uint64{0, 1, 2}, 1, 0, 0, 2, 2, 2},
	} {
		m := measureOrdering(trace, tc.batches, 1, 1, 1)
		if m.CorrectPairs != tc.correct || m.TiedPairs != tc.tied || m.ReversedPairs != tc.reversed || m.UnresolvedPairs != tc.unresolved || m.FairnessViolations != tc.violations || m.MissingPredecessors != tc.missing {
			t.Fatalf("batches=%v: %+v", tc.batches, m)
		}
	}
	// Only three honest replicas support 0<1: a malicious report must not raise
	// that count to qH=4; the observed reversal is outside the formal guarantee.
	trace = makeOrderingTrace(5, 2, 0, 1)
	trace.ReceiptOrders[3] = []int{1, 0}
	if m := measureOrdering(trace, []uint64{2, 1}, 1, 1, 1); m.FairnessEligiblePairs != 0 || m.ReversedPairs != 1 {
		t.Fatalf("oracle used malicious support: %+v", m)
	}
}

func TestOrderingProductionReplay(t *testing.T) {
	for _, ratio := range []float64{0, 10} {
		trace := makeOrderingTrace(10, 24, ratio, 7)
		var digest string
		for b := uint64(0); b <= 2; b++ {
			result := runOrdering(t, trace, 2, b, 1, 5, 7)
			if result.CompletedTransactions != 24 || result.FairnessViolations != 0 {
				t.Fatalf("ratio=%g b=%d: %+v", ratio, b, result)
			}
			if ratio == 0 && result.CorrectPairs != result.TotalPairs {
				t.Fatal("unanimous original order was not preserved")
			}
			if ratio > 0 && result.ReceiveDisagreementPairs == 0 {
				t.Fatal("trace did not introduce receipt differences")
			}
			if b > 0 && result.MaliciousReversedOrders == 0 {
				t.Fatal("malicious production path not exercised")
			}
			if result.RetainedChunkResends == 0 {
				t.Fatal("omitted chunk path not exercised")
			}
			if digest != "" && result.TraceDigest != digest {
				t.Fatal("b changed the input trace")
			}
			digest = result.TraceDigest
		}
	}
}
