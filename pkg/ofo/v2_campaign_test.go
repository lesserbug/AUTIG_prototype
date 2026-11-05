package ofo

import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var v2Campaign = flag.Bool("v2-campaign", false, "Run full finite-profile v2 fairness and granularity campaign")
var v2CampaignSize = flag.Int("v2-campaign-size", 650, "Transactions per v2 campaign profile")
var v2CampaignOutput = flag.String("v2-campaign-output", "", "Machine-readable campaign result")

// The physical cycle audit now runs in every profile; the flag is kept so
// existing reproduction scripts remain valid.
var _ = flag.Bool("v2-cycle-audit", true, "Accepted for compatibility; the physical audit always runs")

type v2CampaignCase struct {
	N, F, Byzantine                     uint64
	Gamma                               float64
	Rho                                 float64
	D                                   uint64
	Seed                                int64
	Transactions, Fragments, OpenEvents int
	CheckedFairness, KnownOrderChecks   int64
	Violations                          int
	MaxBatchFraction                    float64
	MaxSolidExecutionWait               int
	MaxSolidAssignmentWait              int
	MaxExecutionSettlementLag           int
	AtFragment5                         ProgressStats
	Curve                               []ProgressStats
	Scope                               string
	CycleAudit                          *v2CycleAuditResult `json:",omitempty"`
}

func TestV2FairnessCampaign(t *testing.T) {
	if !*v2Campaign {
		t.Skip("opt-in campaign")
	}
	var rows []v2CampaignCase
	for _, cfg := range []struct {
		n, f  uint64
		gamma float64
	}{{10, 2, 1}, {17, 4, 1}, {10, 1, 0.8}, {13, 2, 0.8}} {
		for _, rho := range []float64{.1, 1, 10, 100} {
			for _, delay := range []uint64{0, 2, 4} {
				for _, seed := range []int64{1, 7, 19} {
					for _, byz := range []uint64{0, cfg.f} {
						row := runV2Profile(t, cfg.n, cfg.f, byz, cfg.gamma, rho, delay, seed, *v2CampaignSize)
						rows = append(rows, row)
						if *v2CampaignOutput != "" {
							data, err := json.MarshalIndent(rows, "", "  ")
							if err != nil {
								t.Fatal(err)
							}
							if err = os.MkdirAll(filepath.Dir(*v2CampaignOutput), 0755); err != nil {
								t.Fatal(err)
							}
							if err = os.WriteFile(*v2CampaignOutput, data, 0644); err != nil {
								t.Fatal(err)
							}
						}
						revisionResult(t, "v2_fairness_profile", map[string]any{"n": row.N, "f": row.F, "gamma": row.Gamma, "byzantine": row.Byzantine, "rho": rho, "D": delay, "seed": seed, "transactions": row.Transactions, "fragments": row.Fragments, "checked_fairness": row.CheckedFairness, "known_order_checks": row.KnownOrderChecks, "violations": row.Violations, "open_events": row.OpenEvents, "max_batch_fraction": row.MaxBatchFraction, "max_solid_execution_wait": row.MaxSolidExecutionWait, "max_solid_assignment_wait": row.MaxSolidAssignmentWait, "physical_inversions": row.CycleAudit.PhysicalInversions, "max_execution_settlement_lag": row.MaxExecutionSettlementLag})
					}
				}
			}
		}
	}
}

func runV2Profile(t testing.TB, n, f, byz uint64, gamma, rho float64, delay uint64, seed int64, count int) v2CampaignCase {
	t.Helper()
	const limit = 50
	trace := makeOrderingTrace(n, count, rho, seed)
	s, auth, admission := revisionService(t, n, f, gamma, limit)
	if err := s.EnableContinuation(delay); err != nil {
		t.Fatal(err)
	}
	for _, tx := range trace.txs {
		if err := admission.Store(tx); err != nil {
			t.Fatal(err)
		}
	}
	for _, i := range trace.ReceiptOrders[0] {
		s.HandleMessage(network.Message{Payload: &trace.txs[i]})
	}
	followers := []*OFOService{v2FollowerWithMode(s, StructuralVerification), v2FollowerWithMode(s, SymmetricIncrementalVerification), v2FollowerWithMode(s, ReconstructionVerification)}
	for _, service := range followers {
		for _, i := range trace.ReceiptOrders[1] {
			service.HandleMessage(network.Message{Payload: &trace.txs[i]})
		}
	}
	indexOf := make(map[types.TxID]int, count)
	for i, tx := range trace.txs {
		indexOf[tx.ID] = i
	}
	position := make([][]int, n)
	for r, profile := range trace.ReceiptOrders {
		position[r] = make([]int, count)
		for p, i := range profile {
			position[r][i] = p
		}
	}
	qh := int(types.CalculateHonestThreshold(n, f, gamma))
	pred := make([][]int, count)
	for x := 0; x < count; x++ {
		for y := 0; y < count; y++ {
			if x == y {
				continue
			}
			support := 0
			for r := uint64(0); r < n-byz; r++ {
				if position[r][x] < position[r][y] {
					support++
				}
			}
			if support >= qh {
				pred[y] = append(pred[y], x)
			}
		}
	}
	executed := make([]uint64, count)
	execAt := make([]int, count)
	assignAt := make([]int, count)
	solidAt := make([]int, count)
	settled := make([]bool, count)
	pending := make([][]types.TxID, n)
	hasPending := make([]bool, n)
	row := v2CampaignCase{N: n, F: f, Byzantine: byz, Gamma: gamma, Rho: rho, D: delay, Seed: seed, Transactions: count, Scope: "finite receipt-profile replay; native signatures/construction/three validation modes/commit; fairness at settlement prefixes; waiting measured in fragments, not network wall time"}
	maxRounds := int(n)*((count+limit-1)/limit+3) + int(delay) + 2
	for round := 1; round <= maxRounds; round++ {
		r := uint64(round)
		for sender := uint64(0); sender < n; sender++ {
			if !hasPending[sender] {
				start := int(s.committed.Seq[sender])
				end := min(start+limit, count)
				chunk := make([]types.TxID, 0, end-start)
				for _, i := range trace.ReceiptOrders[sender][start:end] {
					chunk = append(chunk, trace.txs[i].ID)
				}
				if sender >= n-byz {
					for i, j := 0, len(chunk)-1; i < j; i, j = i+1, j-1 {
						chunk[i], chunk[j] = chunk[j], chunk[i]
					}
				}
				pending[sender], hasPending[sender] = chunk, true
			}
		}
		selected := ablationSenders(n, f, r)
		var orders []*types.LocalOrder
		for _, sender := range selected {
			orders = append(orders, signedOrder(t, auth, s.committed, sender, r, pending[sender]...))
		}
		candidate, err := s.constructCandidate(orders)
		if err != nil {
			t.Fatal(err)
		}
		for id, kind := range candidate.preState.states {
			if kind == types.StateSolid {
				i := indexOf[id]
				if solidAt[i] == 0 {
					solidAt[i] = round
				}
			}
		}
		if c := candidate.fragment.Continuation; c != nil && c.Opened {
			row.OpenEvents++
		}
		for _, service := range followers {
			if !service.verifyCandidate(candidate.fragment, 0) {
				t.Fatalf("mode%d rejected profile n%d rho%g D%d seed%d round%d", service.verificationMode, n, rho, delay, seed, round)
			}
			if _, ok := service.CommitPending(candidate.digest); !ok {
				t.Fatal("follower commit")
			}
		}
		batches, ok := s.CommitPending(candidate.digest)
		if !ok {
			t.Fatal("leader commit")
		}
		ordinal := make(map[types.TxID]int)
		component := make(map[types.TxID]int)
		partOffset := 0
		for _, claim := range candidate.fragment.Certificate.Part {
			for _, id := range claim.Transactions {
				component[id] = partOffset
			}
			partOffset++
		}
		if cp := candidate.fragment.ContinuationProof; cp != nil {
			for _, claim := range cp.Part {
				for _, id := range claim.Transactions {
					component[id] = partOffset
				}
				partOffset++
			}
		}
		for _, batch := range batches {
			for _, id := range batch.Transactions {
				i := indexOf[id]
				if executed[i] != 0 {
					t.Fatal("duplicate execution")
				}
				executed[i], execAt[i] = batch.Index, round
				if assignAt[i] == 0 {
					assignAt[i] = round
				}
			}
		}
		if c := candidate.fragment.Continuation; c != nil {
			for _, id := range c.Assigned {
				if i := indexOf[id]; assignAt[i] == 0 {
					assignAt[i] = round
				}
			}
		}
		// Closed SCC order is checked against known edges; continuation
		// order is checked by the full-input physical oracle below.
		ordinalIndex := 0
		for _, batch := range candidate.fragment.FinalOrder.Batches {
			for _, id := range batch.Transactions {
				ordinalIndex++
				ordinal[id] = ordinalIndex
			}
		}
		// Independent oracle reconstructs known pair directions from positions,
		// rather than trusting the cache or the certificate graph.
		for u, ou := range ordinal {
			for v, ov := range ordinal {
				if u == v || component[u] == component[v] {
					continue
				}
				uv, vu := 0, 0
				for sender := uint64(0); sender < n; sender++ {
					a, b := candidate.preState.Pos[sender][u], candidate.preState.Pos[sender][v]
					if a > 0 && b > 0 {
						if a < b {
							uv++
						} else {
							vu++
						}
					}
				}
				threshold := int(n) - qh + 1
				if uv >= threshold && (uv > vu || (uv == vu && types.LessTxID(u, v))) {
					row.KnownOrderChecks++
					if ou >= ov {
						t.Fatal("known inter-SCC dependency reversed")
					}
				}
			}
		}
		var newSettled []types.TxID
		for _, batch := range candidate.fragment.FinalOrder.Batches {
			newSettled = append(newSettled, batch.Transactions...)
		}
		if c := candidate.fragment.Continuation; c != nil {
			newSettled = append(newSettled, c.Settled...)
		}
		for _, id := range newSettled {
			y := indexOf[id]
			if settled[y] {
				t.Fatal("duplicate settlement")
			}
			settled[y] = true
			for _, x := range pred[y] {
				row.CheckedFairness++
				// Assignment prefix: when y executes, x is executed or already
				// assigned to y's open batch; the physical audit checks that a
				// later execution of x stays inside one weak-precedence cycle.
				open, assigned := s.committed.OpenMembers[trace.txs[x].ID]
				if assigned && open.BatchIndex == executed[y] {
					continue
				}
				if executed[x] == 0 || executed[x] > executed[y] {
					row.Violations++
					t.Fatalf("settled fairness x%d y%d n%d rho%g D%d seed%d round%d", x, y, n, rho, delay, seed, round)
				}
			}
			if lag := round - execAt[y]; lag > row.MaxExecutionSettlementLag {
				row.MaxExecutionSettlementLag = lag
			}
		}
		for _, sender := range selected {
			hasPending[sender] = false
			pending[sender] = nil
		}
		progress := s.Progress()
		row.Curve = append(row.Curve, progress)
		row.Fragments = round
		if round == 5 {
			row.AtFragment5 = progress
		}
		for _, service := range followers {
			if service.committed.StateID != s.committed.StateID || service.committed.DoneRoot != s.committed.DoneRoot || service.Progress() != progress {
				t.Fatal("matched verification modes diverged")
			}
		}
		if progress.Settled == count {
			break
		}
	}
	if s.Progress().Settled != count {
		t.Fatalf("finite profile failed to settle n%d rho%g D%d seed%d", n, rho, delay, seed)
	}
	batchSizes := make(map[uint64]int)
	for i, batch := range executed {
		batchSizes[batch]++
		if wait := execAt[i] - solidAt[i]; wait > row.MaxSolidExecutionWait {
			row.MaxSolidExecutionWait = wait
		}
		if assignAt[i] > solidAt[i] {
			wait := assignAt[i] - solidAt[i]
			if wait > row.MaxSolidAssignmentWait {
				row.MaxSolidAssignmentWait = wait
			}
			if wait > int(delay) {
				t.Fatal("public delay bound on batch assignment violated")
			}
		}
	}
	for _, size := range batchSizes {
		fraction := float64(size) / float64(count)
		if fraction > row.MaxBatchFraction {
			row.MaxBatchFraction = fraction
		}
	}
	// Physical cycle-refined fairness: a receipt-order fairness pair may be
	// executed out of order only inside one weak-precedence component of the
	// original correct receipt profiles.
	row.CycleAudit = auditV2InputCycles(s, trace.txs, position, int(n-byz), qh)
	if row.CycleAudit.PhysicalInversions != 0 {
		t.Fatalf("cross-component physical inversion %v n%d gamma%g rho%g D%d seed%d", row.CycleAudit.FirstInversion, n, gamma, rho, delay, seed)
	}
	return row
}

func TestV2ProfileSmoke(t *testing.T) {
	for _, cfg := range []struct {
		n, f, byz uint64
		gamma     float64
	}{{10, 2, 2, 1}, {10, 1, 1, 0.8}, {7, 0, 0, 0.6}} {
		for _, rho := range []float64{.1, 10, 100} {
			for _, d := range []uint64{0, 2} {
				row := runV2Profile(t, cfg.n, cfg.f, cfg.byz, cfg.gamma, rho, d, 7, 80)
				if row.Violations != 0 || row.CheckedFairness == 0 {
					t.Fatal(fmt.Sprint(row))
				}
			}
		}
	}
}
