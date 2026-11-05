package ofo

// Revision experiments call the actual authenticated candidate/commit path.
// Deliberate cache corruption and graph-only witness fixtures are labeled as
// such: they are not claims about attacks through the normal wire protocol.
import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func revisionResult(t testing.TB, kind string, values map[string]any) {
	t.Helper()
	values["experiment"] = kind
	encoded, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("REVISION_RESULT " + string(encoded))
}

func revisionService(t testing.TB, n, f uint64, gamma float64, limit int) (*OFOService, *testAuthenticator, *testAdmission) {
	t.Helper()
	auth, admission := newAblationAuthenticator(n, 20261030), newTestAdmission()
	s, err := NewOFOService(0, n, f, gamma, newTestNetwork(), limit, nil, false, auth, admission,
		types.AuthContext{Epoch: 1, LeaderID: 0, Identifier: [32]byte{7}},
		types.ProtocolGenesis{StateID: [32]byte{1}, FragmentDigest: [32]byte{2}}, func(*types.VerifiableFairOrderFragment, [32]byte) {})
	if err != nil {
		t.Fatal(err)
	}
	s.Stop()
	return s, auth, admission
}

func revisionFollower(s *OFOService) *OFOService {
	return &OFOService{ReplicaID: 1, auth: s.auth, admission: s.admission, authContext: s.authContext, continuationEnabled: s.continuationEnabled, continuationDelay: s.continuationDelay, policyID: s.policyID, txSubmissionTimes: make(map[types.TxID]time.Time),
		committed: cloneForTestReplica(s.committed), latestCommittedFragment: s.latestCommittedFragment,
		replicaCount: s.replicaCount, fFaulty: s.fFaulty, gamma: s.gamma, loMaxSize: s.loMaxSize}
}

func TestRevisionDecimalFeasibilityBoundary(t *testing.T) {
	// Decimal gamma=.56 gives ceil(.56*25)=14, so 28 < 26+2+1.
	// Binary multiplication used to yield 14.000000000000002 and accept 15.
	auth, admission := newAblationAuthenticator(26, 7), newTestAdmission()
	s, err := NewOFOService(0, 26, 1, .56, newTestNetwork(), 10, nil, false, auth, admission,
		types.AuthContext{Epoch: 1, Identifier: [32]byte{7}}, types.ProtocolGenesis{}, func(*types.VerifiableFairOrderFragment, [32]byte) {})
	if err != nil {
		revisionResult(t, "decimal_feasibility", map[string]any{"n": 26, "f": 1, "gamma": .56, "exact_q_h": 14, "invalid_configuration_rejected": true})
		return
	}
	s.Stop()
	ids := make([]types.TxID, 2)
	for i := range ids {
		content := []byte(fmt.Sprintf("decimal-boundary/%d", i))
		ids[i] = types.TransactionID(content)
		if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
			t.Fatal(err)
		}
	}
	// Force ID(y)<ID(x), so a false tie breaks against fourteen correct votes.
	types.SortTxIDs(ids)
	y, x := ids[0], ids[1]
	var orders []*types.LocalOrder
	for r := uint64(0); r < 25; r++ {
		var chunk []types.TxID
		if r < 12 {
			chunk = []types.TxID{x, y}
		} else if r < 24 {
			chunk = []types.TxID{y, x}
		}
		orders = append(orders, signedOrder(t, auth, s.committed, r, 1, chunk...))
	}
	// Sender 23 is Byzantine. Correct 24 reports an empty prefix and correct
	// 25 is omitted; both actually received x before y. Correct support=14.
	candidate, buildErr := s.constructCandidate(orders)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	follower := revisionFollower(s)
	accepted := follower.verifyCandidate(candidate.fragment, 0)
	batches := map[types.TxID]uint64{}
	for _, batch := range candidate.fragment.FinalOrder.Batches {
		for _, id := range batch.Transactions {
			batches[id] = batch.Index
		}
	}
	violates := accepted && batches[y] < batches[x]
	revisionResult(t, "decimal_feasibility", map[string]any{"n": 26, "f": 1, "gamma": .56, "exact_q_h": 14, "implementation_edge_threshold": types.CalculateEdgeThreshold(26, 1, .56), "invalid_configuration_rejected": false, "authenticated_candidate_accepted": accepted, "correct_forward_receivers": 14, "forward_weight": candidate.preState.weights[edgeKey{x, y}], "reverse_weight": candidate.preState.weights[edgeKey{y, x}], "fairness_violation": violates})
	t.Fatal("decimal-infeasible configuration accepted; authenticated fairness witness reproduced")
}

func revisionProfile(group, length int) []int {
	var out []int
	if group == 1 {
		out = []int{0}
	}
	if group == 2 {
		out = []int{1, 0}
	}
	for start := 2 + group; len(out) < length; start += 3 {
		out = append(out, start, start-1, start-2)
	}
	return out[:length]
}

func TestRevisionCurrentPolicyStreamingWitness(t *testing.T) {
	for _, cfg := range []struct {
		n, f          uint64
		rounds, chunk int
	}{{3, 0, 5, 100}, {7, 1, 5, 100}} {
		t.Run(fmt.Sprintf("n%d_f%d", cfg.n, cfg.f), func(t *testing.T) {
			leader, auth, admission := revisionService(t, cfg.n, cfg.f, 1, cfg.chunk)
			follower := revisionFollower(leader)
			ids := make([]types.TxID, cfg.rounds*cfg.chunk+10)
			for i := range ids {
				content := []byte(fmt.Sprintf("streaming/n%d/%d", cfg.n, i))
				ids[i] = types.TransactionID(content)
				if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
					t.Fatal(err)
				}
			}
			profiles := make(map[uint64][]int)
			for r := uint64(0); r < cfg.n; r++ {
				if cfg.f == 1 && r == 0 {
					continue
				}
				group := int(r)
				if cfg.f == 1 {
					group = int((r - 1) / 2)
				}
				profiles[r] = revisionProfile(group, cfg.rounds*cfg.chunk)
			}
			for round := 1; round <= cfg.rounds; round++ {
				var orders []*types.LocalOrder
				for _, r := range ablationSenders(cfg.n, cfg.f, uint64(round)) {
					var chunk []types.TxID
					if p, ok := profiles[r]; ok {
						start := int(leader.committed.Seq[r])
						end := min(start+cfg.chunk, len(p))
						for _, j := range p[start:end] {
							chunk = append(chunk, ids[j])
						}
					}
					orders = append(orders, signedOrder(t, auth, leader.committed, r, uint64(round), chunk...))
				}
				candidate, err := leader.constructCandidate(orders)
				if err != nil {
					t.Fatal(err)
				}
				if len(candidate.fragment.FinalOrder.Batches) != 0 {
					t.Fatal("current-policy counterexample unexpectedly released transactions")
				}
				if !follower.verifyCandidate(candidate.fragment, 0) {
					t.Fatal("follower rejected the valid blocking witness")
				}
				if _, ok := follower.CommitPending(candidate.digest); !ok {
					t.Fatal("follower commit failed")
				}
				if _, ok := leader.CommitPending(candidate.digest); !ok {
					t.Fatal("leader commit failed")
				}
				assertAblationState(t, follower.committed, leader.committed)
				solid, shaded := 0, 0
				for _, status := range leader.committed.states {
					if status == types.StateSolid {
						solid++
					}
					if status == types.StateShaded {
						shaded++
					}
				}
				revisionResult(t, "current_policy_stream", map[string]any{"n": cfg.n, "f": cfg.f, "round": round, "chunk": cfg.chunk, "solid": solid, "shaded": shaded, "live": len(leader.committed.Live), "done": leader.committed.doneCount(), "x0_solid": leader.committed.states[ids[0]] == types.StateSolid, "native_authenticated_path": true})
			}
		})
	}
}

func revisionEmptyOrders(t testing.TB, x *ablationFixture) []*types.LocalOrder {
	var orders []*types.LocalOrder
	for _, r := range ablationSenders(x.base.replicaCount, x.base.fFaulty, x.base.committed.FragmentSeq+1) {
		orders = append(orders, signedOrder(t, x.auth, x.base.committed, r, x.base.committed.FragmentSeq+1))
	}
	return orders
}

func TestRevisionPruningCompletenessWitness(t *testing.T) {
	x := makeAblationFixture(t, 10, 1, .9, ablationCase{name: "pruning", history: 30, fresh: 6, blocked: true}, 7)
	pre := x.base.committed.clone()
	nodes, pairs, err := applyEvidence(pre, revisionEmptyOrders(t, x), pre.Epoch, pre.FragmentSeq+1, 10, 1, .9, 200, x.auth, x.admission)
	if err != nil {
		t.Fatal(err)
	}
	dm := x.base.UtigManager.clone()
	for u := range dm.edges {
		for v := range dm.edges[u] {
			if pre.states[u] == types.StateShaded && pre.states[v] == types.StateSolid {
				delete(dm.edges[u], v)
				delete(dm.inverseEdges[v], u)
			}
		}
	}
	dm.refresh(pre, nodes, pairs)
	wrong, cert, err := dm.buildProposal(pre)
	if err != nil {
		t.Fatal(err)
	}
	rejected := verifyStructuralCertificate(pre, wrong, cert, 10, 1, .9)
	if len(wrong.Batches) == 0 || rejected == nil {
		t.Fatal("missing-edge witness did not reach frontier rejection")
	}
	restored := rebuildDependencyManagerForTest(pre, 10, 1, .9)
	correct, proof, err := restored.buildProposal(pre)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyStructuralCertificate(pre, correct, proof, 10, 1, .9); err != nil {
		t.Fatal(err)
	}
	revisionResult(t, "pruning_completeness", map[string]any{"empty_touched_nodes": len(nodes), "empty_touched_pairs": len(pairs), "unsafe_candidate_rejected": true, "rejection": rejected.Error(), "rebuild_restores_validity": true})
}

func TestRevisionMissingWeightIsNotZeroWitness(t *testing.T) {
	leader, auth, admission := revisionService(t, 5, 1, 1, 10)
	x, y := testTx(t, admission, 73), testTx(t, admission, 74)
	var orders []*types.LocalOrder
	for r := uint64(1); r < 5; r++ {
		var ids []types.TxID
		if r < 3 {
			ids = []types.TxID{x, y}
		} else if r == 3 {
			ids = []types.TxID{y}
		}
		orders = append(orders, signedOrder(t, auth, leader.committed, r, 1, ids...))
	}
	pre := leader.committed.clone()
	if _, _, err := applyEvidence(pre, orders, 1, 1, 5, 1, 1, 10, auth, admission); err != nil {
		t.Fatal(err)
	}
	corrupted := pre.clone()
	clear(corrupted.weights)
	dm := rebuildDependencyManagerForTest(corrupted, 5, 1, 1)
	wrong, cert, err := dm.buildProposal(corrupted)
	if err != nil {
		t.Fatal(err)
	}
	badAccept := verifyStructuralCertificate(corrupted, wrong, cert, 5, 1, 1) == nil
	goodError := verifyStructuralCertificate(pre, wrong, cert, 5, 1, 1)
	goodReject := goodError != nil
	if !badAccept || !goodReject {
		t.Fatal("cache-completeness witness failed")
	}
	revisionResult(t, "cache_completeness_contract", map[string]any{"forced_cache_deletion": true, "normal_api_has_eviction": false, "incomplete_cache_accepts_unsafe_output": badAccept, "complete_cache_rejects_same_output": goodReject, "rejection": goodError.Error(), "shaded_predecessor_visibility": pre.visibility[x], "solid_successor_visibility": pre.visibility[y]})
}

func TestRevisionNilWireObjects(t *testing.T) {
	leader, _, _ := revisionService(t, 5, 1, 1, 10)
	follower := revisionFollower(leader)
	for _, tc := range []struct {
		name    string
		s       *OFOService
		payload any
	}{{"fragment", follower, (*types.VerifiableFairOrderFragment)(nil)}, {"local-order", leader, (*types.LocalOrder)(nil)}, {"transaction", follower, (*types.Transaction)(nil)}} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("nil wire object panicked: %v", recovered)
				}
			}()
			tc.s.HandleMessage(network.Message{From: 0, Payload: tc.payload})
		})
	}
}

func TestRevisionReceiptQueueSurvivesCandidateAbandon(t *testing.T) {
	leader, _, _ := revisionService(t, 5, 1, 1, 10)
	content := []byte("receipt-during-rejected-candidate")
	tx := &types.Transaction{ID: types.TransactionID(content), CanonicalBytes: content}
	leader.HandleMessage(network.Message{Payload: tx})
	before := append([]types.TxID(nil), leader.receiptQueue...)
	var orders []*types.LocalOrder
	for _, r := range ablationSenders(5, 1, 1) {
		orders = append(orders, signedOrder(t, leader.auth, leader.committed, r, 1))
	}
	candidate, err := leader.constructCandidate(orders)
	if err != nil {
		t.Fatal(err)
	}
	if !leader.AbandonPending(candidate.digest) || !reflect.DeepEqual(before, leader.receiptQueue) || !leader.observed[tx.ID] {
		t.Fatal("abandon changed observed receipts")
	}
	revisionResult(t, "receipt_rollback_isolation", map[string]any{"receipt_preserved": true, "committed_sequence": leader.committed.FragmentSeq})
}
