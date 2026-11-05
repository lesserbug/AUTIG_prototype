package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func cloneV2Fragment(t testing.TB, f *types.VerifiableFairOrderFragment) *types.VerifiableFairOrderFragment {
	t.Helper()
	wire, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var copy types.VerifiableFairOrderFragment
	if err = json.Unmarshal(wire, &copy); err != nil {
		t.Fatal(err)
	}
	return &copy
}

func v2Signed(t testing.TB, s *OFOService, f *types.VerifiableFairOrderFragment) {
	t.Helper()
	signature, err := s.auth.SignLeader(s.authContext, types.FragmentDigest(f))
	if err != nil {
		t.Fatal(err)
	}
	f.LeaderSignature = signature
}

func TestV2NativePersistentChain(t *testing.T) {
	for _, n := range []uint64{3, 7} {
		for _, delay := range []uint64{0, 1, 2, 4} {
			t.Run(fmt.Sprintf("n%d_D%d", n, delay), func(t *testing.T) {
				f := uint64(0)
				if n == 7 {
					f = 1
				}
				s, auth, admission := revisionService(t, n, f, 1, 12)
				if err := s.EnableContinuation(delay); err != nil {
					t.Fatal(err)
				}
				followers := []*OFOService{v2FollowerWithMode(s, StructuralVerification), v2FollowerWithMode(s, SymmetricIncrementalVerification), v2FollowerWithMode(s, ReconstructionVerification)}
				const rounds = 15
				ids := make([]types.TxID, rounds*12+12)
				for i := range ids {
					content := []byte(fmt.Sprintf("v2-stream/%d/%d", n, i))
					ids[i] = types.TransactionID(content)
					if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
						t.Fatal(err)
					}
				}
				profiles := make(map[uint64][]int)
				for r := uint64(0); r < n; r++ {
					if f == 1 && r == 0 {
						continue
					}
					group := int(r)
					if f == 1 {
						group = int((r - 1) / 2)
					}
					profiles[r] = revisionProfile(group, rounds*12)
				}
				executed := make(map[types.TxID]ExecutionRecord)
				firstX := uint64(0)
				opened := 0
				for r := uint64(1); r <= rounds; r++ {
					var orders []*types.LocalOrder
					for _, sender := range ablationSenders(n, f, r) {
						var chunk []types.TxID
						if p, correct := profiles[sender]; correct {
							start := int(s.committed.Seq[sender])
							for _, i := range p[start:min(start+12, len(p))] {
								chunk = append(chunk, ids[i])
							}
						}
						orders = append(orders, signedOrder(t, auth, s.committed, sender, r, chunk...))
					}
					candidate, err := s.constructCandidate(orders)
					if err != nil {
						t.Fatal(err)
					}
					if candidate.fragment.Continuation != nil && candidate.fragment.Continuation.Opened {
						opened++
					}
					for _, follower := range followers {
						var before *DependencyManager
						if follower.verificationMode == SymmetricIncrementalVerification {
							before = follower.UtigManager.clone()
						}
						if !follower.verifyCandidate(candidate.fragment, 0) {
							t.Fatalf("mode %d rejected native stream", follower.verificationMode)
						}
						if before != nil && !reflect.DeepEqual(before, follower.UtigManager) {
							t.Fatal("trial changed committed graph before commit")
						}
						if !follower.AbandonPending(candidate.digest) {
							t.Fatal("abandon failed")
						}
						if before != nil && !reflect.DeepEqual(before, follower.UtigManager) {
							t.Fatal("abandon changed graph")
						}
						if !follower.verifyCandidate(candidate.fragment, 0) {
							t.Fatal("reverify after rollback")
						}
						if _, ok := follower.CommitPending(candidate.digest); !ok {
							t.Fatal("follower commit")
						}
					}
					batches, ok := s.CommitPending(candidate.digest)
					if !ok {
						t.Fatal("leader commit")
					}
					ordinal := uint64(0)
					for _, batch := range batches {
						for _, id := range batch.Transactions {
							ordinal++
							if _, duplicate := executed[id]; duplicate {
								t.Fatal("duplicate execution")
							}
							executed[id] = ExecutionRecord{BatchIndex: batch.Index, FragmentSeq: r, Ordinal: ordinal}
							if id == ids[0] {
								firstX = r
							}
						}
					}
					for _, follower := range followers {
						assertAblationState(t, s.committed, follower.committed)
					}
					checkV2SettledFairness(t, s, profiles, ids, executed)
				}
				if firstX != delay+1 {
					t.Fatalf("x0 at %d, want %d", firstX, delay+1)
				}
				if s.committed.doneCount() == 0 || opened == 0 {
					t.Fatal("continuation did not settle native stream")
				}
				progress := s.Progress()
				revisionResult(t, "v2_native_stream", map[string]any{"n": n, "f": f, "D": delay, "fragments": rounds, "first_x0_execution": firstX, "executed": progress.Executed, "settled": progress.Settled, "live": progress.Live, "open_members": progress.OpenMembers, "native_authenticated_commits": true, "settled_fairness_checked": true})
			})
		}
	}
}

func checkV2SettledFairness(t testing.TB, s *OFOService, profiles map[uint64][]int, ids []types.TxID, executed map[types.TxID]ExecutionRecord) {
	t.Helper()
	positions := make(map[uint64]map[types.TxID]int)
	for sender, profile := range profiles {
		p := make(map[types.TxID]int)
		for i, index := range profile {
			p[ids[index]] = i + 1
		}
		positions[sender] = p
	}
	qh := int(types.CalculateHonestThreshold(s.replicaCount, s.fFaulty, s.gamma))
	for y, yr := range s.committed.doneEntries() {
		for _, x := range ids {
			if x == y {
				continue
			}
			support := 0
			received := true
			for _, p := range positions {
				if p[x] == 0 || p[y] == 0 {
					received = false
					break
				}
				if p[x] < p[y] {
					support++
				}
			}
			if !received || support < qh {
				continue
			}
			if open, assigned := s.committed.OpenMembers[x]; assigned && open.BatchIndex == yr.BatchIndex {
				continue
			}
			xr, exists := executed[x]
			if !exists || xr.BatchIndex > yr.BatchIndex {
				t.Fatalf("settled fairness x=%x y=%x", x, y)
			}
		}
	}
}

func v2BlockingCandidate(t testing.TB) (*OFOService, *OFOService, *pendingCandidate) {
	t.Helper()
	s, auth, admission := revisionService(t, 7, 1, 1, 12)
	if err := s.EnableContinuation(0); err != nil {
		t.Fatal(err)
	}
	ids := make([]types.TxID, 30)
	for i := range ids {
		content := []byte(fmt.Sprintf("v2-attack/%d", i))
		ids[i] = types.TransactionID(content)
		if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
			t.Fatal(err)
		}
	}
	var orders []*types.LocalOrder
	for r := uint64(1); r < 7; r++ {
		p := revisionProfile(int((r-1)/2), 12)
		chunk := make([]types.TxID, len(p))
		for i, index := range p {
			chunk[i] = ids[index]
		}
		orders = append(orders, signedOrder(t, auth, s.committed, r, 1, chunk...))
	}
	c, err := s.constructCandidate(orders)
	if err != nil {
		t.Fatal(err)
	}
	if c.fragment.Continuation == nil || len(c.fragment.Continuation.Joins) == 0 || len(c.fragment.Continuation.Assigned) == 0 {
		t.Fatal("fixture did not open with executed and waiting members")
	}
	return s, revisionFollower(s), c
}

func TestV2AuthenticatedContinuationAttacks(t *testing.T) {
	s, follower, c := v2BlockingCandidate(t)
	before := cloneForTestReplica(follower.committed)
	kinds := []string{"nil-transition", "nil-proof", "wrong-policy", "downgrade", "root-omission", "forest-depth", "forest-cycle", "forest-parent", "duplicate-join", "premature-settle", "omit-settle", "wrong-index", "false-close", "early-shaded-execution", "merge-rounds", "drop-assigned", "post-state", "unused-root-parent"}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			bad := cloneV2Fragment(t, c.fragment)
			switch kind {
			case "nil-transition":
				bad.Continuation = nil
			case "nil-proof":
				bad.ContinuationProof = nil
			case "wrong-policy":
				bad.PolicyID[0] ^= 1
			case "downgrade":
				bad.ProtocolVersion = 0
				bad.PolicyID = [32]byte{}
				bad.Continuation = nil
				bad.ContinuationProof = nil
			case "root-omission":
				bad.ContinuationProof.MemberForest = bad.ContinuationProof.MemberForest[1:]
			case "forest-depth":
				bad.ContinuationProof.MemberForest[0].Depth++
			case "forest-cycle":
				r := &bad.ContinuationProof.MemberForest[0]
				r.HasParent = true
				r.Parent = r.Vertex
				r.Depth = 1
			case "forest-parent":
				r := &bad.ContinuationProof.MemberForest[0]
				r.HasParent = true
				r.Parent = types.TransactionID([]byte("absent"))
				r.Depth = 1
			case "duplicate-join":
				bad.Continuation.Joins[0].Transactions = append(bad.Continuation.Joins[0].Transactions, bad.Continuation.Joins[0].Transactions[0])
			case "premature-settle":
				for _, r := range bad.ContinuationProof.MemberForest {
					if c.preState.states[r.Vertex] == types.StateShaded {
						bad.Continuation.Settled = append(bad.Continuation.Settled, r.Vertex)
						break
					}
				}
				types.SortTxIDs(bad.Continuation.Settled)
			case "omit-settle":
				bad.Continuation.Settled = bad.Continuation.Settled[1:]
			case "wrong-index":
				bad.Continuation.BatchIndex++
			case "false-close":
				bad.Continuation.Closed = !bad.Continuation.Closed
			case "early-shaded-execution":
				// The pre-fix rule executed waiting members on absorption.
				id := bad.Continuation.Assigned[0]
				bad.Continuation.Assigned = bad.Continuation.Assigned[1:]
				bad.Continuation.Joins = append(bad.Continuation.Joins, types.FairnessBatch{Index: bad.Continuation.BatchIndex, Transactions: []types.TxID{id}})
				bad.Continuation.Settled = append(bad.Continuation.Settled, id)
				types.SortTxIDs(bad.Continuation.Settled)
			case "merge-rounds":
				var all []types.TxID
				for _, batch := range bad.Continuation.Joins {
					all = append(all, batch.Transactions...)
				}
				types.SortTxIDs(all)
				bad.Continuation.Joins = []types.FairnessBatch{{Index: bad.Continuation.BatchIndex, Transactions: all}}
				if len(c.fragment.Continuation.Joins) < 2 {
					bad.Continuation.Joins = append(bad.Continuation.Joins, types.FairnessBatch{Index: bad.Continuation.BatchIndex, Transactions: bad.Continuation.Assigned[:1]})
				}
			case "drop-assigned":
				bad.Continuation.Assigned = bad.Continuation.Assigned[1:]
			case "post-state":
				bad.CandidatePostStateID[0] ^= 1
			case "unused-root-parent":
				for i := range bad.ContinuationProof.MemberForest {
					if !bad.ContinuationProof.MemberForest[i].HasParent {
						bad.ContinuationProof.MemberForest[i].Parent[0] ^= 1
						break
					}
				}
			}
			v2Signed(t, s, bad)
			if follower.verifyCandidate(bad, 0) {
				t.Fatal("authenticated mutation accepted")
			}
			if follower.pending != nil {
				t.Fatal("rejection installed pending")
			}
			assertAblationState(t, follower.committed, before)
		})
	}
	if !follower.verifyCandidate(c.fragment, 0) {
		t.Fatal("valid control rejected")
	}
	if _, ok := follower.CommitPending(c.digest); !ok {
		t.Fatal("valid control cannot commit")
	}
	if !reflect.DeepEqual(follower.committed.doneEntries(), c.postState.doneEntries()) {
		t.Fatal("settlement differs")
	}
	revisionResult(t, "v2_continuation_attacks", map[string]any{"families": len(kinds), "all_rejected": true, "valid_commit_after_rejection": true})
}
