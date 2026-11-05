package ofo

import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestV2CollectorSkipsPoisonAndSilentRosterMember(t *testing.T) {
	auth := newTestAuthenticator(t, 5)
	admission := newTestAdmission()
	out := make(chan *types.VerifiableFairOrderFragment, 1)
	s, err := NewOFOService(0, 5, 1, 1, newTestNetwork(), 10, nil, false, auth, admission, types.AuthContext{Epoch: 1}, types.ProtocolGenesis{}, func(f *types.VerifiableFairOrderFragment, _ [32]byte) { out <- f })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if err = s.EnableContinuation(0); err != nil {
		t.Fatal(err)
	}
	bad := signedOrder(t, auth, s.committed, 4, 1)
	bad.Signature[0] ^= 1
	s.HandleMessage(network.Message{From: 4, Payload: bad})
	unavailable := signedOrder(t, auth, s.committed, 4, 1, types.TransactionID([]byte("missing-content")))
	s.HandleMessage(network.Message{From: 4, Payload: unavailable})
	for id := uint64(0); id < 3; id++ {
		s.HandleMessage(network.Message{From: id, Payload: signedOrder(t, auth, s.committed, id, 1)})
	}
	select {
	case <-out:
		t.Fatal("invalid/unavailable report consumed quorum slot")
	case <-time.After(20 * time.Millisecond):
	}
	s.HandleMessage(network.Message{From: 3, Payload: signedOrder(t, auth, s.committed, 3, 1)})
	select {
	case f := <-out:
		for _, o := range f.Evidence {
			if o.ReplicaID == 4 {
				t.Fatal("poisoned sender admitted")
			}
		}
		if _, ok := s.CommitPending(types.FragmentDigest(f)); !ok {
			t.Fatal("valid replacement quorum commit")
		}
	case <-time.After(time.Second):
		t.Fatal("silent old-roster member blocked quorum")
	}
	revisionResult(t, "v2_collector_poison", map[string]any{"invalid_and_unavailable_not_counted": true, "silent_old_roster_member_skipped": true, "committed": true})
}

func TestV2FetchedObservationSurvivesLateRejection(t *testing.T) {
	s, _, c := v2BlockingCandidate(t)
	f := revisionFollower(s)
	bad := cloneV2Fragment(t, c.fragment)
	bad.CandidatePostStateID[0] ^= 1
	v2Signed(t, s, bad)
	if f.verifyCandidate(bad, 0) {
		t.Fatal("bad state accepted")
	}
	seen := make(map[types.TxID]bool)
	for _, o := range bad.Evidence {
		for _, id := range o.OrderedTxs {
			seen[id] = true
		}
	}
	for id := range seen {
		if !f.observed[id] {
			t.Fatal("fetched valid transaction not observed")
		}
	}
	if len(f.receiptQueue) != len(seen) || f.committed.FragmentSeq != 0 || f.pending != nil {
		t.Fatal("observation/committed-state isolation")
	}
}

func TestV2StrategicReportSelection(t *testing.T) {
	for _, policy := range []string{"delay-target", "accelerate-target"} {
		for _, seed := range []int64{1, 7, 19} {
			t.Run(fmt.Sprintf("%s_seed%d", policy, seed), func(t *testing.T) {
				const n, f = 7, 1
				s, auth, admission := revisionService(t, n, f, 1, 1)
				if err := s.EnableContinuation(2); err != nil {
					t.Fatal(err)
				}
				follower := revisionFollower(s)
				ids := make([]types.TxID, 24)
				for i := range ids {
					b := []byte(fmt.Sprintf("selection/%d/%d", seed, i))
					ids[i] = types.TransactionID(b)
					if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: b}); err != nil {
						t.Fatal(err)
					}
				}
				profiles := make(map[uint64][]int)
				rng := rand.New(rand.NewSource(seed))
				potential := 0
				for i := uint64(1); i < n; i++ {
					profiles[i] = rng.Perm(len(ids))
					for pos, tx := range profiles[i] {
						if tx == 0 {
							potential += pos + 1
						}
					}
				}
				firstSolid, firstExec := uint64(0), uint64(0)
				executed := make(map[types.TxID]ExecutionRecord)
				for r := uint64(1); r <= uint64(potential)+3; r++ {
					var reports []*types.LocalOrder
					for i := uint64(0); i < n; i++ {
						var chunk []types.TxID
						if i > 0 {
							pos := int(s.committed.Seq[i])
							if pos < len(ids) {
								chunk = []types.TxID{ids[profiles[i][pos]]}
							}
						}
						reports = append(reports, signedOrder(t, auth, s.committed, i, r, chunk...))
					}
					gain := func(o *types.LocalOrder) int {
						for _, id := range o.OrderedTxs {
							if id == ids[0] {
								return 1
							}
						}
						return 0
					}
					sort.SliceStable(reports, func(i, j int) bool {
						if gain(reports[i]) != gain(reports[j]) {
							if policy == "delay-target" {
								return gain(reports[i]) < gain(reports[j])
							}
							return gain(reports[i]) > gain(reports[j])
						}
						return reports[i].ReplicaID < reports[j].ReplicaID
					})
					chosen := append([]*types.LocalOrder(nil), reports[:n-f]...)
					c, err := s.constructCandidate(chosen)
					if err != nil {
						t.Fatal(err)
					}
					if firstSolid == 0 && c.preState.states[ids[0]] == types.StateSolid {
						firstSolid = r
					}
					if !follower.verifyCandidate(c.fragment, 0) {
						t.Fatal("selected valid quorum rejected")
					}
					if _, ok := follower.CommitPending(c.digest); !ok {
						t.Fatal("follower commit")
					}
					batches, ok := s.CommitPending(c.digest)
					if !ok {
						t.Fatal("leader commit")
					}
					for _, b := range batches {
						for _, id := range b.Transactions {
							if _, dup := executed[id]; dup {
								t.Fatal("duplicate")
							}
							executed[id] = ExecutionRecord{BatchIndex: b.Index, FragmentSeq: r}
							if id == ids[0] {
								firstExec = r
							}
						}
					}
					checkV2SettledFairness(t, s, profiles, ids, executed)
					if firstExec > 0 && firstSolid > 0 {
						break
					}
				}
				if firstSolid == 0 || firstSolid > uint64(potential) || firstExec == 0 || firstExec > firstSolid+2 {
					t.Fatalf("P=%d solid=%d execute=%d", potential, firstSolid, firstExec)
				}
				revisionResult(t, "v2_strategic_selection", map[string]any{"policy": policy, "seed": seed, "prefix_work_bound": potential, "first_solid": firstSolid, "first_execution": firstExec, "D": 2, "bound_satisfied": true, "native_authenticated_path": true})
			})
		}
	}
}
