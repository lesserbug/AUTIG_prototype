package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"io"
	"log"
	"runtime"
	"testing"
)

func revisionTerminalOmission(t testing.TB, x *ablationFixture) *types.VerifiableFairOrderFragment {
	f := cloneAblationFragment(t, x.candidate.fragment)
	last := len(f.FinalOrder.Batches) - 1
	if last < 0 {
		t.Fatal("omission fixture needs output")
	}
	f.FinalOrder.Batches = f.FinalOrder.Batches[:last]
	f.Certificate.Part = f.Certificate.Part[:last]
	post := x.candidate.preState.clone()
	finalize(post, NewDependencyManager(x.base.replicaCount, x.base.fFaulty, x.base.gamma), f.FinalOrder, f.Certificate.Part)
	f.CandidatePostStateID = post.StateID
	signAblationFragment(t, x, f)
	if err := verifyStructuralCertificate(x.candidate.preState, f.FinalOrder, f.Certificate, x.base.replicaCount, x.base.fFaulty, x.base.gamma); err == nil || err.Error() != "excluded Solid transaction has no blocker path" {
		t.Fatalf("terminal omission stopped at wrong stage: %v", err)
	}
	return f
}

func TestRevisionLateRejectionAndRecovery(t *testing.T) {
	x := makeAblationFixture(t, 10, 2, 1, ablationCase{name: "late", history: 120, fresh: 8, partialRelease: true}, 7)
	bad := revisionTerminalOmission(t, x)
	s := x.service(false)
	oldLog := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLog)
	for i := 0; i < 100; i++ {
		if s.verifyCandidate(bad, 0) {
			t.Fatal("late-invalid candidate accepted")
		}
		if s.pending != nil {
			t.Fatal("late rejection created pending state")
		}
	}
	if !s.verifyCandidate(x.candidate.fragment, 0) {
		t.Fatal("valid candidate rejected after repeated late failures")
	}
	if _, ok := s.CommitPending(x.candidate.digest); !ok {
		t.Fatal("valid commit failed")
	}
	assertAblationState(t, s.committed, x.candidate.postState)
	revisionResult(t, "repeated_late_rejection", map[string]any{"attempts_same_context": 100, "all_rejected": true, "no_automatic_handoff_claim": true, "valid_commit_after_rejection": true, "stage": "blocker coverage after trees/ranks/frontier"})
}

func BenchmarkRevisionRejectionCost(b *testing.B) {
	oldLog := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLog)
	for _, m := range []int{120, 480} {
		x := makeAblationFixture(b, 10, 2, 1, ablationCase{name: "rejection", history: m, fresh: 8, partialRelease: true}, 7)
		omitted := revisionTerminalOmission(b, x)
		post := cloneAblationFragment(b, x.candidate.fragment)
		post.CandidatePostStateID[0] ^= 1
		signAblationFragment(b, x, post)
		for _, entry := range []struct {
			name string
			f    *types.VerifiableFairOrderFragment
			want bool
		}{{"Valid", x.candidate.fragment, true}, {"BlockerCoverageFailure", omitted, false}, {"FinalPostStateFailure", post, false}} {
			b.Run(fmt.Sprintf("active%d/%s", m+8, entry.name), func(b *testing.B) {
				s := x.service(false)
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					b.StartTimer()
					ok := s.verifyCandidate(entry.f, 0)
					b.StopTimer()
					if ok != entry.want {
						b.Fatal("unexpected acceptance")
					}
					runtime.KeepAlive(s.pending)
					if ok {
						s.AbandonPending(types.FragmentDigest(entry.f))
					}
				}
			})
		}
	}
}

// A deep *valid* blocker certificate against actual authenticated receipt
// evidence. The leader may choose this noncanonical witness without changing
// output or violating external validity; rejection-based handoff cannot help.
func revisionDeepWitness(t testing.TB, length int) (*ablationFixture, *types.VerifiableFairOrderFragment, map[types.TxID]bool, int) {
	leader, auth, admission := revisionService(t, 7, 1, 1, length)
	ids := make([]types.TxID, length+6)
	for i := range ids {
		content := []byte(fmt.Sprintf("deep-witness/%d/%d", length, i))
		ids[i] = types.TransactionID(content)
		if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
			t.Fatal(err)
		}
	}
	var orders []*types.LocalOrder
	for r := uint64(1); r < 7; r++ {
		var chunk []types.TxID
		for _, j := range revisionProfile(int((r-1)/2), length) {
			chunk = append(chunk, ids[j])
		}
		orders = append(orders, signedOrder(t, auth, leader.committed, r, 1, chunk...))
	}
	candidate, err := leader.constructCandidate(orders)
	if err != nil {
		t.Fatal(err)
	}
	x := &ablationFixture{base: leader, orders: orders, candidate: candidate, auth: auth, admission: admission}
	active := make(map[types.TxID]bool)
	maxSolid := -1
	for i, id := range ids {
		if candidate.preState.states[id] == types.StateSolid {
			maxSolid = i
		}
		if candidate.preState.Live[id] && candidate.preState.states[id] != types.StateBlank {
			active[id] = true
		}
	}
	root := maxSolid + 1
	if root >= len(ids) || candidate.preState.states[ids[root]] != types.StateShaded {
		t.Fatal("no shaded frontier root")
	}
	f := cloneAblationFragment(t, candidate.fragment)
	f.Certificate.BlockForest = []types.BlockRecord{{Vertex: ids[root]}}
	for i := maxSolid; i >= 0; i-- {
		if candidate.preState.states[ids[i]] != types.StateSolid {
			t.Fatal("solid interval contains a gap")
		}
		f.Certificate.BlockForest = append(f.Certificate.BlockForest, types.BlockRecord{Vertex: ids[i], Parent: ids[i+1], HasParent: true, Depth: uint64(root - i)})
	}
	signAblationFragment(t, x, f)
	if err := verifyStructuralCertificate(candidate.preState, f.FinalOrder, f.Certificate, 7, 1, 1); err != nil {
		t.Fatal(err)
	}
	return x, f, active, root
}

func TestRevisionDeepAlternativeWitness(t *testing.T) {
	x, f, _, depth := revisionDeepWitness(t, 97)
	s := revisionFollower(x.base)
	if !s.verifyCandidate(f, 0) {
		t.Fatal("valid deep witness rejected")
	}
	if _, ok := s.CommitPending(types.FragmentDigest(f)); !ok {
		t.Fatal("deep witness commit failed")
	}
	assertAblationState(t, s.committed, x.candidate.postState)
	revisionResult(t, "deep_valid_certificate", map[string]any{"n": 7, "f": 1, "depth": depth, "block_records": len(f.Certificate.BlockForest), "accepted": true, "same_authoritative_poststate": true, "legacy_parent_visits": depth * (depth + 3) / 2, "memoized_parent_visits_at_most": 2 * len(f.Certificate.BlockForest), "native_authenticated_evidence": true})
}

func BenchmarkRevisionSharedForest(b *testing.B) {
	for _, length := range []int{97, 385, 769} {
		x, f, active, _ := revisionDeepWitness(b, length)
		for _, legacy := range []bool{true, false} {
			name := "Memoized"
			if legacy {
				name = "OriginalRepeatedPaths"
			}
			b.Run(fmt.Sprintf("records%d/%s", len(f.Certificate.BlockForest), name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					if legacy {
						err = revisionLegacyVerifyBlockForest(x.candidate.preState, active, map[types.TxID]bool{}, f.Certificate.BlockForest, 2)
					} else {
						err = verifyBlockForest(x.candidate.preState, active, map[types.TxID]bool{}, f.Certificate.BlockForest, 2)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
