package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

// Keep m fixed while the next correct reporter supplies a prefix of length k.
// Exactly the prefix becomes Solid. The two selected empty reporters can be
// Byzantine; every remaining selected correct sender has already reported m.
// All fixtures are built and committed through native authenticated routines.
func revisionAlphaFixture(t testing.TB, n uint64, m, k int) *ablationFixture {
	return revisionAlphaFixtureMode(t, n, m, k, false)
}
func revisionAlphaFixtureMode(t testing.TB, n uint64, m, k int, continuation bool) *ablationFixture {
	leader, auth, admission := revisionService(t, n, 2, 1, m)
	if continuation {
		if err := leader.EnableContinuation(2); err != nil {
			t.Fatal(err)
		}
	}
	ids := make([]types.TxID, m)
	for i := range ids {
		content := []byte(fmt.Sprintf("alpha/n%d/m%d/%d", n, m, i))
		ids[i] = types.TransactionID(content)
		if err := admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
			t.Fatal(err)
		}
	}
	initialReporters := uint64(types.CalculateSolidThreshold(n, 2) - 1)
	var initial []*types.LocalOrder
	for r := uint64(0); r < n-2; r++ {
		var chunk []types.TxID
		if r < initialReporters {
			chunk = ids
		}
		initial = append(initial, signedOrder(t, auth, leader.committed, r, 1, chunk...))
	}
	first, err := leader.constructCandidate(initial)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.fragment.FinalOrder.Batches) != 0 {
		t.Fatal("initial alpha working set must remain Shaded")
	}
	follower := revisionFollower(leader)
	if !follower.verifyCandidate(first.fragment, 0) {
		t.Fatal("alpha warm-up verification failed")
	}
	if _, ok := follower.CommitPending(first.digest); !ok {
		t.Fatal("alpha follower warm commit failed")
	}
	if _, ok := leader.CommitPending(first.digest); !ok {
		t.Fatal("alpha leader warm commit failed")
	}
	x := &ablationFixture{base: leader, auth: auth, admission: admission, warmMaxLO: m}
	for r := uint64(0); r < n-2; r++ {
		var chunk []types.TxID
		if r == initialReporters {
			chunk = ids[:k]
		}
		x.orders = append(x.orders, signedOrder(t, auth, leader.committed, r, 2, chunk...))
	}
	branch := x.service(true)
	x.candidate, err = branch.constructCandidate(x.orders)
	if err != nil {
		t.Fatal(err)
	}
	x.metrics = ablationMetrics(x)
	if x.metrics["updated_active"] != float64(m) || x.metrics["output_tx"] != float64(k) {
		t.Fatalf("alpha dimensions differ: %+v", x.metrics)
	}
	return x
}

func TestRevisionAlphaComparators(t *testing.T) {
	for _, n := range []uint64{10, 50} {
		for _, k := range []int{4, 16, 64, 128, 256} {
			x := revisionAlphaFixture(t, n, 256, k)
			checkAblationVerifier(t, x, x.candidate.fragment, 0, true)
			for _, inc := range []bool{false, true} {
				s := x.service(false)
				s.UtigManager = x.base.UtigManager
				if !revisionVerifyByGraph(s, x.candidate.fragment, inc) {
					t.Fatal("alpha graph comparator rejected")
				}
				assertAblationState(t, s.pending.postState, x.candidate.postState)
			}
			revisionResult(t, "alpha_conformance", map[string]any{"n": n, "f": 2, "active": 256, "output": k, "selected_pairs": k * (k - 1) / 2, "frontier_pairs": (256 - k) * k, "omitted_pairs": (256 - k) * (255 - k) / 2, "all_paths_agree": true})
		}
	}
}

func BenchmarkRevisionAlpha(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		for _, k := range []int{4, 16, 64, 128, 256} {
			x := revisionAlphaFixture(b, n, 256, k)
			updated := rebuildDependencyManagerForTest(x.candidate.preState, n, 2, 1)
			for _, branch := range []string{"CoreCertificate", "CoreCachedGraph", "FullCertificate", "FullSymmetricIncremental"} {
				b.Run(fmt.Sprintf("n%d_m256_k%d/%s", n, k, branch), func(b *testing.B) {
					s := x.service(false)
					s.UtigManager = x.base.UtigManager
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						b.StartTimer()
						ok := true
						switch branch {
						case "CoreCertificate":
							ok = verifyStructuralCertificate(x.candidate.preState, x.candidate.fragment.FinalOrder, x.candidate.fragment.Certificate, n, 2, 1) == nil
						case "CoreCachedGraph":
							order := recomputeSafeOrderForTest(x.candidate.preState, updated)
							ok = reflect.DeepEqual(order, x.candidate.fragment.FinalOrder)
							runtime.KeepAlive(order)
						case "FullCertificate":
							ok = s.verifyCandidate(x.candidate.fragment, 0)
						case "FullSymmetricIncremental":
							ok = revisionVerifyByGraph(s, x.candidate.fragment, true)
						}
						b.StopTimer()
						if !ok {
							b.Fatal("alpha verification failed")
						}
						runtime.KeepAlive(s.pending)
						if s.pending != nil {
							s.AbandonPending(x.candidate.digest)
						}
					}
				})
			}
		}
	}
}
