package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"testing"
)

// Three correct replicas, gamma=2/3 (q_h=2, T_e=2, T_s=3), D=0. Full receipt
// orders A: x,y,w  B: y,x,w  C: w,x,y. C's first admitted chunk holds only w.
// A and C receive x before y, the full weak-precedence graph is acyclic
// (x->y->w, x->w), and ID(y)<ID(x). Executing co-absorbed Shaded members by
// identifier would run y before x across components.
func TestV2CertifiedOrderCrossComponentCounterexample(t *testing.T) {
	s, auth, admission := revisionService(t, 3, 0, 2.0/3.0, 12)
	if err := s.EnableContinuation(0); err != nil {
		t.Fatal(err)
	}
	follower := revisionFollower(s)
	var x, y, w types.TxID
	for i := 0; ; i += 2 {
		a, b := []byte(fmt.Sprintf("certified-order/%d", i)), []byte(fmt.Sprintf("certified-order/%d", i+1))
		x, y = types.TransactionID(a), types.TransactionID(b)
		if types.LessTxID(x, y) {
			x, y = y, x
			a, b = b, a
		}
		for _, content := range [][]byte{a, b} {
			if err := admission.Store(types.Transaction{ID: types.TransactionID(content), CanonicalBytes: content}); err != nil {
				t.Fatal(err)
			}
		}
		break
	}
	wc := []byte("certified-order/w")
	w = types.TransactionID(wc)
	if err := admission.Store(types.Transaction{ID: w, CanonicalBytes: wc}); err != nil {
		t.Fatal(err)
	}
	if !types.LessTxID(y, x) {
		t.Fatal("fixture requires ID(y) < ID(x)")
	}
	step := func(seq uint64, chunks [3][]types.TxID) []types.FairnessBatch {
		t.Helper()
		var orders []*types.LocalOrder
		for sender := uint64(0); sender < 3; sender++ {
			orders = append(orders, signedOrder(t, auth, s.committed, sender, seq, chunks[sender]...))
		}
		candidate, err := s.constructCandidate(orders)
		if err != nil {
			t.Fatal(err)
		}
		if !follower.verifyCandidate(candidate.fragment, 0) {
			t.Fatal("follower rejected certified-order candidate")
		}
		if _, ok := follower.CommitPending(candidate.digest); !ok {
			t.Fatal("follower commit")
		}
		batches, ok := s.CommitPending(candidate.digest)
		if !ok {
			t.Fatal("leader commit")
		}
		assertAblationState(t, s.committed, follower.committed)
		return batches
	}
	first := step(1, [3][]types.TxID{{x, y, w}, {y, x, w}, {w}})
	if len(first) != 0 {
		t.Fatalf("undetermined Shaded order executed: %v", first)
	}
	if s.committed.OpenBatch == 0 || len(s.committed.OpenMembers) != 3 {
		t.Fatal("w, x, y must be assigned to the open batch and wait")
	}
	second := step(2, [3][]types.TxID{nil, nil, {x, y}})
	var order []types.TxID
	for _, batch := range second {
		order = append(order, batch.Transactions...)
	}
	if len(order) != 3 || order[0] != x || order[1] != y || order[2] != w {
		t.Fatalf("certified execution order %x, want x, y, w", order)
	}
	if s.committed.OpenBatch != 0 || len(s.committed.OpenMembers) != 0 {
		t.Fatal("open batch did not close after all members executed")
	}
	revisionResult(t, "v2_certified_order_counterexample", map[string]any{"n": 3, "f": 0, "gamma": "2/3", "D": 0, "fragment1_executed": 0, "fragment2_order": "x,y,w", "cross_component_inversion": false})
}
