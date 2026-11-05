package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

// A new replica owns a separate committed index. Ordinary candidate forks use
// clone and share within that replica; fixture initialization is untimed.
func cloneForTestReplica(state *EvidenceState) *EvidenceState {
	copy := state.clone()
	if err := copy.RestoreDoneHistory(state.ExportDoneHistory(), state.DoneRoot); err != nil {
		panic(err)
	}
	return copy
}

func TestV2DoneVersionAndForkIsolation(t *testing.T) {
	base := NewEvidenceState(1, 5, [32]byte{1})
	old := base.clone()
	a, b := types.TransactionID([]byte("a")), types.TransactionID([]byte("b"))
	winner, loser := base.clone(), base.clone()
	winner.FragmentSeq, loser.FragmentSeq = 1, 1
	winner.addDone(a, DoneRecord{FragmentSeq: 1, BatchIndex: 1})
	loser.addDone(b, DoneRecord{FragmentSeq: 1, BatchIndex: 1})
	if _, visible := base.doneLookup(a); visible {
		t.Fatal("speculation leaked")
	}
	if !winner.commitDone() {
		t.Fatal("valid commit failed")
	}
	if _, visible := old.doneLookup(a); visible || old.doneCount() != 0 {
		t.Fatal("old view saw a future commit")
	}
	if _, visible := loser.doneLookup(a); visible {
		t.Fatal("losing fork saw winner")
	}
	if loser.commitDone() {
		t.Fatal("stale fork merged")
	}
	if _, visible := winner.doneLookup(b); visible {
		t.Fatal("abandoned delta leaked")
	}
	if winner.doneCount() != 1 || len(winner.Done) != 0 {
		t.Fatal("commit did not seal overlay")
	}
	if winner.DoneRoot != finalizedSetRoot(winner.doneEntries()) {
		t.Fatal("incremental root mismatch")
	}
}

func TestV2DoneRecoveryAttacks(t *testing.T) {
	s := NewEvidenceState(1, 5, [32]byte{})
	for seq := uint64(1); seq <= 3; seq++ {
		c := s.clone()
		c.FragmentSeq = seq
		for j := 0; j < 2; j++ {
			c.addDone(types.TransactionID([]byte(fmt.Sprintf("%d/%d", seq, j))), DoneRecord{FragmentSeq: seq, BatchIndex: seq})
		}
		if !c.commitDone() {
			t.Fatal("commit")
		}
		s = c
	}
	entries := s.ExportDoneHistory()
	r := NewEvidenceState(1, 5, [32]byte{})
	r.FragmentSeq = s.FragmentSeq
	if err := r.RestoreDoneHistory(entries, s.DoneRoot); err != nil || !reflect.DeepEqual(r.doneEntries(), s.doneEntries()) {
		t.Fatalf("valid restore: %v", err)
	}
	for _, kind := range []string{"omit", "duplicate", "reorder", "record", "index", "future", "root"} {
		t.Run(kind, func(t *testing.T) {
			x := append([]DoneEntry(nil), entries...)
			root := s.DoneRoot
			switch kind {
			case "omit":
				x = x[:len(x)-1]
			case "duplicate":
				x[1].ID = x[0].ID
			case "reorder":
				x[0], x[1] = x[1], x[0]
			case "record":
				x[2].Record.BatchIndex++
			case "index":
				x[3].Record.HistoryIndex++
			case "future":
				x[5].Record.FragmentSeq = 4
			case "root":
				root[0] ^= 1
			}
			before := r.DoneRoot
			if r.RestoreDoneHistory(x, root) == nil {
				t.Fatal("corrupt history accepted")
			}
			if r.DoneRoot != before || r.doneCount() != 6 {
				t.Fatal("failed recovery changed state")
			}
		})
	}
}

func TestV2DoneConcurrentOldReaders(t *testing.T) {
	s := NewEvidenceState(1, 5, [32]byte{})
	view := s.clone()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				if len(view.doneEntries()) != 0 {
					t.Error("old version changed")
				}
			}
		}()
	}
	for i := uint64(1); i <= 100; i++ {
		c := s.clone()
		c.FragmentSeq = i
		c.addDone(types.TransactionID([]byte(fmt.Sprint(i))), DoneRecord{FragmentSeq: i, BatchIndex: i})
		if !c.commitDone() {
			t.Fatal("commit")
		}
		s = c
	}
	wg.Wait()
	if s.doneCount() != 100 {
		t.Fatal("lost history")
	}
}
