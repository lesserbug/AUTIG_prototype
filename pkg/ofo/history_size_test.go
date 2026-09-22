package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// Historical-size fixture, not a long-running deployment. Synthesize a prefix
// of identical signed receipt orders and their singleton outputs without
// replaying historical graphs. All heads, transition IDs, fragment digests and
// Done records follow that prefix. Empty padding fixes the next sender schedule.
// Live-state setup and measured proposals use the production paths below.
func makeHistorySizeFixture(t testing.TB, done int) *ablationFixture {
	t.Helper()
	const historyRounds = 5000 // Cover 10^6 IDs while fixing the source sequence at every size.
	x := &ablationFixture{auth: newAblationAuthenticator(10, 7), admission: newTestAdmission()}
	ctx := types.AuthContext{Epoch: 1, LeaderID: 0, Identifier: [32]byte{7}}
	genesis := types.ProtocolGenesis{StateID: [32]byte{1}, FragmentDigest: [32]byte{2}}
	var err error
	x.base, err = NewOFOService(0, 10, 1, .9, newTestNetwork(), 200, nil, false, x.auth, x.admission, ctx, genesis, func(*types.VerifiableFairOrderFragment, [32]byte) {})
	if err != nil {
		t.Fatal(err)
	}
	x.base.Stop()
	s := x.base.committed
	for seq := uint64(1); seq <= historyRounds; seq++ {
		var ids []types.TxID
		for i := int(seq-1) * 200; i < min(int(seq)*200, done); i++ {
			ids = append(ids, types.TransactionID([]byte(fmt.Sprintf("history-size/done/%d", i))))
		}
		var orders []*types.LocalOrder
		for _, r := range ablationSenders(10, 1, seq) {
			orders = append(orders, signedOrder(t, x.auth, s, r, seq, ids...))
		}
		output, cert := &types.FairOrderFragment{}, &types.StructuralCertificate{}
		for i, id := range ids {
			index := s.FairnessBatchCounter + uint64(i) + 1
			output.Batches = append(output.Batches, types.FairnessBatch{Index: index, Transactions: []types.TxID{id}})
			cert.Part = append(cert.Part, types.SCCClaim{Rank: uint64(i), Transactions: []types.TxID{id}})
			s.Done[id] = DoneRecord{FragmentSeq: seq, BatchIndex: index}
		}
		preID := types.PreStateIdentifier(ctx.Epoch, seq, s.StateID, types.EvidenceBatchDigest(orders))
		postID := types.PostStateIdentifier(ctx.Epoch, seq, preID, output, cert.Part)
		fragment := &types.VerifiableFairOrderFragment{
			Epoch: ctx.Epoch, LeaderID: ctx.LeaderID, AuthContextID: ctx.Identifier, FragmentSeq: seq,
			PreviousStateID: s.StateID, PreviousFragmentDigest: x.base.latestCommittedFragment,
			Evidence: orders, FinalOrder: output, Certificate: cert,
			CandidateEvidenceStateID: preID, CandidatePostStateID: postID,
		}
		x.base.latestCommittedFragment = types.FragmentDigest(fragment)
		s.StateID, s.FragmentSeq = postID, seq
		s.FairnessBatchCounter += uint64(len(ids))
		for _, order := range orders {
			s.Seq[order.ReplicaID], s.Head[order.ReplicaID] = order.NewPosition, order.NewHead
		}
	}
	s.DoneRoot = finalizedSetRoot(s.Done)

	// Seven reporters leave 128 ordered transactions Shaded (Solid threshold 8).
	// In the measured round replica 0 releases the first eight; the other eight
	// senders introduce eight fresh transactions blocked by the retained suffix.
	ids := make([]types.TxID, 136)
	for i := range ids {
		content := []byte(fmt.Sprintf("history-size/live/%d", i))
		ids[i] = types.TransactionID(content)
		if err := x.admission.Store(types.Transaction{ID: ids[i], CanonicalBytes: content}); err != nil {
			t.Fatal(err)
		}
	}
	var warm []*types.LocalOrder
	for _, r := range ablationSenders(10, 1, historyRounds+1) {
		var chunk []types.TxID
		if r <= 7 {
			chunk = ids[:128]
		}
		warm = append(warm, signedOrder(t, x.auth, s, r, historyRounds+1, chunk...))
	}
	x.advanceRound(t, warm)
	for _, r := range ablationSenders(10, 1, historyRounds+2) {
		chunk := ids[128:]
		if r == 0 {
			chunk = ids[:8]
		}
		x.orders = append(x.orders, signedOrder(t, x.auth, x.base.committed, r, historyRounds+2, chunk...))
	}
	x.candidate, err = x.service(true).constructCandidate(x.orders)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestHistorySizeFixture(t *testing.T) {
	var control *ablationFixture
	for _, done := range []int{0, 1000, 10000, 100000, 1000000} {
		t.Run(fmt.Sprintf("Done%d", done), func(t *testing.T) {
			x := makeHistorySizeFixture(t, done)
			pre, post := x.base.committed, x.candidate.postState
			if len(pre.Done) != done || pre.DoneRoot != finalizedSetRoot(pre.Done) || pre.FairnessBatchCounter != uint64(done) ||
				len(pre.Live) != 128 || len(x.candidate.preState.Live) != 136 || len(post.Live) != 128 || len(post.Done) != done+8 || len(x.candidate.fragment.FinalOrder.Batches) != 8 {
				t.Fatal("history, live or output size differs from the controlled workload")
			}
			for id, record := range pre.Done {
				if pre.Live[id] || record.BatchIndex == 0 || record.BatchIndex > uint64(done) || record.FragmentSeq != (record.BatchIndex-1)/200+1 {
					t.Fatal("historical records do not match the synthetic prefix")
				}
			}
			if control != nil {
				if !reflect.DeepEqual(pre.Live, control.base.committed.Live) || !reflect.DeepEqual(pre.weights, control.base.committed.weights) ||
					!reflect.DeepEqual(pre.states, control.base.committed.states) || pre.FragmentSeq != control.base.committed.FragmentSeq {
					t.Fatal("changing Done changed the live graph or fragment sequence")
				}
				for i, order := range x.orders {
					if !sameIDs(order.OrderedTxs, control.orders[i].OrderedTxs) || order.ReplicaID != control.orders[i].ReplicaID {
						t.Fatal("changing Done changed the measured receipt-order workload")
					}
				}
			} else {
				control = x
			}
			checkAblationVerifier(t, x, x.candidate.fragment, 0, true)
			leader := x.service(true)
			candidate, err := leader.constructCandidate(x.orders)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := leader.CommitPending(candidate.digest); !ok {
				t.Fatal("leader could not commit the verified fixture")
			}
			assertAblationState(t, leader.committed, post)
		})
	}
}

// Unlike makeHistorySizeFixture, this run starts at genesis and commits every
// transition on the same leader/follower instances. Use -short to omit this
// bounded 100,000-transaction run from quick developer checks.
func TestHistoryContinuousCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("100,000-transaction continuous commit run")
	}
	auth, admission := newAblationAuthenticator(10, 7), newTestAdmission()
	ctx := types.AuthContext{Epoch: 1, LeaderID: 0, Identifier: [32]byte{7}}
	genesis := types.ProtocolGenesis{StateID: [32]byte{1}, FragmentDigest: [32]byte{2}}
	leader, err := NewOFOService(0, 10, 1, .9, newTestNetwork(), 200, nil, false, auth, admission, ctx, genesis, func(*types.VerifiableFairOrderFragment, [32]byte) {})
	if err != nil {
		t.Fatal(err)
	}
	leader.Stop() // Drive real production transitions synchronously.
	follower, err := NewOFOService(1, 10, 1, .9, newTestNetwork(), 200, nil, false, auth, admission, ctx, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var initial runtime.MemStats
	runtime.ReadMemStats(&initial)
	var backlog []types.TxID
	done, admitted, milestone := 0, 0, 0
	targets := []int{1000, 10000, 100000}
	for milestone < len(targets) {
		seq := leader.committed.FragmentSeq + 1
		remaining := targets[milestone] - done - len(backlog)
		nextSize := 64
		// Adjust an earlier round so each observation ends with a full
		// 64-transaction output and retains 64 live transactions.
		if remaining > 64 && remaining < 128 {
			nextSize = remaining - 64
		}
		next := make([]types.TxID, nextSize)
		for i := range next {
			content := []byte(fmt.Sprintf("continuous-history/%d", admitted))
			admitted++
			next[i] = types.TransactionID(content)
			if err := admission.Store(types.Transaction{ID: next[i], CanonicalBytes: content}); err != nil {
				t.Fatal(err)
			}
		}
		var orders []*types.LocalOrder
		for index, r := range ablationSenders(10, 1, seq) {
			var chunk []types.TxID
			for _, id := range backlog {
				if _, seen := leader.committed.Pos[r][id]; !seen {
					chunk = append(chunk, id)
				}
			}
			// At least eight reporters now cover the old backlog, making it
			// safe; exactly seven cover the new tail, leaving it Shaded.
			if index < 7 {
				chunk = append(chunk, next...)
			}
			orders = append(orders, signedOrder(t, auth, leader.committed, r, seq, chunk...))
		}
		sample := done+len(backlog) == targets[milestone]
		var beforeBuild, afterBuild, beforeVerify, afterVerify, memory runtime.MemStats
		if sample {
			runtime.ReadMemStats(&beforeBuild)
		}
		start := time.Now()
		candidate, err := leader.constructCandidate(orders)
		buildTime := time.Since(start)
		if err != nil {
			t.Fatalf("round %d construction: %v", seq, err)
		}
		if sample {
			runtime.ReadMemStats(&afterBuild)
		}
		// Transport copying is outside the verification measurement; the
		// follower derives its own state from its independently committed prefix.
		fragment := cloneAblationFragment(t, candidate.fragment)
		if sample {
			runtime.ReadMemStats(&beforeVerify)
		}
		start = time.Now()
		accepted := follower.verifyCandidate(fragment, 0)
		verifyTime := time.Since(start)
		if !accepted {
			t.Fatalf("round %d follower rejected the continuous prefix", seq)
		}
		if sample {
			runtime.ReadMemStats(&afterVerify)
		}
		if leader.committed.FragmentSeq != seq-1 || follower.committed.FragmentSeq != seq-1 || len(fragment.FinalOrder.Batches) != len(backlog) {
			t.Fatal("candidate advanced committed state or changed the expected output")
		}
		for i, batch := range fragment.FinalOrder.Batches {
			if batch.Index != uint64(done+i+1) || !sameIDs(batch.Transactions, backlog[i:i+1]) {
				t.Fatal("output does not finalize the exact safe backlog in order")
			}
		}
		start = time.Now()
		for _, s := range []*OFOService{leader, follower} {
			if _, ok := s.CommitPending(candidate.digest); !ok {
				t.Fatalf("round %d commit failed", seq)
			}
		}
		commitTime := time.Since(start)
		done += len(backlog)
		if !reflect.DeepEqual(leader.committed, follower.committed) || leader.committed.FragmentSeq != seq ||
			len(leader.committed.Done) != done || leader.committed.FairnessBatchCounter != uint64(done) ||
			len(leader.committed.Live) != len(next) || leader.latestCommittedFragment != candidate.digest || follower.latestCommittedFragment != candidate.digest ||
			leader.authContext != ctx || follower.authContext != ctx || leader.pending != nil || follower.pending != nil {
			t.Fatalf("round %d committed prefix, live size or authorization diverged", seq)
		}
		if sample {
			if len(backlog) != 64 || len(next) != 64 || leader.committed.DoneRoot != finalizedSetRoot(leader.committed.Done) {
				t.Fatal("observation does not have the fixed workload or a matching DoneRoot")
			}
			runtime.ReadMemStats(&memory)
			t.Logf("done=%d seq=%d live=64 output=64 build_ms=%.3f verify_ms=%.3f commit_ms=%.3f build_B=%d verify_B=%d heap_B=%d cumulative_alloc_B=%d gc=%d elapsed_s=%.3f",
				done, seq, float64(buildTime)/float64(time.Millisecond), float64(verifyTime)/float64(time.Millisecond), float64(commitTime)/float64(time.Millisecond),
				afterBuild.TotalAlloc-beforeBuild.TotalAlloc, afterVerify.TotalAlloc-beforeVerify.TotalAlloc, memory.HeapAlloc, memory.TotalAlloc-initial.TotalAlloc, memory.NumGC-initial.NumGC, time.Since(started).Seconds())
			milestone++
		} else if seq%200 == 0 {
			t.Logf("progress: done=%d seq=%d elapsed_s=%.1f", done, seq, time.Since(started).Seconds())
		}
		backlog = next
	}
	t.Logf("final: admitted=%d finalized=%d live=%d state=%x fragment=%x", admitted, done, len(backlog), leader.committed.StateID, leader.latestCommittedFragment)
}

func BenchmarkHistorySize(b *testing.B) {
	for _, done := range []int{0, 1000, 10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("Done%d", done), func(b *testing.B) {
			x := makeHistorySizeFixture(b, done)
			checkAblationVerifier(b, x, x.candidate.fragment, 0, true)
			for _, operation := range []string{"Construct", "Verify", "Clone", "DoneRoot"} {
				b.Run(operation, func(b *testing.B) {
					s := x.service(operation == "Construct")
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						var result any
						var err error
						accepted := true
						b.StartTimer()
						switch operation {
						case "Construct":
							result, err = s.constructCandidate(x.orders)
						case "Verify":
							accepted = s.verifyCandidate(x.candidate.fragment, 0)
							result = s.pending
						case "Clone":
							result = s.committed.clone()
						case "DoneRoot":
							result = finalizedSetRoot(s.committed.Done)
						}
						b.StopTimer()
						runtime.KeepAlive(result)
						if err != nil || !accepted {
							b.Fatalf("operation failed: %v, accepted=%t", err, accepted)
						}
						if s.pending != nil {
							s.AbandonPending(s.pending.digest)
						}
					}
				})
			}
		})
	}
}
