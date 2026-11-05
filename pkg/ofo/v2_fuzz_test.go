package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"testing"
)

// Authenticated semantic mutations, beyond the network parser: every trial is
// signed by the authorized order leader and reaches native candidate checking.
func FuzzV2AuthenticatedContinuation(f *testing.F) {
	leader, _, candidate := v2BlockingCandidate(f)
	for family := 0; family < 8; family++ {
		f.Add([]byte{byte(family), 0, 1})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 || len(data) > 64 {
			return
		}
		bad := cloneV2Fragment(t, candidate.fragment)
		index := int(data[1])
		delta := uint64(data[2]) + 1
		switch data[0] % 8 {
		case 0:
			bad.CandidatePostStateID[index%32] ^= byte(delta%255 + 1)
		case 1:
			bad.PolicyID[index%32] ^= byte(delta%255 + 1)
		case 2:
			bad.Continuation.BatchIndex += delta
		case 3:
			bad.ContinuationProof.MemberForest[index%len(bad.ContinuationProof.MemberForest)].Depth += delta
		case 4:
			// Execute a waiting member before its order is certified.
			i := index % len(bad.Continuation.Assigned)
			id := bad.Continuation.Assigned[i]
			bad.Continuation.Assigned = append(bad.Continuation.Assigned[:i:i], bad.Continuation.Assigned[i+1:]...)
			round := int(delta) % (len(bad.Continuation.Joins) + 1)
			if round == len(bad.Continuation.Joins) {
				bad.Continuation.Joins = append(bad.Continuation.Joins, types.FairnessBatch{Index: bad.Continuation.BatchIndex})
			}
			joined := append(bad.Continuation.Joins[round].Transactions, id)
			types.SortTxIDs(joined)
			bad.Continuation.Joins[round].Transactions = joined
			bad.Continuation.Settled = append(bad.Continuation.Settled, id)
			types.SortTxIDs(bad.Continuation.Settled)
		case 5:
			bad.Continuation.Settled = append(bad.Continuation.Settled, types.TransactionID(data))
		case 6:
			bad.Continuation.Joins[index%len(bad.Continuation.Joins)].Index += delta
		case 7:
			bad.Continuation.Closed = !bad.Continuation.Closed
		}
		v2Signed(t, leader, bad)
		follower := revisionFollower(leader)
		before := cloneForTestReplica(follower.committed)
		if follower.verifyCandidate(bad, 0) {
			t.Fatal("signed invalid continuation accepted")
		}
		if follower.pending != nil {
			t.Fatal("rejected candidate installed")
		}
		assertAblationState(t, follower.committed, before)
		if !follower.verifyCandidate(candidate.fragment, 0) {
			t.Fatal("valid control rejected after attack")
		}
	})
}
