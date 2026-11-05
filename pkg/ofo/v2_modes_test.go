package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"reflect"
	"testing"
)

func v2FollowerWithMode(s *OFOService, mode VerificationMode) *OFOService {
	f := revisionFollower(s)
	f.verificationMode = mode
	if mode == SymmetricIncrementalVerification {
		f.UtigManager = rebuildMaterialization(f.committed, f.replicaCount, f.fFaulty, f.gamma)
	}
	return f
}

func TestV2UndoRejectsAndRestores(t *testing.T) {
	s, _, c := v2BlockingCandidate(t)
	f := v2FollowerWithMode(s, SymmetricIncrementalVerification)
	before := f.UtigManager.clone()
	for _, kind := range []string{"wrong-output", "wrong-settlement", "late-state"} {
		bad := cloneV2Fragment(t, c.fragment)
		switch kind {
		case "wrong-output":
			bad.Continuation.Joins = bad.Continuation.Joins[1:]
		case "wrong-settlement":
			bad.Continuation.Settled = nil
		case "late-state":
			bad.CandidatePostStateID[0] ^= 1
		}
		v2Signed(t, s, bad)
		if f.verifyCandidate(bad, 0) {
			t.Fatal("wrong transition accepted")
		}
		if f.pending != nil || !reflect.DeepEqual(before, f.UtigManager) {
			t.Fatal("rejection leaked graph edits")
		}
	}
	if !f.verifyCandidate(c.fragment, 0) {
		t.Fatal("valid control")
	}
	if !reflect.DeepEqual(before, f.UtigManager) {
		t.Fatal("acceptance leaked graph edits")
	}
	if _, ok := f.CommitPending(c.digest); !ok {
		t.Fatal("valid commit")
	}
	control := rebuildMaterialization(f.committed, f.replicaCount, f.fFaulty, f.gamma)
	assertAblationGraph(t, f.UtigManager, control)
}

func TestV2MeasuredQueryCounts(t *testing.T) {
	for _, n := range []uint64{10, 50} {
		for _, k := range []int{4, 16, 64, 128, 256} {
			t.Run(fmt.Sprintf("n%d_k%d", n, k), func(t *testing.T) {
				x := v2AlphaFixture(t, n, k)
				f := x.service(false)
				counts := &PredicateCounts{}
				f.committed.QueryCounts = counts
				if !f.verifyCandidate(x.candidate.fragment, 0) {
					t.Fatal("counted control rejected")
				}
				wantInternal := uint64(k * (k - 1) / 2)
				wantFrontier := uint64((256 - k) * k)
				if counts.InternalPairs != wantInternal || counts.FrontierPairs != wantFrontier {
					t.Fatalf("actual count=%+v", counts)
				}
				g := x.service(false)
				g.verificationMode = ReconstructionVerification
				rebuilt := &PredicateCounts{}
				g.committed.QueryCounts = rebuilt
				if !g.verifyCandidate(x.candidate.fragment, 0) {
					t.Fatal("reconstruction control")
				}
				if rebuilt.RebuiltPairs != 256*255/2 {
					t.Fatalf("reference pair enumeration=%d", rebuilt.RebuiltPairs)
				}
				revisionResult(t, "v2_measured_queries", map[string]any{"n": n, "m": 256, "k": k, "internal_pairs": counts.InternalPairs, "frontier_pairs": counts.FrontierPairs, "witness_edges": counts.WitnessEdges, "directional_calls": counts.DirectionalCalls, "weight_reads": counts.WeightReads, "reconstructed_pairs": rebuilt.RebuiltPairs, "counters_timing": "untimed separate run"})
			})
		}
	}
}

// This fixed-input output check is independent of certificate parent choices.
func allOutputIDs(f *types.VerifiableFairOrderFragment) []types.TxID {
	var ids []types.TxID
	for _, b := range outputBatches(f) {
		ids = append(ids, b.Transactions...)
	}
	return ids
}
