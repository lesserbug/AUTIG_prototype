package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"testing"
)

func TestRevisionCanonicalNilBinding(t *testing.T) {
	x := makeAblationFixture(t, 10, 2, 1, ablationCase{name: "empty-binding", history: 30, fresh: 0}, 7)
	valid := x.candidate.fragment
	if len(valid.FinalOrder.Batches) != 0 || len(valid.Certificate.Part) != 0 || len(valid.Certificate.BlockForest) != 0 {
		t.Fatal("fixture must have a valid empty output and certificate")
	}
	digest := types.FragmentDigest(valid)
	// Hash of the same valid fixture before the invalid-object fix. Existing
	// well-formed fragments keep their byte encoding and signatures unchanged.
	if fmt.Sprintf("%x", digest) != "5e6155218405ce1f339f8fe54e7847a926f0a82a8c652cfc498e705abcf9d73b" {
		t.Fatal("valid empty-fragment encoding changed")
	}
	revisionResult(t, "canonical_empty_binding", map[string]any{"valid_digest": fmt.Sprintf("%x", digest)})
	for _, kind := range []string{"nil-output", "nil-certificate"} {
		bad := cloneAblationFragment(t, valid)
		if kind == "nil-output" {
			bad.FinalOrder = nil
		} else {
			bad.Certificate = nil
		}
		alias := types.FragmentDigest(bad) == digest
		originalSignatureStillVerifies := x.auth.VerifyLeader(x.base.authContext, bad.LeaderID, types.FragmentDigest(bad), bad.LeaderSignature)
		revisionResult(t, "canonical_nil_alias", map[string]any{"kind": kind, "aliases_valid_empty_digest": alias, "original_signature_verifies": originalSignatureStillVerifies, "orderfault_not_implemented": true})
		if alias || originalSignatureStillVerifies {
			t.Errorf("invalid %s aliases an authenticated valid empty candidate", kind)
		}
	}
}
