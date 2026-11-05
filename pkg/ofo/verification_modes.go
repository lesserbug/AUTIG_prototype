package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
)

type VerificationMode uint8

const (
	StructuralVerification VerificationMode = iota
	SymmetricIncrementalVerification
	ReconstructionVerification
)

// All validators in a deployment use the configured verification language.
// The modes share evidence, output, continuation, and committed state semantics.
func (s *OFOService) SetVerificationMode(mode VerificationMode) error {
	s.rwMu.Lock()
	defer s.rwMu.Unlock()
	if s.committed.FragmentSeq != 0 || s.pending != nil || mode > ReconstructionVerification {
		return fmt.Errorf("invalid or late verification mode")
	}
	s.verificationMode = mode
	if mode == SymmetricIncrementalVerification && s.UtigManager == nil {
		s.UtigManager = NewDependencyManager(s.replicaCount, s.fFaulty, s.gamma)
	}
	return nil
}

func (s *OFOService) verifyByGraph(pre *EvidenceState, f *types.VerifiableFairOrderFragment, nodes map[types.TxID]bool, pairs map[pairKey]bool) (*EvidenceState, error) {
	var manager *DependencyManager
	mutate := s.verificationMode == ReconstructionVerification
	if mutate {
		manager = rebuildMaterialization(pre, s.replicaCount, s.fFaulty, s.gamma)
	} else {
		if s.UtigManager == nil {
			return nil, fmt.Errorf("missing retained reference graph")
		}
		manager = s.UtigManager
		rollback := beginGraphTrial(manager, pre, nodes, pairs)
		defer rollback()
	}
	post, expected, _, c, _, err := s.buildOutputMode(pre, manager, false, mutate)
	if err != nil {
		return nil, err
	}
	zero := [32]byte{}
	if types.ContinuationStateIdentifier(0, 0, zero, zero, expected, c, zero) != types.ContinuationStateIdentifier(0, 0, zero, zero, f.FinalOrder, f.Continuation, zero) {
		return nil, fmt.Errorf("graph-derived output mismatch")
	}
	return post, nil
}

func (s *OFOService) commitReferenceGraph(candidate *pendingCandidate) {
	if s.isLeader || s.verificationMode != SymmetricIncrementalVerification {
		return
	}
	s.UtigManager.refresh(candidate.preState, candidate.graphNodes, candidate.graphPairs)
	retired := make(map[types.TxID]bool)
	for id := range candidate.preState.Live {
		if !candidate.postState.Live[id] {
			retired[id] = true
		}
	}
	if len(retired) > 0 {
		s.UtigManager.removeFinalized(retired)
	}
}
