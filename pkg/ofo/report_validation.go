package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
)

// Read-only evidence validation is shared by quorum admission and replay.
// An unavailable or malformed extension never consumes a quorum slot.
func validateReport(state *EvidenceState, order *types.LocalOrder, epoch, seq, n uint64, limit int, auth types.Authenticator, admission types.TransactionAdmission, observer ...func(types.TxID)) (map[types.TxID]types.TxID, error) {
	resolved := make(map[types.TxID]types.TxID)
	if order == nil {
		return resolved, fmt.Errorf("nil local order")
	}
	if order.ReplicaID >= n || order.Epoch != epoch || order.FragmentSeq != seq {
		return resolved, fmt.Errorf("local order context mismatch for replica %d", order.ReplicaID)
	}
	if len(order.OrderedTxs) > limit {
		return resolved, fmt.Errorf("local order from replica %d exceeds size limit", order.ReplicaID)
	}
	if order.NewPosition != order.PrevPosition+uint64(len(order.OrderedTxs)) {
		return resolved, fmt.Errorf("local order positions are not contiguous for replica %d", order.ReplicaID)
	}
	if order.PrevPosition != state.Seq[order.ReplicaID] || order.PrevHead != state.Head[order.ReplicaID] {
		return resolved, fmt.Errorf("local order predecessor mismatch for replica %d", order.ReplicaID)
	}
	if order.NewHead != types.LocalOrderHead(order) {
		return resolved, fmt.Errorf("local order hash-chain head mismatch for replica %d", order.ReplicaID)
	}
	if !auth.VerifyReplica(order.ReplicaID, types.LocalOrderDigest(order), order.Signature) {
		return resolved, fmt.Errorf("invalid local order signature for replica %d", order.ReplicaID)
	}
	seen := make(map[types.TxID]bool)
	for _, id := range order.OrderedTxs {
		if seen[id] {
			return resolved, fmt.Errorf("duplicate identifier in replica %d extension", order.ReplicaID)
		}
		seen[id] = true
		if _, done := state.doneLookup(id); done {
			continue
		}
		if _, exists := state.Pos[order.ReplicaID][id]; exists {
			return resolved, fmt.Errorf("replica %d assigned a second live first position", order.ReplicaID)
		}
		canonical, reference, err := admission.Resolve(id)
		if err != nil {
			return resolved, fmt.Errorf("transaction %x is unavailable: %w", id, err)
		}
		if types.TransactionID(canonical) != id || reference != id {
			return resolved, fmt.Errorf("transaction %x has an invalid content commitment", id)
		}
		if err := admission.ValidateAdmission(canonical); err != nil {
			return resolved, fmt.Errorf("transaction %x failed admission: %w", id, err)
		}
		if previous, exists := state.TxRef[id]; exists && previous != reference {
			return resolved, fmt.Errorf("transaction %x changed content reference", id)
		}
		resolved[id] = reference
		if len(observer) > 0 && observer[0] != nil {
			observer[0](id)
		}
	}
	return resolved, nil
}

// Caller holds rwMu. Valid fetched contents are observations even when the
// enclosing candidate is rejected later; rollback never removes them.
func (s *OFOService) observeIDLocked(id types.TxID) {
	if s.observed == nil {
		s.observed = make(map[types.TxID]bool)
	}
	if !s.observed[id] {
		s.observed[id] = true
		s.receiptQueue = append(s.receiptQueue, id)
		if s.onObservation != nil {
			s.onObservation(id)
		}
	}
}

// Instrumentation receives the actual first complete observation, including
// content fetched while validating reports. Install before workload injection.
func (s *OFOService) SetObservationHandler(handler func(types.TxID)) {
	s.rwMu.Lock()
	defer s.rwMu.Unlock()
	s.onObservation = handler
}
