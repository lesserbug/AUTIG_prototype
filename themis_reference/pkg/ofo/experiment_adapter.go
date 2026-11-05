package ofo

import "themisreference/pkg/types"

// The native generation/construction/verification/commit algorithms are reused.
// These accessors only expose their experiment boundary and report transport.
func (s *OFOService) CaptureReportsForExperiment(sink func(*types.ReplicaOrders)) {
	s.reportObserver = sink
}
func (s *OFOService) GenerateReportForExperiment() { s.generateAndSendOrders() }
func (s *OFOService) BuildFromReportsForExperiment(reports []*types.ReplicaOrders) *types.LeaderProposal {
	return s.buildProposal(reports)
}
func (s *OFOService) CommittedSequenceForExperiment() uint64 {
	s.deferredMu.RLock()
	defer s.deferredMu.RUnlock()
	return s.currentBlockHeight
}
