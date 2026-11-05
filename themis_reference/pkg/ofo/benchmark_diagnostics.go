package ofo

import "sync/atomic"

// Observations only: none of these counters affect ordering or scheduling.
type benchmarkCounters struct {
	listCount, listIDs, listMax                atomic.Uint64
	collectorRejected, batchesQueued           atomic.Uint64
	buildCount, buildNil, buildNS, buildMaxNS  atomic.Uint64
	hostingCount, hostingNS, proposalsAccepted atomic.Uint64
}

func benchmarkMax(counter *atomic.Uint64, value uint64) {
	for old := counter.Load(); value > old; old = counter.Load() {
		if counter.CompareAndSwap(old, value) {
			return
		}
	}
}

// BenchmarkDiagnostics is a sampled, non-atomic snapshot. Queue depths and pool
// sizes are instantaneous; counters are cumulative since service construction.
// List sizes are this replica's complete fresh list, not a transaction cap.
type BenchmarkDiagnostics struct {
	ReplicaID         uint64 `json:"replica_id"`
	ListCount         uint64 `json:"list_count"`
	ListIDs           uint64 `json:"list_ids_total"`
	ListMax           uint64 `json:"list_ids_max"`
	CollectorRejected uint64 `json:"collector_rejected"`
	BatchesQueued     uint64 `json:"batches_queued"`
	BuildCount        uint64 `json:"build_count"`
	BuildNil          uint64 `json:"build_nil"`
	BuildNS           uint64 `json:"build_ns_total"`
	BuildMaxNS        uint64 `json:"build_ns_max"`
	HostingCount      uint64 `json:"hosting_count"`
	HostingNS         uint64 `json:"hosting_ns_total"`
	ProposalsAccepted uint64 `json:"proposals_accepted"`
	EvidenceQueue     int    `json:"evidence_queue"`
	BatchQueue        int    `json:"batch_queue"`
	DeferredProposals int    `json:"deferred_proposals"`
	ReceiptPool       int    `json:"receipt_pool"`
}

func (s *OFOService) GetBenchmarkDiagnostics() BenchmarkDiagnostics {
	c := &s.benchmarkCounters
	d := BenchmarkDiagnostics{
		ReplicaID: s.ReplicaID, ListCount: c.listCount.Load(), ListIDs: c.listIDs.Load(), ListMax: c.listMax.Load(),
		CollectorRejected: c.collectorRejected.Load(), BatchesQueued: c.batchesQueued.Load(),
		BuildCount: c.buildCount.Load(), BuildNil: c.buildNil.Load(), BuildNS: c.buildNS.Load(), BuildMaxNS: c.buildMaxNS.Load(),
		HostingCount: c.hostingCount.Load(), HostingNS: c.hostingNS.Load(), ProposalsAccepted: c.proposalsAccepted.Load(),
		EvidenceQueue: len(s.replicaOrdersChan), BatchQueue: len(s.batchReadyChan), ReceiptPool: s.GetTxPoolSize(),
	}
	s.deferredMu.RLock()
	d.DeferredProposals = len(s.deferredProposals)
	s.deferredMu.RUnlock()
	return d
}
