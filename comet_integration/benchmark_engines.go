package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	autonetwork "SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/ofo"
	auto "SpeedFair_simplify/pkg/types"
	tnetwork "themisreference/pkg/network"
	themis "themisreference/pkg/ofo"
	ttype "themisreference/pkg/types"
)

type benchEngine interface {
	Ingest(auto.Transaction)
	Report()
	FlushReports()
	Validate([]byte) bool
	Commit([]byte) ([]auto.TxID, error)
	Count() int
	Settled() int
	State() [32]byte
	Sequence() uint64
	ReportStats() map[string]uint64
	Stop()
}

func gobWire(value any) ([]byte, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(value)
	return b.Bytes(), err
}
func gobRead(wire []byte, value any) error {
	return gob.NewDecoder(bytes.NewReader(wire)).Decode(value)
}

type captureAutoNet struct {
	mu     sync.Mutex
	orders map[uint64]*auto.LocalOrder
}

func (*captureAutoNet) Register(uint64, func(autonetwork.Message)) {}
func (n *captureAutoNet) Send(m autonetwork.Message) bool {
	if o, ok := m.Payload.(*auto.LocalOrder); ok {
		n.mu.Lock()
		n.orders[m.From] = o
		n.mu.Unlock()
	}
	return true
}
func (*captureAutoNet) Stop()                            {}
func (*captureAutoNet) WaitForPeers(time.Duration) error { return nil }

type autoBench struct {
	service *ofo.OFOService
	net     *captureAutoNet
	all     []*autoBench
	tick    uint64
}

func (e *autoBench) Ingest(t auto.Transaction) {
	e.service.HandleMessage(autonetwork.Message{Payload: &t})
}
func (e *autoBench) Report() { e.service.GenerateAndSendLocalOrder() }
func (e *autoBench) FlushReports() {
	if e.all == nil {
		return
	}
	e.tick++
	for offset := 0; offset < len(e.all); offset++ {
		sender := (int(e.tick) + offset) % len(e.all)
		n := e.all[sender].net
		n.mu.Lock()
		o := n.orders[uint64(sender)]
		n.mu.Unlock()
		if o != nil {
			e.service.HandleMessage(autonetwork.Message{From: uint64(sender), Payload: o})
		}
	}
}
func (e *autoBench) Validate(wire []byte) bool {
	var f auto.VerifiableFairOrderFragment
	if gobRead(wire, &f) != nil || f.FinalOrder == nil || f.Certificate == nil {
		return false
	}
	digest := auto.FragmentDigest(&f)
	if old, ok := e.service.PendingDigest(); ok {
		if old == digest {
			return true
		}
		if e.service.ReplicaID == 0 {
			return false
		}
		e.service.AbandonPending(old)
	}
	if e.service.ReplicaID == 0 {
		return false
	}
	e.service.HandleMessage(autonetwork.Message{From: 0, Payload: &f})
	got, ok := e.service.PendingDigest()
	return ok && got == digest
}
func (e *autoBench) Commit(wire []byte) ([]auto.TxID, error) {
	var f auto.VerifiableFairOrderFragment
	if err := gobRead(wire, &f); err != nil {
		return nil, err
	}
	b, ok := e.service.CommitPending(auto.FragmentDigest(&f))
	if !ok {
		return nil, fmt.Errorf("AUTIG commit gate")
	}
	var ids []auto.TxID
	for _, batch := range b {
		ids = append(ids, batch.Transactions...)
	}
	return ids, nil
}
func (e *autoBench) Count() int       { return e.service.Progress().Executed }
func (e *autoBench) Settled() int     { return e.service.Progress().Settled }
func (e *autoBench) State() [32]byte  { return e.service.CommittedStateID() }
func (e *autoBench) Sequence() uint64 { return e.service.Progress().Sequence }
func (e *autoBench) Stop()            { e.service.Stop() }

// ReportStats exposes report-capacity diagnostics: how often a fresh
// extension filled the configured size, and the report lengths.
func (e *autoBench) ReportStats() map[string]uint64 {
	r := e.service.Benchmark.Report(e.service.ReplicaID)
	out := make(map[string]uint64)
	for _, k := range []string{"lo_fresh", "lo_refresh", "lo_capacity_hits", "lo_retransmit_attempts"} {
		out[k] = r.Counts[k]
	}
	ids := r.Samples["lo_fresh_ids"]
	out["lo_fresh_ids_sum"], out["lo_fresh_ids_max"] = ids.Sum, ids.Max
	return out
}

type captureThemisNet struct{}

func (*captureThemisNet) Register(uint64, func(tnetwork.Message)) {}
func (*captureThemisNet) Send(tnetwork.Message) bool              { return true }
func (*captureThemisNet) Stop()                                   {}
func (*captureThemisNet) WaitForPeers(time.Duration) error        { return nil }

type themisBench struct {
	service  *themis.OFOService
	mu       sync.Mutex
	report   *ttype.ReplicaOrders
	all      []*themisBench
	tick     uint64
	cached   [32]byte
	cachedOK bool
	outgoing chan<- []byte
	quorum   int
}

func (e *themisBench) Ingest(t auto.Transaction) {
	e.service.HandleMessage(tnetwork.Message{Payload: &ttype.Transaction{ID: ttype.TransactionID(t.CanonicalBytes), CanonicalBytes: t.CanonicalBytes, SubmissionTime: t.SubmissionTime}})
}
func (e *themisBench) Report() { e.service.GenerateReportForExperiment() }
func (e *themisBench) FlushReports() {
	if e.all == nil {
		return
	}
	e.mu.Lock()
	if e.cachedOK {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	e.tick++
	var reports []*ttype.ReplicaOrders
	for i := 0; i < e.quorum; i++ {
		sender := (int(e.tick) + i) % len(e.all)
		a := e.all[sender]
		a.mu.Lock()
		o := a.report
		a.mu.Unlock()
		if o == nil {
			return
		}
		reports = append(reports, o)
	}
	p := e.service.BuildFromReportsForExperiment(reports)
	if p == nil {
		return
	}
	wire, err := gobWire(p)
	if err != nil {
		panic(err)
	}
	e.mu.Lock()
	e.cached, e.cachedOK = ttype.LeaderProposalDigest(p), true
	e.mu.Unlock()
	e.outgoing <- wire
}
func (e *themisBench) Validate(wire []byte) bool {
	var p ttype.LeaderProposal
	if gobRead(wire, &p) != nil || p.Graph == nil || p.Proof == nil {
		return false
	}
	digest := ttype.LeaderProposalDigest(&p)
	e.mu.Lock()
	if e.cachedOK && e.cached == digest {
		e.mu.Unlock()
		return true
	}
	e.mu.Unlock()
	if !e.service.VerifyProposal(&p) {
		return false
	}
	for id := range p.Graph.Nodes {
		if b, _, err := benchRegistry.Resolve(auto.TxID(id)); err == nil {
			e.Ingest(auto.Transaction{ID: auto.TxID(id), CanonicalBytes: b})
		}
	}
	e.mu.Lock()
	e.cached, e.cachedOK = digest, true
	e.mu.Unlock()
	return true
}
func (e *themisBench) Commit(wire []byte) ([]auto.TxID, error) {
	var p ttype.LeaderProposal
	if err := gobRead(wire, &p); err != nil {
		return nil, err
	}
	e.mu.Lock()
	ok := e.cachedOK && e.cached == ttype.LeaderProposalDigest(&p)
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("Themis commit digest gate")
	}
	before := e.service.GetFinalizedCount()
	if !e.service.CommitVerifiedProposal(&p) {
		return nil, fmt.Errorf("Themis native commit")
	}
	all := e.service.GetFinalizedOrder()
	ids := make([]auto.TxID, len(all)-before)
	for i, id := range all[before:] {
		body, _, err := benchRegistry.Resolve(auto.TxID(id))
		if err != nil {
			return nil, err
		}
		ids[i] = auto.TransactionID(body)
	}
	e.mu.Lock()
	e.cachedOK = false
	e.mu.Unlock()
	return ids, nil
}
func (e *themisBench) Count() int                     { return e.service.GetFinalizedCount() }
func (e *themisBench) Settled() int                   { return e.Count() }
func (e *themisBench) State() [32]byte                { _, _, h := e.service.CommittedContext(); return h }
func (e *themisBench) Sequence() uint64               { return e.service.CommittedSequenceForExperiment() }
func (e *themisBench) Stop()                          { e.service.Stop() }
func (e *themisBench) ReportStats() map[string]uint64 { return nil }

func makeBenchEngines(mode string, delay uint64, loSize int, out chan<- []byte, committee benchCommittee) ([]benchEngine, error) {
	if err := committee.validate(); err != nil {
		return nil, err
	}
	a := newCommitteeAuth(committee.N)
	engines := make([]benchEngine, committee.N)
	if mode == "themis" {
		all := make([]*themisBench, committee.N)
		for i := 0; i < committee.N; i++ {
			s, err := themis.NewOFOService(uint64(i), uint64(committee.N), uint64(committee.F), committee.Gamma, &captureThemisNet{}, 100, 50, false, a)
			if err != nil {
				return nil, err
			}
			e := &themisBench{service: s, outgoing: out, quorum: committee.N - committee.F}
			all[i] = e
			engines[i] = e
			s.CaptureReportsForExperiment(func(o *ttype.ReplicaOrders) { e.mu.Lock(); e.report = o; e.mu.Unlock() })
		}
		all[0].all = all
		return engines, nil
	}
	all := make([]*autoBench, committee.N)
	for i := 0; i < committee.N; i++ {
		net := &captureAutoNet{orders: make(map[uint64]*auto.LocalOrder)}
		var sink ofo.CandidateHandler
		if i == 0 {
			sink = func(f *auto.VerifiableFairOrderFragment, _ [32]byte) {
				wire, err := gobWire(f)
				if err != nil {
					panic(err)
				}
				out <- wire
			}
		}
		s, err := ofo.NewOFOService(uint64(i), uint64(committee.N), uint64(committee.F), committee.Gamma, net, loSize, nil, false, a, &admission{items: make(map[auto.TxID][]byte), fallback: benchRegistry}, auto.AuthContext{Epoch: 1}, auto.ProtocolGenesis{}, sink)
		if err != nil {
			return nil, err
		}
		if err = s.EnableContinuation(delay); err != nil {
			return nil, err
		}
		s.Benchmark.Begin(time.Now(), time.Now().Add(24*time.Hour))
		validation := ofo.StructuralVerification
		switch mode {
		case "certificate":
		case "incremental-undo":
			validation = ofo.SymmetricIncrementalVerification
		case "reconstruction":
			validation = ofo.ReconstructionVerification
		default:
			return nil, fmt.Errorf("unknown benchmark mode")
		}
		if err = s.SetVerificationMode(validation); err != nil {
			return nil, err
		}
		e := &autoBench{service: s, net: net}
		all[i], engines[i] = e, e
	}
	all[0].all = all
	return engines, nil
}

func benchWireHash(wire []byte) [32]byte { return sha256.Sum256(wire) }

var benchRegistry *admission
