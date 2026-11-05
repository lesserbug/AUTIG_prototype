package ofo

import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"bytes"
	"context"
	"encoding/gob"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Observe which real verification stages run without replacing their decisions.
// Only the synchronous follower uses this wrapper.
type proposalAuthTrace struct {
	types.Authenticator
	leaderChecks, replicaChecks int
}

func (a *proposalAuthTrace) VerifyLeader(ctx types.AuthContext, id uint64, digest [32]byte, signature []byte) bool {
	a.leaderChecks++
	return a.Authenticator.VerifyLeader(ctx, id, digest, signature)
}

func (a *proposalAuthTrace) VerifyReplica(id uint64, digest [32]byte, signature []byte) bool {
	a.replicaChecks++
	return a.Authenticator.VerifyReplica(id, digest, signature)
}

func encodeRegressionProposal(t *testing.T, fragment *types.VerifiableFairOrderFragment) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(fragment); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func decodeRegressionProposal(t *testing.T, wire []byte) *types.VerifiableFairOrderFragment {
	t.Helper()
	var fragment types.VerifiableFairOrderFragment
	if err := gob.NewDecoder(bytes.NewReader(wire)).Decode(&fragment); err != nil {
		t.Fatal(err)
	}
	return &fragment
}

// Reuse the production constructor and existing Ed25519/admission fixtures.
// The two transactions and four identical receipt orders yield two singleton
// SCCs with an edge from the first to the second (n=5, f=1, gamma=1).
func newProposalRegressionFixture(t *testing.T) (*OFOService, *OFOService, *pendingCandidate, *proposalAuthTrace) {
	t.Helper()
	auth := newTestAuthenticator(t, 5)
	admission := newTestAdmission()
	ctx := types.AuthContext{Epoch: 7, LeaderID: 0, Identifier: [32]byte{1}}
	genesis := types.ProtocolGenesis{StateID: [32]byte{2}, FragmentDigest: [32]byte{3}}
	leader, err := NewOFOService(0, 5, 1, 1, newTestNetwork(), 10, nil, false, auth, admission, ctx, genesis, func(*types.VerifiableFairOrderFragment, [32]byte) {})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(leader.Stop)
	trace := &proposalAuthTrace{Authenticator: auth}
	follower, err := NewOFOService(4, 5, 1, 1, newTestNetwork(), 10, nil, false, trace, admission, ctx, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(follower.Stop)
	x, y := testTx(t, admission, 21), testTx(t, admission, 22)
	var orders []*types.LocalOrder
	for id := uint64(1); id < 5; id++ {
		orders = append(orders, signedOrder(t, auth, leader.committed, id, 1, x, y))
	}
	candidate, err := leader.constructCandidate(orders)
	if err != nil {
		t.Fatal(err)
	}
	return leader, follower, candidate, trace
}

func requireProposalSource(t *testing.T, s *OFOService, fragment *types.VerifiableFairOrderFragment) {
	t.Helper()
	if fragment.Epoch != s.authContext.Epoch || fragment.LeaderID != s.authContext.LeaderID || fragment.AuthContextID != s.authContext.Identifier ||
		fragment.FragmentSeq != s.committed.FragmentSeq+1 || fragment.PreviousStateID != s.committed.StateID || fragment.PreviousFragmentDigest != s.latestCommittedFragment {
		t.Fatal("fixture does not match the complete current source context")
	}
}

// Exercise the real message handler and commit API; compare all EvidenceState
// fields, including caches, rather than only its identifier. These fixtures use
// an idle leader pipeline and a synchronous follower, so no sleeps are needed.
func requireProposalRejectedWithoutInstall(t *testing.T, s *OFOService, wire []byte) {
	t.Helper()
	before, ctx, predecessor := s.committed.clone(), s.authContext, s.latestCommittedFragment
	if _, ok := s.PendingDigest(); ok {
		t.Fatal("rejection test requires no previously accepted candidate")
	}
	fragment := decodeRegressionProposal(t, wire)
	s.HandleMessage(network.Message{Type: "AUTIGCandidate", From: fragment.LeaderID, To: s.ReplicaID, Payload: fragment})
	if _, ok := s.PendingDigest(); ok {
		t.Fatal("rejected proposal was retained as a candidate")
	}
	if _, ok := s.CommitPending(types.FragmentDigest(fragment)); ok {
		t.Fatal("rejected proposal could be committed")
	}
	if !reflect.DeepEqual(s.committed, before) || s.latestCommittedFragment != predecessor {
		t.Fatal("rejection or attempted commit changed the committed state/predecessor")
	}
	if s.authContext != ctx {
		t.Fatal("rejection changed the authorization context")
	}
}

func TestProposalRegressionCommittedFragmentReplay(t *testing.T) {
	leader, follower, candidate, trace := newProposalRegressionFixture(t)
	wire := encodeRegressionProposal(t, candidate.fragment)
	original := decodeRegressionProposal(t, wire)
	requireProposalSource(t, follower, original)
	follower.HandleMessage(network.Message{From: 0, Payload: original})
	if digest, ok := follower.PendingDigest(); !ok || digest != candidate.digest {
		t.Fatal("original valid proposal did not pass the real message handler")
	}
	ctx := follower.authContext
	for _, s := range []*OFOService{leader, follower} {
		if _, ok := s.CommitPending(candidate.digest); !ok {
			t.Fatal("valid proposal failed the hosting commit interface")
		}
	}
	if follower.committed.FragmentSeq != original.FragmentSeq || follower.committed.StateID != original.CandidatePostStateID ||
		follower.latestCommittedFragment != candidate.digest || follower.authContext != ctx || follower.GetFinalizedCount() != 2 ||
		!reflect.DeepEqual(leader.committed, follower.committed) {
		t.Fatal("commit did not install the expected successor state with unchanged authorization")
	}
	// Baseline is AFTER commit. Each delivery decodes the exact saved bytes.
	trace.leaderChecks, trace.replicaChecks = 0, 0
	for i := 0; i < 3; i++ {
		replay := decodeRegressionProposal(t, wire)
		if types.FragmentDigest(replay) != candidate.digest || !leader.auth.VerifyLeader(ctx, replay.LeaderID, candidate.digest, replay.LeaderSignature) {
			t.Fatal("replay lost its original content binding or valid signature")
		}
		if replay.FragmentSeq == follower.committed.FragmentSeq+1 || replay.PreviousStateID == follower.committed.StateID || replay.PreviousFragmentDigest == follower.latestCommittedFragment {
			t.Fatal("fixture did not advance the sequence and both predecessors")
		}
		requireProposalRejectedWithoutInstall(t, follower, wire)
	}
	if trace.leaderChecks != 0 || trace.replicaChecks != 0 {
		t.Fatal("stale replay passed the current-context gate")
	}
	t.Run("OrderFaultAndWaitDeadlineNotImplemented", func(t *testing.T) {
		t.Skip("no OrderFault path or fragment-wait timer; repeated replay above does not test timeout/handoff timing")
	})
}

func TestProposalRegressionOldEpochContextFilter(t *testing.T) {
	for _, changeLeader := range []bool{false, true} {
		name := "epoch_only"
		if changeLeader {
			name = "epoch_and_authorized_leader"
		}
		t.Run(name, func(t *testing.T) {
			leader, follower, candidate, trace := newProposalRegressionFixture(t)
			wire := encodeRegressionProposal(t, candidate.fragment)
			oldContext := follower.authContext
			requireProposalSource(t, follower, candidate.fragment)
			follower.HandleMessage(network.Message{From: 0, Payload: decodeRegressionProposal(t, wire)})
			if digest, ok := follower.PendingDigest(); !ok || digest != candidate.digest {
				t.Fatal("proposal was not valid in its original context")
			}
			if !follower.AbandonPending(candidate.digest) {
				t.Fatal("could not release the uncommitted validation control")
			}
			before, predecessor := follower.committed.clone(), follower.latestCommittedFragment
			// Context-filter test ONLY: there is no committed handoff API.
			// The epoch-only control isolates the epoch comparison; the other
			// control also models replacement authorization, retaining old keys.
			follower.authContext.Epoch++
			if changeLeader {
				follower.authContext.LeaderID = 1
				follower.authContext.Identifier = [32]byte{4}
			}
			if !reflect.DeepEqual(before, follower.committed) || predecessor != follower.latestCommittedFragment ||
				candidate.fragment.FragmentSeq != follower.committed.FragmentSeq+1 || candidate.fragment.PreviousStateID != follower.committed.StateID || candidate.fragment.PreviousFragmentDigest != predecessor {
				t.Fatal("old-epoch fixture changed sequence, state or predecessors")
			}
			if types.FragmentDigest(decodeRegressionProposal(t, wire)) != candidate.digest || !leader.auth.VerifyLeader(oldContext, 0, candidate.digest, candidate.fragment.LeaderSignature) {
				t.Fatal("old leader's original signature/content binding is invalid")
			}
			trace.leaderChecks, trace.replicaChecks = 0, 0
			requireProposalRejectedWithoutInstall(t, follower, wire)
			if trace.leaderChecks != 0 || trace.replicaChecks != 0 {
				t.Fatal("old authorization passed the current-context gate")
			}
		})
	}
	t.Run("CommittedHandoffFaultAndDeadlineNotImplemented", func(t *testing.T) {
		t.Skip("context was set directly: no committed handoff, OrderFault or fragment-wait deadline integration exists")
	})
}

func TestProposalRegressionSignedMalformedRank(t *testing.T) {
	for _, resign := range []bool{true, false} {
		name := "leader_signed_semantic_error"
		if !resign {
			name = "third_party_tamper_original_signature"
		}
		t.Run(name, func(t *testing.T) {
			leader, follower, candidate, trace := newProposalRegressionFixture(t)
			validWire := encodeRegressionProposal(t, candidate.fragment)
			requireProposalSource(t, follower, candidate.fragment)
			follower.HandleMessage(network.Message{From: 0, Payload: decodeRegressionProposal(t, validWire)})
			if digest, ok := follower.PendingDigest(); !ok || digest != candidate.digest {
				t.Fatal("valid control failed real verification")
			}
			if !follower.AbandonPending(candidate.digest) {
				t.Fatal("could not release the valid control")
			}
			bad := decodeRegressionProposal(t, validWire)
			if len(bad.Certificate.Part) != 2 || len(bad.Certificate.Part[0].Transactions) != 1 || len(bad.Certificate.Part[1].Transactions) != 1 {
				t.Fatal("fixture must contain two singleton SCCs")
			}
			x, y := bad.Certificate.Part[0].Transactions[0], bad.Certificate.Part[1].Transactions[0]
			threshold := types.CalculateEdgeThreshold(5, 1, 1)
			if !localEdgePred(candidate.preState, x, y, threshold) || localEdgePred(candidate.preState, y, x, threshold) {
				t.Fatal("fixture must have the directed cross-component edge C1 -> C2")
			}
			// Reverse component order/ranks together with their matching output
			// membership, preserving batch indices and rank/index consistency.
			// This reaches the actual edge-monotonicity check, not a format check.
			bad.Certificate.Part[0], bad.Certificate.Part[1] = bad.Certificate.Part[1], bad.Certificate.Part[0]
			bad.Certificate.Part[0].Rank, bad.Certificate.Part[1].Rank = 0, 1
			bad.FinalOrder.Batches[0].Transactions, bad.FinalOrder.Batches[1].Transactions = bad.FinalOrder.Batches[1].Transactions, bad.FinalOrder.Batches[0].Transactions
			post := candidate.preState.clone()
			finalize(post, NewDependencyManager(5, 1, 1), bad.FinalOrder, bad.Certificate.Part)
			bad.CandidatePostStateID = post.StateID
			requireProposalSource(t, follower, bad)
			if err := verifyStructuralCertificate(candidate.preState, bad.FinalOrder, bad.Certificate, 5, 1, 1); err == nil || err.Error() != "cross-component edge does not increase rank" {
				t.Fatalf("wanted deterministic rank monotonicity error, got %v", err)
			}
			digest := types.FragmentDigest(bad)
			if digest == candidate.digest {
				t.Fatal("content binding did not change after rank mutation")
			}
			if resign {
				var err error
				bad.LeaderSignature, err = leader.auth.SignLeader(leader.authContext, digest)
				if err != nil {
					t.Fatal(err)
				}
			} else if !bytes.Equal(bad.LeaderSignature, candidate.fragment.LeaderSignature) || !leader.auth.VerifyLeader(leader.authContext, 0, candidate.digest, bad.LeaderSignature) {
				t.Fatal("tampering did not preserve the original signature and signed digest")
			}
			// There is no transmitted digest field: the receiver recomputes it.
			if leader.auth.VerifyLeader(leader.authContext, 0, digest, bad.LeaderSignature) != resign {
				t.Fatal("authentication did not distinguish signed malformation from tampering")
			}
			trace.leaderChecks, trace.replicaChecks = 0, 0
			requireProposalRejectedWithoutInstall(t, follower, encodeRegressionProposal(t, bad))
			wantReplicaChecks := 0
			if resign {
				wantReplicaChecks = len(bad.Evidence)
			}
			if trace.leaderChecks != 1 || trace.replicaChecks != wantReplicaChecks {
				t.Fatalf("wrong rejection stage: leader checks=%d, evidence checks=%d; want 1, %d", trace.leaderChecks, trace.replicaChecks, wantReplicaChecks)
			}
		})
	}
	t.Run("OrderFaultEvidenceNotImplemented", func(t *testing.T) {
		t.Skip("signed semantic error currently returns false; no OrderFault evidence generator, independent verifier or fault-driven handoff exists")
	})
}

func TestProposalRegressionSafeOutputBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		keep int
		want string
	}{
		{"omit_safe_terminal_scc", 0, "excluded Solid transaction has no blocker path"},
		{"hide_incoming_frontier", 1, "selected set has an incoming active frontier edge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, follower, candidate, trace := newProposalRegressionFixture(t)
			valid := candidate.fragment
			if !follower.verifyCandidate(valid, 0) || !follower.AbandonPending(candidate.digest) {
				t.Fatal("valid control failed production verification")
			}
			if len(valid.Certificate.Part) != 2 || len(valid.Certificate.Part[0].Transactions) != 1 || len(valid.Certificate.Part[1].Transactions) != 1 || len(valid.Certificate.BlockForest) != 0 {
				t.Fatal("fixture must output two safe singleton SCCs without blockers")
			}
			x, y := valid.Certificate.Part[0].Transactions[0], valid.Certificate.Part[1].Transactions[0]
			// Derive the evidence on the follower, independently of the leader's
			// materialized graph. Both attacks retain these exact signed orders.
			pre := follower.committed.clone()
			if _, _, err := applyEvidenceWithTouches(pre, valid.Evidence, valid.Epoch, valid.FragmentSeq, 5, 1, 1, 10, follower.auth, follower.admission, false); err != nil {
				t.Fatal(err)
			}
			threshold := types.CalculateEdgeThreshold(5, 1, 1)
			if pre.StateID != valid.CandidateEvidenceStateID || pre.states[x] != types.StateSolid || pre.states[y] != types.StateSolid ||
				!localEdgePred(pre, x, y, threshold) || localEdgePred(pre, y, x, threshold) {
				t.Fatal("fixture must contain safe x -> y, with no reverse edge")
			}
			bad := decodeRegressionProposal(t, encodeRegressionProposal(t, valid))
			bad.FinalOrder.Batches = bad.FinalOrder.Batches[tc.keep : tc.keep+1]
			bad.Certificate.Part = bad.Certificate.Part[tc.keep : tc.keep+1]
			bad.FinalOrder.Batches[0].Index = pre.FairnessBatchCounter + 1
			bad.Certificate.Part[0].Rank = 0
			post := pre.clone()
			finalize(post, NewDependencyManager(5, 1, 1), bad.FinalOrder, bad.Certificate.Part)
			bad.CandidatePostStateID = post.StateID
			requireProposalSource(t, follower, bad)
			digest := types.FragmentDigest(bad)
			var err error
			bad.LeaderSignature, err = leader.auth.SignLeader(leader.authContext, digest)
			if err != nil {
				t.Fatal(err)
			}
			if !leader.auth.VerifyLeader(leader.authContext, 0, digest, bad.LeaderSignature) {
				t.Fatal("mutated proposal is not authenticated")
			}
			// Exact production errors establish that membership, trees, ranks,
			// serialization and (for omission) the frontier check already passed.
			if err := verifyStructuralCertificate(pre, bad.FinalOrder, bad.Certificate, 5, 1, 1); err == nil || err.Error() != tc.want {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
			trace.leaderChecks, trace.replicaChecks = 0, 0
			requireProposalRejectedWithoutInstall(t, follower, encodeRegressionProposal(t, bad))
			if trace.leaderChecks != 1 || trace.replicaChecks != len(bad.Evidence) {
				t.Fatal("message handler rejected before authenticating leader/evidence")
			}
		})
	}
}

type testNetwork struct {
	mu       sync.RWMutex
	handlers map[uint64]func(network.Message)
}

func newTestNetwork() *testNetwork {
	return &testNetwork{handlers: make(map[uint64]func(network.Message))}
}
func (n *testNetwork) Register(id uint64, handler func(network.Message)) {
	n.mu.Lock()
	n.handlers[id] = handler
	n.mu.Unlock()
}
func (n *testNetwork) Send(message network.Message) bool {
	n.mu.RLock()
	handler := n.handlers[message.To]
	n.mu.RUnlock()
	if handler != nil {
		handler(message)
		return true
	}
	return false
}
func (n *testNetwork) Stop()                            {}
func (n *testNetwork) WaitForPeers(time.Duration) error { return nil }

type blockingAdmission struct {
	*testAdmission
	blockedID    types.TxID
	secondID     types.TxID
	blockedStart chan struct{}
	release      chan struct{}
	secondStored chan struct{}
	blockedOnce  sync.Once
	secondOnce   sync.Once
}

func (admission *blockingAdmission) Store(transaction types.Transaction) error {
	if transaction.ID == admission.blockedID {
		admission.blockedOnce.Do(func() { close(admission.blockedStart) })
		<-admission.release
	}
	if err := admission.testAdmission.Store(transaction); err != nil {
		return err
	}
	if transaction.ID == admission.secondID {
		admission.secondOnce.Do(func() { close(admission.secondStored) })
	}
	return nil
}

func TestCandidateIsNotInstalledBeforeHostingCommit(t *testing.T) {
	const n, f = uint64(2), uint64(0)
	auth := newTestAuthenticator(t, n)
	admission := newTestAdmission()
	authContext := types.AuthContext{Epoch: 7, LeaderID: 0, Identifier: [32]byte{1}}
	genesis := types.ProtocolGenesis{StateID: [32]byte{2}, FragmentDigest: [32]byte{3}}
	testNet := newTestNetwork()
	candidates := make(chan *types.VerifiableFairOrderFragment, 1)
	digests := make(chan [32]byte, 1)
	leader, err := NewOFOService(0, n, f, 1, testNet, 10, nil, false, auth, admission, authContext, genesis, func(fragment *types.VerifiableFairOrderFragment, digest [32]byte) {
		candidates <- fragment
		digests <- digest
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Stop()
	follower, err := NewOFOService(1, n, f, 1, testNet, 10, nil, false, auth, admission, authContext, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Stop()
	testNet.Register(0, leader.HandleMessage)
	testNet.Register(1, follower.HandleMessage)

	canonical := []byte{9}
	tx := &types.Transaction{ID: types.TransactionID(canonical), CanonicalBytes: canonical, SubmissionTime: time.Now()}
	testNet.Send(network.Message{Type: "Transaction", From: 0, To: 0, Payload: tx})
	testNet.Send(network.Message{Type: "Transaction", From: 0, To: 1, Payload: tx})
	leader.GenerateAndSendLocalOrder()
	follower.GenerateAndSendLocalOrder()

	var fragment *types.VerifiableFairOrderFragment
	var digest [32]byte
	select {
	case fragment = <-candidates:
		digest = <-digests
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not construct a candidate")
	}
	if leader.GetFinalizedCount() != 0 {
		t.Fatal("leader installed candidate before hosting commit")
	}
	testNet.Send(network.Message{Type: "AUTIGCandidate", From: 0, To: 1, Payload: fragment})
	if follower.GetFinalizedCount() != 0 {
		t.Fatal("follower installed verified candidate before hosting commit")
	}
	if _, pending := follower.PendingDigest(); !pending {
		t.Fatal("follower did not retain verified candidate")
	}
	follower.HandleMessage(network.Message{Type: "AUTIGCommit", From: 0, To: 1, Payload: &struct{ Digest [32]byte }{Digest: digest}})
	if follower.GetFinalizedCount() != 0 {
		t.Fatal("network message installed a candidate without a hosting commit callback")
	}
	if _, ok := leader.CommitPending(digest); !ok {
		t.Fatal("leader rejected matching hosting commit")
	}
	if _, ok := follower.CommitPending(digest); !ok {
		t.Fatal("follower rejected matching hosting commit")
	}
	if leader.GetFinalizedCount() != 1 || follower.GetFinalizedCount() != 1 {
		t.Fatal("matching hosting commit did not install candidate")
	}
}

func TestServiceRejectsUnsafeProtocolParameters(t *testing.T) {
	auth := newTestAuthenticator(t, 5)
	admission := newTestAdmission()
	testNet := newTestNetwork()
	authContext := types.AuthContext{Epoch: 1, LeaderID: 0}
	genesis := types.ProtocolGenesis{StateID: [32]byte{1}, FragmentDigest: [32]byte{2}}
	tests := []struct {
		name  string
		n, f  uint64
		gamma float64
	}{
		{name: "n below three-f-plus-one", n: 3, f: 1, gamma: 1},
		{name: "f not below n", n: 1, f: 1, gamma: 1},
		{name: "gamma at one half", n: 5, f: 1, gamma: 0.5},
		{name: "insufficient honest-quorum overlap", n: 4, f: 1, gamma: 1},
		{name: "ten nodes cannot tolerate three even at gamma one", n: 10, f: 3, gamma: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewOFOService(1, test.n, test.f, test.gamma, testNet, 10, nil, false, auth, admission, authContext, genesis, nil); err == nil {
				t.Fatal("unsafe protocol parameters were accepted")
			}
		})
	}
	service, err := NewOFOService(1, 5, 1, 1, testNet, 10, nil, false, auth, admission, authContext, genesis, nil)
	if err != nil {
		t.Fatalf("valid protocol parameters were rejected: %v", err)
	}
	service.Stop()
}

func TestFirstReceiptOrderSurvivesBlockedAdmission(t *testing.T) {
	canonicalX, canonicalY := []byte("x"), []byte("y")
	txX := types.Transaction{ID: types.TransactionID(canonicalX), CanonicalBytes: canonicalX}
	txY := types.Transaction{ID: types.TransactionID(canonicalY), CanonicalBytes: canonicalY}
	admission := &blockingAdmission{
		testAdmission: newTestAdmission(),
		blockedID:     txX.ID,
		secondID:      txY.ID,
		blockedStart:  make(chan struct{}),
		release:       make(chan struct{}),
		secondStored:  make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(admission.release)
		}
	}()

	distributed, err := network.NewDistributedNetwork(network.NetworkConfig{
		ReplicaID:   0,
		ReplicaAddr: map[uint64]string{0: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(distributed.Stop)
	auth := newTestAuthenticator(t, 1)
	service, err := NewOFOService(
		0, 1, 0, 1, distributed, 10, nil, false, auth, admission,
		types.AuthContext{Epoch: 1, LeaderID: 0},
		types.ProtocolGenesis{StateID: [32]byte{1}, FragmentDigest: [32]byte{2}},
		func(*types.VerifiableFairOrderFragment, [32]byte) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Stop)
	distributed.Register(0, service.HandleMessage)

	distributed.Send(network.Message{Type: "Transaction", From: 0, To: 0, Payload: &txX})
	select {
	case <-admission.blockedStart:
	case <-time.After(time.Second):
		t.Fatal("first transaction did not enter admission")
	}
	distributed.Send(network.Message{Type: "Transaction", From: 0, To: 0, Payload: &txY})
	select {
	case <-admission.secondStored:
		t.Fatal("second transaction overtook the blocked first receipt")
	case <-time.After(100 * time.Millisecond):
	}

	close(admission.release)
	released = true
	select {
	case <-admission.secondStored:
	case <-time.After(time.Second):
		t.Fatal("second transaction was not processed after releasing the first")
	}

	deadline := time.Now().Add(time.Second)
	for {
		service.rwMu.RLock()
		receipts := append([]types.TxID(nil), service.receiptQueue...)
		service.rwMu.RUnlock()
		if len(receipts) == 2 {
			if receipts[0] != txX.ID || receipts[1] != txY.ID {
				t.Fatalf("first-receipt order changed: got %x then %x", receipts[0], receipts[1])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipt log contains %d transactions, want 2", len(receipts))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCollectorWaitsForRoundOutcomeAndDropsCommittedRetransmissions(t *testing.T) {
	pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
	service := &OFOService{
		auth: newTestAuthenticator(t, 5), admission: newTestAdmission(), authContext: types.AuthContext{Epoch: 1}, loMaxSize: 10,
		committed:          NewEvidenceState(1, 5, [32]byte{}),
		pipelineCtx:        pipelineCtx,
		pipelineCancel:     pipelineCancel,
		localOrderChan:     make(chan *types.LocalOrder, 16),
		batchReadyChan:     make(chan []*types.LocalOrder, 2),
		collectorRoundDone: make(chan struct{}),
		replicaCount:       5,
		fFaulty:            1,
	}
	service.pipelineWg.Add(1)
	go service.runCollectorStage()
	defer func() {
		pipelineCancel()
		service.pipelineWg.Wait()
	}()

	for sender := uint64(1); sender < 5; sender++ {
		service.localOrderChan <- signedOrder(t, service.auth, service.committed, sender, 1)
	}
	select {
	case batch := <-service.batchReadyChan:
		if len(batch) != 4 {
			t.Fatalf("collector emitted %d extensions, want 4", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not emit the first batch")
	}

	for sender := uint64(1); sender < 5; sender++ {
		service.localOrderChan <- signedOrder(t, service.auth, service.committed, sender, 1)
	}
	select {
	case batch := <-service.batchReadyChan:
		t.Fatalf("collector emitted another batch before round completion: %+v", batch)
	default:
	}

	service.rwMu.Lock()
	service.committed.FragmentSeq = 1
	service.rwMu.Unlock()
	select {
	case service.collectorRoundDone <- struct{}{}:
	case <-time.After(time.Second):
		t.Fatal("collector was not waiting for the round outcome")
	}

	for _, sender := range []uint64{2, 3, 4, 0} {
		service.localOrderChan <- signedOrder(t, service.auth, service.committed, sender, 2)
	}
	select {
	case batch := <-service.batchReadyChan:
		if len(batch) != 4 {
			t.Fatalf("collector emitted %d extensions, want 4", len(batch))
		}
		for _, order := range batch {
			if order.FragmentSeq != 2 {
				t.Fatalf("collector carried fragment %d retransmission into fragment 2", order.FragmentSeq)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not emit the next committed-sequence batch")
	}
}

// An omitted extension is retained as a prefix: before admission the replica
// may only replace it with a longer extension of the same committed position.
func TestOmittedLocalOrderRetainsChunk(t *testing.T) {
	auth := newTestAuthenticator(t, 5)
	admission := newTestAdmission()
	net := newTestNetwork()
	var sent []*types.LocalOrder
	net.Register(0, func(msg network.Message) { sent = append(sent, msg.Payload.(*types.LocalOrder)) })
	s, err := NewOFOService(1, 5, 1, 1, net, 10, nil, false, auth, admission, types.AuthContext{Epoch: 1, LeaderID: 0}, types.ProtocolGenesis{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Benchmark.Begin(time.Now(), time.Now().Add(time.Minute))
	for _, value := range []string{"x", "y"} {
		b := []byte(value)
		s.HandleMessage(network.Message{Payload: &types.Transaction{ID: types.TransactionID(b), CanonicalBytes: b}})
		if value == "x" {
			s.GenerateAndSendLocalOrder()
		}
	}
	s.committed.FragmentSeq = 1
	s.applyCommittedLocalOrder(nil)
	s.GenerateAndSendLocalOrder()
	if len(sent) != 2 || len(sent[1].OrderedTxs) != 2 || sent[1].OrderedTxs[0] != sent[0].OrderedTxs[0] || sent[1].FragmentSeq != 2 ||
		sent[1].PrevPosition != sent[0].PrevPosition || sent[1].PrevHead != sent[0].PrevHead {
		t.Fatal("omitted chunk was dropped, reordered, or not extended from the same committed position")
	}
	if !auth.VerifyReplica(1, types.LocalOrderDigest(sent[1]), sent[1].Signature) {
		t.Fatal("invalid refreshed signature")
	}
	s.GenerateAndSendLocalOrder() // Nothing new: ordinary retransmission reuses that signature.
	if len(sent) != 3 || types.LocalOrderDigest(sent[2]) != types.LocalOrderDigest(sent[1]) {
		t.Fatal("pending extension was not retransmitted unchanged")
	}
	stats := s.Benchmark.Report(1)
	if stats.Counts["lo_fresh"] != 2 || stats.Counts["lo_refresh"] != 1 || stats.Counts["lo_retransmit_attempts"] != 1 || stats.Counts["lo_signatures"] != 2 {
		t.Fatalf("fresh/refresh/retransmit accounting is incorrect: %+v", stats)
	}
	// Committing the shorter first version consumes the pending predecessor;
	// the position beyond it is reported in the next extension.
	short := sent[0]
	s.committed.Seq[1], s.committed.Head[1] = short.NewPosition, short.NewHead
	s.committed.FragmentSeq = 2
	s.applyCommittedLocalOrder([]*types.LocalOrder{short})
	s.GenerateAndSendLocalOrder()
	last := sent[len(sent)-1]
	if last.PrevPosition != short.NewPosition || len(last.OrderedTxs) != 1 || last.OrderedTxs[0] != sent[1].OrderedTxs[1] {
		t.Fatal("remaining position after a shorter committed version was lost")
	}
}

func TestCandidateReplacementAndMeasurementDeadline(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "before cutoff", true: "after cutoff"}[expired], func(t *testing.T) {
			auth := newTestAuthenticator(t, 2)
			admission := newTestAdmission()
			s, err := NewOFOService(0, 2, 0, 1, newTestNetwork(), 10, nil, false, auth, admission, types.AuthContext{Epoch: 1, LeaderID: 0}, types.ProtocolGenesis{}, func(*types.VerifiableFairOrderFragment, [32]byte) {})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Stop()
			b := []byte("measured")
			id := types.TransactionID(b)
			s.HandleMessage(network.Message{Payload: &types.Transaction{ID: id, CanonicalBytes: b, SubmissionTime: time.Now()}})
			empty, err := s.constructCandidate([]*types.LocalOrder{signedOrder(t, auth, s.committed, 0, 1), signedOrder(t, auth, s.committed, 1, 1)})
			if err != nil {
				t.Fatal(err)
			}
			original := s.CommittedStateID()
			follower, err := NewOFOService(1, 2, 0, 1, newTestNetwork(), 10, nil, false, auth, admission, types.AuthContext{Epoch: 1, LeaderID: 0}, types.ProtocolGenesis{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !follower.verifyCandidate(empty.fragment, 0) || !follower.AbandonPending(empty.digest) {
				t.Fatal("follower did not verify and abandon first candidate")
			}
			if !s.AbandonPending(empty.digest) || s.CommittedStateID() != original {
				t.Fatal("abandon changed committed state")
			}
			candidate, err := s.constructCandidate([]*types.LocalOrder{signedOrder(t, auth, s.committed, 1, 1, id), signedOrder(t, auth, s.committed, 0, 1, id)})
			if err != nil {
				t.Fatal(err)
			}
			if candidate.fragment.Evidence[0].ReplicaID != 0 {
				t.Fatal("candidate evidence not canonical")
			}
			if !follower.verifyCandidate(candidate.fragment, 0) {
				t.Fatal("follower rejected same-sequence replacement")
			}
			if _, ok := s.CommitPending(empty.digest); ok {
				t.Fatal("abandoned candidate committed")
			}
			s.MeasurementDeadline = time.Now().Add(time.Hour)
			if expired {
				s.MeasurementDeadline = time.Now().Add(-time.Second)
			}
			if _, ok := s.CommitPending(candidate.digest); !ok {
				t.Fatal("replacement did not commit")
			}
			if _, ok := follower.CommitPending(candidate.digest); !ok || follower.CommittedStateID() != s.CommittedStateID() {
				t.Fatal("replacement committed states disagree")
			}
			count, _, samples := s.GetMeasurementStats()
			want := 1
			if expired {
				want = 0
			}
			if count != want || samples != int64(want) || s.GetFinalizedCount() != 1 {
				t.Fatalf("count=%d samples=%d, want measured=%d and committed=1", count, samples, want)
			}
		})
	}
}
