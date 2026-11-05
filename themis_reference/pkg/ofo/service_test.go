package ofo

import (
	"themisreference/pkg/network"
	"themisreference/pkg/types"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

type testNetwork struct {
	handler func(network.Message)
	sent    chan network.Message
}

func TestByzantineReportDelay(t *testing.T) {
	for _, tc := range []struct {
		name       string
		malicious  bool
		delay      time.Duration
		cancelWait bool
	}{
		{"default-zero", true, 0, false},
		{"honest-not-delayed", false, time.Hour, false},
		{"malicious-delayed", true, 200 * time.Millisecond, false},
		{"cancel-wait", true, time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			net := &testNetwork{sent: make(chan network.Message, 8)}
			s, err := NewOFOService(1, 10, 2, 1, net, 200, 10, tc.malicious, newTestOrderAuthenticator(10))
			if err != nil {
				t.Fatal(err)
			}
			s.ByzantineLODelay = tc.delay
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			start := time.Now()
			go func() { defer close(done); s.Start(ctx) }()
			if tc.cancelWait {
				time.Sleep(30 * time.Millisecond)
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("delay ignored cancellation")
				}
				select {
				case <-net.sent:
					t.Fatal("cancelled wait sent a report")
				default:
				}
				return
			}
			for i := 0; i < 2; i++ {
				select {
				case <-net.sent:
					if tc.malicious && time.Since(start) < time.Duration(i+1)*tc.delay {
						t.Fatal("report sent before delay elapsed")
					}
				case <-time.After(2 * time.Second):
					t.Fatal("report not sent")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("reporting did not stop")
			}
		})
	}
}

func (network *testNetwork) Register(_ uint64, handler func(network.Message)) {
	network.handler = handler
}
func (net *testNetwork) Send(message network.Message) bool {
	if net.sent != nil {
		net.sent <- message
	}
	return true
}
func (network *testNetwork) Stop()                            {}
func (network *testNetwork) WaitForPeers(time.Duration) error { return nil }

type testOrderAuthenticator struct {
	public  map[uint64]ed25519.PublicKey
	private map[uint64]ed25519.PrivateKey
}

func newTestOrderAuthenticator(n uint64) *testOrderAuthenticator {
	auth := &testOrderAuthenticator{public: make(map[uint64]ed25519.PublicKey), private: make(map[uint64]ed25519.PrivateKey)}
	for replica := uint64(0); replica < n; replica++ {
		seed := sha256.Sum256([]byte(fmt.Sprintf("themis-test-key-%d", replica)))
		privateKey := ed25519.NewKeyFromSeed(seed[:])
		auth.private[replica] = privateKey
		auth.public[replica] = privateKey.Public().(ed25519.PublicKey)
	}
	return auth
}

func (auth *testOrderAuthenticator) SignReplica(replicaID uint64, digest [32]byte) ([]byte, error) {
	return ed25519.Sign(auth.private[replicaID], digest[:]), nil
}

func (auth *testOrderAuthenticator) VerifyReplica(replicaID uint64, digest [32]byte, signature []byte) bool {
	return ed25519.Verify(auth.public[replicaID], digest[:], signature)
}

func signedOrders(t *testing.T, auth *testOrderAuthenticator, n, f, sequence uint64, newOrders map[uint64][][32]byte, updates map[uint64]map[uint64][][32]byte) []*types.ReplicaOrders {
	t.Helper()
	heights := make([]uint64, 0, len(updates))
	for height := range updates {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	result := make([]*types.ReplicaOrders, 0, n-f)
	for replica := uint64(0); replica < n-f; replica++ {
		local := &types.LocalOrder{ReplicaID: replica, Sequence: sequence, OrderedTxs: append([][32]byte(nil), newOrders[replica]...)}
		local.Signature, _ = auth.SignReplica(replica, types.LocalOrderDigest(local))
		orders := &types.ReplicaOrders{ReplicaID: replica, Sequence: sequence, NewTxOrder: local}
		var updateTxs [][32]byte
		for _, height := range heights {
			updateTxs = append(updateTxs, updates[height][replica]...)
		}
		if len(heights) > 0 {
			update := &types.UpdateOrder{ReplicaID: replica, Sequence: sequence, OrderedTxs: updateTxs}
			update.Signature, _ = auth.SignReplica(replica, types.UpdateOrderDigest(update))
			orders.Update = update
		}
		result = append(result, orders)
	}
	return result
}

func putKnownTransactions(service *OFOService, ids ...[32]byte) {
	service.txPoolMu.Lock()
	defer service.txPoolMu.Unlock()
	for i, id := range ids {
		received := time.Unix(0, int64(i+1))
		service.txPool[id] = received
		service.txSubmissionTimes[id] = received
	}
}

func TestFollowerRecomputesAndRejectsMismatchedProposal(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	auth := newTestOrderAuthenticator(n)
	leader, err := NewOFOService(0, n, f, gamma, &testNetwork{}, 20, 100, false, auth)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(leader.Stop)
	follower, err := NewOFOService(1, n, f, gamma, &testNetwork{}, 20, 100, false, auth)
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := testTx(1), testTx(2), testTx(3)
	putKnownTransactions(leader, a, b, c)
	putKnownTransactions(follower, a, b, c)
	newOrders := map[uint64][][32]byte{
		0: {a, b, c}, 1: {a, b, c}, 2: {b, c, a},
		3: {b, c, a}, 4: {c, a, b}, 5: {c, a, b},
	}
	proof := signedOrders(t, auth, n, f, 1, newOrders, nil)
	proposal := leader.buildProposal(proof)
	if proposal == nil || !follower.VerifyProposal(proposal) {
		t.Fatal("follower rejected the leader's correctly recomputable proposal")
	}
	followerProposal := follower.recomputeProposal(1, proof)
	if followerProposal == nil || types.LeaderProposalDigest(proposal) != types.LeaderProposalDigest(followerProposal) {
		t.Fatal("leader and follower produced different results for fixed signed inputs")
	}

	tampered := cloneProposal(proposal)
	for from, targets := range tampered.Graph.Edges {
		if len(targets) > 0 {
			tampered.Graph.Edges[from] = targets[1:]
			break
		}
	}
	if follower.VerifyProposal(tampered) {
		t.Fatal("follower accepted a graph inconsistent with the signed inputs")
	}

	duplicateSender := cloneProposal(proposal)
	duplicateSender.Proof = &types.LocalOrderFragment{ReplicaOrders: append([]*types.ReplicaOrders(nil), proposal.Proof.ReplicaOrders...)}
	duplicateSender.Proof.ReplicaOrders[len(duplicateSender.Proof.ReplicaOrders)-1] = duplicateSender.Proof.ReplicaOrders[0]
	if follower.VerifyProposal(duplicateSender) {
		t.Fatal("follower accepted duplicate-sender LocalOrders as distinct weight")
	}
}

func TestDeferredUpdateAndBatchUnspooling(t *testing.T) {
	const n, f, gamma = uint64(7), uint64(1), 0.9
	auth := newTestOrderAuthenticator(n)
	service, err := NewOFOService(0, n, f, gamma, &testNetwork{}, 20, 100, false, auth)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Stop)
	u, v, solid, later := testTx(1), testTx(2), testTx(3), testTx(4)
	putKnownTransactions(service, u, v, solid, later)

	firstOrders := map[uint64][][32]byte{
		// Both shaded vertices occur four times, split 2:2, below the
		// edge threshold in both directions. Each precedes the solid vertex.
		0: {u, v, solid}, 1: {u, v, solid},
		2: {v, u, solid}, 3: {v, u, solid}, 4: {solid},
	}
	first := service.buildProposal(signedOrders(t, auth, n, f, 1, firstOrders, nil))
	if first == nil || IsTournament(first.Graph) || !service.CommitProposal(first) {
		t.Fatal("failed to install the first deferred proposal")
	}

	secondOrders := map[uint64][][32]byte{0: {later}, 1: {later}, 2: {later}, 3: {later}, 4: {later}}
	emptyFirstUpdates := map[uint64]map[uint64][][32]byte{1: {}}
	second := service.buildProposal(signedOrders(t, auth, n, f, 2, secondOrders, emptyFirstUpdates))
	if second == nil || !IsTournament(second.Graph) || !service.CommitProposal(second) {
		t.Fatal("failed to install the later fully specified proposal")
	}
	if service.GetFinalizedCount() != 0 {
		t.Fatal("batch unspooling finalized a later proposal across an incomplete earlier boundary")
	}

	updateFirst := map[uint64][][32]byte{
		0: {u, v}, 1: {u, v}, 2: {u, v}, 3: {v, u}, 4: {v, u}, 5: {u},
	}
	updateSecond := map[uint64][][32]byte{
		0: {later}, 1: {later}, 2: {later}, 3: {later}, 4: {later}, 5: {later},
	}
	thirdUpdates := map[uint64]map[uint64][][32]byte{1: updateFirst, 2: updateSecond}
	third := service.buildProposal(signedOrders(t, auth, n, f, 3, nil, thirdUpdates))
	if third == nil || !containsTx(third.Updates[1][u], v) || !service.CommitProposal(third) {
		t.Fatal("FairUpdate did not complete the deferred relation")
	}
	want := [][32]byte{u, v, solid, later}
	if got := service.GetFinalizedOrder(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected unspooled order: got %v, want %v", got, want)
	}
}

func TestFullReceiptListAndPoolRetention(t *testing.T) {
	net := &testNetwork{sent: make(chan network.Message, 1)}
	service, err := NewOFOService(1, 5, 1, 1, net, 1, 10, false, newTestOrderAuthenticator(5))
	if err != nil {
		t.Fatal(err)
	}
	a, b := testTx(1), testTx(2)
	putKnownTransactions(service, a, b)
	service.generateAndSendOrders()
	orders := (<-net.sent).Payload.(*types.ReplicaOrders)
	if !reflect.DeepEqual(orders.NewTxOrder.OrderedTxs, [][32]byte{a, b}) {
		t.Fatal("receipt evidence was truncated")
	}
	for i := 0; i < 50000; i++ {
		var id [32]byte
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		service.txPool[id] = time.Time{}
	}
	canonical := []byte("transaction after old pool limit")
	tx := &types.Transaction{ID: types.TransactionID(canonical), CanonicalBytes: canonical, SubmissionTime: time.Now()}
	service.HandleMessage(network.Message{Payload: tx})
	if _, ok := service.txPool[tx.ID]; !ok {
		t.Fatal("received transaction was silently dropped")
	}
}

func TestMaliciousReplicaReversesFreshAndUpdateEvidence(t *testing.T) {
	for _, malicious := range []bool{false, true} {
		t.Run(fmt.Sprintf("malicious=%v", malicious), func(t *testing.T) {
			net := &testNetwork{sent: make(chan network.Message, 1)}
			auth := newTestOrderAuthenticator(20)
			s, err := NewOFOService(19, 20, 4, 1, net, 200, 150, malicious, auth)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Stop)
			a, b, c, d := testTx(1), testTx(2), testTx(3), testTx(4)
			putKnownTransactions(s, a, b, c, d)
			// Exercise both evidence lists; deferred state setup is not a commit.
			s.deferredProposals[1] = &types.LeaderProposal{Graph: &types.DependencyGraph{
				Nodes: map[[32]byte]bool{a: true, b: true}, Edges: make(map[[32]byte][][32]byte),
			}}
			s.generateAndSendOrders()
			orders := (<-net.sent).Payload.(*types.ReplicaOrders)
			fresh, update := [][32]byte{c, d}, [][32]byte{a, b}
			if malicious {
				fresh, update = [][32]byte{d, c}, [][32]byte{b, a}
			}
			if orders.Update == nil || !reflect.DeepEqual(orders.NewTxOrder.OrderedTxs, fresh) || !reflect.DeepEqual(orders.Update.OrderedTxs, update) {
				t.Fatal("unexpected fresh/update receipt order")
			}
			if !s.validateReplicaOrders(orders) || s.fFaulty != 4 || s.replicaCount != 20 || s.gamma != 1 {
				t.Fatal("attack changed evidence validity or protocol parameters")
			}
		})
	}
}

func TestVerificationDoesNotDependOnLocalReceipts(t *testing.T) {
	auth := newTestOrderAuthenticator(5)
	leader, _ := NewOFOService(0, 5, 1, 1, &testNetwork{}, 20, 10, false, auth)
	defer leader.Stop()
	follower, _ := NewOFOService(1, 5, 1, 1, &testNetwork{}, 20, 10, false, auth)
	a, b := testTx(1), testTx(2)
	putKnownTransactions(leader, a, b)
	putKnownTransactions(follower, a)
	proof := signedOrders(t, auth, 5, 1, 1, map[uint64][][32]byte{0: {a}, 1: {a}, 2: {a}, 3: {b}}, nil)
	proposal := leader.buildProposal(proof)
	if proposal == nil || !follower.VerifyProposal(proposal) {
		t.Fatal("blank transaction availability changed proposal validity")
	}
	if !leader.CommitProposal(proposal) {
		t.Fatal("first proposal did not commit")
	}
	next := signedOrders(t, auth, 5, 1, 2, map[uint64][][32]byte{0: {b}, 1: {a, b}, 2: {b}, 3: {b}}, nil)
	proposal = leader.buildProposal(next)
	if proposal == nil || proposal.Graph.Nodes[a] || !proposal.Graph.Nodes[b] {
		t.Fatal("in-flight evidence must filter committed A and retain B")
	}
}

func TestCollectorRetainsEvidenceAcrossDelay(t *testing.T) {
	auth := newTestOrderAuthenticator(5)
	service, _ := NewOFOService(0, 5, 1, 1, &testNetwork{}, 20, 10, false, auth)
	defer service.Stop()
	proposed := make(chan *types.LeaderProposal, 1)
	service.SetProposalSink(func(p *types.LeaderProposal) bool { proposed <- p; return false })
	a := testTx(1)
	orders := signedOrders(t, auth, 5, 1, 1, map[uint64][][32]byte{0: {a}, 1: {a}, 2: {a}, 3: {a}}, nil)
	for i := 0; i < 3; i++ {
		service.HandleMessage(network.Message{From: orders[i].ReplicaID, Payload: orders[i]})
	}
	select {
	case <-proposed:
		t.Fatal("proposed below quorum")
	case <-time.After(60 * time.Millisecond):
	}
	service.HandleMessage(network.Message{From: 3, Payload: orders[3]})
	select {
	case p := <-proposed:
		if !p.Graph.Nodes[a] {
			t.Fatal("missing transaction")
		}
	case <-time.After(time.Second):
		t.Fatal("discarded evidence across delay")
	}
}

func TestMeasurementExcludesLateFinalization(t *testing.T) {
	service, _ := NewOFOService(1, 5, 1, 1, &testNetwork{}, 20, 10, false, newTestOrderAuthenticator(5))
	a, b := testTx(1), testTx(2)
	putKnownTransactions(service, a, b)
	service.MeasurementDeadline = time.Now().Add(time.Second)
	service.commitFinalizedData([][32]byte{a}, nil)
	service.MeasurementDeadline = time.Now().Add(-time.Second)
	service.commitFinalizedData([][32]byte{b}, nil)
	count, _, samples := service.GetMeasurementStats()
	if count != 1 || samples != 1 || service.GetFinalizedCount() != 2 {
		t.Fatalf("measurement=%d samples=%d total=%d", count, samples, service.GetFinalizedCount())
	}
}

func TestStartDeadlineUnblocksLocalEvidenceEnqueue(t *testing.T) {
	// No collector consumes this channel: model a saturated evidence pipeline.
	pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
	defer pipelineCancel()
	service := &OFOService{
		isLeader: true, loInterval: time.Millisecond,
		pipelineCtx: pipelineCtx, pipelineCancel: pipelineCancel,
		replicaOrdersChan: make(chan *types.ReplicaOrders),
		auth:              newTestOrderAuthenticator(5),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { service.Start(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		pipelineCancel()
		<-done
		t.Fatal("measurement deadline did not unblock local evidence enqueue")
	}
}

func TestDeadlineCancelsLargeProposalWithoutChangingCommittedPrefix(t *testing.T) {
	auth := newTestOrderAuthenticator(5)
	service, _ := NewOFOService(0, 5, 1, .95, &testNetwork{}, 200, 1000, false, auth)
	a := testTx(1)
	putKnownTransactions(service, a)
	initial := signedOrders(t, auth, 5, 1, 1, map[uint64][][32]byte{0: {a}, 1: {a}, 2: {a}, 3: {a}}, nil)
	if !service.CommitProposal(service.buildProposal(initial)) {
		t.Fatal("could not commit initial prefix")
	}
	height, state, digest := service.CommittedContext()
	count, _, samples := service.GetMeasurementStats()
	ids := make([][32]byte, 6000)
	for i := range ids {
		binary.BigEndian.PutUint64(ids[i][:8], uint64(i+100))
	}
	orders := signedOrders(t, auth, 5, 1, 2, map[uint64][][32]byte{0: ids, 1: ids, 2: ids, 3: ids}, nil)
	proposed := make(chan struct{}, 1)
	service.SetProposalSink(func(*types.LeaderProposal) bool { proposed <- struct{}{}; return false })
	service.batchReadyChan <- orders
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { service.Start(ctx); service.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("large proposal prevented pipeline shutdown")
	}
	select {
	case <-proposed:
		t.Fatal("cancelled candidate reached hosting adapter")
	default:
	}
	afterHeight, afterState, afterDigest := service.CommittedContext()
	afterCount, _, afterSamples := service.GetMeasurementStats()
	if height != afterHeight || state != afterState || digest != afterDigest || count != afterCount || samples != afterSamples {
		t.Fatal("cancellation changed committed state or measurement")
	}
}
