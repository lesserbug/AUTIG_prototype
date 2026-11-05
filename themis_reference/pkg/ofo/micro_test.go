package ofo

import (
	"themisreference/pkg/types"
	"context"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func checkMicroFixture(t testing.TB, fx *microFixture) {
	t.Helper()
	before := microCloneService(fx.s)
	orders := microDeepProposal(&types.LeaderProposal{Proof: &types.LocalOrderFragment{ReplicaOrders: fx.orders}}).Proof.ReplicaOrders
	p := fx.s.buildProposal(fx.orders)
	r := microGraphCore(fx.prepared)
	if p == nil {
		if len(r.dm.graph.Nodes) != 0 || len(r.updates) != 0 {
			t.Fatal("empty candidate differs from Core")
		}
	} else {
		if !graphEqual(context.Background(), p.Graph, r.dm.graph) || !txStatesEqual(p.TxStates, r.dm.txStates) || !updatesEqual(context.Background(), p.Updates, r.updates) {
			t.Fatal("Core/native construction differ")
		}
		if !fx.s.VerifyProposal(p) || !microCandidateCore(fx.prepared, p) {
			t.Fatal("native/Core check rejected legal proposal")
		}
		// Compare the complete fields as well as canonical digest; ordered proof
		// representation is retained, graph adjacency iteration order is irrelevant.
		if !reflect.DeepEqual(p.Proof, fx.proposal.Proof) || types.LeaderProposalDigest(p) != types.LeaderProposalDigest(fx.proposal) {
			t.Fatal("construction changed candidate")
		}
		batches, remaining := microExpectedFinalize(fx.s, p)
		want := append([][32]byte(nil), fx.s.finalizedOrder...)
		for _, batch := range batches {
			want = append(want, batch...)
		}
		a, b := microCloneService(fx.s), microRestoreService(fx.s)
		if !a.CommitProposal(p) || !b.CommitVerifiedProposal(p) {
			t.Fatal("native commit failed")
		}
		if !microStateEqual(a, b) || !reflect.DeepEqual(a.finalizedOrder, want) || !reflect.DeepEqual(a.deferredProposals, remaining) {
			t.Fatal("batch/order/post-state differ")
		}
		for _, batch := range batches {
			for _, id := range batch {
				if !a.committedTxIDs[id] {
					t.Fatal("missing completed tx")
				}
				if _, ok := a.txPool[id]; ok {
					t.Fatal("receipt not cleaned")
				}
			}
		}
		// A subsequent legal round reuses surviving graph caches and sequence state.
		next := microDeepProposal(p).Proof.ReplicaOrders
		var all [][32]byte
		for _, old := range a.deferredProposals {
			for id := range old.Graph.Nodes {
				all = append(all, id)
			}
		}
		// New receipt identity is disjoint from generated payloads. Signed lists
		// are legal even when a verifier has not locally received the transaction.
		id := types.TransactionID([]byte("micro-next-round"))
		for _, o := range next {
			o.Sequence++
			o.NewTxOrder = &types.LocalOrder{ReplicaID: o.ReplicaID, Sequence: o.Sequence, OrderedTxs: [][32]byte{id}}
			o.NewTxOrder.Signature, _ = a.auth.SignReplica(o.ReplicaID, types.LocalOrderDigest(o.NewTxOrder))
			o.Update = nil
			if len(a.deferredProposals) > 0 {
				o.Update = &types.UpdateOrder{ReplicaID: o.ReplicaID, Sequence: o.Sequence, OrderedTxs: all}
				o.Update.Signature, _ = a.auth.SignReplica(o.ReplicaID, types.UpdateOrderDigest(o.Update))
			}
		}
		pa, pb := a.buildProposal(next), b.buildProposal(next)
		if pa == nil || pb == nil || types.LeaderProposalDigest(pa) != types.LeaderProposalDigest(pb) || !a.CommitProposal(pa) || !b.CommitProposal(pb) || !microStateEqual(a, b) {
			t.Fatal("next round differs")
		}
	}
	if !microStateEqual(fx.s, before) || fx.s.totalLatency != before.totalLatency || !reflect.DeepEqual(fx.orders, orders) {
		t.Fatal("snapshot or signed evidence mutated")
	}
}

func TestMicroDifferential(t *testing.T) {
	for _, n := range []uint64{10, 50} {
		for _, scenario := range []string{"low_release", "small_fresh", "history_few", "history_many", "no_receipts", "late_completed", "cycle"} {
			t.Run(microFixtureName(microConfig{n, 1, .95, 1, 24, 200, 64}, scenario), func(t *testing.T) {
				checkMicroFixture(t, makeMicroFixture(t, microConfig{n, 1, .95, 1, 24, 200, 64}, scenario))
			})
		}
	}
}

func TestMicroReproducible(t *testing.T) {
	for _, seed := range []int64{1, 7, 19} {
		c := microConfig{10, 1, .95, seed, 24, 200, 64}
		a := makeMicroFixture(t, c, "low_release")
		b := makeMicroFixture(t, c, "low_release")
		if a.traceDigest != b.traceDigest || !reflect.DeepEqual(a.metrics, b.metrics) || !microStateEqual(a.s, b.s) || types.LeaderProposalDigest(a.proposal) != types.LeaderProposalDigest(b.proposal) {
			t.Fatal("seed does not reproduce fixture")
		}
	}
}

func TestMicroRejectAndEquivalentProof(t *testing.T) {
	fx := makeMicroFixture(t, microConfig{10, 1, .95, 7, 24, 200, 64}, "low_release")
	auth := fx.s.auth
	resign := func(o *types.ReplicaOrders) {
		if o.NewTxOrder != nil {
			o.NewTxOrder.Signature, _ = auth.SignReplica(o.ReplicaID, types.LocalOrderDigest(o.NewTxOrder))
		}
		if o.Update != nil {
			o.Update.Signature, _ = auth.SignReplica(o.ReplicaID, types.UpdateOrderDigest(o.Update))
		}
	}
	tests := map[string]func(*types.LeaderProposal){
		"height":           func(p *types.LeaderProposal) { p.BlockHeight++ },
		"missing-proof":    func(p *types.LeaderProposal) { p.Proof = nil },
		"missing-sender":   func(p *types.LeaderProposal) { p.Proof.ReplicaOrders = p.Proof.ReplicaOrders[1:] },
		"duplicate-sender": func(p *types.LeaderProposal) { p.Proof.ReplicaOrders[1] = p.Proof.ReplicaOrders[0] },
		"sender-context": func(p *types.LeaderProposal) {
			p.Proof.ReplicaOrders[0].NewTxOrder.ReplicaID = 1
			resign(p.Proof.ReplicaOrders[0])
		},
		"stale-sequence": func(p *types.LeaderProposal) {
			o := p.Proof.ReplicaOrders[0]
			o.Sequence--
			o.NewTxOrder.Sequence = o.Sequence
			o.Update.Sequence = o.Sequence
			resign(o)
		},
		"local-signature":                 func(p *types.LeaderProposal) { p.Proof.ReplicaOrders[0].NewTxOrder.Signature[0] ^= 1 },
		"update-signature-correct-output": func(p *types.LeaderProposal) { p.Proof.ReplicaOrders[0].Update.Signature[0] ^= 1 },
		"duplicate-local": func(p *types.LeaderProposal) {
			o := p.Proof.ReplicaOrders[0]
			o.NewTxOrder.OrderedTxs = append(o.NewTxOrder.OrderedTxs, o.NewTxOrder.OrderedTxs[0])
			resign(o)
		},
		"duplicate-update": func(p *types.LeaderProposal) {
			o := p.Proof.ReplicaOrders[0]
			o.Update.OrderedTxs = append(o.Update.OrderedTxs, o.Update.OrderedTxs[0])
			resign(o)
		},
		"missing-update": func(p *types.LeaderProposal) { p.Proof.ReplicaOrders[0].Update = nil },
		"foreign-update": func(p *types.LeaderProposal) {
			o := p.Proof.ReplicaOrders[0]
			o.Update.OrderedTxs = append(o.Update.OrderedTxs, types.TransactionID([]byte("foreign")))
			resign(o)
		},
		"deferred-in-local": func(p *types.LeaderProposal) {
			o := p.Proof.ReplicaOrders[0]
			o.NewTxOrder.OrderedTxs = append(o.NewTxOrder.OrderedTxs, o.Update.OrderedTxs[0])
			resign(o)
		},
		"signed-reversed-input": func(p *types.LeaderProposal) {
			for _, o := range p.Proof.ReplicaOrders {
				ids := o.NewTxOrder.OrderedTxs
				for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
					ids[i], ids[j] = ids[j], ids[i]
				}
				resign(o)
			}
		},
		"omitted-node": func(p *types.LeaderProposal) {
			for id := range p.Graph.Nodes {
				delete(p.Graph.Nodes, id)
				break
			}
		},
		"unsafe-node": func(p *types.LeaderProposal) { p.Graph.Nodes[types.TransactionID([]byte("unsafe"))] = true },
		"edge-direction": func(p *types.LeaderProposal) {
			for u, vs := range p.Graph.Edges {
				if len(vs) > 0 {
					v := vs[0]
					p.Graph.Edges[u] = vs[1:]
					addEdge(p.Graph, v, u)
					break
				}
			}
		},
		"states": func(p *types.LeaderProposal) {
			for id := range p.TxStates {
				p.TxStates[id] = types.StateBlank
				break
			}
		},
		"missing-update-edge": func(p *types.LeaderProposal) {
			for h := range p.Updates {
				delete(p.Updates, h)
				break
			}
		},
		"foreign-update-height": func(p *types.LeaderProposal) { p.Updates[999] = map[[32]byte][][32]byte{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := microCloneService(fx.s)
			before := microCloneService(s)
			p := microDeepProposal(fx.proposal)
			mutate(p)
			// These edits retain valid signed inputs and must reach the output
			// graph/classification/update comparison in both implementations.
			switch name {
			case "signed-reversed-input", "omitted-node", "unsafe-node", "edge-direction", "states", "missing-update-edge", "foreign-update-height":
				for _, o := range p.Proof.ReplicaOrders {
					if !s.validateReplicaOrders(o) {
						t.Fatal("mutation failed common input checks")
					}
				}
				if microCandidateCore(prepareMicro(s, p.Proof.ReplicaOrders), p) {
					t.Fatal("Core accepted mismatch")
				}
			}
			// There is no outer candidate signature. Evidence mutations above are
			// re-signed where needed, allowing graph/state checks to be reached.
			if s.VerifyProposal(p) || s.CommitProposal(p) {
				t.Fatal("accepted invalid candidate")
			}
			if !microStateEqual(s, before) {
				t.Fatal("invalid candidate installed state")
			}
		})
	}
	// Native proof is a set of signed replica lists. Reordering its representation
	// and graph adjacency is valid; reordering transactions inside a list is not.
	p := microDeepProposal(fx.proposal)
	for i, j := 0, len(p.Proof.ReplicaOrders)-1; i < j; i, j = i+1, j-1 {
		p.Proof.ReplicaOrders[i], p.Proof.ReplicaOrders[j] = p.Proof.ReplicaOrders[j], p.Proof.ReplicaOrders[i]
	}
	for _, vs := range p.Graph.Edges {
		for i, j := 0, len(vs)-1; i < j; i, j = i+1, j-1 {
			vs[i], vs[j] = vs[j], vs[i]
		}
	}
	if !fx.s.VerifyProposal(p) || !microCandidateCore(fx.prepared, p) || types.LeaderProposalDigest(p) != types.LeaderProposalDigest(fx.proposal) {
		t.Fatal("equivalent proof/graph representation rejected")
	}
	a, b := microCloneService(fx.s), microCloneService(fx.s)
	if !a.CommitProposal(p) || !b.CommitProposal(fx.proposal) || !microStateEqual(a, b) {
		t.Fatal("equivalent representation changes committed state")
	}
}

func TestMicroCapEnforced(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestMicroConfigured$", "-micro-n=10", "-micro-f=1", "-micro-gamma=0.95", "-micro-scenarios=low_release", "-micro-receipt-cap=96")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "cap=96 (never truncated)") {
		t.Fatalf("cap failure was hidden: %v %s", err, output)
	}
	fx := makeMicroFixture(t, microConfig{10, 1, .95, 1, 24, 3, 64}, "no_receipts")
	if fx.metrics["warmup_max_receipts"] > 3 || fx.metrics["warmup_rounds"] != 9 || fx.metrics["retained_txs"] != 24 {
		t.Fatal("history not accumulated through bounded legal rounds")
	}
}

func TestMicroNativeContextBoundary(t *testing.T) {
	fx := makeMicroFixture(t, microConfig{10, 1, .95, 1, 24, 200, 64}, "low_release")
	stale := microCloneService(fx.s)
	stale.committedOrderSequences[0] = fx.orders[0].Sequence
	if stale.VerifyProposal(fx.proposal) {
		t.Fatal("stale evidence accepted")
	}
	wrongHistory := microCloneService(fx.s)
	wrongHistory.deferredProposals = make(map[uint64]*types.LeaderProposal)
	if wrongHistory.VerifyProposal(fx.proposal) {
		t.Fatal("updates against absent history accepted")
	}
	// An explicit limitation, not a fabricated predecessor check: VerifyProposal
	// has no finalized-prefix commitment and cannot detect a changed prefix alone.
	noPrefixBinding := microCloneService(fx.s)
	noPrefixBinding.finalizedOrder[0] = types.TransactionID([]byte("different-prefix"))
	if !noPrefixBinding.VerifyProposal(fx.proposal) {
		t.Fatal("native verifier boundary changed; review documentation")
	}
}
