package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"io"
	"log"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
)

// Every structural mutation preserves the authenticated evidence. The two
// evidence mutations change only one sender (in configurations with f>=2),
// re-sign that sender, then re-sign the authorized malicious leader.
func revisionMutate(t testing.TB, x *ablationFixture, data []byte, held bool) (*types.VerifiableFairOrderFragment, string) {
	t.Helper()
	f := cloneAblationFragment(t, x.candidate.fragment)
	at := func(size int) int {
		if len(data) < 2 {
			return 0
		}
		return int(data[1]) % size
	}
	mode := int(data[0]) % 15
	kind := ""
	if held {
		mode %= 6
		records := f.Certificate.BlockForest
		var roots, children []int
		for i, r := range records {
			if r.HasParent {
				children = append(children, i)
			} else {
				roots = append(roots, i)
			}
		}
		if len(roots) == 0 || len(children) == 0 {
			t.Fatal("held mutation fixture lacks witness paths")
		}
		root, child := roots[at(len(roots))], children[at(len(children))]
		switch mode {
		case 0:
			kind = "forest-missing"
			f.Certificate.BlockForest = nil
		case 1:
			kind = "forest-false-root"
			f.Certificate.BlockForest[child].HasParent = false
			f.Certificate.BlockForest[child].Depth = 0
		case 2:
			kind = "forest-cycle"
			f.Certificate.BlockForest[child].Parent = f.Certificate.BlockForest[child].Vertex
		case 3:
			kind = "forest-depth"
			f.Certificate.BlockForest[child].Depth += uint64(1 + at(200))
		case 4:
			kind = "forest-duplicate"
			f.Certificate.BlockForest = append(f.Certificate.BlockForest, records[root])
		case 5:
			kind = "forest-root-depth"
			f.Certificate.BlockForest[root].Depth = uint64(1 + at(200))
		}
	} else {
		big := -1
		for i, c := range f.Certificate.Part {
			if len(c.OutTree) > 0 {
				big = i
				break
			}
		}
		if big < 0 {
			t.Fatal("released fixture needs a nontrivial SCC")
		}
		switch mode {
		case 0:
			kind = "post-state"
			f.CandidatePostStateID[at(32)] ^= 1
		case 1:
			kind = "rank"
			f.Certificate.Part[at(len(f.Certificate.Part))].Rank++
		case 2:
			kind = "batch-index"
			f.FinalOrder.Batches[at(len(f.FinalOrder.Batches))].Index++
		case 3:
			kind = "tree-self-edge"
			e := &f.Certificate.Part[big].OutTree[at(len(f.Certificate.Part[big].OutTree))]
			e.To = e.From
		case 4:
			kind = "tree-missing-edge"
			f.Certificate.Part[big].InTree = f.Certificate.Part[big].InTree[1:]
		case 5:
			kind = "tree-reverse-edge"
			e := &f.Certificate.Part[big].OutTree[at(len(f.Certificate.Part[big].OutTree))]
			e.From, e.To = e.To, e.From
		case 6:
			kind = "duplicate-output"
			ids := f.FinalOrder.Batches[big].Transactions
			f.FinalOrder.Batches[big].Transactions = append(ids, ids[at(len(ids))])
			f.Certificate.Part[big].Transactions = append([]types.TxID(nil), f.FinalOrder.Batches[big].Transactions...)
		case 7:
			kind = "omit-safe-component"
			i := at(len(f.Certificate.Part))
			f.Certificate.Part = append(f.Certificate.Part[:i], f.Certificate.Part[i+1:]...)
			f.FinalOrder.Batches = append(f.FinalOrder.Batches[:i], f.FinalOrder.Batches[i+1:]...)
			for j := range f.Certificate.Part {
				f.Certificate.Part[j].Rank = uint64(j)
				f.FinalOrder.Batches[j].Index = x.base.committed.FairnessBatchCounter + uint64(j) + 1
			}
		case 8:
			kind = "partition-membership"
			f.Certificate.Part[big].Transactions = f.Certificate.Part[big].Transactions[1:]
		case 9:
			kind = "partition-count"
			f.Certificate.Part = append(f.Certificate.Part, f.Certificate.Part[big])
		case 10:
			kind = "nil-certificate"
			f.Certificate = nil
		case 11:
			kind = "nil-output"
			f.FinalOrder = nil
		case 12:
			kind = "evidence-duplicate-position"
			o := f.Evidence[at(len(f.Evidence))]
			o.OrderedTxs = append(o.OrderedTxs, o.OrderedTxs[0])
			signAblationOrder(t, x, o)
		case 13:
			kind = "evidence-predecessor"
			o := f.Evidence[at(len(f.Evidence))]
			o.PrevHead[0] ^= 1
			signAblationOrder(t, x, o)
		case 14:
			kind = "tree-duplicate-parent"
			tree := f.Certificate.Part[big].OutTree
			i := at(len(tree))
			tree[(i+1)%len(tree)] = tree[i]
		}
	}
	if kind != "post-state" && f.FinalOrder != nil && f.Certificate != nil {
		post := x.candidate.preState.clone()
		// Derive the signed transition identifier without applying an invalid
		// duplicate-output mutation to the trusted settlement implementation.
		f.CandidatePostStateID = types.PostStateIdentifier(post.Epoch, post.FragmentSeq, post.StateID, f.FinalOrder, f.Certificate.Part)
	}
	signAblationFragment(t, x, f)
	if !x.auth.VerifyLeader(x.base.authContext, f.LeaderID, types.FragmentDigest(f), f.LeaderSignature) {
		t.Fatal("mutation was not re-signed")
	}
	return f, kind
}

func TestRevisionAuthenticatedRandomMutations(t *testing.T) {
	oldLog := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLog)
	for _, cfg := range []struct{ n, f uint64 }{{10, 2}, {17, 4}} {
		for _, seed := range []int64{1, 7, 19} {
			for _, held := range []bool{false, true} {
				name := fmt.Sprintf("n%d_f%d/seed%d/held%t", cfg.n, cfg.f, seed, held)
				t.Run(name, func(t *testing.T) {
					x := makeAblationFixture(t, cfg.n, cfg.f, 1, ablationCase{name: "random", history: 30, fresh: 6, cycle: true, release: !held}, seed)
					a, b := x.service(false), x.service(false)
					rng := rand.New(rand.NewSource(seed))
					counts := map[string]int{}
					controls := 0
					for i := 0; i < 500; i++ {
						data := []byte{byte(rng.Intn(256)), byte(rng.Intn(256)), byte(rng.Intn(256))}
						bad, kind := revisionMutate(t, x, data, held)
						if a.verifyCandidate(bad, 0) || verifyCandidateByRecomputeForTest(b, bad, 0) {
							t.Fatalf("accepted %s mutation", kind)
						}
						if a.pending != nil || b.pending != nil || !reflect.DeepEqual(a.committed, x.base.committed) || !reflect.DeepEqual(b.committed, x.base.committed) {
							t.Fatal("rejection contaminated state")
						}
						counts[kind]++
						if i%25 == 0 {
							valid := cloneAblationFragment(t, x.candidate.fragment)
							for j := range valid.Certificate.Part {
								rng.Shuffle(len(valid.Certificate.Part[j].OutTree), func(p, q int) {
									valid.Certificate.Part[j].OutTree[p], valid.Certificate.Part[j].OutTree[q] = valid.Certificate.Part[j].OutTree[q], valid.Certificate.Part[j].OutTree[p]
								})
							}
							rng.Shuffle(len(valid.Certificate.BlockForest), func(p, q int) {
								valid.Certificate.BlockForest[p], valid.Certificate.BlockForest[q] = valid.Certificate.BlockForest[q], valid.Certificate.BlockForest[p]
							})
							signAblationFragment(t, x, valid)
							if !a.verifyCandidate(valid, 0) || !verifyCandidateByRecomputeForTest(b, valid, 0) {
								t.Fatal("alternative valid witness rejected")
							}
							if !a.AbandonPending(types.FragmentDigest(valid)) || !b.AbandonPending(types.FragmentDigest(valid)) {
								t.Fatal("cannot abandon positive control")
							}
							controls++
						}
					}
					if !a.verifyCandidate(x.candidate.fragment, 0) || !verifyCandidateByRecomputeForTest(b, x.candidate.fragment, 0) {
						t.Fatal("valid candidate failed after mutations")
					}
					for _, s := range []*OFOService{a, b} {
						if _, ok := s.CommitPending(x.candidate.digest); !ok {
							t.Fatal("post-mutation commit failed")
						}
						assertAblationState(t, s.committed, x.candidate.postState)
					}
					revisionResult(t, "authenticated_random_mutations", map[string]any{"n": cfg.n, "f": cfg.f, "seed": seed, "held": held, "rejected_mutants": 500, "accepted_controls": controls, "families": counts, "state_isolated": true, "valid_commit_after_rejection": true})
				})
			}
		}
	}
}

func FuzzRevisionAuthenticatedCertificate(f *testing.F) {
	x := makeAblationFixture(f, 10, 2, 1, ablationCase{name: "fuzz", history: 12, fresh: 3, cycle: true, release: true}, 7)
	for i := 0; i < 15; i++ {
		f.Add([]byte{byte(i), byte(i * 7)})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 64 {
			return
		}
		bad, _ := revisionMutate(t, x, data, false)
		s := x.service(false)
		if s.verifyCandidate(bad, 0) {
			t.Fatal("authenticated malformed certificate accepted")
		}
		if s.pending != nil || !reflect.DeepEqual(s.committed, x.base.committed) {
			t.Fatal("fuzz rejection changed committed state")
		}
	})
}

// Certificate-free local comparator. Both branches check the exact public
// output against a materialized graph; they do not redundantly check AUTIG
// witnesses. The envelope/digest and all common evidence/state work are shared.
func revisionVerifyByGraph(s *OFOService, f *types.VerifiableFairOrderFragment, incremental bool) bool {
	if f.FragmentSeq != s.committed.FragmentSeq+1 || f.PreviousStateID != s.committed.StateID || f.PreviousFragmentDigest != s.latestCommittedFragment || f.Epoch != s.authContext.Epoch || f.AuthContextID != s.authContext.Identifier {
		return false
	}
	digest := types.FragmentDigest(f)
	if !s.auth.VerifyLeader(s.authContext, f.LeaderID, digest, f.LeaderSignature) {
		return false
	}
	pre := s.committed.clone()
	nodes, pairs, err := applyEvidenceWithTouches(pre, f.Evidence, f.Epoch, f.FragmentSeq, s.replicaCount, s.fFaulty, s.gamma, s.loMaxSize, s.auth, s.admission, incremental)
	if err != nil || pre.StateID != f.CandidateEvidenceStateID {
		return false
	}
	var dm *DependencyManager
	if incremental {
		dm = s.UtigManager.clone()
		dm.refresh(pre, nodes, pairs)
	} else {
		dm = rebuildDependencyManagerForTest(pre, s.replicaCount, s.fFaulty, s.gamma)
	}
	expected := recomputeSafeOrderForTest(pre, dm)
	if !reflect.DeepEqual(expected, f.FinalOrder) {
		return false
	}
	part := make([]types.SCCClaim, len(expected.Batches))
	for i, batch := range expected.Batches {
		part[i] = types.SCCClaim{Transactions: batch.Transactions, Rank: uint64(i)}
	}
	post := pre.clone()
	finalize(post, dm, expected, part)
	if post.StateID != f.CandidatePostStateID {
		return false
	}
	s.pending = &pendingCandidate{digest: digest, preState: pre, postState: post, manager: dm, fragment: f, done: make(chan struct{})}
	return true
}

func TestRevisionComparatorsAgreeOnNonemptyOutput(t *testing.T) {
	for _, n := range []uint64{10, 50} {
		x := makeAblationFixture(t, n, 2, 1, ablationCase{name: "productive", history: 120, fresh: 8, partialRelease: true}, 7)
		if x.metrics["output_tx"] != 8 {
			t.Fatal("nonempty comparator fixture has wrong output")
		}
		for _, incremental := range []bool{false, true} {
			s := x.service(false)
			s.UtigManager = x.base.UtigManager
			if !revisionVerifyByGraph(s, x.candidate.fragment, incremental) {
				t.Fatal("graph comparator rejected reference output")
			}
			assertAblationState(t, s.pending.postState, x.candidate.postState)
		}
		revisionResult(t, "nonempty_comparator_conformance", map[string]any{"n": n, "f": 2, "output": 8, "active": x.metrics["updated_active"], "symmetric_incremental_matches": true, "certificate_free_recompute_matches": true})
	}
}

func BenchmarkRevisionFollowerNonempty(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		for _, seed := range []int64{1, 7, 19} {
			x := makeAblationFixture(b, n, 2, 1, ablationCase{name: "productive", history: 480, fresh: 8, partialRelease: true}, seed)
			for _, branch := range []string{"Certificate", "SymmetricIncremental", "RecomputeNoWitness"} {
				b.Run(fmt.Sprintf("n%d_seed%d/%s", n, seed, branch), func(b *testing.B) {
					s := x.service(false)
					s.UtigManager = x.base.UtigManager
					b.ReportAllocs()
					b.ReportMetric(x.metrics["output_tx"], "output/op")
					b.ReportMetric(x.metrics["updated_active"], "active/op")
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						b.StartTimer()
						accepted := false
						if branch == "Certificate" {
							accepted = s.verifyCandidate(x.candidate.fragment, 0)
						} else {
							accepted = revisionVerifyByGraph(s, x.candidate.fragment, branch == "SymmetricIncremental")
						}
						b.StopTimer()
						if !accepted {
							b.Fatal("verification failed")
						}
						runtime.KeepAlive(s.pending)
						if !s.AbandonPending(x.candidate.digest) {
							b.Fatal("reset failed")
						}
					}
				})
			}
		}
	}
}

func revisionDerivedFromPositions(state *EvidenceState, n, f uint64, gamma float64) *EvidenceState {
	// A cold checkpoint has authoritative fields but no derived caches. Avoid
	// charging a pointless copy of warm pair weights to cold reconstruction.
	authoritative := *state
	authoritative.weights, authoritative.visibility, authoritative.states = nil, nil, nil
	out := authoritative.clone()
	out.weights = make(map[edgeKey]int)
	out.visibility = make(map[types.TxID]int)
	out.states = make(map[types.TxID]types.TxState)
	for _, positions := range out.Pos {
		ids := make([]types.TxID, 0, len(positions))
		for id := range positions {
			ids = append(ids, id)
			out.visibility[id]++
		}
		for i, u := range ids {
			for _, v := range ids[i+1:] {
				if positions[u] < positions[v] {
					out.weights[edgeKey{u, v}]++
				} else {
					out.weights[edgeKey{v, u}]++
				}
			}
		}
	}
	for id := range out.Live {
		status := types.StateBlank
		if out.visibility[id] >= types.CalculateSolidThreshold(n, f) {
			status = types.StateSolid
		} else if out.visibility[id] >= types.CalculateEdgeThreshold(n, f, gamma) {
			status = types.StateShaded
		}
		out.states[id] = status
	}
	return out
}

func BenchmarkRevisionColdMaterialization(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		for _, m := range []int{32, 128, 512} {
			x := makeAblationFixture(b, n, 2, 1, ablationCase{name: "cold", history: m, fresh: 0}, 7)
			b.Run(fmt.Sprintf("n%d_live%d", n, m), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					state := revisionDerivedFromPositions(x.base.committed, n, 2, 1)
					dm := rebuildDependencyManagerForTest(state, n, 2, 1)
					runtime.KeepAlive(dm)
				}
			})
		}
	}
}
