package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

func v2ProductiveFixture(t testing.TB, n uint64, seed int64, delay uint64) *ablationFixture {
	return makeAblationFixture(t, n, 2, 1, ablationCase{name: "productive", history: 480, fresh: 8, partialRelease: true, continuation: true, continuationDelay: delay}, seed)
}

func v2AlphaFixture(t testing.TB, n uint64, k int) *ablationFixture {
	return revisionAlphaFixtureMode(t, n, 256, k, true)
}

func BenchmarkV2Alpha(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		for _, k := range []int{4, 16, 64, 128, 256} {
			x := v2AlphaFixture(b, n, k)
			dm := rebuildMaterialization(x.candidate.preState, n, 2, 1)
			for _, name := range []string{"CoreCertificate", "CoreGraph", "FullCertificate", "FullIncrementalUndo", "FullReconstruction"} {
				b.Run(fmt.Sprintf("n%d_k%d/%s", n, k, name), func(b *testing.B) {
					mode := StructuralVerification
					if name == "FullIncrementalUndo" {
						mode = SymmetricIncrementalVerification
					}
					if name == "FullReconstruction" {
						mode = ReconstructionVerification
					}
					s := v2FollowerWithMode(x.base, mode)
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						b.StartTimer()
						ok := true
						switch name {
						case "CoreCertificate":
							ok = verifyStructuralCertificate(x.candidate.preState, x.candidate.fragment.FinalOrder, x.candidate.fragment.Certificate, n, 2, 1) == nil
						case "CoreGraph":
							order, _, err := dm.buildProposalMode(x.candidate.preState, false)
							ok = err == nil && reflect.DeepEqual(order, x.candidate.fragment.FinalOrder)
							runtime.KeepAlive(order)
						default:
							ok = s.verifyCandidate(x.candidate.fragment, 0)
						}
						b.StopTimer()
						if !ok {
							b.Fatal("alpha control")
						}
						runtime.KeepAlive(s.pending)
						if s.pending != nil && !s.AbandonPending(x.candidate.digest) {
							b.Fatal("reset")
						}
					}
				})
			}
		}
	}
}

func BenchmarkV2Phases(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		x := v2ProductiveFixture(b, n, 7, 2)
		updated := rebuildMaterialization(x.candidate.preState, n, 2, 1)
		for _, name := range []string{"EvidenceClone", "EvidenceApply", "GraphTrialUndo", "GraphExtraction", "CertificateCore", "GraphCloneReference"} {
			b.Run(fmt.Sprintf("n%d/%s", n, name), func(b *testing.B) {
				s := v2FollowerWithMode(x.base, SymmetricIncrementalVerification)
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					pre := x.base.committed.clone()
					nodes, pairs, err := applyEvidence(pre, x.orders, pre.Epoch, pre.FragmentSeq+1, n, 2, 1, s.loMaxSize, s.auth, s.admission)
					if err != nil {
						b.Fatal(err)
					}
					if name == "EvidenceApply" {
						pre = x.base.committed.clone()
					}
					b.StartTimer()
					var value any
					switch name {
					case "EvidenceClone":
						value = x.base.committed.clone()
					case "EvidenceApply":
						value, _, err = applyEvidence(pre, x.orders, pre.Epoch, pre.FragmentSeq+1, n, 2, 1, s.loMaxSize, s.auth, s.admission)
					case "GraphTrialUndo":
						undo := beginGraphTrial(s.UtigManager, pre, nodes, pairs)
						undo()
					case "GraphExtraction":
						value, _, err = updated.buildProposalMode(x.candidate.preState, false)
					case "CertificateCore":
						err = verifyStructuralCertificate(x.candidate.preState, x.candidate.fragment.FinalOrder, x.candidate.fragment.Certificate, n, 2, 1)
					case "GraphCloneReference":
						value = s.UtigManager.clone()
					}
					b.StopTimer()
					runtime.KeepAlive(value)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkV2Productive(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		for _, seed := range []int64{1, 7, 19} {
			x := v2ProductiveFixture(b, n, seed, 2)
			for mode, name := range []string{"Certificate", "IncrementalUndo", "Reconstruction"} {
				b.Run(fmt.Sprintf("n%d_seed%d/%s", n, seed, name), func(b *testing.B) {
					s := v2FollowerWithMode(x.base, VerificationMode(mode))
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						b.StartTimer()
						accepted := s.verifyCandidate(x.candidate.fragment, 0)
						b.StopTimer()
						if !accepted {
							b.Fatal("verification failed")
						}
						runtime.KeepAlive(s.pending)
						if !s.AbandonPending(x.candidate.digest) {
							b.Fatal("reset")
						}
					}
				})
			}
		}
	}
}

func BenchmarkV2History(b *testing.B) {
	for _, size := range []int{0, 1000, 10000, 100000} {
		x := makeHistorySizeFixtureMode(b, size, true)
		for _, op := range []string{"Construct", "Verify", "Clone", "Append8"} {
			b.Run(fmt.Sprintf("Done%d/%s", size, op), func(b *testing.B) {
				s := x.service(op == "Construct")
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					b.StartTimer()
					var value any
					switch op {
					case "Construct":
						c, err := s.constructCandidate(x.orders)
						if err != nil {
							b.Fatal(err)
						}
						value = c
					case "Verify":
						if !s.verifyCandidate(x.candidate.fragment, 0) {
							b.Fatal("verify")
						}
						value = s.pending
					case "Clone":
						value = s.committed.clone()
					case "Append8":
						state := &EvidenceState{Done: make(map[types.TxID]DoneRecord), doneBase: s.committed.doneBase, doneVersion: s.committed.doneVersion, doneBaseCount: s.committed.doneCount(), DoneRoot: s.committed.DoneRoot}
						for j := 0; j < 8; j++ {
							id := types.TransactionID([]byte(fmt.Sprintf("append-bench/%d", j)))
							state.addDone(id, DoneRecord{FragmentSeq: s.committed.FragmentSeq + 1, BatchIndex: 1})
						}
						value = state.DoneRoot
					}
					b.StopTimer()
					runtime.KeepAlive(value)
					if s.pending != nil {
						if !s.AbandonPending(x.candidate.digest) {
							b.Fatal("reset")
						}
					}
				}
			})
		}
	}
}

func BenchmarkV2ColdMaterialization(b *testing.B) {
	for _, n := range []uint64{10, 50} {
		for _, live := range []int{32, 128, 512} {
			x := makeAblationFixture(b, n, 2, 1, ablationCase{name: "cold", history: live, fresh: 0}, 7)
			for _, path := range []string{"RetainedWeights", "PositionsRecovery"} {
				b.Run(fmt.Sprintf("n%d_live%d/%s", n, live, path), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						state := x.base.committed
						if path == "PositionsRecovery" {
							state = revisionDerivedFromPositions(state, n, 2, 1)
						}
						manager := rebuildMaterialization(state, n, 2, 1)
						runtime.KeepAlive(manager)
					}
				})
			}
		}
	}
}
