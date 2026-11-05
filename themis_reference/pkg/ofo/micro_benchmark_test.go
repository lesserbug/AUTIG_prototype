package ofo

import (
	"themisreference/pkg/types"
	"encoding/json"
	"flag"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

var (
	microN          = flag.Uint64("micro-n", 10, "native Themis replicas")
	microF          = flag.Uint64("micro-f", 1, "native Themis faults")
	microGamma      = flag.Float64("micro-gamma", .95, "native gamma_all")
	microSeed       = flag.Int64("micro-seed", 1, "deterministic receipt/payload seed")
	microHistory    = flag.Int("micro-history", 480, "target retained txs for history scenarios")
	microReceiptCap = flag.Int("micro-receipt-cap", 200, "test workload cap on new receipt events per replica/round; NOT LocalOrder limit")
	microTxSize     = flag.Int("micro-tx-size", 512, "canonical transaction payload bytes")
	microScenarios  = flag.String("micro-scenarios", "low_release,history_few,history_many,cycle,no_receipts", "comma-separated representative scenarios")
)

func configuredMicro() microConfig {
	return microConfig{*microN, *microF, *microGamma, *microSeed, *microHistory, *microReceiptCap, *microTxSize}
}

// Also run by the collector before sampling: validates the exact requested
// parameters and emits structural metadata outside all operation timers.
func TestMicroConfigured(t *testing.T) {
	c := configuredMicro()
	for _, scenario := range strings.Split(*microScenarios, ",") {
		fx := makeMicroFixture(t, c, scenario)
		checkMicroFixture(t, fx)
		record := map[string]any{"workload_version": microWorkloadVersion, "sample": microFixtureName(c, scenario), "metrics": fx.metrics, "trace_digest": fmt.Sprintf("%x", fx.traceDigest),
			"native_lo_limit": nil, "native_position_accumulator": nil, "persistent_weights": nil, "block_forest": nil, "excluded_solid": nil, "update_weight_entries": nil, "k": nil,
			"candidate_available": fx.proposal != nil, "output_verifier": nil, "candidate_leader_signature": nil, "pre_post_state_commitment": nil}
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("THEMIS_MICRO_SAMPLE %s\n", data)
	}
}

// Read-only paths have no pending cache/fast return and perform their own native
// allocations. They may safely reuse an immutable snapshot across b.N replays.
// Commit changes state, so its restoration is explicitly outside the timer.
func BenchmarkThemisLocal(b *testing.B) {
	c := configuredMicro()
	if runtime.GOMAXPROCS(0) != 1 {
		b.Fatal("set GOMAXPROCS=1 explicitly")
	}
	for _, scenario := range strings.Split(*microScenarios, ",") {
		b.Run(microFixtureName(c, scenario), func(b *testing.B) {
			fx := makeMicroFixture(b, c, scenario)
			checkMicroFixture(b, fx)
			b.Run("GraphCore", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r := microGraphCore(fx.prepared)
					runtime.KeepAlive(r)
				}
			})
			b.Run("LeaderBuildFull", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					p := fx.s.buildProposal(fx.orders)
					if p != nil {
						digest := types.LeaderProposalDigest(p)
						runtime.KeepAlive(digest)
					}
					runtime.KeepAlive(p)
				}
			})
			if fx.proposal == nil {
				return
			} // N/A; no fake zero verification measurement.
			b.Run("CandidateCheckCore", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ok := microCandidateCore(fx.prepared, fx.proposal)
					runtime.KeepAlive(ok)
				}
			})
			b.Run("FollowerVerifyFull", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ok := fx.s.VerifyProposal(fx.proposal)
					digest := types.LeaderProposalDigest(fx.proposal)
					runtime.KeepAlive(ok)
					runtime.KeepAlive(digest)
				}
			})
			b.Run("CommitFinalizeFull", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					s := microRestoreService(fx.s)
					b.StartTimer()
					ok := s.CommitVerifiedProposal(fx.proposal)
					runtime.KeepAlive(ok)
					runtime.KeepAlive(s)
				}
			})
		})
	}
}

// Input-cap failure has a separate subprocess regression in TestMicroCapEnforced.
func TestMicroParameterSemantics(t *testing.T) {
	for _, n := range []uint64{10, 50} {
		if err := ValidateThemisParameters(n, 1, .95); err != nil {
			t.Fatal(err)
		}
	}
	if edgeThreshold(10, 1, .95) != 3 || edgeThreshold(50, 1, .95) != 5 {
		t.Fatal("native thresholds changed")
	}
}
