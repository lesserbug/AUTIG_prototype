package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	auto "SpeedFair_simplify/pkg/types"
	cmt "github.com/cometbft/cometbft/types"
	ttype "themisreference/pkg/types"
)

func runMatchedBenchmark(output, mode string, count, rate int, rho float64, seed int64, delay uint64, loSize int, committee benchCommittee, limits benchLimits) error {
	if count <= 0 || rate <= 0 {
		return fmt.Errorf("positive count and rate required")
	}
	if limits.RecordBytes <= 0 || limits.RecordBytes > 96<<20 || limits.Broadcast <= 0 || limits.Workload < 0 {
		return fmt.Errorf("record admission must be in (0,96 MiB], RPC timeout positive and workload timeout nonnegative")
	}
	if limits.Workload == 0 {
		limits.Workload = 30*time.Second + time.Duration(float64(count)/float64(rate)*float64(time.Second))
	}
	benchRegistry = &admission{items: make(map[auto.TxID][]byte)}
	out := make(chan []byte, 64)
	engines, err := makeBenchEngines(mode, delay, loSize, out, committee)
	if err != nil {
		return err
	}
	defer func() {
		for _, e := range engines {
			e.Stop()
		}
	}()
	txs := make([]auto.Transaction, count)
	rng := rand.New(rand.NewSource(seed))
	generation := make([]float64, count)
	for i := range txs {
		body := make([]byte, 512)
		binary.BigEndian.PutUint64(body, uint64(i))
		binary.BigEndian.PutUint64(body[8:], uint64(seed))
		txs[i] = auto.Transaction{ID: auto.TransactionID(body), CanonicalBytes: body}
		if err = benchRegistry.Store(txs[i]); err != nil {
			return err
		}
		// Preserve each implementation's native transaction digest encoding.
		// Application-level accounting joins their outputs by identical body.
		benchRegistry.items[auto.TxID(ttype.TransactionID(body))] = append([]byte(nil), body...)
		g := rng.ExpFloat64() / float64(rate)
		if i > 0 {
			generation[i] = generation[i-1] + g
		} else {
			generation[i] = g
		}
	}
	type event struct {
		at       float64
		node, tx int
	}
	var events []event
	for i, g := range generation {
		events = append(events, event{at: g, node: -1, tx: i})
		for node := 0; node < committee.N; node++ {
			events = append(events, event{at: g + rho*rng.ExpFloat64()/float64(rate), node: node, tx: i})
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].at < events[j].at })
	var metricsMu sync.Mutex
	born := make(map[auto.TxID]time.Time, count)
	executed := make(map[auto.TxID]bool, count)
	var latencies []float64
	firstBirth, lastBirth, lastExecution := time.Time{}, time.Time{}, time.Time{}
	apps := make([]*performanceApp, committee.N)
	for i, e := range engines {
		apps[i] = &performanceApp{engine: e, id: i, recordLimit: limits.RecordBytes, onExecution: func(node int, ids []auto.TxID, at time.Time) {
			if node != 0 {
				return
			}
			metricsMu.Lock()
			defer metricsMu.Unlock()
			for _, id := range ids {
				if executed[id] {
					panic("duplicate application execution")
				}
				executed[id] = true
				if start, ok := born[id]; ok {
					latencies = append(latencies, float64(at.Sub(start))/float64(time.Millisecond))
				} else {
					panic("execution before transaction issuance")
				}
				lastExecution = at
			}
		}}
	}
	client, stop, nodeData, err := startPerformanceCluster(apps, limits)
	if err != nil {
		return err
	}
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for _, ev := range events {
			wait := time.Until(start.Add(time.Duration(ev.at * float64(time.Second))))
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
			if ev.node < 0 {
				at := time.Now()
				metricsMu.Lock()
				born[txs[ev.tx].ID] = at
				if firstBirth.IsZero() {
					firstBirth = at
				}
				lastBirth = at
				metricsMu.Unlock()
				txs[ev.tx].SubmissionTime = at
			} else {
				engines[ev.node].Ingest(txs[ev.tx])
			}
		}
	}()
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, e := range engines {
					e.Report()
				}
				engines[0].FlushReports()
			}
		}
	}()
	type sample struct {
		AtMS                                   float64
		Issued, Executed, Settled, Outstanding int
		OldestWaitingMS                        float64
	}
	var samples []sample
	var blocks []map[string]any
	continuationMembers := make(map[auto.TxID]bool)
	openEvents, peakLive, peakOpen, peakDone := 0, 0, 0, 0
	go func() {
		defer wg.Done()
		monitor := time.NewTicker(50 * time.Millisecond)
		defer monitor.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-monitor.C:
				settled := engines[0].Settled()
				metricsMu.Lock()
				now := time.Now()
				s := sample{AtMS: float64(now.Sub(start)) / float64(time.Millisecond), Issued: len(born), Executed: len(executed), Settled: settled}
				s.Outstanding = s.Issued - s.Executed
				for id, at := range born {
					if !executed[id] {
						age := float64(now.Sub(at)) / float64(time.Millisecond)
						if age > s.OldestWaitingMS {
							s.OldestWaitingMS = age
						}
					}
				}
				samples = append(samples, s)
				metricsMu.Unlock()
			}
		}
	}()
	deadline := time.NewTimer(limits.Workload)
	defer deadline.Stop()
	for engines[0].Settled() < count {
		select {
		case wire := <-out:
			begin := time.Now()
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), limits.Broadcast)
			result, e := client.BroadcastTxCommit(rpcCtx, cmt.Tx(wire))
			rpcCancel()
			if e != nil {
				cancel()
				wg.Wait()
				return e
			}
			if result.CheckTx.Code != 0 || result.TxResult.Code != 0 {
				cancel()
				wg.Wait()
				if err := os.WriteFile(output+".failed_candidate.gob", wire, 0644); err != nil {
					return fmt.Errorf("consensus rejected candidate; could not preserve wire: %w", err)
				}
				return fmt.Errorf("consensus rejected native candidate: bytes=%d check_code=%d check_log=%q execution_code=%d execution_log=%q sequence=%d", len(wire), result.CheckTx.Code, result.CheckTx.Log, result.TxResult.Code, result.TxResult.Log, engines[0].Sequence())
			}
			target := engines[0].Sequence()
			until := time.Now().Add(5 * time.Second)
			for {
				agreed := true
				for _, e := range engines {
					if e.Sequence() != target {
						agreed = false
					}
				}
				if agreed {
					break
				}
				if time.Now().After(until) {
					cancel()
					wg.Wait()
					return fmt.Errorf("native replicas did not catch up")
				}
				time.Sleep(time.Millisecond)
			}
			block := map[string]any{"bft_height": result.Height, "native_sequence": target, "wire_bytes": len(wire), "broadcast_commit_ns": time.Since(begin).Nanoseconds()}
			if e, ok := engines[0].(*autoBench); ok {
				var f auto.VerifiableFairOrderFragment
				if gobRead(wire, &f) != nil {
					return fmt.Errorf("statistics decode")
				}
				if f.Continuation != nil && f.Continuation.Opened {
					openEvents++
				}
				closedCount := 0
				for _, batch := range f.FinalOrder.Batches {
					closedCount += len(batch.Transactions)
					for _, id := range batch.Transactions {
						delete(continuationMembers, id)
					}
				}
				block["closed_executed"] = closedCount
				if c := f.Continuation; c != nil {
					for _, id := range c.Assigned {
						continuationMembers[id] = true
					}
					for _, batch := range c.Joins {
						for _, id := range batch.Transactions {
							continuationMembers[id] = true
						}
					}
					block["continuation_members_before_execution"] = len(continuationMembers)
					block["continuation_assigned"] = len(c.Assigned)
					block["continuation_executed"] = len(c.Settled)
					block["continuation_rounds"] = len(c.Joins)
					for _, id := range c.Settled {
						delete(continuationMembers, id)
					}
				}
				p := e.service.Progress()
				block["committed_live"], block["committed_pending_members"] = p.Live, p.OpenMembers
				if len(continuationMembers) != p.OpenMembers {
					cancel()
					wg.Wait()
					return fmt.Errorf("continuation membership accounting mismatch")
				}
				if p.Live > peakLive {
					peakLive = p.Live
				}
				if p.OpenMembers > peakOpen {
					peakOpen = p.OpenMembers
				}
				if p.Settled > peakDone {
					peakDone = p.Settled
				}
			}
			blocks = append(blocks, block)
		case <-deadline.C:
			cancel()
			wg.Wait()
			return fmt.Errorf("workload did not settle: executed%d settled%d expected%d", engines[0].Count(), engines[0].Settled(), count)
		}
	}
	cancel()
	wg.Wait()
	for _, e := range engines {
		if e.Count() != count || e.Settled() != count {
			return fmt.Errorf("replica completion mismatch")
		}
	}
	state := engines[0].State()
	for _, e := range engines[1:] {
		if e.State() != state {
			return fmt.Errorf("native state mismatch")
		}
	}
	metricsMu.Lock()
	sort.Float64s(latencies)
	lat := append([]float64(nil), latencies...)
	completed := len(executed)
	metricsMu.Unlock()
	if completed != count || len(lat) != count {
		return fmt.Errorf("completion accounting mismatch")
	}
	mean := 0.0
	for _, v := range lat {
		mean += v
	}
	mean /= float64(len(lat))
	p95 := lat[int(float64(len(lat)-1)*.95)]
	var validators []map[string]any
	for _, a := range apps {
		a.mu.Lock()
		validators = append(validators, map[string]any{"id": a.id, "prepare_ns": a.prepareNS, "process_ns": a.processNS, "finalize_ns": a.finalizeNS, "commit_ns": a.commitNS, "validation_all_hooks_ns": a.validationNS, "process_calls": a.processCalls, "nonempty_blocks": a.blocks})
		a.mu.Unlock()
	}
	result := map[string]any{"mode": mode, "n": committee.N, "f": committee.F, "gamma": committee.Gamma, "D": delay, "rho": rho, "seed": seed, "transactions": count, "configured_rate": rate, "actual_issued_rate": float64(count-1) / lastBirth.Sub(firstBirth).Seconds(), "drain_throughput": float64(count) / lastExecution.Sub(firstBirth).Seconds(), "mean_execution_latency_ms": mean, "p95_execution_latency_ms": p95, "completion_ratio": 1, "outstanding_final": 0, "all_native_states_agree": true, "blocks": blocks, "validators": validators, "waiting_curve": samples, "node_data": nodeData, "scope": fmt.Sprintf("%d real loopback CometBFT validators in one process; scheduled client arrivals with actual birth timestamps; local hosted content availability", committee.N), "app_hash": "commit-order proposal log root; native states independently compared after completion", "report_quorum": committee.N - committee.F, "cluster_ready_validators": committee.N, "go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "logical_cpus": runtime.NumCPU()}
	result["continuation_open_events"], result["peak_committed_live"], result["peak_open_members"], result["peak_done"] = openEvents, peakLive, peakOpen, peakDone
	result["application_record_limit_bytes"] = limits.RecordBytes
	result["broadcast_timeout_seconds"] = limits.Broadcast.Seconds()
	result["workload_timeout_seconds"] = limits.Workload.Seconds()
	result["reporting"] = fmt.Sprintf("50ms; AUTIG pending extension refreshed before admission, max %d positions; native Themis untruncated new/update lists", loSize)
	reportStats := make(map[string]uint64)
	for _, e := range engines {
		for k, v := range e.ReportStats() {
			if k == "lo_fresh_ids_max" {
				reportStats[k] = max(reportStats[k], v)
			} else {
				reportStats[k] += v
			}
		}
	}
	if len(reportStats) > 0 {
		result["report_stats"] = reportStats
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(output, encoded, 0644); err != nil {
		return err
	}
	fmt.Printf("MATCHED n=%d f=%d mode=%s rho=%g seed=%d transactions=%d latency_ms=%.3f throughput=%.3f all_states_agree=true\n", committee.N, committee.F, mode, rho, seed, count, mean, float64(count)/lastExecution.Sub(firstBirth).Seconds())
	return nil
}
