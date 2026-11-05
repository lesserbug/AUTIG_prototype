package ofo

// Fixed finite-profile supplement. No ticker, network consensus, or LO cap.
// Deliberately one test, reusing native receipt/sign/build/verify/commit paths.
import (
	"themisreference/pkg/network"
	"themisreference/pkg/types"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

var orderingReplayInput = flag.String("ordering-replay-input", "", "existing AUTIG directory containing r*-seed*-b0-input.json")
var orderingReplayOutput = flag.String("ordering-replay-output", "", "new JSONL result file")
var orderingReplayCost = flag.Bool("ordering-replay-cost", false, "Time only the nine r=10 matched cases, leader and one follower")
var orderingReplayThree = flag.Bool("ordering-replay-three-byzantine", false, "Use three r=10 traces with n=13,F=3,b=0..3; requires cost mode")

func TestThemisOrderingReplay(t *testing.T) {
	if *orderingReplayInput == "" {
		t.Skip("explicit finite-profile replay only")
	}
	n, f, expectedFiles := 10, 2, 15
	if *orderingReplayThree {
		if !*orderingReplayCost {
			t.Fatal("extended sweep requires cost mode")
		}
		n, f, expectedFiles = 13, 3, 3
	}
	selectedSenders := make([]int, 0, n-f)
	for r := f; r < n; r++ {
		selectedSenders = append(selectedSenders, r)
	}
	files, err := filepath.Glob(filepath.Join(*orderingReplayInput, "r*-seed*-b0-input.json"))
	if err != nil || len(files) != expectedFiles {
		t.Fatalf("expected %d paired input traces: %d, %v", expectedFiles, len(files), err)
	}
	out, err := os.OpenFile(*orderingReplayOutput, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	encoder := json.NewEncoder(out)
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var trace struct {
			ReceiptOrders  [][]int  `json:"receipt_orders"`
			TransactionIDs []string `json:"transaction_ids"`
		}
		if err := json.Unmarshal(data, &trace); err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(strings.TrimSuffix(filepath.Base(path), "-b0-input.json"), "-seed")
		ratio, err := strconv.ParseFloat(parts[0][1:], 64)
		if err != nil {
			t.Fatal(err)
		}
		seed, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatal(err)
		}
		if *orderingReplayCost && ratio != 10 {
			continue
		}
		if len(trace.ReceiptOrders) != n || len(trace.TransactionIDs) != 1000 {
			t.Fatalf("requires n=%d, M=1000", n)
		}
		txs := make([]types.Transaction, 1000)
		indices := make(map[[32]byte]int, 1000)
		positions := make([][]int, n)
		for i := range txs {
			// AUTIG hashes a length-prefixed 16-byte payload; Themis hashes
			// its bytes directly. Preserve IDs through test input encoding.
			payload := make([]byte, 24)
			binary.BigEndian.PutUint64(payload, 16)
			binary.BigEndian.PutUint64(payload[8:], uint64(i))
			binary.BigEndian.PutUint64(payload[16:], uint64(seed))
			txs[i] = types.Transaction{ID: types.TransactionID(payload), CanonicalBytes: payload}
			if fmt.Sprintf("%x", txs[i].ID) != trace.TransactionIDs[i] {
				t.Fatal("transaction identity differs from saved input")
			}
			indices[txs[i].ID] = i
		}
		for r, order := range trace.ReceiptOrders {
			positions[r] = make([]int, 1000)
			for pos, tx := range order {
				positions[r][tx] = pos
			}
		}
		var baseline []int
		for b := 0; b <= f; b++ {
			t.Run(fmt.Sprintf("r%g/seed%d/b%d", ratio, seed, b), func(t *testing.T) {
				auth := newTestOrderAuthenticator(uint64(n))
				services := make([]*OFOService, n)
				nets := make([]*testNetwork, n)
				for r := range services {
					nets[r] = &testNetwork{sent: make(chan network.Message, 1)}
					s, err := NewOFOService(1, uint64(n), uint64(f), 1, nets[r], 0, 100, r >= n-b, auth)
					if err != nil {
						t.Fatal(err)
					}
					s.ReplicaID = uint64(r)
					services[r] = s
					for pos, i := range trace.ReceiptOrders[r] {
						s.handleMessage(network.Message{Payload: &txs[i]})
						s.txPool[txs[i].ID] = time.Unix(0, int64(pos+1))
					}
					s.generateAndSendOrders()
				}
				// Controlled quorum includes all actual attackers (last b nodes).
				// AUTIG's rotating sender schedule is NOT claimed to be reproduced.
				evidence := make([]*types.ReplicaOrders, 0, n-f)
				claimed := make([][]int, 0, n-f)
				adopted := 0
				for _, r := range selectedSenders {
					o := (<-nets[r].sent).Payload.(*types.ReplicaOrders)
					evidence = append(evidence, o)
					list := make([]int, len(o.NewTxOrder.OrderedTxs))
					for pos, id := range o.NewTxOrder.OrderedTxs {
						list[pos] = indices[id]
					}
					claimed = append(claimed, list)
					if r >= n-b {
						adopted++
					}
				}
				leader, follower := services[0], services[1]
				started := time.Now()
				proposal := leader.buildProposal(evidence)
				buildNS := time.Since(started).Nanoseconds()
				if proposal == nil {
					t.Fatal("native candidate construction/verification failed")
				}
				cost := map[string]int64{"leader_build_ns": buildNS}
				if *orderingReplayCost {
					started = time.Now()
					if !follower.VerifyProposal(proposal) || !follower.CommitVerifiedProposal(proposal) {
						t.Fatal("timed follower verify/commit failed")
					}
					cost["follower_accept_ns"] = time.Since(started).Nanoseconds()
					started = time.Now()
					if !leader.CommitVerifiedProposal(proposal) {
						t.Fatal("timed leader commit failed")
					}
					cost["leader_commit_ns"] = time.Since(started).Nanoseconds()
				} else if !follower.VerifyProposal(proposal) {
					t.Fatal("native candidate verification failed")
				}
				// SCCs describe output coarseness only; original receipt positions
				// independently check cycle witnesses and fairness below.
				components := tarjanSCC(context.Background(), proposal.Graph)
				componentOf := make(map[[32]byte]int, 1000)
				maxSCC := 0
				for c, ids := range components {
					if len(ids) > maxSCC {
						maxSCC = len(ids)
					}
					for _, id := range ids {
						componentOf[id] = c
					}
				}
				if !*orderingReplayCost && (!leader.CommitVerifiedProposal(proposal) || !follower.CommitVerifiedProposal(proposal)) {
					t.Fatal("native commit failed")
				}
				if !reflect.DeepEqual(leader.finalizedOrder, follower.finalizedOrder) {
					t.Fatal("leader/follower outputs differ")
				}
				batches := make([]int, 1000)
				order := make([]int, 0, len(leader.finalizedOrder))
				groups := make([][]int, len(components))
				batch, previous := 0, -1
				for _, id := range leader.finalizedOrder {
					c := componentOf[id]
					if c != previous {
						batch++
						previous = c
					}
					i := indices[id]
					batches[i] = batch
					order = append(order, i)
					groups[c] = append(groups[c], i)
				}
				cycleViolations, adjacencyViolations := 0, 0
				// Paper III.2: for gamma=1 each cycle link needs >=1 original
				// honest receipt supporter. Check the actual emitted cycle incl. wrap.
				for _, group := range groups {
					if len(group) <= 1 {
						continue
					}
					for k, a := range group {
						z := group[(k+1)%len(group)]
						support := 0
						for r := 0; r < n-b; r++ {
							if positions[r][a] < positions[r][z] {
								support++
							}
						}
						if support < 1 {
							cycleViolations++
						}
					}
				}
				for k := 1; k < len(order); k++ {
					support := 0
					for r := 0; r < n-b; r++ {
						if positions[r][order[k-1]] < positions[r][order[k]] {
							support++
						}
					}
					if support < 1 {
						adjacencyViolations++
					}
				}
				eligible, checked, violations, fairTies := 0, 0, 0, 0
				// Paper III.1, gamma*n ORIGINAL nodes. Not AUTIG's qH=n-F.
				scopes := map[string]map[string]int{}
				for _, scope := range []string{"d1", "d5", "d10", "all"} {
					scopes[scope] = map[string]int{"pairs": 0, "strict": 0, "tie": 0, "reverse": 0, "unresolved": 0, "baseline_strict": 0, "kept": 0, "to_tie": 0, "to_reverse": 0, "to_unresolved": 0}
				}
				for i := 0; i < 1000; i++ {
					for j := i + 1; j < 1000; j++ {
						support := 0
						for r := 0; r < n; r++ {
							if positions[r][i] < positions[r][j] {
								support++
							}
						}
						if support == 0 || support == n {
							eligible++
							a, z := i, j
							if support == 0 {
								a, z = j, i
							}
							if batches[z] != 0 {
								checked++
								if batches[a] == 0 || batches[a] > batches[z] {
									violations++
								} else if batches[a] == batches[z] {
									fairTies++
								}
							}
						}
						for _, scope := range []string{"d1", "d5", "d10", "all"} {
							if scope == "d1" && j-i != 1 || scope == "d5" && j-i > 5 || scope == "d10" && j-i > 10 {
								continue
							}
							m := scopes[scope]
							m["pairs"]++
							switch {
							case batches[i] == 0 || batches[j] == 0:
								m["unresolved"]++
							case batches[i] < batches[j]:
								m["strict"]++
							case batches[i] == batches[j]:
								m["tie"]++
							default:
								m["reverse"]++
							}
							if b > 0 && baseline[i] != 0 && baseline[j] != 0 && baseline[i] != baseline[j] {
								m["baseline_strict"]++
								switch {
								case batches[i] == 0 || batches[j] == 0:
									m["to_unresolved"]++
								case batches[i] == batches[j]:
									m["to_tie"]++
								case (batches[i] < batches[j]) == (baseline[i] < baseline[j]):
									m["kept"]++
								default:
									m["to_reverse"]++
								}
							}
						}
					}
				}
				if b == 0 {
					baseline = append([]int(nil), batches...)
				}
				edges := 0
				for _, targets := range proposal.Graph.Edges {
					edges += len(targets)
				}
				row := map[string]any{"ratio": ratio, "seed": seed, "b": b, "n": n, "F": f, "gamma": 1, "transactions": 1000, "input_file": filepath.Base(path), "input_sha256": fmt.Sprintf("%x", sha256.Sum256(data)), "selected_senders": selectedSenders, "adopted_malicious": adopted, "claimed_lists": claimed, "graph_nodes": len(proposal.Graph.Nodes), "graph_edges": edges, "scc_count": len(components), "max_scc": maxSCC, "completed": len(order), "deferred_proposals": len(leader.deferredProposals), "batch_indices": batches, "final_order": order, "fairness_threshold": n, "fairness_eligible": eligible, "fairness_checked": checked, "fairness_violations": violations, "fairness_ties": fairTies, "cycle_witness_violations": cycleViolations, "adjacency_violations": adjacencyViolations, "scopes": scopes}
				if *orderingReplayCost {
					row["cost"] = cost
				}
				if err := encoder.Encode(row); err != nil {
					t.Fatal(err)
				}
				fmt.Printf("THEMIS_ORDERING r=%g seed=%d b=%d adopted=%d completed=%d scc=%d max_scc=%d violations=%d cycle=%d adjacency=%d\n", ratio, seed, b, adopted, len(order), len(components), maxSCC, violations, cycleViolations, adjacencyViolations)
				if len(order) != 1000 || violations != 0 || cycleViolations != 0 || adjacencyViolations != 0 {
					t.Error("incomplete output or fairness witness failure; result preserved")
				}
			})
		}
	}
}
