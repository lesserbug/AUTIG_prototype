package ofo

// Controlled receipt histories, not a network or throughput experiment.
// The small expected graphs below are derived from the paper's voting rules;
// they do not use BuildGraph, FairUpdate, Tarjan, or ComputeFairOrder as oracles.
import (
	"themisreference/pkg/network"
	"themisreference/pkg/types"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"
)

type mechanismHarness struct {
	t       *testing.T
	seed    int
	s       []*OFOService
	nets    []*testNetwork
	txs     map[string]*types.Transaction
	labels  map[[32]byte]string
	history [][]string
	clock   int64
}

func newMechanismHarness(t *testing.T, seed int) *mechanismHarness {
	h := &mechanismHarness{t: t, seed: seed, txs: map[string]*types.Transaction{}, labels: map[[32]byte]string{}, history: make([][]string, 20)}
	auth := newTestOrderAuthenticator(20)
	for i := 0; i < 20; i++ {
		net := &testNetwork{sent: make(chan network.Message, 1)}
		s, err := NewOFOService(1, 20, 4, 1, net, 0, 100, false, auth)
		if err != nil {
			t.Fatal(err)
		}
		s.ReplicaID = uint64(i)
		h.s = append(h.s, s)
		h.nets = append(h.nets, net)
	}
	return h
}

func (h *mechanismHarness) deliver(i int, names []string) {
	for _, name := range names {
		tx := h.txs[name]
		if tx == nil {
			payload := make([]byte, 512)
			copy(payload, fmt.Sprintf("themis-mechanism/seed=%d/%s", h.seed, name))
			tx = &types.Transaction{ID: types.TransactionID(payload), CanonicalBytes: payload, SubmissionTime: time.Unix(1780000000, 0)}
			h.txs[name] = tx
			h.labels[tx.ID] = name
		}
		if _, ok := h.s[i].txPool[tx.ID]; ok {
			continue
		}
		if h.s[i].committedTxIDs[tx.ID] {
			h.t.Fatal("fixture redelivers completed transaction")
		}
		h.s[i].handleMessage(network.Message{Payload: tx})
		if _, ok := h.s[i].txPool[tx.ID]; !ok {
			h.t.Fatal("native admission did not create receipt")
		}
		h.clock++
		h.s[i].txPool[tx.ID] = time.Unix(0, h.clock)
		h.history[i] = append(h.history[i], name)
	}
}

func (h *mechanismHarness) names(ids [][32]byte) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, h.labels[id])
	}
	return out
}

// Independent direct list scan: A contributes iff present and B absent or later.
func mechanismVotes(lists [][]string, a, b string) (int, int) {
	ab, ba := 0, 0
	for _, list := range lists {
		pa, pb := -1, -1
		for i, x := range list {
			if x == a {
				pa = i
			}
			if x == b {
				pb = i
			}
		}
		if pa >= 0 && (pb < 0 || pa < pb) {
			ab++
		}
		if pb >= 0 && (pa < 0 || pb < pa) {
			ba++
		}
	}
	return ab, ba
}

// Small-graph reachability oracle; deliberately separate from native Tarjan.
func mechanismGraphStats(g *types.DependencyGraph) map[string]int {
	ids := make([][32]byte, 0, len(g.Nodes))
	for id := range g.Nodes {
		ids = append(ids, id)
	}
	index := map[[32]byte]int{}
	for i, id := range ids {
		index[id] = i
	}
	n := len(ids)
	reach := make([][]bool, n)
	edges := 0
	for i := range reach {
		reach[i] = make([]bool, n)
		reach[i][i] = true
	}
	for a, bs := range g.Edges {
		for _, b := range bs {
			reach[index[a]][index[b]] = true
			edges++
		}
	}
	missing := 0
	for i, a := range ids {
		for _, b := range ids[i+1:] {
			ab, ba := false, false
			for _, v := range g.Edges[a] {
				ab = ab || v == b
			}
			for _, v := range g.Edges[b] {
				ba = ba || v == a
			}
			if !ab && !ba {
				missing++
			}
		}
	}
	for k := 0; k < n; k++ {
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				reach[i][j] = reach[i][j] || (reach[i][k] && reach[k][j])
			}
		}
	}
	seen := make([]bool, n)
	sccs, maxSCC := 0, 0
	for i := 0; i < n; i++ {
		if seen[i] {
			continue
		}
		size := 0
		sccs++
		for j := 0; j < n; j++ {
			if reach[i][j] && reach[j][i] {
				seen[j] = true
				size++
			}
		}
		if size > maxSCC {
			maxSCC = size
		}
	}
	return map[string]int{"nodes": n, "edges": edges, "missing_pairs": missing, "sccs": sccs, "max_scc": maxSCC}
}

func (h *mechanismHarness) checkGraph(p *types.LeaderProposal, nodes, edges []string, shaded map[string]bool) {
	h.t.Helper()
	gotNodes := make([]string, 0, len(p.Graph.Nodes))
	gotEdges := []string{}
	for id := range p.Graph.Nodes {
		name := h.labels[id]
		gotNodes = append(gotNodes, name)
		want := types.StateSolid
		if shaded[name] {
			want = types.StateShaded
		}
		if p.TxStates[id] != want {
			h.t.Fatalf("%s state=%s want=%s", name, p.TxStates[id], want)
		}
	}
	for a, bs := range p.Graph.Edges {
		for _, b := range bs {
			gotEdges = append(gotEdges, h.labels[a]+">"+h.labels[b])
		}
	}
	sort.Strings(gotNodes)
	sort.Strings(gotEdges)
	nodes = append([]string(nil), nodes...)
	edges = append([]string(nil), edges...)
	sort.Strings(nodes)
	sort.Strings(edges)
	if !reflect.DeepEqual(gotNodes, nodes) || !reflect.DeepEqual(gotEdges, edges) {
		h.t.Fatalf("graph got %v %v want %v %v", gotNodes, gotEdges, nodes, edges)
	}
}

// Select senders 0..15 explicitly: no arrival-time, faulty-sender, ticker or
// network claim. Every one of the 20 logical replicas verifies and commits.
func (h *mechanismHarness) round(scenario string, nodes, edges []string, shaded map[string]bool, wantUpdate []string, wantOutput []string, cycle bool, wantRetained, wantBlocked int) {
	h.t.Helper()
	orders := []*types.ReplicaOrders{}
	lists, updates := [][]string{}, [][]string{}
	for i := 0; i < 16; i++ {
		h.s[i].generateAndSendOrders()
		o := (<-h.nets[i].sent).Payload.(*types.ReplicaOrders)
		orders = append(orders, o)
		lists = append(lists, h.names(o.NewTxOrder.OrderedTxs))
		if o.Update == nil {
			updates = append(updates, []string{})
		} else {
			updates = append(updates, h.names(o.Update.OrderedTxs))
		}
	}
	s := h.s[0]
	p := s.buildProposal(orders)
	if p == nil {
		h.t.Fatal("no candidate")
	}
	h.checkGraph(p, nodes, edges, shaded)
	actualUpdates := []string{}
	for height, es := range p.Updates {
		for a, bs := range es {
			for _, b := range bs {
				actualUpdates = append(actualUpdates, fmt.Sprintf("%d:%s>%s", height, h.labels[a], h.labels[b]))
			}
		}
	}
	sort.Strings(actualUpdates)
	if wantUpdate == nil {
		wantUpdate = []string{}
	}
	if !reflect.DeepEqual(actualUpdates, wantUpdate) {
		h.t.Fatalf("updates %v want %v", actualUpdates, wantUpdate)
	}
	before := []map[string]any{}
	heights := []uint64{}
	for height := range s.deferredProposals {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	for _, height := range heights {
		before = append(before, map[string]any{"height": height, "stats": mechanismGraphStats(s.deferredProposals[height].Graph)})
	}
	previous := len(s.finalizedOrder)
	for _, replica := range h.s {
		if !replica.VerifyProposal(p) {
			h.t.Fatalf("replica %d rejected", replica.ReplicaID)
		}
		if !replica.CommitVerifiedProposal(p) {
			h.t.Fatal("commit failed")
		}
	}
	output := h.names(s.finalizedOrder[previous:])
	if wantOutput == nil {
		wantOutput = []string{}
	}
	if cycle {
		// A complete three-cycle may rotate: require permutation and each
		// Hamiltonian path edge; do not manufacture a globally transitive order.
		sorted := append([]string(nil), output...)
		sort.Strings(sorted)
		if !reflect.DeepEqual(sorted, []string{"A", "B", "C"}) {
			h.t.Fatal("cycle output is not a permutation")
		}
		allowed := map[string]bool{"A>B": true, "B>C": true, "C>A": true}
		for i := 1; i < len(output); i++ {
			if !allowed[output[i-1]+">"+output[i]] {
				h.t.Fatal("not a cycle Hamiltonian path")
			}
		}
	} else if !reflect.DeepEqual(output, wantOutput) {
		h.t.Fatalf("output %v want %v", output, wantOutput)
	}
	retained, blocked := 0, 0
	after := []map[string]any{}
	heights = nil
	for height := range s.deferredProposals {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	for _, height := range heights {
		stats := mechanismGraphStats(s.deferredProposals[height].Graph)
		retained += stats["nodes"]
		if stats["missing_pairs"] == 0 {
			blocked += stats["nodes"]
		}
		after = append(after, map[string]any{"height": height, "stats": stats})
	}
	if retained != wantRetained || blocked != wantBlocked {
		h.t.Fatalf("retained=%d complete-but-blocked=%d want=%d,%d", retained, blocked, wantRetained, wantBlocked)
	}
	for _, replica := range h.s[1:] {
		if !reflect.DeepEqual(replica.finalizedOrder, s.finalizedOrder) || !reflect.DeepEqual(replica.deferredProposals, s.deferredProposals) || replica.lastCommittedDigest != s.lastCommittedDigest || !reflect.DeepEqual(replica.committedOrderSequences, s.committedOrderSequences) {
			h.t.Fatal("replica protocol state disagreement")
		}
		for _, id := range s.finalizedOrder {
			if !replica.committedTxIDs[id] {
				h.t.Fatal("missing completion")
			}
			if _, ok := replica.txPool[id]; ok {
				h.t.Fatal("completed receipt not removed")
			}
		}
	}
	ab, ba := mechanismVotes(lists, "A", "B")
	uab, uba := mechanismVotes(updates, "A", "B")
	row := map[string]any{"scenario": scenario, "seed": h.seed, "round": p.BlockHeight, "new_graph": mechanismGraphStats(p.Graph), "old_before": before, "old_after": after, "update_edges": actualUpdates, "released": output, "retained_txs": retained, "complete_but_blocked_txs": blocked, "local_lists": lists, "update_lists": updates, "local_AB_votes": []int{ab, ba}, "update_AB_votes": []int{uab, uba}, "receipt_history": h.history, "replicas_verified": 20, "selected_senders": "0..15", "n": 20, "F": 4, "gamma": 1, "edge_threshold": 5, "solid_threshold": 12, "candidate_digest": fmt.Sprintf("%x", types.LeaderProposalDigest(p))}
	data, err := json.Marshal(row)
	if err != nil {
		h.t.Fatal(err)
	}
	fmt.Printf("THEMIS_STATE_ROW %s\n", data)
}

func TestThemisMechanismStates(t *testing.T) {
	for _, seed := range []int{1, 7, 19} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Run("unanimous", func(t *testing.T) {
				h := newMechanismHarness(t, seed)
				for i := 0; i < 20; i++ {
					h.deliver(i, []string{"A", "B", "C"})
				}
				h.round("unanimous", []string{"A", "B", "C"}, []string{"A>B", "A>C", "B>C"}, nil, nil, []string{"A", "B", "C"}, false, 0, 0)
			})
			t.Run("cycle", func(t *testing.T) {
				h := newMechanismHarness(t, seed)
				rotations := [][]string{{"A", "B", "C"}, {"B", "C", "A"}, {"C", "A", "B"}}
				for i := 0; i < 20; i++ {
					h.deliver(i, rotations[i%3])
				}
				ab, ba := mechanismVotes(h.history[:16], "A", "B")
				bc, cb := mechanismVotes(h.history[:16], "B", "C")
				ca, ac := mechanismVotes(h.history[:16], "C", "A")
				if ab != 11 || ba != 5 || bc != 11 || cb != 5 || ca != 10 || ac != 6 {
					t.Fatal("cycle vote derivation failed")
				}
				h.round("complete_cycle", []string{"A", "B", "C"}, []string{"A>B", "B>C", "C>A"}, nil, nil, nil, true, 0, 0)
			})
			t.Run("deferred", func(t *testing.T) {
				h := newMechanismHarness(t, seed)
				for i := 0; i < 20; i++ {
					switch {
					case i < 4:
						h.deliver(i, []string{"A", "B", "Z"})
					case i < 8:
						h.deliver(i, []string{"B", "A", "Z"})
					case i < 12:
						h.deliver(i, []string{"Z"})
					}
				}
				ab, ba := mechanismVotes(h.history[:16], "A", "B")
				az, za := mechanismVotes(h.history[:16], "A", "Z")
				if ab != 4 || ba != 4 || az != 8 || za != 4 {
					t.Fatal("initial vote derivation failed")
				}
				h.round("deferred", []string{"A", "B", "Z"}, []string{"A>Z", "B>Z"}, map[string]bool{"A": true, "B": true}, nil, nil, false, 3, 0)
				for i := 0; i < 20; i++ {
					h.deliver(i, []string{"D0", "D1", "D2", "D3"})
				}
				h.round("deferred", []string{"D0", "D1", "D2", "D3"}, []string{"D0>D1", "D0>D2", "D0>D3", "D1>D2", "D1>D3", "D2>D3"}, nil, nil, nil, false, 7, 4)
				for i := 0; i < 20; i++ {
					h.deliver(i, []string{"A", "B", "Z", "E0", "E1"})
				}
				ab, ba = mechanismVotes(h.history[:16], "A", "B")
				if ab != 12 || ba != 4 {
					t.Fatal("late receipt derivation failed")
				}
				h.round("deferred", []string{"E0", "E1"}, []string{"E0>E1"}, nil, []string{"1:A>B"}, []string{"A", "B", "Z", "D0", "D1", "D2", "D3", "E0", "E1"}, false, 0, 0)
			})
		})
	}
}

func TestThemisMechanismVoteOracle(t *testing.T) {
	ab, ba := mechanismVotes([][]string{{"A"}, {"B"}, {}, {"A", "B"}, {"B", "A"}}, "A", "B")
	if ab != 2 || ba != 2 {
		t.Fatal("single-absent/both-absent vote oracle")
	}
}

var mechanismProfile = flag.String("mechanism-profile", "", "optional CPU profile of FollowerVerifyFull, fixture setup excluded")
var mechanismProfileDuration = flag.Duration("mechanism-profile-duration", 5*time.Second, "auxiliary profiling duration")
var mechanismProfileStage = flag.String("mechanism-profile-stage", "FollowerVerifyFull", "FollowerVerifyFull or LeaderBuildFull")

func TestThemisMechanismProfile(t *testing.T) {
	if *mechanismProfile == "" {
		t.Skip("auxiliary profile not requested")
	}
	if *mechanismProfileDuration <= 0 {
		t.Fatal("profile duration must be positive")
	}
	if *mechanismProfileStage != "FollowerVerifyFull" && *mechanismProfileStage != "LeaderBuildFull" {
		t.Fatal("invalid profile stage")
	}
	scenario := *microScenarios
	if strings.Contains(scenario, ",") {
		t.Fatal("profile needs exactly one micro scenario")
	}
	fx := makeMicroFixture(t, configuredMicro(), scenario)
	checkMicroFixture(t, fx)
	if fx.proposal == nil {
		t.Fatal("no candidate to profile")
	}
	f, err := os.Create(*mechanismProfile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(*mechanismProfileDuration)
	calls := 0
	valid := true
	for time.Now().Before(deadline) {
		if *mechanismProfileStage == "LeaderBuildFull" {
			p := fx.s.buildProposal(fx.orders)
			valid = p != nil && valid
			if p != nil {
				_ = types.LeaderProposalDigest(p)
			}
		} else {
			valid = fx.s.VerifyProposal(fx.proposal) && valid
			_ = types.LeaderProposalDigest(fx.proposal)
		}
		calls++
	}
	pprof.StopCPUProfile()
	if !valid {
		t.Fatal("profile verification failed")
	}
	fmt.Printf("THEMIS_PROFILE scenario=%s calls=%d duration=%s stage=%s\n", scenario, calls, *mechanismProfileDuration, *mechanismProfileStage)
}
