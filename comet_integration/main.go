// A local, real five-validator CometBFT composition experiment. The order leader
// constructs authentic AUTIG fragments; rotating CometBFT proposers transport
// them. No AUTIG production exports or consensus shortcuts are added.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	autonetwork "SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/ofo"
	auto "SpeedFair_simplify/pkg/types"
	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	cmtnode "github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	cmt "github.com/cometbft/cometbft/types"
)

type auth struct {
	keys   map[uint64]ed25519.PrivateKey
	leader ed25519.PrivateKey
}

func newAuth() *auth {
	return newCommitteeAuth(5)
}

func newCommitteeAuth(n int) *auth {
	a := &auth{keys: map[uint64]ed25519.PrivateKey{}}
	for i := uint64(0); i < uint64(n); i++ {
		seed := sha256.Sum256([]byte(fmt.Sprintf("AUTIG/local-comet/replica/%d", i)))
		a.keys[i] = ed25519.NewKeyFromSeed(seed[:])
	}
	seed := sha256.Sum256([]byte("AUTIG/local-comet/order-leader"))
	a.leader = ed25519.NewKeyFromSeed(seed[:])
	return a
}
func (a *auth) SignReplica(i uint64, d [32]byte) ([]byte, error) {
	return ed25519.Sign(a.keys[i], d[:]), nil
}
func (a *auth) VerifyReplica(i uint64, d [32]byte, s []byte) bool {
	k, ok := a.keys[i]
	return ok && ed25519.Verify(k.Public().(ed25519.PublicKey), d[:], s)
}
func (a *auth) SignLeader(_ auto.AuthContext, d [32]byte) ([]byte, error) {
	return ed25519.Sign(a.leader, d[:]), nil
}
func (a *auth) VerifyLeader(_ auto.AuthContext, i uint64, d [32]byte, s []byte) bool {
	return i == 0 && ed25519.Verify(a.leader.Public().(ed25519.PublicKey), d[:], s)
}

type admission struct {
	fallback *admission
	mu       sync.RWMutex
	items    map[auto.TxID][]byte
}

func (a *admission) Store(t auto.Transaction) error {
	if auto.TransactionID(t.CanonicalBytes) != t.ID {
		return fmt.Errorf("content digest mismatch")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.items[t.ID] = append([]byte(nil), t.CanonicalBytes...)
	return nil
}
func (a *admission) Resolve(id auto.TxID) ([]byte, auto.TxID, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	b, ok := a.items[id]
	if !ok {
		if a.fallback != nil {
			return a.fallback.Resolve(id)
		}
		return nil, auto.TxID{}, fmt.Errorf("unavailable")
	}
	return append([]byte(nil), b...), id, nil
}
func (*admission) ValidateAdmission(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("empty content")
	}
	return nil
}

type localNet struct{}

func (localNet) Register(uint64, func(autonetwork.Message)) {}
func (localNet) Send(autonetwork.Message) bool              { return true }
func (localNet) Stop()                                      {}
func (localNet) WaitForPeers(time.Duration) error           { return nil }

type candidate struct {
	fragment *auto.VerifiableFairOrderFragment
	digest   [32]byte
}
type app struct {
	abci.BaseApplication
	mu             sync.Mutex
	service        *ofo.OFOService
	id             int
	height         int64
	appHash        []byte
	preparedHeight int64
	toCommit       *[32]byte
	counts         map[string]int
	verification   time.Duration
}

func (a *app) Info(context.Context, *abci.RequestInfo) (*abci.ResponseInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return &abci.ResponseInfo{Data: "AUTIG local composition", LastBlockHeight: a.height, LastBlockAppHash: append([]byte(nil), a.appHash...)}, nil
}
func (a *app) InitChain(context.Context, *abci.RequestInitChain) (*abci.ResponseInitChain, error) {
	return &abci.ResponseInitChain{AppHash: append([]byte(nil), a.appHash...)}, nil
}
func (a *app) CheckTx(_ context.Context, r *abci.RequestCheckTx) (*abci.ResponseCheckTx, error) {
	var f auto.VerifiableFairOrderFragment
	if len(r.Tx) > 1<<20 || json.Unmarshal(r.Tx, &f) != nil {
		return &abci.ResponseCheckTx{Code: 1}, nil
	}
	seq, _, _ := a.service.CommittedContext()
	if f.FragmentSeq != seq+1 {
		return &abci.ResponseCheckTx{Code: 2}, nil
	}
	return &abci.ResponseCheckTx{}, nil
}

// All semantic checks are done before the validator votes. A cached native
// leader candidate is already constructed/checked by its own authenticated path.
func (a *app) validate(wire []byte) bool {
	var f auto.VerifiableFairOrderFragment
	if json.Unmarshal(wire, &f) != nil || f.FinalOrder == nil || f.Certificate == nil {
		return false
	}
	digest := auto.FragmentDigest(&f)
	if old, ok := a.service.PendingDigest(); ok {
		if old == digest {
			return true
		}
		if a.id == 0 {
			return false
		}
		a.service.AbandonPending(old)
	}
	if a.id == 0 {
		return false
	}
	a.service.HandleMessage(autonetwork.Message{From: 0, Payload: &f})
	got, ok := a.service.PendingDigest()
	return ok && got == digest
}
func (a *app) PrepareProposal(_ context.Context, r *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts["prepare"]++
	for _, wire := range r.Txs {
		if int64(len(wire)) <= r.MaxTxBytes && a.validate(wire) {
			return &abci.ResponsePrepareProposal{Txs: [][]byte{wire}}, nil
		}
	}
	return &abci.ResponsePrepareProposal{}, nil
}
func (a *app) ProcessProposal(_ context.Context, r *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts["process"]++
	start := time.Now()
	defer func() { a.verification += time.Since(start) }()
	if len(r.Txs) > 1 {
		return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
	}
	for _, wire := range r.Txs {
		if !a.validate(wire) {
			a.counts["rejected"]++
			return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
		}
	}
	return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_ACCEPT}, nil
}
func (a *app) FinalizeBlock(_ context.Context, r *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts["finalize"]++
	a.preparedHeight = r.Height
	result := &abci.ResponseFinalizeBlock{AppHash: append([]byte(nil), a.appHash...)}
	a.toCommit = nil
	for _, wire := range r.Txs {
		if !a.validate(wire) {
			return nil, fmt.Errorf("decided block failed AUTIG validation")
		}
		var f auto.VerifiableFairOrderFragment
		if err := json.Unmarshal(wire, &f); err != nil {
			return nil, err
		}
		digest := auto.FragmentDigest(&f)
		a.toCommit = &digest
		result.AppHash = append([]byte(nil), f.CandidatePostStateID[:]...)
		result.TxResults = append(result.TxResults, &abci.ExecTxResult{})
	}
	return result, nil
}
func (a *app) Commit(context.Context, *abci.RequestCommit) (*abci.ResponseCommit, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts["commit"]++
	if a.toCommit != nil {
		if _, ok := a.service.CommitPending(*a.toCommit); !ok {
			return nil, fmt.Errorf("native commit digest mismatch")
		}
		a.toCommit = nil
	}
	a.height = a.preparedHeight
	hash := a.service.CommittedStateID()
	a.appHash = append([]byte(nil), hash[:]...)
	return &abci.ResponseCommit{}, nil
}

func port() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	p := l.Addr().(*net.TCPAddr).Port
	err = l.Close()
	return p, err
}

func run(output string, fragments int) error {
	parent := filepath.Join("..", "comet_nodes")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	root, err := os.MkdirTemp(parent, "run-")
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	authenticator := newAuth()
	contextID := sha256.Sum256([]byte("AUTIG/CometBFT/v0.38.17/5/1/1/local-integration"))
	authContext := auto.AuthContext{Epoch: 1, LeaderID: 0, Identifier: contextID}
	genesis := auto.ProtocolGenesis{StateID: sha256.Sum256([]byte("autig-comet-genesis")), FragmentDigest: sha256.Sum256([]byte("autig-comet-fragment-genesis"))}
	apps := make([]*app, 5)
	admissions := make([]*admission, 5)
	candidates := make(chan candidate, 2)
	for i := 0; i < 5; i++ {
		admissions[i] = &admission{items: map[auto.TxID][]byte{}}
		var onCandidate ofo.CandidateHandler
		if i == 0 {
			onCandidate = func(f *auto.VerifiableFairOrderFragment, d [32]byte) { candidates <- candidate{f, d} }
		}
		s, e := ofo.NewOFOService(uint64(i), 5, 1, 1, localNet{}, 64, nil, false, authenticator, admissions[i], authContext, genesis, onCandidate)
		if e != nil {
			return e
		}
		defer s.Stop()
		apps[i] = &app{service: s, id: i, appHash: append([]byte(nil), genesis.StateID[:]...), counts: map[string]int{}}
	}
	configs := make([]*cfg.Config, 5)
	pvs := make([]*privval.FilePV, 5)
	keys := make([]*p2p.NodeKey, 5)
	addresses := make([]string, 5)
	rpcAddresses := make([]string, 5)
	validators := make([]cmt.GenesisValidator, 5)
	for i := 0; i < 5; i++ {
		c := cfg.DefaultConfig().SetRoot(filepath.Join(root, fmt.Sprintf("node%d", i)))
		configs[i] = c
		c.Moniker = fmt.Sprintf("revision-%d", i)
		c.DBBackend = "goleveldb"
		c.P2P.AddrBookStrict = false
		c.P2P.AllowDuplicateIP = true
		c.P2P.PexReactor = false
		c.Consensus.CreateEmptyBlocks = false
		c.Consensus.TimeoutPropose = 500 * time.Millisecond
		c.Consensus.TimeoutProposeDelta = 100 * time.Millisecond
		c.Consensus.TimeoutPrevote = 100 * time.Millisecond
		c.Consensus.TimeoutPrecommit = 100 * time.Millisecond
		c.Consensus.TimeoutCommit = 100 * time.Millisecond
		c.RPC.TimeoutBroadcastTxCommit = 30 * time.Second
		for _, d := range []string{"config", "data"} {
			if err := os.MkdirAll(filepath.Join(c.RootDir, d), 0700); err != nil {
				return err
			}
		}
		p, e := port()
		if e != nil {
			return e
		}
		c.P2P.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%d", p)
		rp, e := port()
		if e != nil {
			return e
		}
		c.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%d", rp)
		rpcAddresses[i] = fmt.Sprintf("http://127.0.0.1:%d", rp)
		pvs[i] = privval.LoadOrGenFilePV(c.PrivValidatorKeyFile(), c.PrivValidatorStateFile())
		keys[i], e = p2p.LoadOrGenNodeKey(c.NodeKeyFile())
		if e != nil {
			return e
		}
		public, e := pvs[i].GetPubKey()
		if e != nil {
			return e
		}
		validators[i] = cmt.GenesisValidator{Address: public.Address(), PubKey: public, Power: 1, Name: c.Moniker}
		addresses[i] = fmt.Sprintf("%s@127.0.0.1:%d", keys[i].ID(), p)
	}
	doc := &cmt.GenesisDoc{GenesisTime: time.Now().UTC(), ChainID: "autig-local-revision", ConsensusParams: cmt.DefaultConsensusParams(), Validators: validators, AppHash: append([]byte(nil), genesis.StateID[:]...)}
	nodes := make([]*cmtnode.Node, 5)
	defer func() {
		for _, n := range nodes {
			if n != nil && n.IsRunning() {
				n.Stop()
			}
		}
		for _, n := range nodes {
			if n != nil {
				n.Wait()
			}
		}
	}()
	for i, c := range configs {
		var peers string
		for j, address := range addresses {
			if i != j {
				if peers != "" {
					peers += ","
				}
				peers += address
			}
		}
		c.P2P.PersistentPeers = peers
		if err := doc.SaveAs(c.GenesisFile()); err != nil {
			return err
		}
		n, e := cmtnode.NewNode(c, pvs[i], keys[i], proxy.NewLocalClientCreator(apps[i]), cmtnode.DefaultGenesisDocProviderFunc(c), cfg.DefaultDBProvider, cmtnode.DefaultMetricsProvider(c.Instrumentation), cmtlog.NewNopLogger())
		if e != nil {
			return e
		}
		nodes[i] = n
		if e = n.Start(); e != nil {
			return e
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ready := true
		for _, n := range nodes {
			if n.Switch().Peers().Size() < 4 {
				ready = false
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("five-validator mesh did not connect")
		}
		time.Sleep(50 * time.Millisecond)
	}
	client, err := rpchttp.New(rpcAddresses[0], "/websocket")
	if err != nil {
		return err
	}
	logs := make([][]auto.TxID, 5)
	positions := make([]uint64, 5)
	heads := make([][32]byte, 5)
	var samples []map[string]any
	for r := 1; r <= fragments; r++ {
		for j := 0; j < 8; j++ {
			payload := []byte(fmt.Sprintf("comet-transaction/%d/%d", r, j))
			tx := auto.Transaction{ID: auto.TransactionID(payload), CanonicalBytes: payload, SubmissionTime: time.Now()}
			for i, a := range apps {
				a.service.HandleMessage(autonetwork.Message{Payload: &tx})
				logs[i] = append(logs[i], tx.ID)
			}
		}
		construction := time.Now()
		var admitted []*auto.LocalOrder
		for i := uint64(0); i < 5; i++ {
			if (i+5-uint64(r)%5)%5 >= 4 {
				continue
			}
			chunk := append([]auto.TxID(nil), logs[i][positions[i]:]...)
			o := &auto.LocalOrder{ReplicaID: i, Epoch: 1, FragmentSeq: uint64(r), PrevPosition: positions[i], PrevHead: heads[i], OrderedTxs: chunk}
			o.NewPosition = o.PrevPosition + uint64(len(chunk))
			o.NewHead = auto.LocalOrderHead(o)
			o.Signature, err = authenticator.SignReplica(i, auto.LocalOrderDigest(o))
			if err != nil {
				return err
			}
			admitted = append(admitted, o)
			apps[0].service.HandleMessage(autonetwork.Message{From: i, Payload: o})
		}
		var c candidate
		select {
		case c = <-candidates:
		case <-time.After(10 * time.Second):
			return fmt.Errorf("native order leader did not construct fragment %d", r)
		}
		constructNS := time.Since(construction).Nanoseconds()
		// A correctly authenticated semantic error must be rejected before voting.
		wire, err := json.Marshal(c.fragment)
		if err != nil {
			return err
		}
		var malformed auto.VerifiableFairOrderFragment
		if err = json.Unmarshal(wire, &malformed); err != nil {
			return err
		}
		malformed.Certificate.Part[0].Rank++
		malformed.LeaderSignature, err = authenticator.SignLeader(authContext, auto.FragmentDigest(&malformed))
		if err != nil {
			return err
		}
		badWire, err := json.Marshal(&malformed)
		if err != nil {
			return err
		}
		for _, a := range apps {
			response, e := a.ProcessProposal(context.Background(), &abci.RequestProcessProposal{Txs: [][]byte{badWire}})
			if e != nil || response.Status != abci.ResponseProcessProposal_REJECT {
				return fmt.Errorf("validator %d accepted authenticated malformed certificate", a.id)
			}
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		result, e := client.BroadcastTxCommit(ctx, cmt.Tx(wire))
		cancel()
		if e != nil {
			return e
		}
		if result.CheckTx.Code != 0 || result.TxResult.Code != 0 {
			return fmt.Errorf("BFT transaction failed: %+v", result)
		}
		consensusNS := time.Since(start).Nanoseconds()
		deadline = time.Now().Add(10 * time.Second)
		for {
			agreed := true
			for _, a := range apps {
				seq, _, digest := a.service.CommittedContext()
				if seq != uint64(r) || digest != c.digest {
					agreed = false
				}
			}
			if agreed {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("application replicas did not agree at fragment %d", r)
			}
			time.Sleep(10 * time.Millisecond)
		}
		for _, o := range admitted {
			positions[o.ReplicaID] = o.NewPosition
			heads[o.ReplicaID] = o.NewHead
		}
		for _, a := range apps {
			if a.service.GetFinalizedCount() != r*8 || a.service.CommittedStateID() != c.fragment.CandidatePostStateID {
				return fmt.Errorf("application state/output mismatch")
			}
		}
		samples = append(samples, map[string]any{"fragment": r, "bft_height": result.Height, "output_transactions": 8, "construct_ns": constructNS, "broadcast_to_bft_commit_ns": consensusNS, "wire_bytes": len(wire), "all_five_states_agree": true})
	}
	var validatorsReport []map[string]any
	for _, a := range apps {
		a.mu.Lock()
		counts := make(map[string]int, len(a.counts))
		for name, count := range a.counts {
			counts[name] = count
		}
		validatorsReport = append(validatorsReport, map[string]any{"validator": a.id, "abci_calls": counts, "process_proposal_wall_ns": a.verification.Nanoseconds(), "finalized": a.service.GetFinalizedCount()})
		a.mu.Unlock()
	}
	report := map[string]any{"consensus": "CometBFT v0.38.17", "scope": "actual five-validator loopback consensus, local composition and validation; not WAN capacity, pipelining, or order-leader handoff", "order_leader": 0, "consensus_proposers": "CometBFT rotating validator proposers", "node_data": root, "fragments": fragments, "transactions": fragments * 8, "samples": samples, "validators": validatorsReport, "commit_gate": "FinalizeBlock stages; ABCI Commit calls native CommitPending with exact decided digest"}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(output, data, 0644); err != nil {
		return err
	}
	fmt.Printf("COMET_RESULT validators=5 fragments=%d transactions=%d all_states_agree=true output=%s\n", fragments, fragments*8, output)
	return nil
}

func main() {
	matched := flag.Bool("matched-benchmark", false, "Run controlled native AUTIG/Themis CometBFT benchmark")
	n := flag.Int("n", 5, "Matched benchmark validator count")
	f := flag.Int("f", 1, "Matched benchmark Byzantine fault budget (no fault injection)")
	gamma := flag.Float64("gamma", 1, "Matched benchmark fairness parameter")
	recordMiB := flag.Int("record-limit-mib", 16, "Common application admission limit in MiB (at most 96)")
	broadcastTimeout := flag.Duration("rpc-timeout", 20*time.Second, "Matched benchmark BroadcastTxCommit timeout")
	workloadTimeout := flag.Duration("workload-timeout", 0, "Matched benchmark workload timeout; zero uses the original load-based timeout")
	mode := flag.String("mode", "certificate", "certificate, incremental-undo, reconstruction, or themis")
	transactions := flag.Int("transactions", 240, "Matched client transaction count")
	rate := flag.Int("rate", 400, "Matched client issuance rate")
	rho := flag.Float64("rho", .1, "Mean client propagation delay / generation interval")
	seed := flag.Int64("seed", 1, "Matched workload seed")
	delay := flag.Uint64("D", 2, "Certified continuation delay")
	output := flag.String("output", "../results/comet_integration.json", "result JSON path")
	fragments := flag.Int("fragments", 8, "number of native AUTIG fragments")
	loSize := flag.Int("lo-size", 100, "AUTIG maximum LocalOrder extension size in the matched benchmark")
	flag.Parse()
	if *matched {
		if err := runMatchedBenchmark(*output, *mode, *transactions, *rate, *rho, *seed, *delay, *loSize, benchCommittee{N: *n, F: *f, Gamma: *gamma}, benchLimits{RecordBytes: *recordMiB << 20, Broadcast: *broadcastTimeout, Workload: *workloadTimeout}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(*output, *fragments); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
