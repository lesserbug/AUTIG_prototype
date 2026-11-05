package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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

type performanceApp struct {
	abci.BaseApplication
	mu                                                       sync.Mutex
	engine                                                   benchEngine
	id                                                       int
	recordLimit                                              int
	height, nextHeight                                       int64
	hash, nextHash                                           [32]byte
	pending                                                  []byte
	prepareNS, processNS, finalizeNS, commitNS, validationNS int64
	prepareCalls, processCalls, blocks                       int
	onExecution                                              func(int, []auto.TxID, time.Time)
}

func (a *performanceApp) Info(context.Context, *abci.RequestInfo) (*abci.ResponseInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return &abci.ResponseInfo{LastBlockHeight: a.height, LastBlockAppHash: append([]byte(nil), a.hash[:]...)}, nil
}
func (a *performanceApp) InitChain(context.Context, *abci.RequestInitChain) (*abci.ResponseInitChain, error) {
	return &abci.ResponseInitChain{AppHash: make([]byte, 32)}, nil
}
func (a *performanceApp) CheckTx(_ context.Context, r *abci.RequestCheckTx) (*abci.ResponseCheckTx, error) {
	if len(r.Tx) == 0 || len(r.Tx) > a.recordLimit {
		return &abci.ResponseCheckTx{Code: 1, Log: fmt.Sprintf("record size %d outside admission limit 1..%d", len(r.Tx), a.recordLimit)}, nil
	}
	return &abci.ResponseCheckTx{}, nil
}
func (a *performanceApp) validate(wire []byte) bool {
	start := time.Now()
	ok := len(wire) <= a.recordLimit && a.engine.Validate(wire)
	a.validationNS += time.Since(start).Nanoseconds()
	return ok
}
func (a *performanceApp) PrepareProposal(_ context.Context, r *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := time.Now()
	defer func() { a.prepareNS += time.Since(start).Nanoseconds() }()
	a.prepareCalls++
	for _, wire := range r.Txs {
		if int64(len(wire)) <= r.MaxTxBytes && a.validate(wire) {
			return &abci.ResponsePrepareProposal{Txs: [][]byte{wire}}, nil
		}
	}
	return &abci.ResponsePrepareProposal{}, nil
}
func (a *performanceApp) ProcessProposal(_ context.Context, r *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := time.Now()
	defer func() { a.processNS += time.Since(start).Nanoseconds() }()
	a.processCalls++
	if len(r.Txs) > 1 {
		return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
	}
	for _, wire := range r.Txs {
		if !a.validate(wire) {
			return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
		}
	}
	return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_ACCEPT}, nil
}
func (a *performanceApp) FinalizeBlock(_ context.Context, r *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := time.Now()
	defer func() { a.finalizeNS += time.Since(start).Nanoseconds() }()
	a.pending = nil
	a.nextHash = a.hash
	a.nextHeight = r.Height
	response := &abci.ResponseFinalizeBlock{AppHash: append([]byte(nil), a.hash[:]...)}
	for _, wire := range r.Txs {
		if !a.validate(wire) {
			return nil, fmt.Errorf("decided invalid record")
		}
		a.pending = append([]byte(nil), wire...)
		input := append(append([]byte(nil), a.hash[:]...), wire...)
		a.nextHash = sha256.Sum256(input)
		response.AppHash = append([]byte(nil), a.nextHash[:]...)
		response.TxResults = append(response.TxResults, &abci.ExecTxResult{})
	}
	return response, nil
}
func (a *performanceApp) Commit(context.Context, *abci.RequestCommit) (*abci.ResponseCommit, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := time.Now()
	defer func() { a.commitNS += time.Since(start).Nanoseconds() }()
	if len(a.pending) > 0 {
		ids, err := a.engine.Commit(a.pending)
		if err != nil {
			return nil, err
		}
		a.blocks++
		a.onExecution(a.id, ids, time.Now())
	}
	a.pending = nil
	a.hash = a.nextHash
	a.height = a.nextHeight
	return &abci.ResponseCommit{}, nil
}

func startPerformanceCluster(apps []*performanceApp, limits benchLimits) (*rpchttp.HTTP, func(), string, error) {
	n := len(apps)
	parent := filepath.Join("..", "comet_nodes")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, nil, "", err
	}
	root, err := os.MkdirTemp(parent, "matched-")
	if err != nil {
		return nil, nil, "", err
	}
	configs := make([]*cfg.Config, n)
	pvs := make([]*privval.FilePV, n)
	keys := make([]*p2p.NodeKey, n)
	peers := make([]string, n)
	rpc := make([]string, n)
	validators := make([]cmt.GenesisValidator, n)
	for i := 0; i < n; i++ {
		c := cfg.DefaultConfig().SetRoot(filepath.Join(root, fmt.Sprint(i)))
		configs[i] = c
		c.Moniker = fmt.Sprintf("matched-%d", i)
		c.DBBackend = "goleveldb"
		c.P2P.AddrBookStrict = false
		c.P2P.AllowDuplicateIP = true
		c.P2P.PexReactor = false
		c.Consensus.CreateEmptyBlocks = false
		c.Consensus.TimeoutCommit = 20 * time.Millisecond
		c.Consensus.TimeoutPropose = 500 * time.Millisecond
		c.Consensus.TimeoutPrevote = 100 * time.Millisecond
		c.Consensus.TimeoutPrecommit = 100 * time.Millisecond
		// Identical, non-binding limits for both systems: a native Themis
		// record can exceed CometBFT's 16 MiB default transaction size.
		c.Mempool.MaxTxBytes = 96 << 20
		c.Mempool.MaxTxsBytes = 256 << 20
		c.RPC.MaxBodyBytes = 256 << 20
		c.P2P.SendRate = 512 << 20
		c.P2P.RecvRate = 512 << 20
		c.RPC.TimeoutBroadcastTxCommit = limits.Broadcast + 10*time.Second
		for _, d := range []string{"config", "data"} {
			if err := os.MkdirAll(filepath.Join(c.RootDir, d), 0700); err != nil {
				return nil, nil, root, err
			}
		}
		p, e := port()
		if e != nil {
			return nil, nil, root, e
		}
		r, e := port()
		if e != nil {
			return nil, nil, root, e
		}
		c.P2P.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%d", p)
		c.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%d", r)
		rpc[i] = fmt.Sprintf("http://127.0.0.1:%d", r)
		pvs[i] = privval.LoadOrGenFilePV(c.PrivValidatorKeyFile(), c.PrivValidatorStateFile())
		keys[i], e = p2p.LoadOrGenNodeKey(c.NodeKeyFile())
		if e != nil {
			return nil, nil, root, e
		}
		public, e := pvs[i].GetPubKey()
		if e != nil {
			return nil, nil, root, e
		}
		validators[i] = cmt.GenesisValidator{Address: public.Address(), PubKey: public, Power: 1}
		peers[i] = fmt.Sprintf("%s@127.0.0.1:%d", keys[i].ID(), p)
	}
	params := cmt.DefaultConsensusParams()
	params.Block.MaxBytes = 100 << 20
	doc := &cmt.GenesisDoc{GenesisTime: time.Now().UTC(), ChainID: "matched-autig-themis", ConsensusParams: params, Validators: validators, AppHash: make([]byte, 32)}
	nodes := make([]*cmtnode.Node, n)
	stop := func() {
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
	}
	for i, c := range configs {
		for j, peer := range peers {
			if i != j {
				if c.P2P.PersistentPeers != "" {
					c.P2P.PersistentPeers += ","
				}
				c.P2P.PersistentPeers += peer
			}
		}
		if err = doc.SaveAs(c.GenesisFile()); err != nil {
			stop()
			return nil, nil, root, err
		}
		nodes[i], err = cmtnode.NewNode(c, pvs[i], keys[i], proxy.NewLocalClientCreator(apps[i]), cmtnode.DefaultGenesisDocProviderFunc(c), cfg.DefaultDBProvider, cmtnode.DefaultMetricsProvider(c.Instrumentation), cmtlog.NewNopLogger())
		if err != nil {
			stop()
			return nil, nil, root, err
		}
		if err = nodes[i].Start(); err != nil {
			stop()
			return nil, nil, root, err
		}
	}
	client, err := rpchttp.New(rpc[0], "/websocket")
	if err != nil {
		stop()
		return nil, nil, root, err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		status, e := client.Status(ctx)
		cancel()
		ready := e == nil && status.SyncInfo.LatestBlockHeight >= 1
		for _, node := range nodes {
			if node.Switch().Peers().Size() != n-1 || node.BlockStore().Height() < 1 {
				ready = false
			}
		}
		if ready {
			return client, stop, root, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	return nil, nil, root, fmt.Errorf("local consensus startup timeout")
}
