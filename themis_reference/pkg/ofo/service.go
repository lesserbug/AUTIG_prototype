// // pkg/ofo/service.go
// package ofo

// import (
// 	"themisreference/pkg/network"
// 	"themisreference/pkg/types"
// 	"context"
// 	"sort"
// 	"sync"
// 	"time"
// )

// const (
// 	replicaOrdersChanSize = 512
// 	batchReadyChanSize    = 256
// 	LEADER_REPLICA_ID     = 0
// 	TX_POOL_CAPACITY      = 50000
// )

// type OFOService struct {
// 	ReplicaID   uint64
// 	isLeader    bool
// 	isMalicious bool
// 	network     network.NetworkInterface

// 	// --- State with fine-grained locks ---
// 	txPoolMu          sync.RWMutex
// 	txPool            map[[32]byte]time.Time
// 	txSubmissionTimes map[[32]byte]time.Time

// 	committedMu    sync.RWMutex
// 	committedTxIDs map[[32]byte]bool

// 	deferredMu        sync.RWMutex
// 	deferredProposals map[uint64]*types.LeaderProposal

// 	currentBlockHeight uint64

// 	latencyMu                sync.Mutex
// 	totalLatency             time.Duration
// 	finalizedCountForLatency int64

// 	// --- Leader-specific pipeline ---
// 	pipelineCtx           context.Context
// 	pipelineCancel        context.CancelFunc
// 	pipelineWg            sync.WaitGroup
// 	replicaOrdersChan     chan *types.ReplicaOrders
// 	batchReadyChan        chan []*types.ReplicaOrders
// 	leaderCompletionChan  chan [][32]byte
// 	lastProposedOrderSize int

// 	// --- Config ---
// 	replicaCount uint64
// 	fFaulty      uint64
// 	gamma        float64
// 	loMaxSize    int
// 	loInterval   time.Duration
// }

// func NewOFOService(id, n, f uint64, gamma float64, net network.NetworkInterface, loSize int, loIntervalMs int, malicious bool) *OFOService {
// 	s := &OFOService{
// 		ReplicaID:         id,
// 		isLeader:          id == LEADER_REPLICA_ID,
// 		isMalicious:       malicious,
// 		network:           net,
// 		replicaCount:      n,
// 		fFaulty:           f,
// 		gamma:             gamma,
// 		loMaxSize:         loSize,
// 		loInterval:        time.Duration(loIntervalMs) * time.Millisecond,
// 		txPool:            make(map[[32]byte]time.Time),
// 		txSubmissionTimes: make(map[[32]byte]time.Time),
// 		committedTxIDs:    make(map[[32]byte]bool),
// 		deferredProposals: make(map[uint64]*types.LeaderProposal),
// 	}
// 	if s.isLeader {
// 		s.pipelineCtx, s.pipelineCancel = context.WithCancel(context.Background())
// 		s.replicaOrdersChan = make(chan *types.ReplicaOrders, replicaOrdersChanSize)
// 		s.batchReadyChan = make(chan []*types.ReplicaOrders, batchReadyChanSize)
// 		s.leaderCompletionChan = make(chan [][32]byte, 100)
// 		s.pipelineWg.Add(2)
// 		go s.runCollectorStage()
// 		go s.runProposerStage()
// 	}
// 	net.Register(id, s.handleMessage)
// 	return s
// }

// func (s *OFOService) Network() network.NetworkInterface { return s.network }

// func (s *OFOService) Start(ctx context.Context) {
// 	ticker := time.NewTicker(s.loInterval)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case <-ticker.C:
// 			s.generateAndSendOrders()
// 		case <-ctx.Done():
// 			return
// 		}
// 	}
// }

// func (s *OFOService) Stop() {
// 	if s.isLeader {
// 		s.pipelineCancel()
// 		s.pipelineWg.Wait()
// 		close(s.leaderCompletionChan)
// 	}
// }

// func (s *OFOService) handleMessage(msg network.Message) {
// 	switch payload := msg.Payload.(type) {
// 	case *types.Transaction:
// 		s.committedMu.RLock()
// 		isCommitted := s.committedTxIDs[payload.ID]
// 		s.committedMu.RUnlock()
// 		if !isCommitted {
// 			s.txPoolMu.Lock()
// 			if _, exists := s.txPool[payload.ID]; !exists && len(s.txPool) < TX_POOL_CAPACITY {
// 				s.txPool[payload.ID] = time.Now()
// 				s.txSubmissionTimes[payload.ID] = payload.SubmissionTime
// 			}
// 			s.txPoolMu.Unlock()
// 		}
// 	case *types.ReplicaOrders:
// 		if s.isLeader && s.replicaOrdersChan != nil {
// 			select {
// 			case s.replicaOrdersChan <- payload:
// 			case <-s.pipelineCtx.Done():
// 			}
// 		}
// 	case *types.LeaderProposal:
// 		go s.handleProposal(payload)
// 	}
// }

// func (s *OFOService) generateAndSendOrders() {
// 	// Part 1: New transactions
// 	s.txPoolMu.RLock()
// 	var newTxOrder *types.LocalOrder
// 	if len(s.txPool) > 0 {
// 		poolTxs := make([]types.PoolTx, 0, len(s.txPool))
// 		for id, t := range s.txPool {
// 			poolTxs = append(poolTxs, types.PoolTx{ID: id, Time: t})
// 		}
// 		s.txPoolMu.RUnlock()

// 		sort.Slice(poolTxs, func(i, j int) bool { return poolTxs[i].Time.Before(poolTxs[j].Time) })
// 		batchSize := len(poolTxs)
// 		if s.loMaxSize > 0 && batchSize > s.loMaxSize {
// 			batchSize = s.loMaxSize
// 		}
// 		batchTxHashes := make([][32]byte, batchSize)
// 		for i := 0; i < batchSize; i++ {
// 			batchTxHashes[i] = poolTxs[i].ID
// 		}
// 		newTxOrder = &types.LocalOrder{ReplicaID: s.ReplicaID, OrderedTxs: batchTxHashes}
// 	} else {
// 		s.txPoolMu.RUnlock()
// 	}

// 	// Part 2: Deferred transactions (UpdateOrder)
// 	s.deferredMu.RLock()
// 	var deferredOrders []*types.UpdateOrder
// 	if len(s.deferredProposals) > 0 {
// 		s.txPoolMu.RLock()
// 		for height, proposal := range s.deferredProposals {
// 			txsInProposal := make([]types.PoolTx, 0, len(proposal.Graph.Nodes))
// 			for txID := range proposal.Graph.Nodes {
// 				if receiveTime, ok := s.txPool[txID]; ok {
// 					txsInProposal = append(txsInProposal, types.PoolTx{ID: txID, Time: receiveTime})
// 				}
// 			}
// 			sort.Slice(txsInProposal, func(i, j int) bool {
// 				return txsInProposal[i].Time.Before(txsInProposal[j].Time)
// 			})

// 			orderedTxHashes := make([][32]byte, len(txsInProposal))
// 			for i, ptx := range txsInProposal {
// 				orderedTxHashes[i] = ptx.ID
// 			}

// 			if len(orderedTxHashes) > 0 {
// 				deferredOrders = append(deferredOrders, &types.UpdateOrder{
// 					BlockHeight: height, OrderedTxs: orderedTxHashes,
// 				})
// 			}
// 		}
// 		s.txPoolMu.RUnlock()
// 	}
// 	s.deferredMu.RUnlock()

// 	// Part 3: Send the message
// 	orders := &types.ReplicaOrders{
// 		ReplicaID:   s.ReplicaID,
// 		NewTxOrder:  newTxOrder,
// 		DeferredTxs: deferredOrders,
// 	}

// 	msg := network.Message{From: s.ReplicaID, Payload: orders}
// 	if s.isLeader {
// 		s.handleMessage(msg)
// 	} else {
// 		msg.Type = "ReplicaOrders"
// 		msg.To = LEADER_REPLICA_ID
// 		s.network.Send(msg)
// 	}
// }

// func (s *OFOService) runCollectorStage() {
// 	defer s.pipelineWg.Done()
// 	if s.batchReadyChan != nil {
// 		defer close(s.batchReadyChan)
// 	}

// 	requiredOrders := s.replicaCount - s.fFaulty
// 	var pendingOrders []*types.ReplicaOrders
// 	receivedFrom := make(map[uint64]bool)

// 	timer := time.NewTimer(s.loInterval * 2)
// 	if !timer.Stop() {
// 		<-timer.C
// 	}

// 	sendBatch := func() {
// 		if len(pendingOrders) > 0 {
// 			batch := make([]*types.ReplicaOrders, len(pendingOrders))
// 			copy(batch, pendingOrders)
// 			select {
// 			case s.batchReadyChan <- batch:
// 			case <-s.pipelineCtx.Done():
// 			}
// 		}
// 		pendingOrders = nil
// 		receivedFrom = make(map[uint64]bool)
// 	}

// 	for {
// 		select {
// 		case order, ok := <-s.replicaOrdersChan:
// 			if !ok {
// 				sendBatch()
// 				return
// 			}
// 			if len(pendingOrders) == 0 {
// 				timer.Reset(s.loInterval * 2)
// 			}
// 			if !receivedFrom[order.ReplicaID] {
// 				pendingOrders = append(pendingOrders, order)
// 				receivedFrom[order.ReplicaID] = true
// 			}
// 			if uint64(len(pendingOrders)) >= requiredOrders {
// 				if !timer.Stop() {
// 					select {
// 					case <-timer.C:
// 					default:
// 					}
// 				}
// 				sendBatch()
// 			}
// 		case <-timer.C:
// 			sendBatch()
// 		case <-s.pipelineCtx.Done():
// 			return
// 		}
// 	}
// }

// func (s *OFOService) runProposerStage() {
// 	defer s.pipelineWg.Done()
// 	for {
// 		select {
// 		case <-s.pipelineCtx.Done():
// 			return
// 		case batchOrders, ok := <-s.batchReadyChan:
// 			if !ok {
// 				return
// 			}
// 			if uint64(len(batchOrders)) < s.replicaCount-s.fFaulty {
// 				continue
// 			}

// 			s.deferredMu.Lock()
// 			s.currentBlockHeight++
// 			currentHeight := s.currentBlockHeight
// 			deferredClone := make(map[uint64]*types.LeaderProposal)
// 			for h, p := range s.deferredProposals {
// 				deferredClone[h] = p
// 			}
// 			s.deferredMu.Unlock()

// 			s.committedMu.RLock()
// 			committedSnapshot := make(map[[32]byte]bool)
// 			for id := range s.committedTxIDs {
// 				committedSnapshot[id] = true
// 			}
// 			s.committedMu.RUnlock()

// 			newTxOrders, updateOrdersByHeight := s.separateOrderTypes(batchOrders)

// 			dm := NewDependencyManager()
// 			graphBuilt := dm.BuildGraphAndClassifyTxs(newTxOrders, s.replicaCount, s.fFaulty, s.gamma, committedSnapshot)

// 			if graphBuilt {
// 				dm.CutShadedTail()
// 			}

// 			updatesForProposal := make(map[uint64]map[[32]byte][][32]byte)
// 			for height, uos := range updateOrdersByHeight {
// 				if deferredProp, exists := deferredClone[height]; exists {
// 					newEdges := FairUpdate(uos, deferredProp.Graph, s.replicaCount, s.fFaulty, s.gamma)
// 					if len(newEdges) > 0 {
// 						updatesForProposal[height] = newEdges
// 					}
// 				}
// 			}

// 			s.lastProposedOrderSize = len(dm.graph.Nodes)
// 			proposal := &types.LeaderProposal{
// 				BlockHeight:    currentHeight,
// 				Graph:          dm.graph,
// 				TxStates:       dm.txStates,
// 				Updates:        updatesForProposal,
// 				Proof:          &types.LocalOrderFragment{ReplicaOrders: batchOrders},
// 				CommittedSoFar: committedSnapshot,
// 			}

// 			for i := uint64(0); i < s.replicaCount; i++ {
// 				msg := network.Message{From: s.ReplicaID, Payload: proposal}
// 				if i == s.ReplicaID {
// 					s.handleMessage(msg)
// 				} else {
// 					msg.To = i
// 					msg.Type = "LeaderProposal"
// 					s.network.Send(msg)
// 				}
// 			}
// 		}
// 	}
// }

// func (s *OFOService) separateOrderTypes(batchOrders []*types.ReplicaOrders) ([]*types.LocalOrder, map[uint64][]*types.UpdateOrder) {
// 	newTxOrders := make([]*types.LocalOrder, 0, len(batchOrders))
// 	updateOrdersByHeight := make(map[uint64][]*types.UpdateOrder)
// 	for _, ro := range batchOrders {
// 		if ro.NewTxOrder != nil && len(ro.NewTxOrder.OrderedTxs) > 0 {
// 			newTxOrders = append(newTxOrders, ro.NewTxOrder)
// 		}
// 		for _, uo := range ro.DeferredTxs {
// 			updateOrdersByHeight[uo.BlockHeight] = append(updateOrdersByHeight[uo.BlockHeight], uo)
// 		}
// 	}
// 	return newTxOrders, updateOrdersByHeight
// }

// func (s *OFOService) handleProposal(proposal *types.LeaderProposal) {
// 	if proposal == nil || proposal.Proof == nil || uint64(len(proposal.Proof.ReplicaOrders)) < s.replicaCount-s.fFaulty {
// 		return
// 	}

// 	s.deferredMu.Lock()
// 	if proposal.Updates != nil {
// 		for height, newEdges := range proposal.Updates {
// 			if deferredP, ok := s.deferredProposals[height]; ok {
// 				for u, vs := range newEdges {
// 					for _, v := range vs {
// 						addEdge(deferredP.Graph, u, v)
// 					}
// 				}
// 			}
// 		}
// 	}
// 	if proposal.Graph != nil && len(proposal.Graph.Nodes) > 0 {
// 		if _, exists := s.deferredProposals[proposal.BlockHeight]; !exists {
// 			s.deferredProposals[proposal.BlockHeight] = proposal
// 		}
// 	}

// 	// *** KEY FIX: Create a race-safe snapshot with deep-copied graphs ***
// 	deferredProposalsSnapshot := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals))
// 	for h, p := range s.deferredProposals {
// 		if p == nil || p.Graph == nil {
// 			continue
// 		}
// 		// Create a lightweight deep copy of the graph
// 		ng := &types.DependencyGraph{
// 			Nodes: make(map[[32]byte]bool, len(p.Graph.Nodes)),
// 			Edges: make(map[[32]byte][][32]byte, len(p.Graph.Edges)),
// 		}
// 		for u := range p.Graph.Nodes {
// 			ng.Nodes[u] = true
// 		}
// 		for u, nbrs := range p.Graph.Edges {
// 			// Copy the slice to avoid sharing the underlying array
// 			cp := make([][32]byte, len(nbrs))
// 			copy(cp, nbrs)
// 			ng.Edges[u] = cp
// 		}

// 		// Create a shallow copy of the proposal struct, but replace the graph pointer
// 		clone := *p
// 		clone.Graph = ng
// 		deferredProposalsSnapshot[h] = &clone
// 	}
// 	s.deferredMu.Unlock()
// 	// *** END OF KEY FIX ***

// 	var allFinalizedTxs [][32]byte
// 	var heightsToFinalize []uint64

// 	sortedHeights := make([]uint64, 0, len(deferredProposalsSnapshot))
// 	for h := range deferredProposalsSnapshot {
// 		sortedHeights = append(sortedHeights, h)
// 	}
// 	sort.Slice(sortedHeights, func(i, j int) bool { return sortedHeights[i] < sortedHeights[j] })

// 	for _, height := range sortedHeights {
// 		deferredP := deferredProposalsSnapshot[height]
// 		dm := NewDependencyManager()
// 		dm.graph = deferredP.Graph
// 		dm.txStates = deferredP.TxStates

// 		if orderedSegment := dm.ComputeFairOrder(); len(orderedSegment) > 0 {
// 			allFinalizedTxs = append(allFinalizedTxs, orderedSegment...)
// 			heightsToFinalize = append(heightsToFinalize, height)
// 		}
// 	}

// 	if len(allFinalizedTxs) > 0 {
// 		s.commitFinalizedData(allFinalizedTxs, heightsToFinalize)
// 	}
// }

// func (s *OFOService) commitFinalizedData(finalizedTxs [][32]byte, heightsToFinalize []uint64) {
// 	if len(finalizedTxs) == 0 {
// 		return
// 	}
// 	finalizeTime := time.Now()

// 	s.deferredMu.Lock()
// 	for _, height := range heightsToFinalize {
// 		delete(s.deferredProposals, height)
// 	}
// 	s.deferredMu.Unlock()

// 	newlyFinalized := [][32]byte{}
// 	s.committedMu.Lock()
// 	for _, txID := range finalizedTxs {
// 		if !s.committedTxIDs[txID] {
// 			s.committedTxIDs[txID] = true
// 			newlyFinalized = append(newlyFinalized, txID)
// 		}
// 	}
// 	s.committedMu.Unlock()

// 	if len(newlyFinalized) == 0 {
// 		return
// 	}

// 	s.txPoolMu.Lock()
// 	s.latencyMu.Lock()
// 	for _, txID := range newlyFinalized {
// 		delete(s.txPool, txID)
// 		if submitTime, found := s.txSubmissionTimes[txID]; found {
// 			s.totalLatency += finalizeTime.Sub(submitTime)
// 			s.finalizedCountForLatency++
// 			delete(s.txSubmissionTimes, txID)
// 		}
// 	}
// 	s.latencyMu.Unlock()
// 	s.txPoolMu.Unlock()

// 	if s.isLeader && s.leaderCompletionChan != nil && len(newlyFinalized) > 0 {
// 		select {
// 		case s.leaderCompletionChan <- newlyFinalized:
// 		default:
// 		}
// 	}
// }

// // --- Getters with correct locking ---
// func (s *OFOService) GetLeaderCompletionChan() <-chan [][32]byte { return s.leaderCompletionChan }
// func (s *OFOService) GetFinalizedCount() int {
// 	s.committedMu.RLock()
// 	defer s.committedMu.RUnlock()
// 	return len(s.committedTxIDs)
// }
// func (s *OFOService) GetTxPoolSize() int {
// 	s.txPoolMu.RLock()
// 	defer s.txPoolMu.RUnlock()
// 	return len(s.txPool)
// }
// func (s *OFOService) GetLastProposedOrderSize() int { return s.lastProposedOrderSize }
// func (s *OFOService) GetLatencyStats() (avgLatency time.Duration, count int64) {
// 	s.latencyMu.Lock()
// 	defer s.latencyMu.Unlock()
// 	if s.finalizedCountForLatency == 0 {
// 		return 0, 0
// 	}
// 	return s.totalLatency / time.Duration(s.finalizedCountForLatency), s.finalizedCountForLatency
// }

package ofo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"sync"
	"themisreference/pkg/network"
	"themisreference/pkg/types"
	"time"
)

const (
	replicaOrdersChanSize = 512
	batchReadyChanSize    = 256
	LEADER_REPLICA_ID     = 0
)

type OFOService struct {
	reportObserver           func(*types.ReplicaOrders)
	ReplicaID                uint64
	isLeader                 bool
	isMalicious              bool
	network                  network.NetworkInterface
	txPoolMu                 sync.RWMutex
	txPool                   map[[32]byte]time.Time
	txSubmissionTimes        map[[32]byte]time.Time
	committedMu              sync.RWMutex
	committedTxIDs           map[[32]byte]bool
	finalizedOrder           [][32]byte
	deferredMu               sync.RWMutex
	deferredProposals        map[uint64]*types.LeaderProposal
	currentBlockHeight       uint64
	localOrderSequence       uint64
	committedOrderSequences  map[uint64]uint64
	latencyMu                sync.Mutex
	totalLatency             time.Duration
	finalizedCountForLatency int64
	measuredFinalized        int
	MeasurementDeadline      time.Time
	ByzantineLODelay         time.Duration // Set before Start; only delays malicious report generation.
	lastCommittedDigest      [32]byte
	pipelineCtx              context.Context
	pipelineCancel           context.CancelFunc
	pipelineWg               sync.WaitGroup
	replicaOrdersChan        chan *types.ReplicaOrders
	batchReadyChan           chan []*types.ReplicaOrders
	leaderCompletionChan     chan [][32]byte
	lastProposedOrderSize    int
	replicaCount             uint64
	fFaulty                  uint64
	gamma                    float64
	loInterval               time.Duration
	auth                     types.OrderAuthenticator
	proposalSink             func(*types.LeaderProposal) bool
	benchmarkCounters        benchmarkCounters
	benchmarkFailureOnce     sync.Once
}

func NewOFOService(id, n, f uint64, gamma float64, net network.NetworkInterface, loSize int, loIntervalMs int, malicious bool, auth types.OrderAuthenticator) (*OFOService, error) {
	if err := ValidateThemisParameters(n, f, gamma); err != nil {
		return nil, err
	}
	if auth == nil {
		return nil, fmt.Errorf("Themis requires an order authenticator")
	}
	if loIntervalMs <= 0 {
		return nil, fmt.Errorf("Themis reporting interval must be positive")
	}
	s := &OFOService{
		ReplicaID:               id,
		isLeader:                id == LEADER_REPLICA_ID,
		isMalicious:             malicious,
		network:                 net,
		replicaCount:            n,
		fFaulty:                 f,
		gamma:                   gamma,
		loInterval:              time.Duration(loIntervalMs) * time.Millisecond,
		txPool:                  make(map[[32]byte]time.Time),
		txSubmissionTimes:       make(map[[32]byte]time.Time),
		committedTxIDs:          make(map[[32]byte]bool),
		deferredProposals:       make(map[uint64]*types.LeaderProposal),
		committedOrderSequences: make(map[uint64]uint64),
		auth:                    auth,
	}
	if s.isLeader {
		s.pipelineCtx, s.pipelineCancel = context.WithCancel(context.Background())
		s.replicaOrdersChan = make(chan *types.ReplicaOrders, replicaOrdersChanSize)
		s.batchReadyChan = make(chan []*types.ReplicaOrders, batchReadyChanSize)
		s.leaderCompletionChan = make(chan [][32]byte, 100)
		s.pipelineWg.Add(2)
		go s.runCollectorStage()
		go s.runProposerStage()
	}
	net.Register(id, s.handleMessage)
	return s, nil
}

// ... Network, Start, Stop, handleMessage methods are unchanged from your last version ...
// (We will only show modified functions below for clarity)
func (s *OFOService) Network() network.NetworkInterface {
	return s.network
}

func (s *OFOService) SetProposalSink(sink func(*types.LeaderProposal) bool) {
	s.proposalSink = sink
}

func (s *OFOService) HandleMessage(msg network.Message) {
	s.handleMessage(msg)
}

func (s *OFOService) Start(ctx context.Context) {
	if s.isLeader {
		// Cancel even when local evidence generation is blocked on a full queue.
		stopCancel := context.AfterFunc(ctx, s.pipelineCancel)
		defer stopCancel()
	}
	ticker := time.NewTicker(s.loInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			if s.isMalicious && s.ByzantineLODelay > 0 {
				timer := time.NewTimer(s.ByzantineLODelay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
				if ctx.Err() != nil {
					return
				}
			}
			s.generateAndSendOrders()
		case <-ctx.Done():
			return
		}
	}
}

func (s *OFOService) Stop() {
	if s.isLeader {
		s.pipelineCancel()
		s.pipelineWg.Wait()
		if s.leaderCompletionChan != nil {
			close(s.leaderCompletionChan)
		}
	}
}

func (s *OFOService) handleMessage(msg network.Message) {
	switch payload := msg.Payload.(type) {
	case *types.Transaction:
		if payload.ID != types.TransactionID(payload.CanonicalBytes) {
			return
		}
		s.committedMu.RLock()
		isCommitted := s.committedTxIDs[payload.ID]
		if !isCommitted {
			s.txPoolMu.Lock()
			if _, exists := s.txPool[payload.ID]; !exists {
				s.txPool[payload.ID] = time.Now()
				s.txSubmissionTimes[payload.ID] = payload.SubmissionTime
			}
			s.txPoolMu.Unlock()
		}
		s.committedMu.RUnlock()
	case *types.ReplicaOrders:
		if s.isLeader && s.replicaOrdersChan != nil && msg.From == payload.ReplicaID {
			if s.pipelineCtx.Err() != nil {
				return
			}
			select {
			case s.replicaOrdersChan <- payload:
			case <-s.pipelineCtx.Done():
			default:
				// A blocking enqueue here can prevent the single network worker
				// from delivering the verification ACK that the proposer awaits.
				// Fail this measurement rather than silently discard evidence in
				// a valid run or introduce a new protocol scheduling policy.
				s.benchmarkFailureOnce.Do(func() {
					log.Printf("BENCHMARK INVALID: evidence queue exhausted at replica=%d evidence_queue=%d batch_queue=%d", s.ReplicaID, len(s.replicaOrdersChan), len(s.batchReadyChan))
					s.pipelineCancel()
				})
			}
		}
	case *types.LeaderProposal:
		log.Printf("Replica %d ignored a proposal outside the benchmark hosting adapter", s.ReplicaID)
	}
}

func (s *OFOService) generateAndSendOrders() {
	s.deferredMu.RLock()
	deferred := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals))
	previouslyProposed := make(map[[32]byte]bool)
	for height, proposal := range s.deferredProposals {
		deferred[height] = proposal
		if proposal != nil && proposal.Graph != nil {
			for id := range proposal.Graph.Nodes {
				previouslyProposed[id] = true
			}
		}
	}
	s.deferredMu.RUnlock()

	s.txPoolMu.RLock()
	var batchTxHashes [][32]byte
	if len(s.txPool) > 0 {
		poolTxs := make([]types.PoolTx, 0, len(s.txPool))
		for id, t := range s.txPool {
			if !previouslyProposed[id] {
				poolTxs = append(poolTxs, types.PoolTx{ID: id, Time: t})
			}
		}
		s.txPoolMu.RUnlock()

		sort.Slice(poolTxs, func(i, j int) bool {
			if poolTxs[i].Time.Equal(poolTxs[j].Time) {
				return bytes.Compare(poolTxs[i].ID[:], poolTxs[j].ID[:]) < 0
			}
			return poolTxs[i].Time.Before(poolTxs[j].Time)
		})

		if s.isMalicious {
			for left, right := 0, len(poolTxs)-1; left < right; left, right = left+1, right-1 {
				poolTxs[left], poolTxs[right] = poolTxs[right], poolTxs[left]
			}
		}

		batchSize := len(poolTxs)
		// Paper IV: List_i contains all unproposed receipts; truncation can stall progress.
		batchTxHashes = make([][32]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			batchTxHashes[i] = poolTxs[i].ID
		}
	} else {
		s.txPoolMu.RUnlock()
	}

	var update *types.UpdateOrder
	if len(deferred) > 0 {
		s.txPoolMu.RLock()
		deferredTxs := make([]types.PoolTx, 0, len(previouslyProposed))
		for txID := range previouslyProposed {
			if receiveTime, ok := s.txPool[txID]; ok {
				deferredTxs = append(deferredTxs, types.PoolTx{ID: txID, Time: receiveTime})
			}
		}
		s.txPoolMu.RUnlock()
		sort.Slice(deferredTxs, func(i, j int) bool {
			if deferredTxs[i].Time.Equal(deferredTxs[j].Time) {
				return bytes.Compare(deferredTxs[i].ID[:], deferredTxs[j].ID[:]) < 0
			}
			return deferredTxs[i].Time.Before(deferredTxs[j].Time)
		})
		orderedTxHashes := make([][32]byte, len(deferredTxs))
		for i, ptx := range deferredTxs {
			orderedTxHashes[i] = ptx.ID
		}
		if s.isMalicious {
			for left, right := 0, len(orderedTxHashes)-1; left < right; left, right = left+1, right-1 {
				orderedTxHashes[left], orderedTxHashes[right] = orderedTxHashes[right], orderedTxHashes[left]
			}
		}
		update = &types.UpdateOrder{OrderedTxs: orderedTxHashes}
	}

	s.benchmarkCounters.listCount.Add(1)
	s.benchmarkCounters.listIDs.Add(uint64(len(batchTxHashes)))
	benchmarkMax(&s.benchmarkCounters.listMax, uint64(len(batchTxHashes)))
	s.localOrderSequence++
	sequence := s.localOrderSequence
	newTxOrder := &types.LocalOrder{ReplicaID: s.ReplicaID, Sequence: sequence, OrderedTxs: batchTxHashes}
	signature, err := s.auth.SignReplica(s.ReplicaID, types.LocalOrderDigest(newTxOrder))
	if err != nil {
		log.Printf("Replica %d could not sign LocalOrder: %v", s.ReplicaID, err)
		return
	}
	newTxOrder.Signature = signature
	if update != nil {
		update.ReplicaID = s.ReplicaID
		update.Sequence = sequence
		signature, err = s.auth.SignReplica(s.ReplicaID, types.UpdateOrderDigest(update))
		if err != nil {
			log.Printf("Replica %d could not sign UpdateOrder: %v", s.ReplicaID, err)
			return
		}
		update.Signature = signature
	}
	orders := &types.ReplicaOrders{
		ReplicaID:  s.ReplicaID,
		Sequence:   sequence,
		NewTxOrder: newTxOrder,
		Update:     update,
	}
	if s.reportObserver != nil {
		s.reportObserver(orders)
		return
	}
	msg := network.Message{From: s.ReplicaID, Payload: orders}
	if s.isLeader {
		s.handleMessage(msg)
	} else {
		msg.Type = "ReplicaOrders"
		msg.To = LEADER_REPLICA_ID
		if !s.network.Send(msg) {
			log.Printf("BENCHMARK LOCAL SEND FAILURE: LocalOrder from replica %d", s.ReplicaID)
		}
	}
}

func (s *OFOService) validateReplicaOrders(ro *types.ReplicaOrders) bool {
	if ro == nil || ro.ReplicaID >= s.replicaCount || ro.NewTxOrder == nil {
		return false
	}
	if ro.Sequence == 0 || ro.NewTxOrder.ReplicaID != ro.ReplicaID || ro.NewTxOrder.Sequence != ro.Sequence {
		return false
	}
	s.committedMu.RLock()
	lastSequence := s.committedOrderSequences[ro.ReplicaID]
	s.committedMu.RUnlock()
	if ro.Sequence <= lastSequence || !s.auth.VerifyReplica(ro.ReplicaID, types.LocalOrderDigest(ro.NewTxOrder), ro.NewTxOrder.Signature) {
		return false
	}

	s.deferredMu.RLock()
	deferred := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals))
	previouslyProposed := make(map[[32]byte]bool)
	for height, proposal := range s.deferredProposals {
		deferred[height] = proposal
		for id := range proposal.Graph.Nodes {
			previouslyProposed[id] = true
		}
	}
	s.deferredMu.RUnlock()

	// Validate signed evidence independently of the local receipt cache.
	seen := make(map[[32]byte]bool)
	for _, id := range ro.NewTxOrder.OrderedTxs {
		if seen[id] || previouslyProposed[id] {
			return false
		}
		seen[id] = true
	}

	if len(deferred) == 0 {
		return ro.Update == nil
	}
	if ro.Update == nil || ro.Update.ReplicaID != ro.ReplicaID || ro.Update.Sequence != ro.Sequence ||
		!s.auth.VerifyReplica(ro.ReplicaID, types.UpdateOrderDigest(ro.Update), ro.Update.Signature) {
		return false
	}
	seenUpdateTxs := make(map[[32]byte]bool)
	for _, id := range ro.Update.OrderedTxs {
		if seenUpdateTxs[id] || !previouslyProposed[id] {
			return false
		}
		seenUpdateTxs[id] = true
	}
	return true
}

func (s *OFOService) runCollectorStage() {
	defer s.pipelineWg.Done()
	defer close(s.batchReadyChan)
	requiredOrders := s.replicaCount - s.fFaulty
	pending := make(map[uint64]*types.ReplicaOrders)
	for {
		select {
		case order := <-s.replicaOrdersChan:
			if s.pipelineCtx.Err() != nil {
				return
			}
			if !s.validateReplicaOrders(order) {
				s.benchmarkCounters.collectorRejected.Add(1)
				continue
			}
			if previous := pending[order.ReplicaID]; previous == nil || order.Sequence > previous.Sequence {
				pending[order.ReplicaID] = order
			}
			if uint64(len(pending)) == requiredOrders {
				batch := make([]*types.ReplicaOrders, 0, len(pending))
				for _, orders := range pending {
					batch = append(batch, orders)
				}
				sort.Slice(batch, func(i, j int) bool { return batch[i].ReplicaID < batch[j].ReplicaID })
				select {
				case s.batchReadyChan <- batch:
					s.benchmarkCounters.batchesQueued.Add(1)
				case <-s.pipelineCtx.Done():
					return
				}
				pending = make(map[uint64]*types.ReplicaOrders)
			}
		case <-s.pipelineCtx.Done():
			return
		}
	}
}

func (s *OFOService) runProposerStage() {
	defer s.pipelineWg.Done()
	for {
		select {
		case <-s.pipelineCtx.Done():
			return
		case batchOrders, ok := <-s.batchReadyChan:
			if !ok || s.pipelineCtx.Err() != nil {
				return
			}

			if uint64(len(batchOrders)) < s.replicaCount-s.fFaulty {
				continue
			}

			started := time.Now()
			proposal := s.buildProposal(batchOrders)
			s.benchmarkCounters.buildCount.Add(1)
			elapsed := uint64(time.Since(started))
			s.benchmarkCounters.buildNS.Add(elapsed)
			benchmarkMax(&s.benchmarkCounters.buildMaxNS, elapsed)
			if proposal == nil {
				// Includes empty/no-progress inputs, invalidated queued evidence,
				// and cancellation. It is not a count of correctness failures.
				s.benchmarkCounters.buildNil.Add(1)
			}
			if proposal == nil || s.proposalSink == nil || s.pipelineCtx.Err() != nil {
				continue
			}
			started = time.Now()
			if s.proposalSink(proposal) {
				s.benchmarkCounters.proposalsAccepted.Add(1)
				s.lastProposedOrderSize = len(proposal.Graph.Nodes)
			}
			s.benchmarkCounters.hostingCount.Add(1)
			s.benchmarkCounters.hostingNS.Add(uint64(time.Since(started)))
		}
	}
}

func (s *OFOService) buildProposal(batchOrders []*types.ReplicaOrders) *types.LeaderProposal {
	s.deferredMu.RLock()
	height := s.currentBlockHeight + 1
	s.deferredMu.RUnlock()
	return s.recomputeProposal(height, batchOrders)
}

func (s *OFOService) recomputeProposal(height uint64, batchOrders []*types.ReplicaOrders) *types.LeaderProposal {
	ctx := context.Background()
	if s.isLeader {
		ctx = s.pipelineCtx
	}
	if ctx.Err() != nil {
		return nil
	}
	if uint64(len(batchOrders)) != s.replicaCount-s.fFaulty {
		return nil
	}
	seenSenders := make(map[uint64]bool)
	for _, orders := range batchOrders {
		if orders == nil || seenSenders[orders.ReplicaID] || !s.validateReplicaOrders(orders) {
			return nil
		}
		seenSenders[orders.ReplicaID] = true
	}

	s.deferredMu.RLock()
	deferred := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals))
	excluded := make(map[[32]byte]bool)
	for proposalHeight, proposal := range s.deferredProposals {
		if ctx.Err() != nil {
			s.deferredMu.RUnlock()
			return nil
		}
		deferred[proposalHeight] = cloneProposal(proposal)
		for id := range proposal.Graph.Nodes {
			excluded[id] = true
		}
	}
	s.deferredMu.RUnlock()
	s.committedMu.RLock()
	for id := range s.committedTxIDs {
		excluded[id] = true
	}
	s.committedMu.RUnlock()

	newTxOrders, updateOrders := s.separateOrderTypes(batchOrders)
	dm := NewDependencyManager()
	dm.ctx = ctx
	if dm.BuildGraphAndClassifyTxs(newTxOrders, s.replicaCount, s.fFaulty, s.gamma, excluded) {
		dm.CutShadedTail()
	}
	updates := make(map[uint64]map[[32]byte][][32]byte)
	for proposalHeight, deferredProposal := range deferred {
		if ctx.Err() != nil {
			return nil
		}
		if uint64(len(updateOrders)) != s.replicaCount-s.fFaulty {
			return nil
		}
		if newEdges := FairUpdate(ctx, updateOrders, deferredProposal.Graph, s.replicaCount, s.fFaulty, s.gamma); len(newEdges) > 0 {
			updates[proposalHeight] = newEdges
		}
	}
	if len(deferred) == 0 && len(updateOrders) != 0 {
		return nil
	}
	if ctx.Err() != nil || (len(dm.graph.Nodes) == 0 && len(updates) == 0) {
		return nil
	}
	return &types.LeaderProposal{
		BlockHeight: height,
		Graph:       dm.graph,
		Updates:     updates,
		Proof:       &types.LocalOrderFragment{ReplicaOrders: append([]*types.ReplicaOrders(nil), batchOrders...)},
		TxStates:    dm.txStates,
	}
}

func (s *OFOService) VerifyProposal(proposal *types.LeaderProposal) bool {
	ctx := context.Background()
	if s.isLeader {
		ctx = s.pipelineCtx
	}
	if ctx.Err() != nil {
		return false
	}
	if proposal == nil || proposal.Proof == nil || proposal.Graph == nil {
		log.Printf("THEMIS PROPOSAL VERIFICATION FAILED: replica=%d reason=malformed", s.ReplicaID)
		return false
	}
	s.deferredMu.RLock()
	expectedHeight := s.currentBlockHeight + 1
	s.deferredMu.RUnlock()
	if proposal.BlockHeight != expectedHeight {
		log.Printf("THEMIS PROPOSAL VERIFICATION FAILED: replica=%d height=%d expected=%d reason=height", s.ReplicaID, proposal.BlockHeight, expectedHeight)
		return false
	}
	expected := s.recomputeProposal(proposal.BlockHeight, proposal.Proof.ReplicaOrders)
	if ctx.Err() != nil {
		return false
	}
	if expected == nil {
		log.Printf("THEMIS PROPOSAL VERIFICATION FAILED: replica=%d height=%d reason=invalid-signed-inputs", s.ReplicaID, proposal.BlockHeight)
		return false
	}
	if !graphEqual(ctx, proposal.Graph, expected.Graph) {
		if ctx.Err() != nil {
			return false
		}
		log.Printf("THEMIS PROPOSAL VERIFICATION FAILED: replica=%d height=%d reason=graph", s.ReplicaID, proposal.BlockHeight)
		return false
	}
	if !txStatesEqual(proposal.TxStates, expected.TxStates) {
		log.Printf("THEMIS PROPOSAL VERIFICATION FAILED: replica=%d height=%d reason=states", s.ReplicaID, proposal.BlockHeight)
		return false
	}
	if !updatesEqual(ctx, proposal.Updates, expected.Updates) {
		if ctx.Err() != nil {
			return false
		}
		log.Printf("THEMIS PROPOSAL VERIFICATION FAILED: replica=%d height=%d reason=updates", s.ReplicaID, proposal.BlockHeight)
		return false
	}
	return true
}

func (s *OFOService) CommitProposal(proposal *types.LeaderProposal) bool {
	if !s.VerifyProposal(proposal) {
		return false
	}
	return s.commitVerifiedProposal(proposal)
}

// CommitVerifiedProposal is the benchmark composition boundary used after the
// exact proposal has already passed VerifyProposal and the hosting adapter has
// confirmed its digest. It does not model a BFT commit certificate.
func (s *OFOService) CommitVerifiedProposal(proposal *types.LeaderProposal) bool {
	return s.commitVerifiedProposal(proposal)
}

func (s *OFOService) commitVerifiedProposal(proposal *types.LeaderProposal) bool {
	if proposal == nil || proposal.Proof == nil || proposal.Graph == nil {
		return false
	}
	s.deferredMu.Lock()
	if proposal.BlockHeight != s.currentBlockHeight+1 {
		s.deferredMu.Unlock()
		return false
	}
	stagedDeferred := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals)+1)
	for height, deferredProposal := range s.deferredProposals {
		stagedDeferred[height] = cloneProposal(deferredProposal)
	}
	for height, edges := range proposal.Updates {
		deferredProposal := stagedDeferred[height]
		if deferredProposal == nil {
			s.deferredMu.Unlock()
			return false
		}
		for from, targets := range edges {
			for _, to := range targets {
				addEdge(deferredProposal.Graph, from, to)
			}
		}
	}
	if len(proposal.Graph.Nodes) > 0 {
		stagedDeferred[proposal.BlockHeight] = cloneProposal(proposal)
	}

	heights := make([]uint64, 0, len(stagedDeferred))
	for height := range stagedDeferred {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	var finalized [][32]byte
	var finalizedHeights []uint64
	for _, height := range heights {
		deferredProposal := stagedDeferred[height]
		if !IsTournament(deferredProposal.Graph) {
			break
		}
		dm := NewDependencyManager()
		dm.graph = deferredProposal.Graph
		dm.txStates = deferredProposal.TxStates
		segment := dm.ComputeFairOrder()
		if len(segment) != len(deferredProposal.Graph.Nodes) {
			sccs := tarjanSCC(context.Background(), deferredProposal.Graph)
			sizes := make([]int, len(sccs))
			solids := make([]int, len(sccs))
			for i, component := range sccs {
				sizes[i] = len(component)
				for _, txID := range component {
					if deferredProposal.TxStates[txID] == types.StateSolid {
						solids[i]++
					}
				}
			}
			log.Printf("THEMIS ORDER EXTRACTION FAILED: replica=%d proposal=%d deferred=%d nodes=%d scc-sizes=%v scc-solids=%v", s.ReplicaID, proposal.BlockHeight, height, len(deferredProposal.Graph.Nodes), sizes, solids)
			s.deferredMu.Unlock()
			return false
		}
		finalized = append(finalized, segment...)
		finalizedHeights = append(finalizedHeights, height)
	}
	s.deferredProposals = stagedDeferred
	s.currentBlockHeight = proposal.BlockHeight
	s.lastCommittedDigest = types.LeaderProposalDigest(proposal)
	s.deferredMu.Unlock()

	s.committedMu.Lock()
	for _, orders := range proposal.Proof.ReplicaOrders {
		s.committedOrderSequences[orders.ReplicaID] = orders.Sequence
	}
	s.committedMu.Unlock()

	if len(finalized) > 0 {
		s.commitFinalizedData(finalized, finalizedHeights)
	}
	return true
}

func cloneProposal(proposal *types.LeaderProposal) *types.LeaderProposal {
	if proposal == nil {
		return nil
	}
	clone := *proposal
	clone.Graph = cloneGraph(proposal.Graph)
	clone.TxStates = make(map[[32]byte]types.TxState, len(proposal.TxStates))
	for id, state := range proposal.TxStates {
		clone.TxStates[id] = state
	}
	return &clone
}

func cloneGraph(graph *types.DependencyGraph) *types.DependencyGraph {
	if graph == nil {
		return nil
	}
	clone := &types.DependencyGraph{Nodes: make(map[[32]byte]bool, len(graph.Nodes)), Edges: make(map[[32]byte][][32]byte, len(graph.Edges))}
	for node := range graph.Nodes {
		clone.Nodes[node] = true
	}
	for from, targets := range graph.Edges {
		clone.Edges[from] = append([][32]byte(nil), targets...)
	}
	return clone
}

func graphEqual(ctx context.Context, left, right *types.DependencyGraph) bool {
	if left == nil || right == nil || len(left.Nodes) != len(right.Nodes) {
		return left == nil && right == nil
	}
	for node := range left.Nodes {
		if !right.Nodes[node] {
			return false
		}
	}
	return edgeMapsEqual(ctx, left, left.Edges, right.Edges) && edgeMapsEqual(ctx, right, right.Edges, left.Edges)
}

func edgeMapsEqual(ctx context.Context, graph *types.DependencyGraph, left, right map[[32]byte][][32]byte) bool {
	for from, targets := range left {
		if !graph.Nodes[from] {
			return false
		}
		seen := make(map[[32]byte]bool)
		for _, to := range targets {
			if ctx.Err() != nil {
				return false
			}
			if !graph.Nodes[to] || seen[to] || !containsTx(right[from], to) {
				return false
			}
			seen[to] = true
		}
		if len(seen) != len(right[from]) {
			return false
		}
	}
	return true
}

func containsTx(ids [][32]byte, target [32]byte) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func txStatesEqual(left, right map[[32]byte]types.TxState) bool {
	if len(left) != len(right) {
		return false
	}
	for id, state := range left {
		if right[id] != state {
			return false
		}
	}
	return true
}

func updatesEqual(ctx context.Context, left, right map[uint64]map[[32]byte][][32]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for height, leftEdges := range left {
		rightEdges, exists := right[height]
		if !exists || !edgeMapsEqualForUpdates(ctx, leftEdges, rightEdges) || !edgeMapsEqualForUpdates(ctx, rightEdges, leftEdges) {
			return false
		}
	}
	return true
}

func edgeMapsEqualForUpdates(ctx context.Context, left, right map[[32]byte][][32]byte) bool {
	for from, targets := range left {
		seen := make(map[[32]byte]bool)
		for _, to := range targets {
			if ctx.Err() != nil {
				return false
			}
			if seen[to] || !containsTx(right[from], to) {
				return false
			}
			seen[to] = true
		}
		if len(seen) != len(right[from]) {
			return false
		}
	}
	return true
}

// ... commitFinalizedData and all Getter methods are unchanged ...
// They can be copied directly from your last working version.
func (s *OFOService) separateOrderTypes(batchOrders []*types.ReplicaOrders) ([]*types.LocalOrder, []*types.UpdateOrder) {
	newTxOrders := make([]*types.LocalOrder, 0, len(batchOrders))
	updateOrders := make([]*types.UpdateOrder, 0, len(batchOrders))
	for _, ro := range batchOrders {
		if ro == nil {
			continue
		}
		if ro.NewTxOrder != nil {
			newTxOrders = append(newTxOrders, ro.NewTxOrder)
		}
		if ro.Update != nil {
			updateOrders = append(updateOrders, ro.Update)
		}
	}
	return newTxOrders, updateOrders
}

func (s *OFOService) commitFinalizedData(finalizedTxs [][32]byte, heightsToFinalize []uint64) {
	if len(finalizedTxs) == 0 {
		return
	}
	s.deferredMu.Lock()
	for _, height := range heightsToFinalize {
		delete(s.deferredProposals, height)
	}
	s.deferredMu.Unlock()
	newlyFinalized := [][32]byte{}
	s.committedMu.Lock()
	for _, txID := range finalizedTxs {
		if !s.committedTxIDs[txID] {
			s.committedTxIDs[txID] = true
			newlyFinalized = append(newlyFinalized, txID)
			s.finalizedOrder = append(s.finalizedOrder, txID)
		}
	}
	s.committedMu.Unlock()
	if len(newlyFinalized) == 0 {
		return
	}
	s.txPoolMu.Lock()
	s.latencyMu.Lock()
	finalizeTime := time.Now()
	measured := s.MeasurementDeadline.IsZero() || finalizeTime.Before(s.MeasurementDeadline)
	for _, txID := range newlyFinalized {
		if measured {
			s.measuredFinalized++
		}
		delete(s.txPool, txID)
		if submitTime, found := s.txSubmissionTimes[txID]; found {
			if measured {
				s.totalLatency += finalizeTime.Sub(submitTime)
				s.finalizedCountForLatency++
			}
			delete(s.txSubmissionTimes, txID)
		}
	}
	s.latencyMu.Unlock()
	s.txPoolMu.Unlock()
	if s.isLeader && s.leaderCompletionChan != nil && len(newlyFinalized) > 0 {
		select {
		case s.leaderCompletionChan <- newlyFinalized:
		default:
		}
	}
}
func (s *OFOService) GetLeaderCompletionChan() <-chan [][32]byte {
	return s.leaderCompletionChan
}
func (s *OFOService) GetFinalizedCount() int {
	s.committedMu.RLock()
	defer s.committedMu.RUnlock()
	return len(s.committedTxIDs)
}
func (s *OFOService) GetFinalizedOrder() [][32]byte {
	s.committedMu.RLock()
	defer s.committedMu.RUnlock()
	return append([][32]byte(nil), s.finalizedOrder...)
}
func (s *OFOService) GetTxPoolSize() int {
	s.txPoolMu.RLock()
	defer s.txPoolMu.RUnlock()
	return len(s.txPool)
}
func (s *OFOService) GetLastProposedOrderSize() int {
	return s.lastProposedOrderSize
}
func (s *OFOService) GetLatencyStats() (avgLatency time.Duration, count int64) {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()
	if s.finalizedCountForLatency == 0 {
		return 0, 0
	}
	return s.totalLatency / time.Duration(s.finalizedCountForLatency), s.finalizedCountForLatency
}

// GetMeasurementStats reads the cutoff-limited counters atomically.
func (s *OFOService) GetMeasurementStats() (int, time.Duration, int64) {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()
	var average time.Duration
	if s.finalizedCountForLatency > 0 {
		average = s.totalLatency / time.Duration(s.finalizedCountForLatency)
	}
	return s.measuredFinalized, average, s.finalizedCountForLatency
}

// CommittedContext is used only after the benchmark finish barrier.
func (s *OFOService) CommittedContext() (uint64, [32]byte, [32]byte) {
	s.deferredMu.RLock()
	defer s.deferredMu.RUnlock()
	s.committedMu.RLock()
	defer s.committedMu.RUnlock()
	var state bytes.Buffer
	binary.Write(&state, binary.BigEndian, uint64(len(s.finalizedOrder)))
	for _, id := range s.finalizedOrder {
		state.Write(id[:])
	}
	heights := make([]uint64, 0, len(s.deferredProposals))
	for height := range s.deferredProposals {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	for _, height := range heights {
		digest := types.LeaderProposalDigest(s.deferredProposals[height])
		state.Write(digest[:])
	}
	for id := uint64(0); id < s.replicaCount; id++ {
		binary.Write(&state, binary.BigEndian, s.committedOrderSequences[id])
	}
	return s.currentBlockHeight, sha256.Sum256(state.Bytes()), s.lastCommittedDigest
}
