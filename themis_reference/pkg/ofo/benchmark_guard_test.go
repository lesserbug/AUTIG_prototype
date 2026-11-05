package ofo

import (
	"themisreference/pkg/network"
	"themisreference/pkg/types"
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"
)

func TestEvidenceQueueExhaustionReleasesWorkerAndInvalidatesRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &OFOService{
		isLeader: true, pipelineCtx: ctx, pipelineCancel: cancel,
		replicaOrdersChan: make(chan *types.ReplicaOrders, replicaOrdersChanSize),
		batchReadyChan:    make(chan []*types.ReplicaOrders, batchReadyChanSize),
	}
	order := &types.ReplicaOrders{ReplicaID: 1}
	for i := 0; i < cap(s.replicaOrdersChan); i++ {
		s.replicaOrdersChan <- order
	}
	for i := 0; i < cap(s.batchReadyChan); i++ {
		s.batchReadyChan <- []*types.ReplicaOrders{order}
	}
	var output bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(oldWriter)
	// The verification ACK is the next message on a single synchronous worker.
	ackDelivered := make(chan struct{})
	go func() {
		s.HandleMessage(network.Message{From: 1, Payload: order})
		close(ackDelivered)
	}()
	select {
	case <-ackDelivered:
	case <-time.After(time.Second):
		cancel()
		<-ackDelivered
		t.Fatal("evidence blocked the worker from delivering its next ACK")
	}
	if ctx.Err() == nil || !strings.Contains(output.String(), "BENCHMARK INVALID: evidence queue exhausted") {
		t.Fatal("queue exhaustion must invalidate and cancel the pipeline")
	}
	s.HandleMessage(network.Message{From: 1, Payload: order})
	if strings.Count(output.String(), "BENCHMARK INVALID") != 1 {
		t.Fatal("repeated invalidation")
	}
	if len(s.replicaOrdersChan) != replicaOrdersChanSize || len(s.batchReadyChan) != batchReadyChanSize {
		t.Fatal("guard must not rewrite queued evidence")
	}
}

func TestEvidenceGuardRetainsAvailableQueueInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &OFOService{isLeader: true, pipelineCtx: ctx, pipelineCancel: cancel, replicaOrdersChan: make(chan *types.ReplicaOrders, 1)}
	order := &types.ReplicaOrders{ReplicaID: 1, Sequence: 7}
	s.HandleMessage(network.Message{From: 1, Payload: order})
	if ctx.Err() != nil {
		t.Fatal("available queue invalidated")
	}
	if got := <-s.replicaOrdersChan; got != order {
		t.Fatal("evidence changed")
	}
}
