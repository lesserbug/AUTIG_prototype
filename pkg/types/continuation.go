package types

import (
	"bytes"
	"crypto/sha256"
)

// Joins are the execution rounds of this fragment; each executed member is
// Solid and settles when it executes. Assigned names new members that receive
// the open batch index but wait for a certified execution order.
type ContinuationTransition struct {
	BatchIndex uint64
	Opened     bool
	Closed     bool
	Joins      []FairnessBatch
	Assigned   []TxID
	Settled    []TxID
}

type ContinuationCertificate struct {
	Part         []SCCClaim
	MemberForest []BlockRecord
}

func ContinuationPolicyID(n, f, qh, delay uint64) [32]byte {
	var b bytes.Buffer
	b.WriteString("AUTIG/continuation-policy/v3/certified-order")
	for _, v := range []uint64{n, f, qh, delay} {
		writeUint64(&b, v)
	}
	return sha256.Sum256(b.Bytes())
}

func writeBatches(b *bytes.Buffer, batches []FairnessBatch) {
	writeUint64(b, uint64(len(batches)))
	for _, batch := range batches {
		writeUint64(b, batch.Index)
		writeTxIDs(b, batch.Transactions)
	}
}

func writeContinuation(b *bytes.Buffer, c *ContinuationTransition) {
	if c == nil {
		writeUint64(b, ^uint64(0))
		return
	}
	writeUint64(b, c.BatchIndex)
	if c.Opened {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	if c.Closed {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	writeBatches(b, c.Joins)
	writeTxIDs(b, c.Assigned)
	writeTxIDs(b, c.Settled)
}

func writeContinuationCertificate(b *bytes.Buffer, c *ContinuationCertificate) {
	if c == nil {
		writeUint64(b, ^uint64(0))
		return
	}
	writeUint64(b, uint64(len(c.Part)))
	for _, claim := range c.Part {
		writeTxIDs(b, claim.Transactions)
		writeTree(b, claim.InTree)
		writeTree(b, claim.OutTree)
		writeUint64(b, claim.Rank)
	}
	writeUint64(b, uint64(len(c.MemberForest)))
	for _, r := range c.MemberForest {
		b.Write(r.Vertex[:])
		b.Write(r.Parent[:])
		if r.HasParent {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
		writeUint64(b, r.Depth)
	}
}

func ContinuationStateIdentifier(epoch, seq uint64, pre, policy [32]byte, closed *FairOrderFragment, c *ContinuationTransition, doneRoot [32]byte) [32]byte {
	var b bytes.Buffer
	b.WriteString("AUTIG/poststate/v3")
	writeUint64(&b, epoch)
	writeUint64(&b, seq)
	b.Write(pre[:])
	b.Write(policy[:])
	if closed == nil {
		writeUint64(&b, ^uint64(0))
	} else {
		writeBatches(&b, closed.Batches)
	}
	writeContinuation(&b, c)
	b.Write(doneRoot[:])
	return sha256.Sum256(b.Bytes())
}
