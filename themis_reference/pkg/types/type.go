// pkg/types/types.go
package types

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"
)

// Transaction - 模拟的交易，增加了提交时间戳用于计算延迟
type Transaction struct {
	ID             [32]byte
	CanonicalBytes []byte
	SubmissionTime time.Time
}

// PoolTx - 用于在内存池中对交易进行排序的辅助结构
type PoolTx struct {
	ID   [32]byte
	Time time.Time
}

// TxState - 交易在图中的状态 (Solid, Shaded, Blank)
type TxState string

const (
	StateSolid  TxState = "solid"
	StateShaded TxState = "shaded"
	StateBlank  TxState = "blank"
)

// LocalOrder - 节点本地对【新】交易的排序结果
type LocalOrder struct {
	ReplicaID  uint64
	Sequence   uint64
	OrderedTxs [][32]byte // 按接收顺序排列的交易哈希
	Signature  []byte
}

// UpdateOrder - 节点本地对【延迟】交易的排序结果
type UpdateOrder struct {
	ReplicaID  uint64
	Sequence   uint64
	OrderedTxs [][32]byte
	Signature  []byte
}

// ReplicaOrders - 节点发送给 Leader 的完整排序信息
type ReplicaOrders struct {
	ReplicaID  uint64
	Sequence   uint64
	NewTxOrder *LocalOrder  // 对 txPool 中新交易的排序
	Update     *UpdateOrder // 对所有尚未完成 proposal 中交易的一份排序
}

// DependencyGraph - Themis论文中定义的依赖图 G
type DependencyGraph struct {
	Nodes map[[32]byte]bool       // 图中的节点 (non-blank txs)
	Edges map[[32]byte][][32]byte // 记录交易间的依赖关系 u -> [v1, v2...]
}

// SCCInfo - 强连通分量的信息，用于排序和剪枝
type SCCInfo struct {
	ID      int
	Txs     [][32]byte
	IsSolid bool
}

// LocalOrderFragment - 作为验证证据(π)的数据结构
type LocalOrderFragment struct {
	ReplicaOrders []*ReplicaOrders // n-f个节点的完整排序信息
}

// LeaderProposal - 代表 Leader 的完整提案 B = (G, E_updates, π)
type LeaderProposal struct {
	BlockHeight uint64
	Graph       *DependencyGraph                   // 新交易的依赖图 G_new
	Updates     map[uint64]map[[32]byte][][32]byte // 对旧区块的更新 E_updates, map[blockHeight]edges
	Proof       *LocalOrderFragment                // 用于验证的证据 π
	TxStates    map[[32]byte]TxState               // Leader为新图计算出的交易状态
}

type OrderAuthenticator interface {
	SignReplica(replicaID uint64, digest [32]byte) ([]byte, error)
	VerifyReplica(replicaID uint64, digest [32]byte, signature []byte) bool
}

func TransactionID(canonicalBytes []byte) [32]byte {
	var b bytes.Buffer
	b.WriteString("AUTIG/transaction")
	b.Write(canonicalBytes)
	return sha256.Sum256(b.Bytes())
}

func LocalOrderDigest(order *LocalOrder) [32]byte {
	var b bytes.Buffer
	b.WriteString("Themis/local-order/v1")
	writeUint64(&b, order.ReplicaID)
	writeUint64(&b, order.Sequence)
	writeTxIDs(&b, order.OrderedTxs)
	return sha256.Sum256(b.Bytes())
}

func UpdateOrderDigest(order *UpdateOrder) [32]byte {
	var b bytes.Buffer
	b.WriteString("Themis/update-order/v1")
	writeUint64(&b, order.ReplicaID)
	writeUint64(&b, order.Sequence)
	writeTxIDs(&b, order.OrderedTxs)
	return sha256.Sum256(b.Bytes())
}

func LeaderProposalDigest(proposal *LeaderProposal) [32]byte {
	var b bytes.Buffer
	b.WriteString("Themis/leader-proposal/v1")
	writeUint64(&b, proposal.BlockHeight)
	writeGraph(&b, proposal.Graph)

	heights := make([]uint64, 0, len(proposal.Updates))
	for height := range proposal.Updates {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	writeUint64(&b, uint64(len(heights)))
	for _, height := range heights {
		writeUint64(&b, height)
		writeEdges(&b, proposal.Updates[height])
	}

	stateIDs := make([][32]byte, 0, len(proposal.TxStates))
	for id := range proposal.TxStates {
		stateIDs = append(stateIDs, id)
	}
	sortTxIDs(stateIDs)
	writeUint64(&b, uint64(len(stateIDs)))
	for _, id := range stateIDs {
		b.Write(id[:])
		state := []byte(proposal.TxStates[id])
		writeUint64(&b, uint64(len(state)))
		b.Write(state)
	}

	var replicaOrders []*ReplicaOrders
	if proposal.Proof != nil {
		replicaOrders = append(replicaOrders, proposal.Proof.ReplicaOrders...)
	}
	sort.Slice(replicaOrders, func(i, j int) bool { return replicaOrders[i].ReplicaID < replicaOrders[j].ReplicaID })
	writeUint64(&b, uint64(len(replicaOrders)))
	for _, orders := range replicaOrders {
		writeUint64(&b, orders.ReplicaID)
		writeUint64(&b, orders.Sequence)
		if orders.NewTxOrder == nil {
			writeUint64(&b, 0)
		} else {
			writeUint64(&b, 1)
			digest := LocalOrderDigest(orders.NewTxOrder)
			b.Write(digest[:])
			writeBytes(&b, orders.NewTxOrder.Signature)
		}
		if orders.Update == nil {
			writeUint64(&b, 0)
		} else {
			writeUint64(&b, 1)
			digest := UpdateOrderDigest(orders.Update)
			b.Write(digest[:])
			writeBytes(&b, orders.Update.Signature)
		}
	}
	return sha256.Sum256(b.Bytes())
}

func writeGraph(b *bytes.Buffer, graph *DependencyGraph) {
	if graph == nil {
		writeUint64(b, 0)
		return
	}
	nodes := make([][32]byte, 0, len(graph.Nodes))
	for node := range graph.Nodes {
		nodes = append(nodes, node)
	}
	sortTxIDs(nodes)
	writeUint64(b, uint64(len(nodes)))
	for _, node := range nodes {
		b.Write(node[:])
	}
	writeEdges(b, graph.Edges)
}

func writeEdges(b *bytes.Buffer, edges map[[32]byte][][32]byte) {
	sources := make([][32]byte, 0, len(edges))
	for source := range edges {
		sources = append(sources, source)
	}
	sortTxIDs(sources)
	writeUint64(b, uint64(len(sources)))
	for _, source := range sources {
		b.Write(source[:])
		targets := append([][32]byte(nil), edges[source]...)
		sortTxIDs(targets)
		writeTxIDs(b, targets)
	}
}

func writeTxIDs(b *bytes.Buffer, ids [][32]byte) {
	writeUint64(b, uint64(len(ids)))
	for _, id := range ids {
		b.Write(id[:])
	}
}

func writeBytes(b *bytes.Buffer, value []byte) {
	writeUint64(b, uint64(len(value)))
	b.Write(value)
}

func writeUint64(b *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	b.Write(encoded[:])
}

func sortTxIDs(ids [][32]byte) {
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
}
