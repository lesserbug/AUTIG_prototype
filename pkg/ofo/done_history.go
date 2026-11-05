package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
)

// doneIndex is append-only committed storage. A view admits records only up to
// its captured version; candidates write solely to their private Done overlay.
type doneIndex struct {
	mu      sync.RWMutex
	records map[types.TxID]DoneRecord
	version uint64
}

func emptyDoneRoot() [32]byte { return sha256.Sum256([]byte("AUTIG/done-history/v2")) }

func appendDoneRoot(root [32]byte, id types.TxID, record DoneRecord) [32]byte {
	var b bytes.Buffer
	b.WriteString("AUTIG/done-history/record/v2")
	b.Write(root[:])
	b.Write(id[:])
	writeU64(&b, record.FragmentSeq)
	writeU64(&b, record.BatchIndex)
	writeU64(&b, record.HistoryIndex)
	writeU64(&b, record.ExecutionSeq)
	writeU64(&b, record.ExecutionOrdinal)
	return sha256.Sum256(b.Bytes())
}

func (state *EvidenceState) doneLookup(id types.TxID) (DoneRecord, bool) {
	if record, ok := state.Done[id]; ok {
		return record, true
	}
	state.doneBase.mu.RLock()
	record, ok := state.doneBase.records[id]
	state.doneBase.mu.RUnlock()
	return record, ok && record.FragmentSeq <= state.doneVersion
}

func (state *EvidenceState) doneCount() int { return state.doneBaseCount + len(state.Done) }

func (state *EvidenceState) addDone(id types.TxID, record DoneRecord) {
	if _, exists := state.doneLookup(id); exists {
		panic("verified transition attempted duplicate settlement")
	}
	record.HistoryIndex = uint64(state.doneCount()) + 1
	if record.ExecutionSeq == 0 {
		record.ExecutionSeq = record.FragmentSeq
	}
	if record.ExecutionOrdinal == 0 {
		record.ExecutionOrdinal = record.HistoryIndex
	}
	state.Done[id] = record
	state.doneOrder = append(state.doneOrder, id)
	state.DoneRoot = appendDoneRoot(state.DoneRoot, id, record)
}

// Called under the service commit lock, only for the decided pending digest.
func (state *EvidenceState) commitDone() bool {
	base := state.doneBase
	base.mu.Lock()
	defer base.mu.Unlock()
	if base.version != state.doneVersion || state.FragmentSeq < state.doneVersion {
		return false
	}
	for _, id := range state.doneOrder {
		if _, exists := base.records[id]; exists {
			return false
		}
		if state.Done[id].FragmentSeq > state.FragmentSeq {
			return false
		}
	}
	for _, id := range state.doneOrder {
		base.records[id] = state.Done[id]
	}
	state.doneBaseCount += len(state.Done)
	base.version = state.FragmentSeq
	state.doneVersion = state.FragmentSeq
	state.Done = make(map[types.TxID]DoneRecord)
	state.doneOrder = nil
	return true
}

// Enumeration is reserved for recovery/export and independent tests, never the
// ordinary validation or commit path.
func (state *EvidenceState) doneEntries() map[types.TxID]DoneRecord {
	all := make(map[types.TxID]DoneRecord, state.doneCount())
	state.doneBase.mu.RLock()
	for id, record := range state.doneBase.records {
		if record.FragmentSeq <= state.doneVersion {
			all[id] = record
		}
	}
	state.doneBase.mu.RUnlock()
	for id, record := range state.Done {
		all[id] = record
	}
	return all
}

type DoneEntry struct {
	ID     types.TxID
	Record DoneRecord
}

func (state *EvidenceState) ExportDoneHistory() []DoneEntry {
	entries := make([]DoneEntry, 0, state.doneCount())
	for id, record := range state.doneEntries() {
		entries = append(entries, DoneEntry{ID: id, Record: record})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Record.HistoryIndex < entries[j].Record.HistoryIndex })
	return entries
}

// RestoreDoneHistory reconstructs the membership index from the authenticated
// ordered records. A chain root by itself is not a membership proof or a digest
// of an independently supplied unordered map.
func (state *EvidenceState) RestoreDoneHistory(entries []DoneEntry, expected [32]byte) error {
	index := &doneIndex{records: make(map[types.TxID]DoneRecord, len(entries)), version: state.FragmentSeq}
	root := emptyDoneRoot()
	var previousSequence uint64
	for i, entry := range entries {
		r := entry.Record
		if r.HistoryIndex != uint64(i)+1 || r.FragmentSeq < previousSequence || r.FragmentSeq > state.FragmentSeq || r.ExecutionSeq > r.FragmentSeq || r.ExecutionOrdinal == 0 {
			return fmt.Errorf("noncanonical done-history position or sequence")
		}
		if _, duplicate := index.records[entry.ID]; duplicate {
			return fmt.Errorf("duplicate done-history identifier")
		}
		index.records[entry.ID] = r
		root = appendDoneRoot(root, entry.ID, r)
		previousSequence = r.FragmentSeq
	}
	if root != expected {
		return fmt.Errorf("done-history commitment mismatch")
	}
	state.doneBase, state.doneVersion, state.doneBaseCount = index, state.FragmentSeq, len(entries)
	state.Done, state.doneOrder, state.DoneRoot = make(map[types.TxID]DoneRecord), nil, root
	return nil
}

// Independent full-history oracle used by checkpoint and regression fixtures.
func finalizedSetRoot(done map[types.TxID]DoneRecord) [32]byte {
	ids := make([]types.TxID, 0, len(done))
	for id := range done {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return done[ids[i]].HistoryIndex < done[ids[j]].HistoryIndex })
	root := emptyDoneRoot()
	for _, id := range ids {
		root = appendDoneRoot(root, id, done[id])
	}
	return root
}
