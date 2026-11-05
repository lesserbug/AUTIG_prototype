package ofo

// Optional counters are enabled only in untimed conformance runs. A pair
// decision, a directional EdgePred call, and two weight reads are distinct units.
type PredicateCounts struct {
	InternalPairs    uint64
	FrontierPairs    uint64
	WitnessEdges     uint64
	DirectionalCalls uint64
	WeightReads      uint64
	RebuiltPairs     uint64
}
