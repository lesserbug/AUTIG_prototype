package main

import (
	"fmt"
	"math"
	"time"

	auto "SpeedFair_simplify/pkg/types"
	themis "themisreference/pkg/ofo"
)

type benchCommittee struct {
	N, F  int
	Gamma float64
}

type benchLimits struct {
	RecordBytes int
	Broadcast   time.Duration
	Workload    time.Duration
}

func (c benchCommittee) validate() error {
	if c.N <= 0 || c.F < 0 || c.F >= c.N || c.F > (c.N-1)/3 {
		return fmt.Errorf("matched benchmark requires n >= 3f+1 and f >= 0")
	}
	if math.IsNaN(c.Gamma) || c.Gamma <= .5 || c.Gamma > 1 {
		return fmt.Errorf("gamma must be in (1/2, 1]")
	}
	qh := auto.CalculateHonestThreshold(uint64(c.N), uint64(c.F), c.Gamma)
	if 2*qh < uint64(c.N+2*c.F+1) {
		return fmt.Errorf("matched AUTIG parameters violate integer feasibility")
	}
	// Require the same admissible committee for all comparison modes.
	return themis.ValidateThemisParameters(uint64(c.N), uint64(c.F), c.Gamma)
}
