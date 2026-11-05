package ofo

import (
	"sort"
	"time"
)

// Include transactions that have not executed at the observation cutoff.
// Zero timestamps from synthetic/admission-only fixtures are not measurements.
func (s *OFOService) PendingAges(cutoff time.Time) []time.Duration {
	s.rwMu.RLock()
	defer s.rwMu.RUnlock()
	ages := make([]time.Duration, 0, len(s.txSubmissionTimes))
	for _, arrival := range s.txSubmissionTimes {
		if !arrival.IsZero() && !arrival.After(cutoff) {
			ages = append(ages, cutoff.Sub(arrival))
		}
	}
	sort.Slice(ages, func(i, j int) bool { return ages[i] < ages[j] })
	return ages
}
