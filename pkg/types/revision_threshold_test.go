package types

import "testing"

func TestRevisionExactDecimalThresholds(t *testing.T) {
	cases := 0
	for n := uint64(1); n <= 256; n++ {
		for f := uint64(0); f < n && f < 8; f++ {
			for hundredths := uint64(51); hundredths <= 100; hundredths++ {
				gamma := float64(hundredths) / 100
				want := (hundredths*(n-f) + 99) / 100
				if got := CalculateHonestThreshold(n, f, gamma); got != want {
					t.Fatalf("n=%d f=%d gamma=%g got %d want %d", n, f, gamma, got, want)
				}
				if got := CalculateEdgeThreshold(n, f, gamma); got != int(n-want+1) {
					t.Fatal("edge threshold disagrees with exact support count")
				}
				cases++
			}
		}
	}
	t.Logf("exact decimal support/edge thresholds checked for %d configurations", cases)
}
