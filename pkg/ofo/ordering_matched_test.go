package ofo

// Reuse the ordering fixture for the nine r=10 comparisons. Production paths
// and the original rotating/chunked experiment are unchanged.
import (
	"SpeedFair_simplify/pkg/types"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var (
	orderingMatchedInput  = flag.String("ordering-matched-input", "", "Original AUTIG input directory")
	orderingMatchedThemis = flag.String("ordering-matched-themis", "", "Existing Themis measurements.jsonl")
	orderingMatchedOut    = flag.String("ordering-matched-output", "", "New matched result JSONL file")
	orderingSweepOut      = flag.String("ordering-cost-sweep-output", "", "New n=13,F=3,b=0..3 exploratory cost JSONL; exports three traces alongside")
)

func TestOrderingCostThreeByzantine(t *testing.T) {
	if *orderingSweepOut == "" {
		t.Skip("opt-in legal three-Byzantine cost sweep")
	}
	if !*orderingCost {
		t.Fatal("requires -ordering-cost")
	}
	out, err := os.OpenFile(*orderingSweepOut, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	encoder := json.NewEncoder(out)
	for _, seed := range []int64{1, 19, 7} {
		trace := makeOrderingTrace(13, 1000, 10, seed)
		data, err := json.Marshal(trace)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(filepath.Dir(*orderingSweepOut), fmt.Sprintf("r10-seed%d-b0-input.json", seed))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write(data)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("trace write failed: %v %v", err, closeErr)
		}
		for b := uint64(0); b <= 3; b++ {
			t.Run(fmt.Sprintf("seed%d/b%d", seed, b), func(t *testing.T) {
				result := runOrdering(t, trace, 3, b, 1, 1000, seed, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
				result.Ratio = 10
				if result.CompletedTransactions != 1000 || result.Rounds != 1 || result.FairnessViolations != 0 ||
					!result.ReplicaStatesAgree || result.Cost == nil || result.MaliciousReversedOrders != int(b) {
					t.Fatal("extended cost case failed correctness checks")
				}
				if err := encoder.Encode(result); err != nil {
					t.Fatal(err)
				}
				t.Logf("completed=%d b=%d reversed=%d fairness_violations=%d", result.CompletedTransactions, b, result.MaliciousReversedOrders, result.FairnessViolations)
			})
		}
	}
}

func TestOrderingMatchedEvidence(t *testing.T) {
	trace := makeOrderingTrace(10, 24, 10, 7)
	senders := []uint64{2, 3, 4, 5, 6, 7, 8, 9}
	for b := uint64(0); b <= 2; b++ {
		result := runOrdering(t, trace, 2, b, 1, 24, 7, senders...)
		if result.Rounds != 1 || result.CompletedTransactions != 24 || result.FairnessViolations != 0 ||
			result.MaliciousReversedOrders != int(b) || !reflect.DeepEqual(result.SelectedSenders, senders) {
			t.Fatalf("unexpected matched result: %+v", result)
		}
		for k, r := range senders {
			want := append([]int(nil), trace.ReceiptOrders[r]...)
			if r >= 10-b {
				for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
					want[i], want[j] = want[j], want[i]
				}
			}
			if !reflect.DeepEqual(result.ClaimedLists[k], want) {
				t.Fatal("matched evidence changed the full original or reversed order")
			}
		}
	}
}

func TestOrderingMatchedThemis(t *testing.T) {
	if *orderingMatchedInput == "" {
		t.Skip("opt-in; requires original inputs and existing Themis results")
	}
	in, err := os.Open(*orderingMatchedThemis)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	decoder := json.NewDecoder(in)
	// Decode only the fields used to establish the matched input and comparison.
	type reference struct {
		N               uint64   `json:"n"`
		F               uint64   `json:"F"`
		B               uint64   `json:"b"`
		Gamma           float64  `json:"gamma"`
		Ratio           float64  `json:"ratio"`
		Seed            int64    `json:"seed"`
		InputFile       string   `json:"input_file"`
		InputSHA256     string   `json:"input_sha256"`
		SelectedSenders []uint64 `json:"selected_senders"`
		ClaimedLists    [][]int  `json:"claimed_lists"`
		Batches         []uint64 `json:"batch_indices"`
		Completed       int      `json:"completed"`
		Eligible        int      `json:"fairness_eligible"`
		Checked         int      `json:"fairness_checked"`
		Violations      int      `json:"fairness_violations"`
	}
	var refs []reference
	seen := make(map[[2]int64]bool)
	for {
		var ref reference
		if err := decoder.Decode(&ref); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if ref.Ratio != 10 {
			continue
		}
		key := [2]int64{ref.Seed, int64(ref.B)}
		if ref.N != 10 || ref.F != 2 || ref.Gamma != 1 || ref.B > 2 ||
			(ref.Seed != 1 && ref.Seed != 7 && ref.Seed != 19) || seen[key] ||
			ref.Completed != 1000 || len(ref.Batches) != 1000 ||
			!reflect.DeepEqual(ref.SelectedSenders, []uint64{2, 3, 4, 5, 6, 7, 8, 9}) {
			t.Fatal("unexpected or duplicate Themis reference case")
		}
		seen[key] = true
		refs = append(refs, ref)
	}
	if len(refs) != 9 {
		t.Fatal("require all nine r=10 Themis reference cases")
	}
	out, err := os.OpenFile(*orderingMatchedOut, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	encoder := json.NewEncoder(out)
	for _, ref := range refs {
		t.Run(fmt.Sprintf("seed%d/b%d", ref.Seed, ref.B), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(*orderingMatchedInput, filepath.Base(ref.InputFile)))
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != ref.InputSHA256 {
				t.Fatalf("original input hash mismatch: %v", err)
			}
			var trace orderingTrace
			if err := json.Unmarshal(data, &trace); err != nil {
				t.Fatal(err)
			}
			if len(trace.ReceiptOrders) != 10 || len(trace.TransactionIDs) != 1000 {
				t.Fatal("invalid saved trace dimensions")
			}
			trace.txs = make([]types.Transaction, 1000)
			for i := range trace.txs {
				content := make([]byte, 16)
				binary.BigEndian.PutUint64(content, uint64(i))
				binary.BigEndian.PutUint64(content[8:], uint64(ref.Seed))
				trace.txs[i] = types.Transaction{ID: types.TransactionID(content), CanonicalBytes: content}
				if fmt.Sprintf("%x", trace.txs[i].ID) != trace.TransactionIDs[i] {
					t.Fatal("transaction identity differs from saved input")
				}
			}
			result := runOrdering(t, trace, 2, ref.B, 1, 1000, ref.Seed, ref.SelectedSenders...)
			result.Ratio = 10
			if result.TraceDigest != ref.InputSHA256 || !reflect.DeepEqual(result.ClaimedLists, ref.ClaimedLists) {
				t.Fatal("AUTIG and Themis did not receive identical ordering evidence")
			}
			// Shared all-node premise: q=10. This oracle-only f=0,b=0 call counts
			// all original profiles; it does NOT alter the execution's F=2 or b.
			common := measureOrdering(trace, result.Batches, 0, 0, 1)
			themisCommon := measureOrdering(trace, ref.Batches, 0, 0, 1)
			if themisCommon.FairnessEligiblePairs != ref.Eligible || themisCommon.FairnessCheckedPairs != ref.Checked ||
				themisCommon.FairnessViolations != ref.Violations {
				t.Fatal("saved Themis all-node oracle counts failed independent recount")
			}
			equal := reflect.DeepEqual(result.Batches, ref.Batches)
			if *orderingCost && (!equal || result.Cost == nil || result.Cost.LeaderBuildNS <= 0 || result.Cost.FollowerAcceptNS <= 0) {
				t.Fatal("cost comparison requires identical batches and valid timing")
			}
			if err := encoder.Encode(struct {
				orderingResult
				SameBatches bool            `json:"same_batch_indices_as_themis"`
				Common      orderingMetrics `json:"common_all_node_oracle"`
				Themis      orderingMetrics `json:"themis_common_all_node_oracle"`
			}{result, equal, common, themisCommon}); err != nil {
				t.Fatal(err)
			}
			t.Logf("completed=%d rounds=%d fairness_violations=%d shared_violations=%d same_batches=%t",
				result.CompletedTransactions, result.Rounds, result.FairnessViolations, common.FairnessViolations, equal)
			if result.FairnessViolations != 0 || common.FairnessViolations != 0 || result.UnresolvedPairs != 0 {
				t.Fatal("matched AUTIG fairness or completion check failed; result preserved")
			}
		})
	}
}
