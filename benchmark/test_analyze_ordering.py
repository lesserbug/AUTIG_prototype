import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import analyze_ordering as analysis


def fixture(m=4):
    total = m * (m-1) // 2
    trace = dict(generation_times=list(range(m)), receipt_orders=[list(range(m)) for _ in range(5)])
    row = dict(nodes=5, faults=1, gamma=1, byzantine=0, transactions=m, lo_size=2,
               ratio=1, seed=7, trace_digest="paired", batch_indices=list(range(1, m+1)),
               total_pairs=total, completed_pairs=total, correct_pairs=total, tied_pairs=0,
               reversed_pairs=0, unresolved_pairs=0, completed_transactions=m,
               fairness_eligible_pairs=total, fairness_checked_pairs=total, fairness_violations=0,
               missing_predecessors=0, receive_disagreement_pairs=0, max_batch_size=1,
               replica_states_agree=True)
    return row, trace


class OrderingSupplementTests(unittest.TestCase):
    def test_scopes_ties_reverse_and_coarseness(self):
        baseline, trace = fixture()
        row = dict(baseline, byzantine=1, batch_indices=[1, 1, 3, 2], correct_pairs=4,
                   tied_pairs=1, reversed_pairs=1, fairness_violations=1, max_batch_size=2)
        case, pairs = analysis.analyze_case(row, trace, baseline)
        self.assertEqual([pairs[0][k + "_pairs"] for k in analysis.CATEGORIES], [1, 1, 1, 0])
        self.assertEqual(case["obligation_coverage"], 1)
        self.assertEqual(case["obligations_satisfied_strictly"], 4)
        self.assertEqual(case["obligations_satisfied_in_ties"], 1)
        self.assertEqual(case["fair_batch_count"], 3)
        self.assertEqual(case["largest_batch_fraction"], .5)
        self.assertEqual(case["baseline_stayed_strict_fraction"], 4/6)
        self.assertEqual(case["baseline_reversal_fraction"], 1/6)

    def test_scope_denominators_are_cumulative_generation_distances(self):
        row, trace = fixture(12)
        _, pairs = analysis.analyze_case(row, trace, row)
        self.assertEqual([r["total_pairs"] for r in pairs], [11, 45, 65, 66])

    def test_honest_disagreement_and_byzantine_exclusion(self):
        row, trace = fixture()
        trace["receipt_orders"][-1].reverse()
        row["receive_disagreement_pairs"] = 6
        case, _ = analysis.analyze_case(row, trace, row)
        self.assertEqual(case["honest_pairwise_disagreement"], .4)
        self.assertEqual(case["honest_nonunanimous_pair_fraction"], 1)
        case, _ = analysis.analyze_case(dict(row, byzantine=1), trace, row)
        self.assertEqual(case["honest_pairwise_disagreement"], 0)
        trace["receipt_orders"][-1].reverse()
        trace["receipt_orders"][-2].reverse()
        attacked = dict(row, byzantine=1, fairness_eligible_pairs=0, fairness_checked_pairs=0)
        case, _ = analysis.analyze_case(attacked, trace, row)
        self.assertEqual(case["obligation_coverage"], 0)
        self.assertIsNone(case["obligation_tie_fraction"])

    def test_fairness_direction_can_oppose_generation_order(self):
        row, trace = fixture()
        trace["receipt_orders"] = [list(reversed(order)) for order in trace["receipt_orders"]]
        row.update(batch_indices=[4, 3, 2, 1], correct_pairs=0, reversed_pairs=6)
        case, _ = analysis.analyze_case(row, trace, row)
        self.assertEqual(case["violations"], 0)
        self.assertEqual(case["obligations_satisfied_strictly"], 6)

    def test_missing_predecessors_remain_violations(self):
        baseline, trace = fixture()
        row = dict(baseline, byzantine=1, batch_indices=[0, 1, 2, 3], completed_transactions=3,
                   completed_pairs=3, correct_pairs=3, unresolved_pairs=3,
                   fairness_violations=3, missing_predecessors=3)
        case, pairs = analysis.analyze_case(row, trace, baseline)
        self.assertEqual(case["checked_obligations"], 6)
        self.assertEqual(case["violations"], 3)
        self.assertEqual(pairs[0]["pair_coverage"], 2/3)
        self.assertEqual(case["baseline_unresolved_fraction"], .5)
        row.update(batch_indices=[0]*4, completed_transactions=0, completed_pairs=0,
                   correct_pairs=0, unresolved_pairs=6, fairness_checked_pairs=0,
                   fairness_violations=0, missing_predecessors=0, max_batch_size=0)
        case, pairs = analysis.analyze_case(row, trace, baseline)
        self.assertEqual(case["obligation_check_coverage"], 0)
        self.assertIsNone(pairs[0]["strict_fraction"])

    def test_undefined_baseline_and_duplicate_seed(self):
        row, trace = fixture()
        row.update(batch_indices=[1]*4, correct_pairs=0, tied_pairs=6, max_batch_size=4)
        case, _ = analysis.analyze_case(row, trace, row)
        self.assertEqual(case["obligation_tie_fraction"], 1)
        self.assertIsNone(case["baseline_stayed_strict_fraction"])
        summary = analysis.summarize([case], ("ratio", "byzantine"))[0]
        self.assertEqual(summary["baseline_tie_fraction_valid_seeds"], 0)
        with self.assertRaises(ValueError):
            analysis.summarize([case, dict(case)], ("ratio", "byzantine"))

    def test_corrupt_trace_or_saved_counts_are_rejected(self):
        row, trace = fixture()
        for changes in (dict(fairness_eligible_pairs=5), dict(max_batch_size=2), dict(trace_digest="wrong")):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                analysis.analyze_case(dict(row, **changes), trace, row)
        trace["receipt_orders"][0] = [0, 0, 2, 3]
        with self.assertRaises(ValueError):
            analysis.analyze_case(row, trace, row)

    def test_cli_preserves_source_and_refuses_overwrite(self):
        row, trace = fixture()
        encoded = json.dumps(trace).encode()
        row["trace_digest"] = hashlib.sha256(encoded).hexdigest()
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder)
            (source / "r1-seed7-b0-input.json").write_bytes(encoded)
            raw = source / "measurements.json"
            raw.write_text(json.dumps([row]), encoding="utf-8")
            before = raw.read_bytes()
            command = [sys.executable, str(Path(analysis.__file__)), str(source)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(raw.read_bytes(), before)
            metadata = json.loads((source / "supplement/metadata.json").read_text())
            self.assertEqual(metadata["protocol_reruns"], 0)
            self.assertEqual(metadata["source_sha256"][raw.name], hashlib.sha256(before).hexdigest())
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
