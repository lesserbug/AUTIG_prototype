import copy
import json
import unittest

import run_ordering


def result():
    return dict(nodes=10, faults=2, byzantine=0, gamma=1, transactions=3, lo_size=2, seed=7,
                ratio=1, trace_digest="same-input", total_pairs=3, completed_pairs=3,
                correct_pairs=3, tied_pairs=0, reversed_pairs=0, unresolved_pairs=0,
                completed_transactions=3, batch_indices=[1, 2, 3], receive_disagreement_pairs=1,
                fairness_checked_pairs=2, fairness_violations=0, replica_states_agree=True)


def parse(row):
    return run_ordering.parse_result("test output\nORDERING_RESULT " + json.dumps(row) + "\nPASS\n")


class OrderingRunnerTests(unittest.TestCase):
    def test_denominators_and_zero_completion(self):
        row = parse(result())
        self.assertEqual(row["strict_accuracy"], 1)
        self.assertEqual(row["pair_coverage"], 1)
        empty = result()
        empty.update(completed_pairs=0, correct_pairs=0, unresolved_pairs=3,
                     completed_transactions=0, batch_indices=[0, 0, 0])
        row = parse(empty)
        self.assertIsNone(row["strict_accuracy"])
        self.assertEqual(row["pair_coverage"], 0)

    def test_invalid_and_duplicate_results_rejected(self):
        for changes in [dict(total_pairs=4), dict(correct_pairs=2), dict(unresolved_pairs=1),
                        dict(replica_states_agree=False), dict(completed_transactions=2),
                        dict(batch_indices=[1, 2])]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                parse(dict(result(), **changes))
        with self.assertRaises(ValueError):
            run_ordering.parse_result("ORDERING_RESULT {}\nORDERING_RESULT {}")

    def test_paired_attack_distinguishes_reversal_tie_and_missing(self):
        baseline = parse(result())
        for batches, expected in [([3, 2, 1], (3, 0, 0)), ([1, 1, 2], (0, 1, 0)),
                                  ([0, 1, 2], (0, 0, 2)), ([1, 2, 3], (0, 0, 0))]:
            row = dict(baseline, byzantine=1, batch_indices=batches)
            run_ordering.add_paired_comparison(row, baseline)
            self.assertEqual(row["baseline_strict_pairs"], 3)
            self.assertEqual((row["baseline_pairs_reversed"], row["baseline_pairs_tied"],
                              row["baseline_pairs_unresolved"]), expected)
        with self.assertRaises(ValueError):
            run_ordering.add_paired_comparison(dict(baseline, trace_digest="different"), baseline)

    def test_baseline_ties_are_not_strict_reference_pairs(self):
        baseline = dict(parse(result()), batch_indices=[1, 1, 1])
        row = copy.deepcopy(baseline)
        run_ordering.add_paired_comparison(row, baseline)
        self.assertEqual(row["baseline_strict_pairs"], 0)
        self.assertIsNone(row["baseline_reversal_fraction"])

    def test_summary_spread_is_across_seeds(self):
        first = parse(result())
        run_ordering.add_paired_comparison(first, first)
        second = dict(first, seed=19, strict_accuracy=.5, fairness_violations=1)
        summary = run_ordering.summarize([first, second])[0]
        self.assertEqual(summary["seeds"], 2)
        self.assertEqual(summary["strict_accuracy_mean"], .75)
        self.assertAlmostEqual(summary["strict_accuracy_stddev"], .5 / 2**.5)
        self.assertEqual(summary["fairness_violations"], 1)

    def test_summary_reports_undefined_baseline_denominators(self):
        first = parse(result())
        run_ordering.add_paired_comparison(first, first)
        second = dict(first, seed=19, baseline_reversal_fraction=None, baseline_tie_fraction=None)
        summary = run_ordering.summarize([first, second])[0]
        self.assertEqual(summary["seeds"], 2)
        self.assertEqual(summary["strict_accuracy_valid_seeds"], 2)
        self.assertEqual(summary["baseline_reversal_fraction_valid_seeds"], 1)
        self.assertIsNone(summary["baseline_reversal_fraction_stddev"])


if __name__ == "__main__":
    unittest.main()
