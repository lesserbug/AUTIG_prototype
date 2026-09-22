"""Supplement saved ordering runs without executing or changing the protocol.

Usage: python benchmark/analyze_ordering.py benchmark/results/ordering-...
Standard library only. Source measurements and traces are read-only; output
must be a new directory. Fractions use explicit denominators; undefined is null.
"""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

from run_ordering import add_paired_comparison, parse_result, write_csv

SCOPES = {"adjacent_d1": 1, "near_d_le5": 5, "local_d_le10": 10, "all": None}
CATEGORIES = ("strict", "tie", "reverse", "unresolved")
CONFIG = ("nodes", "faults", "gamma", "transactions", "lo_size")


def divide(a, b):
    return a / b if b else None


def analyze_case(row, trace, baseline):
    """Independently recount saved output, including both obligation directions."""
    row = parse_result("ORDERING_RESULT " + json.dumps(row))
    add_paired_comparison(row, baseline)
    m, n, b = row["transactions"], row["nodes"], row["byzantine"]
    batches = row["batch_indices"]
    honest = n - b
    q = math.ceil(row["gamma"] * (n - row["faults"]))
    if not 0 <= b <= row["faults"] or 2*q <= n:
        raise ValueError("Require valid b and at most one eligible direction per pair")
    orders = trace["receipt_orders"]
    if len(orders) != n or len(trace["generation_times"]) != m:
        raise ValueError("Trace dimensions do not match measurements")
    if any(x >= y for x, y in zip(trace["generation_times"], trace["generation_times"][1:])):
        raise ValueError("Transaction indices must follow strict generation order")
    positions = []
    for order in orders:
        if sorted(order) != list(range(m)):
            raise ValueError("Receipt order must be a complete permutation")
        pos = [0] * m
        for index, tx in enumerate(order):
            pos[tx] = index
        positions.append(pos)
    sizes = Counter(batch for batch in batches if batch > 0)
    counts = {scope: dict.fromkeys(CATEGORIES, 0) for scope in SCOPES}
    eligible = checked = violations = missing = fair_tied = fair_strict = 0
    all_disagreed = honest_disagreed = replica_pair_disagreements = 0
    for i in range(m):
        for j in range(i + 1, m):
            left, right = batches[i], batches[j]
            category = ("unresolved" if not left or not right else "strict" if left < right
                        else "tie" if left == right else "reverse")
            for scope, distance in SCOPES.items():
                if distance is None or j-i <= distance:
                    counts[scope][category] += 1
            votes = [pos[i] < pos[j] for pos in positions]
            support, all_support = sum(votes[:honest]), sum(votes)
            all_disagreed += 0 < all_support < n
            honest_disagreed += 0 < support < honest
            replica_pair_disagreements += support * (honest - support)
            if support >= q:
                predecessor, successor = left, right
            elif honest - support >= q:
                predecessor, successor = right, left
            else:
                continue
            eligible += 1
            if successor:
                checked += 1
                if not predecessor or predecessor > successor:
                    violations += 1
                    missing += not predecessor
                elif predecessor == successor:
                    fair_tied += 1
                else:
                    fair_strict += 1
    actual = dict(fairness_eligible_pairs=eligible, fairness_checked_pairs=checked,
                  fairness_violations=violations, missing_predecessors=missing,
                  receive_disagreement_pairs=all_disagreed,
                  max_batch_size=max(sizes.values(), default=0))
    actual.update(zip(("correct_pairs", "tied_pairs", "reversed_pairs", "unresolved_pairs"),
                      (counts["all"][k] for k in CATEGORIES)))
    for key, value in actual.items():
        if row[key] != value:
            raise ValueError(f"Recount disagrees with saved {key}: {value} != {row[key]}")
    key = dict(ratio=row["ratio"], byzantine=b, seed=row["seed"])
    pairs = []
    for scope, values in counts.items():
        total = sum(values.values())
        completed = total - values["unresolved"]
        pairs.append(dict(key, scope=scope, total_pairs=total, completed_pairs=completed,
                          **{k+"_pairs": v for k, v in values.items()},
                          **{k+"_fraction": divide(values[k], completed) for k in CATEGORIES[:3]},
                          pair_coverage=divide(completed, total)))
    reference = row["baseline_strict_pairs"]
    reversed_count, tied_count, unresolved_count = (row["baseline_pairs_" + k]
                                                   for k in ("reversed", "tied", "unresolved"))
    stayed = reference - reversed_count - tied_count - unresolved_count
    case = dict(key, transactions=m, honest_replicas=honest, q_h=q, total_pairs=row["total_pairs"],
                eligible_obligations=eligible, checked_obligations=checked, violations=violations,
                obligation_coverage=divide(eligible, row["total_pairs"]),
                obligation_check_coverage=divide(checked, eligible),
                obligations_satisfied_strictly=fair_strict, obligations_satisfied_in_ties=fair_tied,
                obligation_tie_fraction=divide(fair_tied, checked),
                fair_batch_count=len(sizes), largest_batch_size=actual["max_batch_size"],
                largest_batch_fraction=divide(actual["max_batch_size"], m),
                completed_transactions=sum(sizes.values()),
                honest_nonunanimous_pair_fraction=divide(honest_disagreed, row["total_pairs"]),
                honest_pairwise_disagreement=divide(replica_pair_disagreements,
                                                   math.comb(honest, 2) * row["total_pairs"]),
                baseline_strict_pairs=reference,
                baseline_stayed_strict_fraction=divide(stayed, reference),
                baseline_tie_fraction=divide(tied_count, reference),
                baseline_reversal_fraction=divide(reversed_count, reference),
                baseline_unresolved_fraction=divide(unresolved_count, reference))
    return case, pairs


def summarize(rows, keys):
    summaries = []
    for group_key in sorted({tuple(row[k] for k in keys) for row in rows}):
        group = [row for row in rows if tuple(row[k] for k in keys) == group_key]
        if len({row["seed"] for row in group}) != len(group):
            raise ValueError("Duplicate seeds must not be treated as independent samples")
        result = dict(zip(keys, group_key), seeds=len(group))
        for metric in rows[0]:
            if metric in (*keys, "seed"):
                continue
            values = [row[metric] for row in group if row[metric] is not None]
            result[metric + "_valid_seeds"] = len(values)
            result[metric + "_mean"] = statistics.mean(values) if values else None
            result[metric + "_stddev"] = statistics.stdev(values) if len(values) > 1 else None
            if metric in ("total_pairs", "eligible_obligations", "checked_obligations", "violations"):
                result[metric + "_sum"] = sum(values)
        summaries.append(result)
    return summaries


def write_tables(output, cases, pairs):
    def value(row, name, percent=False):
        avg, sd = row[name + "_mean"], row[name + "_stddev"]
        if avg is None:
            return "NA"
        scale = 100 if percent else 1
        return f"{avg*scale:.4f}" + (f" ± {sd*scale:.4f}" if sd is not None else " (SD: NA)")

    lines = ["# Ordering supplement: existing runs only", "",
             "Values are per-seed means ± sample SD; percentages are marked %. Counts labelled sum are totals across seeds.",
             "NA denotes an undefined denominator, never zero. CSV/JSON contain each case, denominators and valid-seed counts.", "",
             "## Fairness obligations and batch coarseness", "",
             "Coverage = eligible obligations / all unordered transaction pairs. Compare checked with eligible; incomplete successors remain unchecked.",
             "Obligation ties are allowed by the batch predicate; they are not strict order preservation.", "",
             "| r | b | seeds | eligible sum | checked sum | coverage % | violations sum | tied / checked % | # batches | largest batch / M % |",
             "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in cases:
        lines.append(f"| {row['ratio']:g} | {row['byzantine']} | {row['seeds']} | {row['eligible_obligations_sum']} | "
                     f"{row['checked_obligations_sum']} | {value(row, 'obligation_coverage', True)} | "
                     f"{row['violations_sum']} | {value(row, 'obligation_tie_fraction', True)} | "
                     f"{value(row, 'fair_batch_count')} | {value(row, 'largest_batch_fraction', True)} |")
    lines += ["", "## Generation-position distance", "",
              "Scopes overlap: d=1, d<=5, d<=10, all. d is generation-index distance, not elapsed time or final adjacency.",
              "Strict/tie/reverse divide by completed pairs in that scope; pair coverage uses all pairs in the scope.", "",
              "| r | b | scope | pairs per seed | strict % | tie % | reverse % | pair coverage % |",
              "|---:|---:|---|---:|---:|---:|---:|---:|"]
    for row in sorted(pairs, key=lambda r: (r["ratio"], r["byzantine"], list(SCOPES).index(r["scope"]))):
        lines.append(f"| {row['ratio']:g} | {row['byzantine']} | {row['scope']} | {row['total_pairs_mean']:g} | "
                     + " | ".join(value(row, k, True) for k in
                                  ("strict_fraction", "tie_fraction", "reverse_fraction", "pair_coverage")) + " |")
    lines += ["", "## Original honest receipt disagreement", "",
              "Pairwise disagreement averages normalized Kendall disagreement over unordered honest replica pairs.",
              "Nonunanimous counts transaction pairs with any disagreement among honest replicas. These are different metrics.",
              "The original all-node input is paired across b; the honest subset has n-b nodes and changes with b.", "",
              "| r | b | pairwise disagreement % | nonunanimous transaction pairs % |",
              "|---:|---:|---:|---:|"]
    for row in cases:
        lines.append(f"| {row['ratio']:g} | {row['byzantine']} | {value(row, 'honest_pairwise_disagreement', True)} | "
                     f"{value(row, 'honest_nonunanimous_pair_fraction', True)} |")
    lines += ["", "## Paired transitions from b=0", "",
              "Reference: baseline pairs in distinct committed batches, including baseline generation-order reversals.",
              "Stay/reverse refer to that baseline direction, not generation order. Baseline ties are excluded.",
              "Zero reference pairs give NA; valid seeds can be fewer than the experiment's seeds.", "",
              "| r | b | valid seeds | mean reference pairs (all seeds) | stay strict % | become tied % | reverse % | unresolved % |",
              "|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in cases:
        if row["byzantine"] == 0:
            continue
        lines.append(f"| {row['ratio']:g} | {row['byzantine']} | {row['baseline_tie_fraction_valid_seeds']} | "
                     f"{row['baseline_strict_pairs_mean']:g} | " + " | ".join(value(row, k, True) for k in
                     ("baseline_stayed_strict_fraction", "baseline_tie_fraction", "baseline_reversal_fraction",
                      "baseline_unresolved_fraction")) + " |")
    (output / "tables.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source = args.source.resolve()
    output = (args.output or source / "supplement").resolve()
    raw = source / "measurements.json"
    rows = json.loads(raw.read_text(encoding="utf-8"))
    if not rows or len({tuple(r[k] for k in CONFIG) for r in rows}) != 1:
        raise ValueError("Require a nonempty matrix with fixed protocol and workload parameters")
    baselines = {(r["ratio"], r["seed"]): r for r in rows if r["byzantine"] == 0}
    traces, hashes = {}, {raw.name: hashlib.sha256(raw.read_bytes()).hexdigest()}
    for path in sorted(source.glob("*-b0-input.json")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        traces[digest] = json.loads(path.read_text(encoding="utf-8"))
        hashes[path.name] = digest
    output.mkdir(parents=True, exist_ok=False)
    cases, pairs = [], []
    for row in rows:
        case, scoped = analyze_case(row, traces[row["trace_digest"]], baselines[(row["ratio"], row["seed"])])
        cases.append(case)
        pairs.extend(scoped)
        print(f"Recounted r={row['ratio']:g}, seed={row['seed']}, b={row['byzantine']}", flush=True)
    case_summary = summarize(cases, ("ratio", "byzantine"))
    pair_summary = summarize(pairs, ("ratio", "byzantine", "scope"))
    for name, data in (("cases", cases), ("pair_scopes", pairs),
                       ("case_summary", case_summary), ("pair_summary", pair_summary)):
        (output / (name + ".json")).write_text(json.dumps(data, indent=2), encoding="utf-8")
        write_csv(output / (name + ".csv"), data)
    write_tables(output, case_summary, pair_summary)
    metadata = dict(source=str(source), source_sha256=hashes, protocol_reruns=0,
                    script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    runner_sha256=hashlib.sha256(Path(__file__).with_name("run_ordering.py").read_bytes()).hexdigest(),
                    command=sys.argv, python=sys.version, cases=len(cases),
                    note="All original fairness/category counts independently recounted and matched; per-seed descriptive SD.")
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"Supplement: {output}")
    if any(c["violations"] or c["completed_transactions"] != c["transactions"] for c in cases):
        raise SystemExit("Saved run contains violations or incomplete output; inspect preserved supplement")


if __name__ == "__main__":
    main()
