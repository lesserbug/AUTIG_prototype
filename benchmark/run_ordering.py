"""Finite-input AUTIG ordering experiment using production LO/malicious logic.

Standard library only unless --plot is requested. No AWS or protocol changes.
Each (ratio, seed, b) runs in a fresh Go test process; varying b reuses the exact
same original receive trace and fixed F. Seeds, not repeated deterministic runs,
are the independent observations for descriptive mean/stddev summaries.
"""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def parse_result(text):
    rows = [json.loads(line[len("ORDERING_RESULT "):]) for line in text.splitlines()
            if line.startswith("ORDERING_RESULT ")]
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one ordering result, got {len(rows)}")
    row = rows[0]
    n = row["transactions"]
    if row["total_pairs"] != n * (n - 1) // 2 or len(row["batch_indices"]) != n:
        raise ValueError("Wrong finite input/output size")
    if row["correct_pairs"] + row["tied_pairs"] + row["reversed_pairs"] != row["completed_pairs"]:
        raise ValueError("Completed-pair categories do not partition the denominator")
    if row["completed_pairs"] + row["unresolved_pairs"] != row["total_pairs"]:
        raise ValueError("Unresolved pairs are missing from the accounting")
    if not row["replica_states_agree"]:
        raise ValueError("Replica state disagreement")
    completed = sum(batch > 0 for batch in row["batch_indices"])
    if completed != row["completed_transactions"] or row["completed_pairs"] != completed * (completed - 1) // 2:
        raise ValueError("Output batches disagree with completion accounting")
    for name, numerator, denominator in [
        ("strict_accuracy", "correct_pairs", "completed_pairs"),
        ("tie_fraction", "tied_pairs", "completed_pairs"),
        ("reverse_fraction", "reversed_pairs", "completed_pairs"),
        ("pair_coverage", "completed_pairs", "total_pairs"),
        ("receive_disagreement_fraction", "receive_disagreement_pairs", "total_pairs"),
    ]:
        row[name] = row[numerator] / row[denominator] if row[denominator] else None
    return row


def add_paired_comparison(row, baseline):
    keys = ("nodes", "faults", "gamma", "transactions", "lo_size", "seed", "ratio", "trace_digest")
    if baseline["byzantine"] != 0 or any(row[key] != baseline[key] for key in keys):
        raise ValueError("Attack and baseline are not paired on the same input and protocol parameters")
    strict = reversed_pairs = tied = unresolved = 0
    original, attacked = baseline["batch_indices"], row["batch_indices"]
    for i in range(len(original)):
        for j in range(i + 1, len(original)):
            if not original[i] or not original[j] or original[i] == original[j]:
                continue
            strict += 1
            if not attacked[i] or not attacked[j]:
                unresolved += 1
            elif attacked[i] == attacked[j]:
                tied += 1
            elif (original[i] < original[j]) != (attacked[i] < attacked[j]):
                reversed_pairs += 1
    row.update(baseline_strict_pairs=strict, baseline_pairs_reversed=reversed_pairs,
               baseline_pairs_tied=tied, baseline_pairs_unresolved=unresolved,
               baseline_reversal_fraction=reversed_pairs / strict if strict else None,
               baseline_tie_fraction=tied / strict if strict else None)


def summarize(rows):
    summaries = []
    for ratio, b in sorted({(r["ratio"], r["byzantine"]) for r in rows}):
        group = [r for r in rows if r["ratio"] == ratio and r["byzantine"] == b]
        summary = dict(ratio=ratio, byzantine=b, seeds=len(group),
                       fairness_violations=sum(r["fairness_violations"] for r in group),
                       fairness_checked_pairs=sum(r["fairness_checked_pairs"] for r in group),
                       unresolved_pairs=sum(r["unresolved_pairs"] for r in group))
        for metric in ("strict_accuracy", "tie_fraction", "reverse_fraction", "pair_coverage",
                       "receive_disagreement_fraction", "baseline_reversal_fraction", "baseline_tie_fraction"):
            values = [r[metric] for r in group if r[metric] is not None]
            summary[metric + "_valid_seeds"] = len(values)
            summary[metric + "_mean"] = statistics.mean(values) if values else None
            summary[metric + "_stddev"] = statistics.stdev(values) if len(values) > 1 else None
        summaries.append(summary)
    return summaries


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot_results(summaries, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.7), constrained_layout=True)
    colors = ["#2673b8", "#d17b16", "#168b72", "#8557aa", "#bd425c"]
    for index, b in enumerate(sorted({r["byzantine"] for r in summaries})):
        color = colors[index % len(colors)]
        rows = [r for r in summaries if r["byzantine"] == b]
        for ax, metric, title in zip(axes, ["strict_accuracy", "tie_fraction", "reverse_fraction"],
                                    ["Strict generation-order agreement", "Same fairness batch", "Reverse generation order"]):
            ax.errorbar([r["ratio"] for r in rows], [100*r[metric + "_mean"] for r in rows],
                        yerr=[100*(r[metric + "_stddev"] or 0) for r in rows],
                        label=f"b = {b}", color=color, marker="o", markersize=4, capsize=3)
            ax.set_xscale("symlog", linthresh=.1)
            ax.set_xlabel("Mean delay / mean generation interval (r)")
            ax.set_ylabel("Completed transaction pairs (%)")
            ax.set_title(title, fontsize=10)
            ax.grid(alpha=.2)
    axes[0].legend(frameon=False)
    for suffix in ["png", "pdf", "svg"]:
        fig.savefig(output / f"ordering-quality.{suffix}", dpi=180)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--faults", type=int, default=2)
    parser.add_argument("--gamma", type=float, default=1)
    parser.add_argument("--byzantine", default="0,1,2")
    parser.add_argument("--ratios", default="0,0.1,1,10,100")
    parser.add_argument("--seeds", default="1,7,19")
    parser.add_argument("--transactions", type=int, default=200)
    parser.add_argument("--lo-size", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=600, help="Maximum seconds per Go process")
    parser.add_argument("--plot", action="store_true", help="Requires matplotlib; saves PNG/PDF/SVG")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        counts = sorted({int(x) for x in args.byzantine.split(",")})
        ratios = sorted({float(x) for x in args.ratios.split(",")})
        seeds = sorted({int(x) for x in args.seeds.split(",")})
    except ValueError:
        parser.error("byzantine/seeds must be integer lists; ratios must be numeric")
    if args.nodes < 1 or args.faults < 0 or args.nodes < 3*args.faults+1 or not .5 < args.gamma <= 1:
        parser.error("invalid n/F/gamma")
    if 2*math.ceil(args.gamma*(args.nodes-args.faults)) < args.nodes+2*args.faults+1:
        parser.error("parameters violate the protocol quorum-overlap constraint")
    if counts[0] != 0 or counts[-1] > args.faults or any(not math.isfinite(r) or r < 0 for r in ratios):
        parser.error("include b=0; require 0<=b<=F and finite nonnegative ratios")
    if args.transactions < 2 or args.lo_size < 1 or args.timeout < 1:
        parser.error("require transactions>=2, lo-size>=1, timeout>=1")
    if args.plot:
        import matplotlib  # Fail before running experiments if plotting is unavailable.
    output = (args.output or ROOT / "benchmark" / "results" / ("ordering-" + time.strftime("%Y%m%d-%H%M%S"))).resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOMAXPROCS="1")
    def capture(command):
        return subprocess.check_output(command, cwd=ROOT, text=True).strip()
    metadata = dict(parameters={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                    commit=capture(["git", "rev-parse", "HEAD"]), git_status=capture(["git", "status", "--short"]),
                    go_version=capture(["go", "version"]), platform=platform.platform(),
                    gomaxprocs=1, model="finite inputs available before ordering; Exp(1) generation and Exp(mean=r) delays",
                    attack="production per-fresh-LO reversal, last b replicas; no artificial LO delay", commands=[])
    metadata["source_sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in [ROOT / "pkg/ofo/ordering_experiment_test.go", ROOT / "pkg/ofo/service.go",
                                            ROOT / "pkg/ofo/dependency.go", Path(__file__).resolve()]}
    def execute(command, log):
        metadata["commands"].append(command)
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        with log.open("w", encoding="utf-8") as handle:
            subprocess.run(command, cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT,
                           timeout=args.timeout, check=True)
    execute(["go", "test", "./pkg/ofo", "-run=^TestOrdering", "-count=1"], output / "correctness.log")
    binary = output / ("ordering.test.exe" if os.name == "nt" else "ordering.test")
    execute(["go", "test", "-c", "-o", str(binary), "./pkg/ofo"], output / "build.log")
    all_rows = []
    for ratio in ratios:
        for seed in seeds:
            baseline = None
            for b in counts:
                label = f"r{ratio}-seed{seed}-b{b}"
                command = [str(binary), "-test.run=^TestOrderingExperiment$", "-test.count=1", "-ordering-experiment",
                           f"-ordering-nodes={args.nodes}", f"-ordering-faults={args.faults}",
                           f"-ordering-gamma={args.gamma}", f"-ordering-byzantine={b}",
                           f"-ordering-transactions={args.transactions}", f"-ordering-lo-size={args.lo_size}",
                           f"-ordering-ratio={ratio}", f"-ordering-seed={seed}"]
                if b == 0:
                    command.append(f"-ordering-trace-file={output / (label + '-input.json')}")
                execute(command, output / (label + ".log"))
                row = parse_result((output / (label + ".log")).read_text(encoding="utf-8"))
                if b == 0:
                    baseline = row
                add_paired_comparison(row, baseline)
                all_rows.append(row)
                (output / "measurements.json").write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
                print(f"{label}: strict={row['strict_accuracy']!s} ties={row['tie_fraction']!s} "
                      f"reversed={row['reverse_fraction']!s} violations={row['fairness_violations']} "
                      f"completed={row['completed_transactions']}/{args.transactions}", flush=True)
    summaries = summarize(all_rows)
    write_csv(output / "summary.csv", summaries)
    write_csv(output / "measurements.csv", [{k: v for k, v in row.items() if k != "batch_indices"} for row in all_rows])
    if args.plot and all(r["completed_pairs"] > 0 for r in all_rows):
        plot_results(summaries, output)
    violations = sum(row["fairness_violations"] for row in all_rows)
    unfinished = sum(row["unresolved_pairs"] for row in all_rows)
    (output / "summary.json").write_text(json.dumps(dict(results=summaries, fairness_violations=violations,
                                                        unresolved_pairs=unfinished), indent=2), encoding="utf-8")
    print(f"Results: {output}")
    if violations or unfinished:
        raise SystemExit("Ordering check failed: inspect preserved violations / unresolved pairs")


if __name__ == "__main__":
    main()
