# Local ordering correctness and quality experiment

This experiment follows the existing ablation pattern: build a Go test binary,
then run independent local processes over controlled inputs. **No production Go
code is changed.** `pkg/ofo/ordering_experiment_test.go` calls the existing
`GenerateAndSendLocalOrder`, `constructCandidate`, `verifyCandidate`, and
`CommitPending` methods. All followers verify every candidate and the replicas'
committed state and fragment digests must agree after each round.

## Run

From the repository root (Go and Python on PATH):

```bash
python benchmark/run_ordering.py --nodes 10 --faults 2 --gamma 1 --byzantine 0,1,2 --transactions 1000 --lo-size 200 --ratios 0,0.1,1,10,100 --seeds 1,7,19
```

The default is a smaller 200-transaction set with LO size 50. Add `--plot` if
matplotlib is installed to save PNG, PDF and SVG figures. Otherwise the runner
uses only Python's standard library. `--output` must name a new directory;
default outputs are under `benchmark/results/ordering-<timestamp>`.

For a quick check:

```bash
python benchmark/run_ordering.py --transactions 96 --lo-size 24 --ratios 0,10,100 --seeds 1
```

The runner first tests the Go oracle and replay fixtures, then compiles once.
Additional checks can be run with:

```bash
go test ./...
python -m unittest discover -s benchmark -p test_run_ordering.py
```

## Controlled inputs and reused attack

- Fix n, protocol bound F, gamma, transaction count and LO size. Only b changes
  inside each matched group. For n=10, F=2, gamma=1, use b=0,1,2.
- Generate successive creation intervals from an exponential distribution with
  mean 1. Independently sample each transaction-to-replica propagation delay
  from an exponential distribution with mean r. Sort creation time plus delay
  to obtain each replica's complete original receipt profile. r=0 is the
  unanimous-order sanity check. r is dimensionless, **not milliseconds or TPS**.
- Deliver the full finite profiles through production `HandleMessage` before
  starting ordering. Thus all contents are available to all replicas; this is
  a finite-profile ordering-layer experiment, not an online latency simulation.
- The last b replicas have the production `isMalicious` flag. Their **fresh LO
  chunks are reversed by the existing production function**. The harness does
  not implement a second attack algorithm. It checks the emitted reversal,
  honest order preservation, and unchanged retained chunks on retransmission.
- Generate LOs at all replicas and collect the same rotating n-F sender set
  used by the benchmark. Omitted pending chunks remain pending and are re-signed
  for later rounds by production code. Drain the finite profiles, including
  late occurrences of already-finalized transactions.
- Real Ed25519 signatures use the existing deterministic ablation test
  authenticator. Synchronous test orchestration replaces collector goroutines,
  network transport and hosting consensus. No sleeps or artificial LO delay
  are injected, and no TPS/latency claim is made from this experiment.
- Reuse identical generation times, arrival profiles and transaction IDs across
  b for each (r, seed). Export the baseline input and verify matching trace
  SHA-256 digests before comparing attack results. Different seeds are distinct
  input samples; repeated deterministic runs of one seed are not extra samples.

The simulation idea is inspired by the Themis fairness experiments described
in [the author's dissertation, chapter 7](https://mahimnakelkar.github.io/dissertation.pdf).
This is **not a reproduction of Themis or a head-to-head comparison**. In
particular, the existing AUTIG attack reverses each LO chunk, not the entire
finite input sequence. LO size is consequently part of the attack definition
and must remain fixed when varying b.

## Independent metrics

For each pair i<j in original **generation order**, use their committed fairness
batch indices, not arbitrary transaction-ID serialization inside a batch:

| Outcome | Category |
|---|---|
| batch(i) < batch(j) | Strict generation-order agreement |
| batch(i) = batch(j) | Same batch; order not distinguished |
| batch(i) > batch(j) | Reverse generation order |
| Either transaction has no committed batch | Unresolved |

Strict accuracy, tie fraction and reverse fraction have **completed pairs** as
their denominator and sum to one. Pair coverage uses all M(M-1)/2 pairs.
Zero completed pairs give null accuracy, never 100%. Unresolved pairs remain
visible and cause the runner to exit unsuccessfully after preserving results.

Separately, the formal fairness oracle counts **original honest receipt
positions**, excluding the last b nodes. It uses the implementation's fixed
qH=ceil(gamma*(n-F)), not ceil(gamma*(n-b)). Whenever qH honest replicas saw x
before y and y was committed, x must be committed in the same or an earlier
fairness batch. Missing predecessors and reversed batches are violations;
same-batch pairs are allowed. This oracle does not use claimed malicious LO
order, graph edges or certificates as ground truth. Report the number of
eligible/checked obligations as well as violations; a zero count of violations
is empirical evidence for these inputs, not a proof for all executions.

Matched b=0 comparison also records how many baseline strictly ordered pairs
became reversed, tied or unresolved under attack. Baseline ties are excluded
from that reference denominator. These are separate from generation-order
accuracy: attack-induced changes do not necessarily all worsen send order.

## Outputs and interpretation

- `metadata.json`: parameters, environment, source fingerprints and commands.
- `r...-seed...-b0-input.json`: actual generation/arrival times, receipt profiles
  and transaction IDs, shared across b.
- Per-case logs and `measurements.json`: batch assignments, raw category counts,
  attack-path counters, pair coverage and independent fairness checks.
- `measurements.csv`, `summary.csv`, `summary.json`: case results and descriptive
  means/sample standard deviations across seeds. Pair observations share
  transactions and are not treated as independent statistical samples.
  Each metric also reports its valid-seed count: a baseline consisting of one
  batch has no strictly ordered reference pairs, so its paired-attack fractions
  are undefined and must not be presented as zero or as a three-seed average.
- Optional `ordering-quality.png/.pdf/.svg`: strict agreement, ties and reversals
  versus r; error bars are one sample standard deviation across seeds.

The script preserves outputs but exits with an error if any formal fairness
violation or unresolved pair is found. Verification/state disagreement fails
the underlying test process. Inspect failures rather than filtering them out.

Expect unanimous inputs to retain their order. Increasing receive disagreement
or b need not degrade every metric monotonically. A lower strict-agreement
score can come from larger SCC batches rather than more reversals; this is why
the three categories must be reported together. All-pair accuracy is also
influenced by the finite set size and many easily ordered distant pairs; keep
that size fixed and disclose it. These results describe the ordering layer
under finite inputs and the specified attack, not WAN performance, continuous
arrival liveness or general front-running resistance.

## Supplement existing runs without rerunning the protocol

The measured supplement and interpretation are in
[ORDERING_SUPPLEMENT_RESULTS.md](ORDERING_SUPPLEMENT_RESULTS.md).

```bash
python benchmark/analyze_ordering.py benchmark/results/ordering-1000-20260913
python -m unittest discover -s benchmark -p test_analyze_ordering.py
```

The standard-library postprocessor writes a new `supplement/` directory (or
`--output PATH`). Existing output directories are refused. Original measurements,
logs, plots and traces remain untouched. It verifies trace SHA-256 pairing and
independently recounts all original pair categories and fairness obligations;
any disagreement with saved counts stops analysis. No Go process is launched.

The three required additions are:

- **Obligation coverage:** eligible obligations / M(M-1)/2, separately for each
  (r,b,seed), plus checked/eligible, violations, and the fraction of checked
  obligations satisfied in the same batch. At most one direction per pair can
  qualify under the supported quorum constraint. Same-batch satisfaction is
  permitted, not strict order preservation. In incomplete outputs, an eligible
  obligation whose successor is uncommitted is not checked; this stays visible.
- **Generation-position scopes:** d=1, d<=5, d<=10, and all pairs, where d=j-i
  in generation order. These are overlapping scopes, not disjoint bins or
  final-commit adjacency. Each reports strict/tie/reverse over completed pairs
  in that scope, unresolved counts, and completed/all coverage. Generation
  position is a proxy for proximity, not a fixed elapsed-time window.
- **Batch coarseness:** number of nonempty committed fairness batches, largest
  batch size, and largest batch size / M, alongside existing same-batch fractions.

Two inexpensive supplementary views are also exported. Honest pairwise receipt
disagreement equals sum over transaction pairs of s(H-s), divided by
choose(H,2)*choose(M,2), where H=n-b and s honest nodes originally saw i before j.
This averages normalized Kendall disagreement across honest replica pairs.
It differs from the fraction of transaction pairs with *any* disagreement,
which is also exported. Both exclude the last b nodes; the original full trace
is unchanged across b, while the honest subset changes.

The paired-b=0 table reports baseline strict relations staying strict, becoming
tied, reversing, or becoming unresolved. Direction is relative to the baseline,
which can itself disagree with generation order. Baseline ties are excluded;
zero reference pairs give null/NA and zero valid seeds, not a zero attack effect.

`cases` and `pair_scopes` CSV/JSON contain per-seed values and denominators.
`case_summary` and `pair_summary` contain means, sample SD and valid-seed counts.
`tables.md` provides all groups for review. Counts marked sum aggregate seeds;
coverage means average per-seed fractions. Seeds are the independent input
samples; overlapping pair scopes and pairs sharing transactions are not
independent observations. This supplement retains the original three seeds
and makes descriptive claims, without significance tests.

For n=10, F=2, feasibility requires 2*qH>=15, so qH=8=n-F is the only feasible
integer honest-support threshold. Gamma=0.9 and gamma=1 both give qH=8. Choosing
gamma=1 therefore does not omit a different feasible threshold in this setup.
This statement is specific to these n,F values, not general gamma sensitivity.

## Matched evidence against the saved Themis replay

`TestOrderingMatchedThemis` adds only the nine r=10 cases (seeds 1/7/19,
b=0/1/2). It reads the original traces and existing Themis JSONL, uses full
1000-transaction LOs, and selects senders 2..9 for a single proposal. Both
implementations therefore consume the same eight transaction-order lists,
including all actual attackers reversing their complete lists. Normal callers
of `runOrdering` retain the original rotating, 200-transaction-chunk experiment.
Production code and the AWS settings are unchanged.

Example from the AUTIG repository root (create a new output directory first):

```powershell
$env:GOMAXPROCS='1'
go test ./pkg/ofo -run '^TestOrderingMatchedThemis$' -count=1 -v -timeout=10m -args -ordering-matched-input=./benchmark/results/ordering-1000-20260913 -ordering-matched-themis=/path/to/themis/benchmark/ordering-results/20260914-native-replay-final/measurements.jsonl -ordering-matched-output=/path/to/new-output/measurements.jsonl
```

The output file must not already exist. The test verifies saved input SHA-256,
transaction identities, exact selected lists (against the saved Themis reports),
all follower candidate checks, replica agreement, and completion in one proposal.
It records whether **every transaction's batch index** matches Themis; a
different partition is retained for analysis, not filtered out as a failed run.

AUTIG's original honest-support oracle remains in place (qH=8). A separate
shared oracle uses all ten original receipt profiles and threshold ten for
both outputs, and checks the saved Themis oracle counts by recomputation.
The oracle-only parameter choice does not change execution F=2 or actual b.
Neither comparison asserts equal within-SCC execution order: Themis uses its
native linearization, while AUTIG's same-batch ordering is deterministic by ID.

The outcome and comparison with the original native configurations are in
[ORDERING_MATCHED_RESULTS.md](ORDERING_MATCHED_RESULTS.md).
