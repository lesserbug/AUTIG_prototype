# AUTIG research prototype

AUTIG implements evidence-based ordering, structural certificate construction
and verification, and local evaluation tools in Go.

This directory is a sanitized publication candidate. It contains the current
working-tree source and tests, including local changes; it is not an exact
export of an existing Git commit. No original Git history is included.

## Requirements and quick checks

- Go 1.22.2 or later (the module uses only the Go standard library).
- Python 3 for the optional experiment runners. AWS/Fabric controller
  dependencies are listed in `benchmark/requirements.txt`.

From this directory:

```sh
go build ./...
go test ./... -short -count=1
go test ./pkg/ofo -run '^TestProposalRegression' -count=1 -v
```

To check the optional Python controller and analysis tools, use a Python
virtual environment and run:

```sh
python -m pip install -r benchmark/requirements.txt
python -B -m unittest discover -s benchmark -p 'test_*.py'
```

These Python unit tests do not launch AWS instances.

Some tests intentionally skip missing protocol modules or require explicit
experiment inputs. A passing test suite is not a full protocol safety proof.
The continuous-history test is skipped with `-short`; run it explicitly with:

```sh
go test ./pkg/ofo -run '^TestHistoryContinuousCommit$' -count=1 -cpu=1 -timeout=15m -v
```

For the fixed-live history-size microbenchmark:

```sh
go test ./pkg/ofo -run '^TestHistorySizeFixture$' -count=1 -cpu=1 -timeout=10m
go test ./pkg/ofo -run '^$' -bench '^BenchmarkHistorySize$' -benchmem -benchtime=10x -count=5 -cpu=1 -timeout=15m
```

Existing `pkg/ofo/history_*.txt` and `boundary_history_*.txt` are historical
measurement/test outputs, not measurements from this export validation. Runs
from different sessions or platforms should not be combined into one timing
series. These logs are retained unchanged.

## Scope and limitations

The repository includes candidate verification and an explicit `CommitPending`
hosting interface. The benchmark hosting adapter uses a fixed leader; it does
not implement a complete BFT consensus or leader-replacement protocol.
OrderFault evidence generation/independent validation, committed handoff,
fragment-wait deadline integration, and checkpoint recovery are not implemented.

The current finalized-state representation copies historical metadata and
recomputes the finalized-set commitment. Fixed-live experiments reveal
history-dependent transition costs, including at one million finalized IDs.
Avoiding persistent follower dependency graphs does not make the complete
authoritative state history-independent.

The Go regression fixtures generate their own test credentials. To generate
fresh deployment/test keys locally, use:

```sh
go run ./cmd/autig-keygen -replicas 5 -out keys
```

Never reuse keys from old test artifacts in a deployment. No key files are
distributed in this candidate.

## Experiments and configuration

See [benchmark/README.md](benchmark/README.md) for the benchmark controller and
[benchmark/ORDERING_EXPERIMENT.md](benchmark/ORDERING_EXPERIMENT.md) for local
ordering experiments. The Python runners and result summaries are included.
Full AWS logs/results and external Themis replay inputs are not included;
matched comparisons need those separately supplied inputs. This export alone
does not reproduce every result in the paper.

`pkg/config.json` contains loopback addresses. `benchmark/settings.json`
contains placeholder SSH settings; configure these before deployment.
Preparing this candidate does not run AWS or change experiment algorithms.

Excluded from this export: manuscript/review/revision notes, internal audit
reports, temporary directories and environments, binary/profile artifacts,
private/public key files, local deployment addresses, and original Git history.
Only example configuration and personal paths in documentation were sanitized;
Go and Python source files were copied unchanged.

## Publication status

A license has not yet been selected. Before describing this repository as
open source, the code rights holders must authorize distribution and add the
chosen license, preserving any applicable third-party notices. This candidate
has not been uploaded or pushed. Cite an immutable release/commit when the
publication version is finalized.
