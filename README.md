# AUTIG

AUTIG is a Go prototype of graph-based fair ordering for Byzantine
fault-tolerant systems. It uses replicas' authenticated transaction-receipt
observations to constrain ordering and separates the evidence that defines a
valid order from the graph used to compute it.

The ordering leader incrementally maintains an Unconfirmed-Transaction
Incremental Graph (UTIG). Followers verify structural certificates for the
exact closed output, including its predecessors and justified exclusions,
without maintaining the leader's graph. Certified continuation supports
transaction progress under continuing arrivals, and the commit interface
installs a verified candidate only when its exact digest is committed.

This repository contains the AUTIG implementation, AWS deployment scripts,
a Themis-style reference, CometBFT integration, and supporting tests.

## Quick start

Requirements: Go 1.24.5 or newer and Python 3.10 or newer for the experiment
scripts. Run the AUTIG tests from the repository root:

```sh
go test -short -count=1 ./...
```

## AWS deployment

Fabric supports local runs and single-region or multi-region AWS deployment.
Follow [benchmark/README.md](benchmark/README.md) to install the dependencies
and configure local settings and AWS access. From `benchmark/`:

```sh
fab local
fab remote
fab logs
```

Local configuration, keys and generated outputs are excluded from version
control.

## Experiments

See [EXPERIMENTS.md](EXPERIMENTS.md) for validation benchmarks, fairness replay
and model checking.
