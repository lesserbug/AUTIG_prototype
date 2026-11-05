# AUTIG

AUTIG is a Go prototype of graph-based fair ordering for Byzantine
fault-tolerant systems. It separates authenticated receipt evidence from
dependency-graph computation: the ordering leader maintains an incremental
graph, while followers validate structural certificates. Certified continuation
supports transaction progress under continuing arrivals.

This repository contains the AUTIG implementation, AWS deployment scripts,
a Themis-style reference, CometBFT integration, and supporting tests.

## Quick start

Requirements: Go 1.24.5 or newer and Python 3.10 or newer.
Run the following commands from the repository root:

```sh
python scripts/reproduce.py check
```

## AWS deployment

Fabric supports local runs and single-region or multi-region AWS experiments:

```sh
python -m pip install -r benchmark/requirements.txt
cd benchmark
cp settings.example.json settings.json
```

Configure your EC2 key pair, regions and workload in `settings.json`, and set
up AWS credentials through a local profile or environment variables. With the
configured EC2 instances running:

```sh
fab local
fab info
fab install
fab remote
fab logs
```

See [benchmark/README.md](benchmark/README.md) for setup, instance provisioning
and workload options. Local configuration, keys and generated outputs are
excluded from version control.

## CometBFT and other experiments

From the repository root, run a local CometBFT integration check:

```sh
python scripts/reproduce.py smoke --committee 10
```

See [EXPERIMENTS.md](EXPERIMENTS.md) for validation benchmarks, fairness replay,
model checking and CometBFT experiments.
