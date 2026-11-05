# AUTIG

AUTIG is a Go prototype of graph-based fair ordering for Byzantine
fault-tolerant systems. It separates authenticated receipt evidence from
dependency-graph computation: the ordering leader maintains an incremental
graph, while followers validate structural certificates. Certified continuation
supports transaction progress under continuing arrivals.

This repository contains the AUTIG implementation, a Themis-style reference,
CometBFT integration, and supporting tests and experiment scripts.

## Quick start

Requirements: Go 1.24.5 or newer and Python 3.10 or newer.
Run the following commands from the repository root:

```sh
python scripts/reproduce.py check
python scripts/reproduce.py smoke --committee 10
```

See [EXPERIMENTS.md](EXPERIMENTS.md) for experiment and reproduction commands.
Generated outputs are stored locally under `runtime/`.
