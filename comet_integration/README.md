# CometBFT integration

This module composes AUTIG and a native Themis-style reference with CometBFT
to check BFT commitment and native-state agreement.

## Running

Run commands from the repository root. A short composition check:

```sh
python scripts/reproduce.py smoke --committee 10
```

The complete experiment matrices:

```sh
python scripts/reproduce.py comet --committee 5
python scripts/reproduce.py comet --committee 10
```

Outputs are written under the ignored `runtime/` directory.

## Configuration and metrics

| Configuration | Five validators | Ten validators |
| --- | --- | --- |
| Fault budget | 1 | 2 |
| Fairness parameter | 1 | 1 |
| Assignment delay | 2 fragments | 2 fragments |
| Transactions per run | 3,000 | 3,000 |
| Nominal issue rate | 200/s | 100/s |
| Disagreement | 0.1, 100 | 0.1, 100 |
| Seeds | 1, 7, 19 | 1, 7, 19 |
| Modes | Certificate, retained/undo, reconstruction, Themis-style | Certificate, reconstruction, Themis-style |

The matrices are separate within-committee comparisons. All modes within one
configuration share the workload settings, report interval, admission limits
and commit interface. Actual issue rate, all-transaction execution latency,
drain throughput, serialized record size, ABCI validation time, completion and
native-state agreement are recorded by the driver.

Follower validation work is total validation wall time across native followers
divided by their total nonempty committed-block count. It includes first
validation and repeated checks of the same pending candidate across ABCI hooks.
Receipt reports and transaction contents use the local adapter; validator
consensus traffic uses TCP loopback.

The runner records the original process exit code. If the CometBFT background
peer-query routine encounters a closed LevelDB during shutdown after the
checked result has been written, the output is preserved with a distinct
shutdown annotation. Other failures stop the runner.

For AWS deployment of the networked ordering layer, see the
[deployment guide](../benchmark/README.md). Other experiment commands are in
[EXPERIMENTS.md](../EXPERIMENTS.md).
