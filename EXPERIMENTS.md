# Reproducing experiments

Run commands from the repository root. Output files are generated locally;
the source package contains no previous measurement results.

## AWS and networked ordering

The Fabric tasks support local, single-region and multi-region runs of the
standalone ordering layer. See [benchmark/README.md](benchmark/README.md) for
configuration and the `fab local`, `fab remote` and `fab logs` commands.

## Build and regression checks

```sh
python scripts/reproduce.py check
```

For AUTIG alone:

```sh
go test -short -count=1 ./...
go vet ./...
```

Targeted continuation, authenticated-mutation, streaming-progress and refresh
tests can also be selected directly:

```sh
go test ./pkg/ofo -count=1 -run '^(TestV2CertifiedOrderCrossComponentCounterexample|TestV2NativePersistentChain|TestV2AuthenticatedContinuationAttacks|TestOmittedLocalOrderRetainsChunk|TestV2ProfileSmoke)$'
```

On a supported Linux Go toolchain with a C compiler, add `-race` to run the
same tests with race instrumentation.

## Matched validation and history benchmarks

```sh
python scripts/reproduce.py benchmarks
```

The benchmark families include:

| Family | Measurement |
| --- | --- |
| `BenchmarkV2Productive` | Nonempty-output validation across certificate, retained/undo and reconstruction modes |
| `BenchmarkV2Alpha` | Released-fraction sweep |
| `BenchmarkV2Phases` | Isolated validation stages |
| `BenchmarkV2History` | Fixed-live history growth |
| `BenchmarkV2ColdMaterialization` | Materialization from weights or receipt positions |
| `BenchmarkRevisionSharedForest` | Shared blocker-forest traversal |
| `BenchmarkRevisionRejectionCost` | Authenticated rejection and acceptance paths |

For one family:

```sh
go test ./pkg/ofo -run '^$' -bench '^BenchmarkV2Productive$' -benchmem -benchtime=5x -count=5 -cpu=1 -timeout=30m
```

## Fairness replay and authenticated fuzzing

```sh
python scripts/reproduce.py campaign
go test ./pkg/ofo -run '^$' -fuzz '^FuzzV2AuthenticatedContinuation$' -fuzztime=60s
```

The campaign uses 650 transactions per case and checks both assignment-prefix
fairness and physical execution against independently generated receipt
profiles. Use `--size 200` for a smaller replay campaign.

## Independent finite-state model

```sh
python scripts/reproduce.py model
```

The checker covers three- and four-transaction families and a five-replica
Byzantine family. It also evaluates an identifier-order reference rule as an
oracle control. The report distinguishes that rule from certified execution.

## Committed CometBFT execution

A short composition run:

```sh
python scripts/reproduce.py smoke --committee 10
```

The complete matrices:

```sh
python scripts/reproduce.py comet --committee 5
python scripts/reproduce.py comet --committee 10
```

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

The AWS Fabric tasks configure the standalone driver for separately hosted
replicas and collect their logs; see the deployment guide linked above.

## Checking the publication tree

Before uploading source contents:

```sh
python scripts/check_public_tree.py
```

This checks for credential material, private machine paths, non-loopback IPv4
endpoints, generated result files, binaries, databases and cache directories.
