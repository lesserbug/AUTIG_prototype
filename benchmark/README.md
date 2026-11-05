# AWS ordering-layer benchmarks

Fabric tasks build and run the standalone AUTIG ordering layer on local or AWS
hosts, then collect per-node logs and workload measurements.

## Setup

From the repository root:

```sh
python -m pip install -r benchmark/requirements.txt
cd benchmark
cp settings.example.json settings.json
```

In PowerShell, use `Copy-Item settings.example.json settings.json` instead of
`cp`. Edit the local `settings.json` before running AWS tasks:

- `key.name` and `key.path`: EC2 key-pair name and local SSH private-key path.
- `instances.regions`: one region for a single-region run, or several regions
  for a geographically distributed run.
- `testbed`: the EC2 `Name` tag used to select this experiment's instances.
- `benchmark.remote`: committee sizes, offered rates, repetitions and protocol
  parameters. `nodes` and `rate` are lists; `byzantine_count` is also a list.
- `repo`: the public repository and branch built on the remote hosts.

AWS authentication uses the standard [Boto3 credential chain](https://docs.aws.amazon.com/boto3/latest/guide/credentials.html),
including local profiles and environment variables. Credentials are not placed
in settings.
Use Ubuntu x86-64 instances reachable by SSH. The controller connects over
public addresses; allow the configured protocol TCP port between the hosts and
SSH from the controller. For multiple regions, the selected EC2 key pair must
be available in each region.

## Commands

Run from `benchmark/`:

```sh
fab --list
fab local
fab info
fab install
fab remote
fab logs
```

`local` builds and runs the local configuration without contacting AWS.
`info` lists tagged instances. `install` installs Go and builds AUTIG on running
instances. `remote` updates and builds the configured branch, spreads replicas
across the configured regions, runs the workload matrix, and downloads logs.
`logs` summarizes logs already downloaded. Each remote replica receives its
own generated signing key and the public verification keys.

To provision instances with the included helper, `fab create --nodes 5`
requests five instances **in each configured region**. These are billable EC2
resources. Wait until they are running and SSH-ready before `fab install`.
`fab stop` stops tagged running instances; `fab start` restarts stopped ones;
`fab destroy` terminates tagged instances. Use a dedicated `testbed` tag.
`fab kill` stops benchmark sessions without stopping the instances.

For a committee-size sweep, set `benchmark.remote.nodes` to the desired sizes,
for example `[5, 10, 30, 60]`, and provide enough running tagged instances.
Each committee must satisfy the configured AUTIG integer thresholds.
`continuation_delay` selects the assignment delay and `verification_mode`
selects `certificate`, `incremental-undo`, or `reconstruction`.

## Outputs

Local-only generated files are stored in `.build/`, `.runtime/`, `logs/` and
`results/`. Run summaries include actual offered rate, throughput, completed
transaction latency, completion ratio, outstanding transactions, and committed
state agreement across replicas. Stage timings can be enabled with
`stage_timing: true` in the corresponding benchmark configuration.

These tasks run the networked ordering layer. The separate CometBFT commands
in the root experiment guide exercise BFT consensus and committed execution.
Local settings, keys and generated outputs are excluded from version control.
