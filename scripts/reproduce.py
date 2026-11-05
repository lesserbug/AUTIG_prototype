"""Reproduce source checks and experiments using relative, local paths."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(command, cwd, log_path, env):
    print("RUN", " ".join(map(str, command)), flush=True)
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(list(map(str, command)), cwd=cwd, env=env,
                                   stdout=log, stderr=subprocess.STDOUT,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    if completed.returncode:
        raise RuntimeError(f"Command exited {completed.returncode}; see {log_path.name}")


def verify_measurement(path, n, mode, rho, seed, count):
    data = json.loads(path.read_text(encoding="utf-8"))
    expected = dict(n=n, f=1 if n == 5 else 2, gamma=1, D=2,
                    mode=mode, rho=rho, seed=seed, transactions=count,
                    report_quorum=4 if n == 5 else 8)
    for key, value in expected.items():
        if data.get(key) != value:
            raise RuntimeError(f"Measurement metadata mismatch: {key}")
    if (data["completion_ratio"] != 1 or data["outstanding_final"] != 0
            or not data["all_native_states_agree"] or len(data["validators"]) != n):
        raise RuntimeError("Incomplete workload or native-state disagreement")
    if any(v["nonempty_blocks"] != len(data["blocks"]) for v in data["validators"]):
        raise RuntimeError("Nonempty block counts differ")
    if data.get("report_stats", {}).get("lo_capacity_hits", 0):
        raise RuntimeError("Report capacity was reached")


def run_comet(args, output, env):
    module = ROOT / "comet_integration"
    exe = output / ("autig_comet.exe" if os.name == "nt" else "autig_comet")
    run(["go", "build", "-mod=readonly", "-trimpath", "-o", exe, "."],
        module, output / "build.log", env)
    n = args.committee
    modes = ("certificate", "incremental-undo", "reconstruction", "themis") if n == 5 else ("certificate", "reconstruction", "themis")
    if args.task == "smoke":
        modes = ("certificate", "reconstruction", "themis")
    seeds = (1, 7, 19) if args.task == "comet" else (1,)
    rhos = (.1, 100) if args.task == "comet" else (100,)
    count = 3000 if args.task == "comet" else 400
    rate = 200 if args.task == "comet" and n == 5 else 100
    outcomes = []
    for index, seed in enumerate(seeds):
        ordered_modes = modes[index:] + modes[:index]
        for rho in rhos:
            for mode in ordered_modes:
                name = f"n{n}_rho{rho:g}_s{seed}_{mode}"
                path, log_path = output / f"{name}.json", output / f"{name}.log"
                command = [str(exe), "-matched-benchmark", "-n", str(n), "-f", str(1 if n == 5 else 2),
                           "-gamma", "1", "-D", "2", "-mode", mode, "-transactions", str(count),
                           "-rate", str(rate), "-rho", str(rho), "-seed", str(seed),
                           "-lo-size", "5000", "-output", str(path)]
                if n == 10:
                    command += ["-record-limit-mib", "96", "-rpc-timeout", "120s", "-workload-timeout", "180s"]
                print("RUN", name, flush=True)
                with log_path.open("w", encoding="utf-8") as log:
                    process = subprocess.Popen(command, cwd=module, env=env, stdout=log, stderr=subprocess.STDOUT,
                                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                    try:
                        code = process.wait(timeout=300)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        process.wait(timeout=10)
                        raise RuntimeError(f"External timeout: {name}")
                text = log_path.read_text(encoding="utf-8", errors="replace")
                lines = text.splitlines()
                shutdown = bool(code == 2 and len(lines) > 1
                                and lines[0].startswith("MATCHED n=")
                                and "all_states_agree=true" in lines[0]
                                and lines[1] == "panic: leveldb: closed"
                                and "queryMaj23Routine" in text)
                outcome = dict(run=name, exit_code=code, result_written=path.exists(),
                               post_output_shutdown_error=shutdown, measurement_verified=False)
                outcomes.append(outcome)
                if code == 0 or shutdown:
                    verify_measurement(path, n, mode, rho, seed, count)
                    outcome["measurement_verified"] = True
                (output / "run_status.json").write_text(json.dumps(outcomes, indent=2), encoding="utf-8")
                if not outcome["measurement_verified"]:
                    raise RuntimeError(f"Incomplete run: {name}; inspect its log")
                print(f"DONE {name} measurement_verified=true exit={code}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("task", choices=("check", "benchmarks", "campaign", "model", "smoke", "comet"))
    parser.add_argument("--committee", type=int, choices=(5, 10), default=10)
    parser.add_argument("--size", type=int, default=650, help="Transactions per fairness replay case")
    parser.add_argument("--offline", action="store_true", help="Use only cached Go dependencies")
    parser.add_argument("--output", type=Path, help="New output directory; must not already exist")
    args = parser.parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = args.output.resolve() if args.output else ROOT / "runtime" / f"{args.task}_{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env["GOWORK"] = "off"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if args.offline:
        env.update(GOPROXY="off", GOSUMDB="off")
    if args.task == "check":
        for name, folder in (("autig", ROOT), ("themis", ROOT / "themis_reference"), ("comet", ROOT / "comet_integration")):
            for label, command in (("build", ["go", "build", "-mod=readonly", "./..."]),
                                   ("vet", ["go", "vet", "-mod=readonly", "./..."]),
                                   ("test", ["go", "test", "-mod=readonly", "-short", "-count=1", "./..."])):
                run(command, folder, output / f"{name}_{label}.log", env)
    elif args.task == "benchmarks":
        family = "^(BenchmarkV2Productive|BenchmarkV2Alpha|BenchmarkV2History|BenchmarkV2Phases|BenchmarkV2ColdMaterialization|BenchmarkRevisionSharedForest|BenchmarkRevisionRejectionCost)$"
        run(["go", "test", "-mod=readonly", "./pkg/ofo", "-run", "^$", "-bench", family,
             "-benchmem", "-benchtime=5x", "-count=5", "-cpu=1", "-timeout=30m"],
            ROOT, output / "benchmarks.log", env)
    elif args.task == "campaign":
        run(["go", "test", "-mod=readonly", "./pkg/ofo", "-run", "^TestV2FairnessCampaign$",
             "-count=1", "-timeout=60m", "-args", "-v2-campaign", f"-v2-campaign-size={args.size}",
             f"-v2-campaign-output={output / 'campaign.json'}"], ROOT, output / "campaign.log", env)
    elif args.task == "model":
        run([sys.executable, ROOT / "models/check_certified_order.py", "--four", "--output", output / "model.json"],
            ROOT, output / "model.log", env)
    else:
        run_comet(args, output, env)
    print(f"COMPLETE {args.task}; output: {output}", flush=True)


if __name__ == "__main__":
    main()
