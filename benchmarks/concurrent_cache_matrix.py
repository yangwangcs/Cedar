#!/usr/bin/env python3
"""Measure four concurrent Cedar lanes on a new dedicated copy per interval."""

import argparse
import json
from pathlib import Path
import shutil
import statistics
import subprocess

from cache_dataset_manifest import manifest
from columnar_cache_matrix import (
    capture, file_sha256, memory_events_delta, parse_concurrent_measurement,
    plan_runs, read_optional, snapshot, write_json,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--source-dataset", type=Path, required=True)
    parser.add_argument("--copy-directory", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--mount-kind", choices=("named-volume", "linux-bind", "overlay"), required=True)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--storage-read-working-bytes", type=int, default=64 << 20)
    parser.add_argument("--storage-reader-metadata-bytes", type=int, default=16 << 20,
                        help="experiment fixture reader-metadata pool; 0 keeps the profile automatic share")
    parser.add_argument("--query-memory-bytes", type=int, default=64 << 20)
    args = parser.parse_args()
    if (args.seconds < 1 or args.repetitions < 1 or args.timeout_seconds <= args.seconds or
            args.storage_read_working_bytes < 1 or args.query_memory_bytes < 1 or
            args.storage_reader_metadata_bytes < 0):
        parser.error("invalid duration, repetition or memory bounds")
    source, copies, output = (path.resolve() for path in
                              (args.source_dataset, args.copy_directory, args.output_directory))
    paths = (source, copies, output)
    if any(left == right or left in right.parents or right in left.parents
           for index, left in enumerate(paths) for right in paths[index + 1:]):
        parser.error("source, copies and evidence must not overlap")
    runner = args.runner.resolve()
    if not runner.is_file() or not source.is_dir() or copies.exists() or output.exists():
        parser.error("require existing runner/source and new copy/evidence directories")
    descriptor = json.loads((source / "cedar-cache-bench-manifest.json").read_text())
    if not descriptor.get("facts_flushed") or descriptor.get("entities", 0) < 1:
        parser.error("source must be a dedicated flushed fixture")
    original = manifest(source)
    copies.mkdir(parents=True)
    output.mkdir(parents=True)
    write_json(output / "environment.json", {
        "runner_sha256": file_sha256(runner),
        "engine_identity": read_optional(runner.parent / "cedar-engine-identity.txt"),
        "source": original, "descriptor": descriptor,
        "configuration": {key: str(value) if isinstance(value, Path) else value
                          for key, value in vars(args).items()},
        "interpretation": "debug-small-thresholds mechanism test; cold_unverified; not production latency",
        "experiment_fixture_metadata": "not a production default",
        "findmnt": capture(["findmnt", "-J", "-T", str(copies)]),
    })
    runs = []
    for index, plan in enumerate(plan_runs(args.repetitions, ["concurrent"])):
        name = f"{index:03d}-{plan['admission']}"
        directory = copies / name
        shutil.copytree(source, directory)
        initial = manifest(directory)
        if initial["manifest_sha256"] != original["manifest_sha256"]:
            raise RuntimeError("fixture copy differs before measurement")
        command = [str(runner), "--db-path", str(directory), "--write-existing",
                   "--workload", "concurrent", "--seconds", str(args.seconds),
                   "--entities", str(descriptor["entities"]), "--seed", str(descriptor["seed"]),
                   "--part-id", str(descriptor["part_id"]), "--cache-admission", plan["admission"],
                   "--storage-read-working-bytes", str(args.storage_read_working_bytes),
                   "--storage-reader-metadata-bytes", str(args.storage_reader_metadata_bytes),
                   "--query-memory-bytes", str(args.query_memory_bytes)]
        before = snapshot()
        try:
            process = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout_seconds)
            stdout, stderr, code = process.stdout, process.stderr, process.returncode
        except subprocess.TimeoutExpired as error:
            stdout, stderr, code = error.stdout or b"", error.stderr or b"", 124
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            stderr += "\nexternal timeout\n"
        except OSError as error:
            stdout, stderr, code = "", str(error), -1
        after = snapshot()
        (output / (name + ".csv")).write_text(stdout, encoding="utf-8")
        (output / (name + ".stderr.log")).write_text(stderr, encoding="utf-8")
        run = dict(plan, **parse_concurrent_measurement(stdout, code, plan["admission"]))
        run["memory_events_delta"] = memory_events_delta(before, after)
        if run["memory_events_delta"] and any(run["memory_events_delta"].get(key, 0)
                                               for key in ("oom", "oom_kill", "oom_group_kill")):
            run["success"] = False
            run["reason"] += "; interval OOM"
        runs.append(run)
        write_json(output / (name + ".json"), dict(run, command=command, before=before,
                                                   after=after, dataset_before=initial,
                                                   dataset_after=manifest(directory)))
        print(f"{name}: success={run['success']} {run['reason']}", flush=True)
    summaries = []
    for policy in ("lookup-only", "admit"):
        selected = [run for run in runs if run["admission"] == policy]
        group = {"policy": policy, "attempts": len(selected),
                 "successes": sum(run["success"] for run in selected)}
        for lane in ("point", "scan", "wal", "maintenance"):
            values = [int(run["lanes"][lane]["p99_us"]) for run in selected if run["success"]]
            group[lane + "_p99_us"] = ({"median": statistics.median(values), "min": min(values),
                                        "max": max(values)} if values else None)
        summaries.append(group)
    unchanged = manifest(source)["manifest_sha256"] == original["manifest_sha256"]
    write_json(output / "summary.json", {"groups": summaries, "source_unchanged": unchanged,
                                         "failed_intervals": [index for index, run in enumerate(runs)
                                                              if not run["success"]]})
    return 0 if unchanged and all(run["success"] for run in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
