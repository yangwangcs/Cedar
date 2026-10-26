#!/usr/bin/env python3
"""Alternate read-only cache policies on one dedicated, already flushed dataset."""

import argparse
import csv
import datetime
import hashlib
import io
import json
from pathlib import Path
import platform
import statistics
import subprocess
import time

from cache_dataset_manifest import manifest


def plan_runs(repetitions, workloads):
    runs = []
    for repetition in range(repetitions):
        policies = ["lookup-only", "admit"] if repetition % 2 == 0 else ["admit", "lookup-only"]
        for workload in workloads:
            for policy in policies:
                runs.append({"round": repetition, "workload": workload, "admission": policy})
    return runs


def parse_measurement(stdout, returncode, workload, admission):
    result = {"success": False, "returncode": returncode, "reason": "", "row": {}}
    try:
        rows = list(csv.DictReader(io.StringIO(stdout)))
        if len(rows) != 1:
            raise ValueError("expected exactly one CSV measurement")
        row = rows[0]
        result["row"] = row
        if None in row or any(value is None for value in row.values()):
            raise ValueError("incomplete or extra CSV cells")
        if row["workload"] != workload or row["cache_admission"] != admission:
            raise ValueError("measurement does not match requested workload/policy")
        numeric = ("error", "operations", "rows", "checksum", "file_read_calls", "physical_bytes",
                   "column_decodes", "cache_hits", "cache_misses", "p50_us", "p95_us", "p99_us",
                   "rss_bytes", "resident_bytes", "pinned_bytes", "working_peak_bytes",
                   "working_rejections", "elapsed_us", "decode_us")
        values = {key: int(row[key]) for key in numeric}
        if any(value < 0 for value in values.values()):
            raise ValueError("negative measurement counter")
        if returncode != 0 or values["error"] != 0:
            raise ValueError("benchmark reported an error")
        if not values["operations"] or not values["rows"] or not values["elapsed_us"]:
            raise ValueError("benchmark produced no completed work")
        if row.get("measurement_phase") == "after_warmup":
            if any(int(row.get(key, "0")) <= 0 for key in
                   ("warmup_operations", "warmup_file_read_calls", "warmup_column_decodes")):
                raise ValueError("warm interval lacks initial Parquet decode evidence")
            if not values["file_read_calls"] and not values["cache_hits"]:
                raise ValueError("warm interval observed neither page reads nor cache hits")
        elif not values["file_read_calls"] or not values["column_decodes"]:
            raise ValueError("fresh reader did not observe Parquet page reads/decodes")
        if row["cache_state"] != "cold_unverified":
            raise ValueError("unsupported cache-state assertion")
        result["success"] = True
    except (ValueError, KeyError, TypeError, csv.Error) as error:
        result["reason"] = str(error)
    return result


def parse_concurrent_measurement(stdout, returncode, admission):
    result = {"success": False, "returncode": returncode, "reason": "", "lanes": {}}
    try:
        rows = list(csv.DictReader(io.StringIO(stdout)))
        if len(rows) != 4 or {row.get("lane") for row in rows} != {"point", "scan", "wal", "maintenance"}:
            raise ValueError("expected four distinct concurrent lanes")
        if returncode != 0:
            raise ValueError("benchmark failed or timed out")
        for row in rows:
            if None in row or any(value is None for value in row.values()):
                raise ValueError("incomplete or extra CSV cells")
            if row["workload"] != "concurrent" or row["cache_admission"] != admission:
                raise ValueError("concurrent workload/policy mismatch")
            positive = ("operations", "rows", "elapsed_us", "facts_compactions",
                        "four_lane_facts_compaction_overlap_ns", "wal_sync_writes",
                        "file_read_calls", "column_decodes")
            if any(int(row[key]) <= 0 for key in positive):
                raise ValueError("missing completed work, Parquet reads or facts compaction overlap")
            if any(int(row[key]) != 0 for key in ("errors", "wal_unsynced_writes", "background_errors")):
                raise ValueError("concurrent lane, WAL or background error")
            if row["accepted"] != "true":
                raise ValueError("benchmark rejected concurrent interval")
            if any(int(row[key]) < 0 for key in ("checksum", "p50_us", "p95_us", "p99_us")):
                raise ValueError("negative concurrent measurement")
            result["lanes"][row["lane"]] = row
        if int(result["lanes"]["wal"]["operations"]) > int(rows[0]["wal_sync_writes"]):
            raise ValueError("completed WAL operations lack synchronous write evidence")
        result["success"] = True
    except (ValueError, KeyError, TypeError, csv.Error) as error:
        result["reason"] = str(error)
    return result


def summarize(runs):
    groups = {}
    for run in runs:
        groups.setdefault((run["workload"], run["admission"]), []).append(run)
    summaries = []
    for (workload, admission), attempts in sorted(groups.items()):
        successful = [run["row"] for run in attempts if run["success"]]
        group = {"workload": workload, "admission": admission, "attempts": len(attempts),
                 "successes": len(successful), "failures": len(attempts) - len(successful)}
        for key in ("p50_us", "p95_us", "p99_us", "physical_bytes", "file_read_calls",
                    "column_decodes", "cache_hits", "cache_misses", "working_peak_bytes",
                    "rss_bytes", "operations", "decode_us"):
            values = [int(row[key]) for row in successful]
            group[key] = ({"median": statistics.median(values), "min": min(values), "max": max(values)}
                          if values else None)
        summaries.append(group)
    return {"groups": summaries,
            "failed_rounds": [run["round"] for run in runs if not run["success"]]}


def capture(command):
    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=30)
        return {"available": result.returncode == 0, "returncode": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"available": False, "error": str(error)}


def read_optional(path):
    try:
        return {"available": True, "text": Path(path).read_text(encoding="utf-8")}
    except OSError as error:
        return {"available": False, "reason": str(error)}


def snapshot():
    names = ("memory.max", "memory.high", "memory.current", "memory.stat", "memory.events",
             "memory.pressure", "io.stat", "io.pressure", "cpu.stat")
    return {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "cgroup": {name: read_optional("/sys/fs/cgroup/" + name) for name in names},
            "guest_diskstats": read_optional("/proc/diskstats")}


def memory_events_delta(before, after):
    try:
        counts = []
        for snapshot_value in (before, after):
            event = snapshot_value["cgroup"]["memory.events"]
            if not event["available"]:
                return None
            counts.append({key: int(value) for key, value in
                           (line.split() for line in event["text"].splitlines())})
        if any(key not in count for count in counts for key in ("oom", "oom_kill")):
            return None
        delta = {key: counts[1][key] - value for key, value in counts[0].items()
                 if key in counts[1]}
        return delta if all(value >= 0 for value in delta.values()) else None
    except (KeyError, TypeError, ValueError):
        return None


def write_json(path, value):
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2, ensure_ascii=True)
        output.write("\n")


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--mount-kind", choices=("overlay", "named-volume", "linux-bind", "windows-bind"),
                        required=True, help="operator label, retained beside actual findmnt evidence")
    parser.add_argument("--entities", type=int, required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--part-id", type=int, default=0)
    parser.add_argument("--sessions", type=int, default=1)
    parser.add_argument("--seconds", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--cache-bytes", type=int, default=64 << 20)
    parser.add_argument("--storage-read-working-bytes", type=int, default=64 << 20)
    parser.add_argument("--storage-reader-metadata-bytes", type=int, default=16 << 20,
                        help="experiment fixture reader-metadata pool; 0 keeps the factory/profile fallback")
    parser.add_argument("--query-memory-bytes", type=int, default=64 << 20)
    parser.add_argument("--warmup-operations", type=int, default=0)
    parser.add_argument("--workloads", nargs="+", choices=("point", "narrow", "wide", "temporal", "mixed"),
                        default=["point", "narrow", "wide", "temporal"])
    parser.add_argument("--prewarm-system", action="store_true",
                        help="buffered-read SST files before each run; does not prove residency")
    parser.add_argument("--smoke", action="store_true", help="allow Debug/fewer rounds; no performance claim")
    args = parser.parse_args()
    if (args.entities < 1 or args.seed < 0 or args.part_id < 0 or args.seconds < 1 or
            not 1 <= args.sessions <= 64 or args.repetitions < 1 or args.cache_bytes < 0 or
            args.storage_read_working_bytes < 1 or args.query_memory_bytes < 1 or
            args.storage_reader_metadata_bytes < 0 or
            not 0 <= args.warmup_operations <= 1024):
        parser.error("invalid workload bounds")
    if args.repetitions < 5 and not args.smoke:
        parser.error("performance runs require at least five repetitions")
    runner = args.runner.resolve()
    directory = args.db_path.resolve()
    destination = args.output_directory.resolve()
    if (not runner.is_file() or not directory.is_dir() or
            not (directory / "cedar-cache-bench-manifest.json").is_file()):
        parser.error("runner and existing dedicated flushed dataset are required")
    if destination == directory or destination in directory.parents or directory in destination.parents:
        parser.error("evidence and dataset directories must not overlap")
    identity = read_optional(runner.parent / "cedar-engine-identity.txt")
    if not args.smoke and (not identity["available"] or
                           "build_type=RelWithDebInfo\n" not in identity.get("text", "")):
        parser.error("performance runs require a recorded RelWithDebInfo engine identity")
    before = manifest(directory)
    descriptor_hash = hashlib.sha256((directory / "cedar-cache-bench-manifest.json").read_bytes()).hexdigest()
    destination.mkdir(parents=True, exist_ok=False)
    write_json(destination / "environment.json", {
        "platform": platform.platform(), "mount_kind_operator_label": args.mount_kind,
        "findmnt": capture(["findmnt", "-J", "-T", str(directory)]),
        "filesystem": capture(["df", "-T", "--", str(directory)]),
        "engine_identity": identity,
        "runner_sha256": file_sha256(runner),
        "dataset_descriptor_sha256": descriptor_hash, "dataset": before,
        "configuration": {key: str(value) if isinstance(value, Path) else value
                          for key, value in vars(args).items()},
        "experiment_fixture_metadata": "not a production default",
        "interpretation": {"cold_state": "unverified", "scope": "read-shape matrix only; no WAL/compaction",
                           "cgroup_scope": "whole cgroup, may include other work",
                           "guest_io_scope": "whole guest, not per dataset or host VHDX",
                           "prewarm_residency": "unverified", "smoke_only": args.smoke,
                           "interval": "after explicit warmup" if args.warmup_operations else
                                       "fresh decoded reader plus repeated reads in each process"}})
    runs = []
    for index, plan in enumerate(plan_runs(args.repetitions, args.workloads)):
        prefix = destination / (str(index).zfill(3) + "-" + plan["workload"] + "-" + plan["admission"])
        prewarm = {"requested": args.prewarm_system, "bytes": 0, "elapsed_seconds": 0}
        if args.prewarm_system:
            started = time.monotonic()
            for file in before["files"]:
                if not file["path"].endswith(".sst"):
                    continue
                with (directory / file["path"]).open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        prewarm["bytes"] += len(chunk)
            prewarm["elapsed_seconds"] = time.monotonic() - started
        command = [str(runner), "--db-path", str(directory), "--read-existing", "--entities", str(args.entities),
                   "--seed", str(args.seed), "--part-id", str(args.part_id), "--sessions", str(args.sessions),
                   "--seconds", str(args.seconds), "--workload", plan["workload"],
                   "--cache-admission", plan["admission"], "--cache-bytes", str(args.cache_bytes),
                   "--storage-read-working-bytes", str(args.storage_read_working_bytes),
                   "--storage-reader-metadata-bytes", str(args.storage_reader_metadata_bytes),
                   "--query-memory-bytes", str(args.query_memory_bytes),
                   "--warmup-operations", str(args.warmup_operations)]
        start = snapshot()
        try:
            result = subprocess.run(command, text=True, capture_output=True, timeout=args.seconds + 180)
            stdout, stderr, returncode = result.stdout, result.stderr, result.returncode
        except (OSError, subprocess.TimeoutExpired) as error:
            stdout, stderr, returncode = "", str(error), -1
        end = snapshot()
        with prefix.with_suffix(".csv").open("x", encoding="utf-8") as output:
            output.write(stdout)
        with prefix.with_suffix(".stderr.log").open("x", encoding="utf-8") as output:
            output.write(stderr)
        run = dict(plan, **parse_measurement(stdout, returncode, plan["workload"], plan["admission"]))
        run["memory_events_delta"] = memory_events_delta(start, end)
        if run["memory_events_delta"] is not None:
            oom = {key: value for key, value in run["memory_events_delta"].items()
                   if key in ("oom", "oom_kill", "oom_group_kill") and value}
            if oom:
                run["success"] = False
                run["reason"] = "; ".join(filter(None, (run["reason"], f"interval OOM: {oom}")))
        run["index"] = index
        runs.append(run)
        write_json(prefix.with_suffix(".json"), dict(run, command=command, before=start, after=end,
                                                     prewarm=prewarm))
        print(f"{index + 1}: {plan['workload']} {plan['admission']} success={run['success']}", flush=True)
    after = manifest(directory)
    unchanged = before["manifest_sha256"] == after["manifest_sha256"]
    report = dict(summarize(runs), dataset_unchanged=unchanged, dataset_after=after,
                  smoke_only=args.smoke, cache_state="cold_unverified")
    write_json(destination / "summary.json", report)
    return 0 if unchanged and all(run["success"] for run in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
