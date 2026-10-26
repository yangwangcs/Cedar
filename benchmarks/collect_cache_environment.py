#!/usr/bin/env python3
"""Collect read-only Docker/Linux cache context for a dedicated benchmark run."""

import argparse
import datetime
import json
from pathlib import Path
import platform
import subprocess


def capture(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True,
                                encoding="utf-8", errors="replace", timeout=30)
        return {"available": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"available": False, "error": str(error)}


def collect(container, data_directory, source_directory, build_directory,
            dataset_manifest=False):
    def inside(*command):
        return capture(["docker", "exec", container, *command])

    inspected = capture(["docker", "inspect", "--format",
                         "{{json .}}", container])
    if inspected["available"]:
        try:
            raw = json.loads(inspected.pop("stdout"))
            # Keep the raw runtime/storage configuration, without application env.
            inspected["runtime"] = {key: raw.get(key) for key in
                                    ("Id", "Name", "Driver", "Mounts", "State")}
            host = raw.get("HostConfig", {})
            inspected["limits"] = {key: host.get(key) for key in
                                  ("Memory", "MemorySwap", "MemoryReservation",
                                   "NanoCpus", "CpusetCpus", "BlkioWeight")}
        except (ValueError, KeyError) as error:
            inspected = {"available": False, "error": str(error)}
    cgroup = {}
    for name in ("memory.max", "memory.high", "memory.current", "memory.stat",
                 "memory.events", "memory.pressure", "io.stat", "io.pressure"):
        cgroup[name] = inside("cat", "/sys/fs/cgroup/" + name)
    build = inside("cat", build_directory.rstrip("/") + "/CMakeCache.txt")
    if build["available"]:
        lines = build.pop("stdout").splitlines()
        build["configuration"] = [line for line in lines if line.startswith(
            ("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:", "CEDAR_ROCKSDB_",
             "CEDAR_ENABLE_ASAN:", "CEDAR_ENABLE_UBSAN:"))]
    source = source_directory.rstrip("/")
    dataset = inside("python3", source + "/benchmarks/cache_dataset_manifest.py",
                     data_directory) if dataset_manifest else {"available": False,
                     "reason": "dataset manifest not requested"}
    if dataset["available"]:
        try:
            dataset["manifest"] = json.loads(dataset.pop("stdout"))
        except ValueError as error:
            dataset = {"available": False, "error": str(error)}
    return {
        "schema_version": 1,
        "collected_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "host": {"platform": platform.platform(), "system": platform.system()},
        "container": container,
        "data_directory": data_directory,
        "dataset": dataset,
        "data_directory_exists": inside("test", "-d", data_directory),
        "docker": inspected,
        "kernel": inside("uname", "-a"),
        "filesystem": inside("df", "-T", "--", data_directory),
        "mount": inside("findmnt", "-J", "-T", data_directory),
        "cgroup_v2": cgroup,
        "guest_devices": inside("cat", "/proc/diskstats"),
        "cgroup_membership": inside("cat", "/proc/self/cgroup"),
        "source_revision": inside("git", "-C", source, "rev-parse", "HEAD"),
        "source_changes": inside("git", "-C", source, "status", "--short"),
        "source_file_hashes": inside("sha256sum", source + "/CMakeLists.txt",
            source + "/src/storage/rocks/rocks_adapter.cc",
            source + "/src/engine/rocksdb/table/cedar_parquet/"
                     "cedar_parquet_table_reader.cc"),
        "build": build,
        "engine_identity": inside("cat", build_directory.rstrip("/") +
                                  "/cedar-engine-identity.txt"),
        "host_wsl": capture(["wsl.exe", "--version"])
                    if platform.system() == "Windows" else {"available": False},
        "interpretation": {
            "file_memory_scope": "whole cgroup, not this database",
            "device_io_scope": "guest device totals, not per database",
            "host_vhdx_io": "unavailable",
            "cold_state": "unverified",
            "docker_build_cache": "not a runtime data cache",
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", required=True)
    parser.add_argument("--data-directory", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--source-directory", default="/src/cedar")
    parser.add_argument("--build-directory", default="/build/retest-debug")
    parser.add_argument("--dataset-manifest", action="store_true",
                        help="stat dedicated dataset files; do not read payloads")
    arguments = parser.parse_args()
    report = collect(arguments.container, arguments.data_directory,
                     arguments.source_directory, arguments.build_directory,
                     arguments.dataset_manifest)
    arguments.output_directory.mkdir(parents=True, exist_ok=True)
    destination = arguments.output_directory / "environment.json"
    # Each run owns its evidence; never silently overwrite earlier observations.
    with destination.open("x", encoding="utf-8") as output:
        json.dump(report, output, ensure_ascii=True, indent=2)
        output.write("\n")
    print(destination)
    return 0 if report["data_directory_exists"]["available"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
