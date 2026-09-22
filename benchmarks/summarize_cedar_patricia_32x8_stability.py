#!/usr/bin/env python3
import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict


SEEDS = (20260920, 20260921, 20260922, 20260923, 20260924)
VARIANTS_BY_WRITERS = {
    1: ("nibble", "byte", "baseline", "candidate"),
    4: ("nibble", "skiplist", "baseline", "candidate"),
    8: ("nibble", "skiplist", "baseline", "candidate"),
}


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(11)


def at_most(value, limit):
    return value < limit or math.isclose(value, limit, rel_tol=1e-12,
                                         abs_tol=1e-12)


def evaluate(rows, test_mode=False):
    seeds = (20260920,) if test_mode else SEEDS
    expected_ops = 64 if test_mode else 131072
    expected_samples = {1: 1, 4: 1, 8: 2} if test_mode else {1: 5, 4: 5, 8: 10}
    groups = defaultdict(list)
    hashes = defaultdict(set)
    repeats = defaultdict(set)

    for row in rows:
        if None in row or any(value is None for value in row.values()):
            fail("malformed matrix row")
        try:
            variant = row["variant"]
            seed = int(row["seed"])
            writers = int(row["writers"])
            repeat = int(row["repeat"])
            phase = row["phase"]
            elapsed_ns = float(row["elapsed_ns"])
            arena_bytes = float(row["arena_bytes"])
            operations = int(row["ops"])
            errors = int(row["errors"])
            result_hash = row["result_hash"]
        except (KeyError, TypeError, ValueError) as error:
            fail(f"malformed matrix row: {error}")
        if seed not in seeds or writers not in VARIANTS_BY_WRITERS:
            fail(f"unexpected seed/writers: {seed}/{writers}")
        if variant not in VARIANTS_BY_WRITERS[writers] or phase != "memtable":
            fail(f"unexpected variant/phase: {variant}/{phase}/{writers}")
        if operations != expected_ops or errors != 0 or not result_hash:
            fail(f"invalid result for {variant}/{seed}/{writers}/{repeat}")
        key = (variant, seed, writers)
        if repeat in repeats[key]:
            fail(f"duplicate repeat for {key}: {repeat}")
        repeats[key].add(repeat)
        groups[key].append((elapsed_ns, arena_bytes))
        hashes[(seed, writers, repeat)].add(result_hash)

    for seed in seeds:
        for writers, variants in VARIANTS_BY_WRITERS.items():
            expected_repeat_set = set(range(1, expected_samples[writers] + 1))
            for variant in variants:
                key = (variant, seed, writers)
                if repeats[key] != expected_repeat_set:
                    fail(f"sample repeats for {key}: got {sorted(repeats[key])}, "
                         f"expected {sorted(expected_repeat_set)}")
            for repeat in expected_repeat_set:
                hash_key = (seed, writers, repeat)
                if len(hashes[hash_key]) != 1:
                    fail(f"hash mismatch for {hash_key}: {sorted(hashes[hash_key])}")

    result = {"seeds": {}, "all_gates_pass": True}
    for seed in seeds:
        summaries = {}
        for writers, variants in VARIANTS_BY_WRITERS.items():
            for variant in variants:
                values = groups[(variant, seed, writers)]
                elapsed = [item[0] for item in values]
                arena = [item[1] for item in values]
                summaries[f"{variant}/{writers}"] = {
                    "samples": len(values),
                    "elapsed_ns_median": statistics.median(elapsed),
                    "elapsed_ns_pstdev": statistics.pstdev(elapsed),
                    "arena_bytes_median": statistics.median(arena),
                    "result_hashes": sorted(
                        {next(iter(hashes[(seed, writers, repeat)]))
                         for repeat in repeats[(variant, seed, writers)]}),
                }

        def metric(variant, writers, name):
            return summaries[f"{variant}/{writers}"][name]

        ratios = {
            "one_writer_vs_byte":
                metric("candidate", 1, "elapsed_ns_median") /
                metric("byte", 1, "elapsed_ns_median"),
            "four_writer_vs_skiplist":
                metric("candidate", 4, "elapsed_ns_median") /
                metric("skiplist", 4, "elapsed_ns_median"),
            "eight_writer_vs_skiplist":
                metric("candidate", 8, "elapsed_ns_median") /
                metric("skiplist", 8, "elapsed_ns_median"),
        }
        arena_ratios = {
            str(writers): metric("candidate", writers, "arena_bytes_median") /
                          metric("nibble", writers, "arena_bytes_median")
            for writers in (1, 4, 8)
        }
        candidate_cv = {}
        for writers in (1, 4, 8):
            mean = statistics.mean(
                item[0] for item in groups[("candidate", seed, writers)])
            if mean <= 0:
                fail(f"nonpositive candidate mean for {seed}/{writers}")
            candidate_cv[str(writers)] = (
                metric("candidate", writers, "elapsed_ns_pstdev") / mean)
        gates = {
            "one_writer_vs_byte": at_most(ratios["one_writer_vs_byte"], 1.05),
            "four_writer_vs_skiplist": at_most(
                ratios["four_writer_vs_skiplist"], 0.80),
            "eight_writer_vs_skiplist": at_most(
                ratios["eight_writer_vs_skiplist"], 0.85),
            "eight_writer_cv": at_most(candidate_cv["8"], 0.05),
            "arena_1": at_most(arena_ratios["1"], 1.25),
            "arena_4": at_most(arena_ratios["4"], 1.25),
            "arena_8": at_most(arena_ratios["8"], 1.25),
        }
        result["seeds"][str(seed)] = {
            "groups": summaries,
            "ratios": ratios,
            "arena_ratios": arena_ratios,
            "candidate_cv": candidate_cv,
            "gates": gates,
        }
        result["all_gates_pass"] = result["all_gates_pass"] and all(gates.values())
    return result


def make_self_test_rows():
    rows = []
    elapsed = {
        ("nibble", 1): [100] * 5,
        ("byte", 1): [100] * 5,
        ("baseline", 1): [104] * 5,
        ("candidate", 1): [105] * 5,
        ("nibble", 4): [80] * 5,
        ("skiplist", 4): [100] * 5,
        ("baseline", 4): [82] * 5,
        ("candidate", 4): [80] * 5,
        ("nibble", 8): [80] * 10,
        ("skiplist", 8): [100] * 10,
        ("baseline", 8): [88] * 10,
        ("candidate", 8): [80.75] * 5 + [89.25] * 5,
    }
    for seed in SEEDS:
        for (variant, writers), values in elapsed.items():
            for repeat, value in enumerate(values, 1):
                arena = 100 if variant == "nibble" else 125
                rows.append({
                    "variant": variant,
                    "seed": str(seed),
                    "writers": str(writers),
                    "repeat": str(repeat),
                    "phase": "memtable",
                    "ops": "131072",
                    "errors": "0",
                    "elapsed_ns": str(value),
                    "arena_bytes": str(arena),
                    "result_hash": f"hash-{seed}-{writers}",
                })
    return rows


def require_malformed(rows, label):
    try:
        evaluate(rows)
    except SystemExit as error:
        assert error.code == 11
    else:
        raise AssertionError(f"{label} was accepted")


def self_test():
    rows = make_self_test_rows()
    exact = evaluate(rows)
    assert exact["all_gates_pass"]
    assert at_most(exact["seeds"]["20260920"]["candidate_cv"]["8"], 0.05)

    ratio_fail = [
        {**row, "elapsed_ns": "85.0001"}
        if row["variant"] == "candidate" and row["writers"] == "8" else row
        for row in rows
    ]
    assert not evaluate(ratio_fail)["all_gates_pass"]

    cv_values = [80.749915, 89.250085] * 5
    cv_index = 0
    cv_fail = []
    for row in rows:
        if row["variant"] == "candidate" and row["writers"] == "8":
            cv_fail.append({**row, "elapsed_ns": str(cv_values[cv_index % 10])})
            cv_index += 1
        else:
            cv_fail.append(row)
    assert not evaluate(cv_fail)["all_gates_pass"]

    arena_fail = [
        {**row, "arena_bytes": "125.0001"}
        if row["variant"] == "candidate" and row["writers"] == "4" else row
        for row in rows
    ]
    assert not evaluate(arena_fail)["all_gates_pass"]

    error_rows = [dict(row) for row in rows]
    error_rows[0]["errors"] = "1"
    require_malformed(error_rows, "nonzero errors")

    hash_rows = [dict(row) for row in rows]
    hash_rows[1]["result_hash"] = "different"
    require_malformed(hash_rows, "hash mismatch")

    missing = [
        row for row in rows
        if not (row["seed"] == "20260924" and
                row["variant"] == "candidate" and
                row["writers"] == "8" and row["repeat"] == "10")
    ]
    require_malformed(missing, "missing tenth eight-writer sample")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix")
    parser.add_argument("--output")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.matrix or not args.output:
        parser.error("--matrix and --output are required")
    try:
        with open(args.matrix, newline="", encoding="utf-8") as stream:
            result = evaluate(csv.DictReader(stream), args.test_mode)
    except OSError as error:
        fail(str(error))
    try:
        with open(args.output, "w", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write("\n")
    except OSError as error:
        fail(str(error))
    return 0 if result["all_gates_pass"] else 11


if __name__ == "__main__":
    sys.exit(main())
