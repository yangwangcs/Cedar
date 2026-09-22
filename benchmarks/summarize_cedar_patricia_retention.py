#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict

SEEDS = (20260920, 20260921, 20260922)


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(11)


def evaluate(rows, candidate, expected_samples=5):
    groups = defaultdict(list)
    hashes = defaultdict(set)
    expected_ops = 131072 if expected_samples == 5 else 64
    for row in rows:
        try:
            variant = row["variant"]
            seed = int(row["seed"])
            writers = int(row["writers"])
            phase = row["phase"]
            if int(row["ops"]) != expected_ops:
                fail(f"operations mismatch for {variant}/{seed}/{phase}/{writers}")
            if int(row["errors"]) != 0:
                fail(f"nonzero errors for {variant}/{seed}/{phase}/{writers}")
            groups[(variant, seed, phase, writers)].append(
                (float(row["elapsed_ns"]), float(row["arena_bytes"])))
            hashes[(seed, phase, writers)].add(row["result_hash"])
        except (KeyError, ValueError) as error:
            fail(f"malformed matrix row: {error}")
    for key, values in groups.items():
        if len(values) != expected_samples:
            fail(f"sample count for {key}: got {len(values)}, expected {expected_samples}")
    for key, values in hashes.items():
        if len(values) != 1:
            fail(f"hash mismatch for {key}: {sorted(values)}")

    seeds = SEEDS if expected_samples == 5 else (20260920,)
    result = {"candidate": candidate, "seeds": {}, "all_gates_pass": True,
              "only_single_writer_failed": True}
    saw_single_writer_failure = False
    for seed in seeds:
        def med(variant, phase, writers, field):
            key = (variant, seed, phase, writers)
            if key not in groups:
                fail(f"sample count for {key}: got 0, expected {expected_samples}")
            return statistics.median(item[field] for item in groups[key])

        one = med(candidate, "memtable", 1, 0)
        byte = med("byte", "memtable", 1, 0)
        four = med(candidate, "memtable", 4, 0)
        four_skip = med("skiplist", "memtable", 4, 0)
        eight = med(candidate, "memtable", 8, 0)
        eight_skip = med("skiplist", "memtable", 8, 0)
        arena_ratios = {}
        arena_ok = True
        for writers in (1, 4, 8):
            ratio = med(candidate, "memtable", writers, 1) / med("nibble", "memtable", writers, 1)
            arena_ratios[str(writers)] = ratio
            arena_ok = arena_ok and ratio <= 1.25
        ratios = {"single_writer_vs_byte": one / byte,
                  "four_writer_vs_skiplist": four / four_skip,
                  "eight_writer_vs_skiplist": eight / eight_skip}
        gates = {"single_writer_vs_byte": ratios["single_writer_vs_byte"] <= 1.05,
                 "four_writer_vs_skiplist": ratios["four_writer_vs_skiplist"] <= 0.80,
                 "eight_writer_vs_skiplist": ratios["eight_writer_vs_skiplist"] <= 0.85,
                 "arena": arena_ok}
        result["seeds"][str(seed)] = {
            "ratios": ratios, "arena_ratios": arena_ratios, "gates": gates,
            "vector_historical_gate": one <= 1.10 * med("vector", "memtable", 1, 0),
        }
        result["all_gates_pass"] &= all(gates.values())
        non_single_gates_pass = all(
            value for name, value in gates.items()
            if name != "single_writer_vs_byte")
        if not gates["single_writer_vs_byte"]:
            saw_single_writer_failure = True
            result["only_single_writer_failed"] &= non_single_gates_pass
        else:
            result["only_single_writer_failed"] &= all(gates.values())
    result["only_single_writer_failed"] &= saw_single_writer_failure
    return result


def make_self_test_rows():
    rows = []
    specs = {"nibble": {1: 100, 4: 70, 8: 75}, "byte": {1: 100, 4: 90, 8: 90},
             "vector": {1: 13}, "skiplist": {4: 100, 8: 100}}
    for seed in SEEDS:
        for variant, writers_map in specs.items():
            for writers, elapsed in writers_map.items():
                for _ in range(5):
                    rows.append({"variant": variant, "seed": str(seed), "writers": str(writers),
                                 "phase": "memtable", "ops": "131072", "errors": "0",
                                 "elapsed_ns": str(elapsed), "arena_bytes": "100",
                                 "result_hash": f"hash-{seed}"})
    return rows


def self_test():
    rows = make_self_test_rows()
    assert evaluate(rows, "nibble")["all_gates_pass"]
    one_fail = [{**row, "elapsed_ns": "106"} if row["variant"] == "nibble" and row["writers"] == "1" and row["seed"] == "20260920" else row for row in rows]
    assert evaluate(one_fail, "nibble")["only_single_writer_failed"]
    hard_fail = [{**row, "elapsed_ns": "81"} if row["variant"] == "nibble" and row["writers"] == "4" else row for row in rows]
    hard_result = evaluate(hard_fail, "nibble")
    assert not hard_result["all_gates_pass"] and not hard_result["only_single_writer_failed"]
    try:
        evaluate(rows[:-1], "nibble")
    except SystemExit as error:
        assert error.code == 11
    else:
        raise AssertionError("truncated matrix was accepted")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix")
    parser.add_argument("--candidate", choices=("nibble", "candidate"))
    parser.add_argument("--output")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.matrix or not args.candidate or not args.output:
        parser.error("--matrix, --candidate, and --output are required")
    with open(args.matrix, newline="") as stream:
        result = evaluate(csv.DictReader(stream), args.candidate, 1 if args.test_mode else 5)
    with open(args.output, "w", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
    if result["all_gates_pass"]:
        return 0
    return 10 if result["only_single_writer_failed"] else 11


if __name__ == "__main__":
    sys.exit(main())
