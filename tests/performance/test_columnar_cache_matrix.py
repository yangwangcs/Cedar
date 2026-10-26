import csv
import io
from pathlib import Path
import sys
import unittest
import hashlib
import tempfile
import json
import subprocess
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "benchmarks"))
from columnar_cache_matrix import file_sha256, parse_measurement, plan_runs, summarize
import columnar_cache_matrix as matrix


def output(**changes):
    row = dict(workload="narrow", cache_admission="admit", cache_state="cold_unverified",
               error="0", operations="10", rows="30", checksum="120", file_read_calls="3",
               physical_bytes="100", column_decodes="2", cache_hits="50", cache_misses="2",
               p50_us="10", p95_us="20", p99_us="30", rss_bytes="4096",
               resident_bytes="200", pinned_bytes="0", working_peak_bytes="1000",
               working_rejections="0", elapsed_us="1000000", device_read_bytes="", decode_us="7")
    row.update(changes)
    result = io.StringIO()
    writer = csv.DictWriter(result, fieldnames=row)
    writer.writeheader()
    writer.writerow(row)
    return result.getvalue()


class CacheMatrixTest(unittest.TestCase):
    def test_concurrent_acceptance_requires_all_lanes_and_real_overlap(self):
        def concurrent(**changes):
            rows = []
            for lane in ("point", "scan", "wal", "maintenance"):
                row = dict(workload="concurrent", lane=lane, operations="2", rows="3",
                           checksum="7", errors="0", cache_admission="lookup-only",
                           accepted="true", facts_compactions="1",
                           four_lane_facts_compaction_overlap_ns="10", wal_sync_writes="2",
                           wal_unsynced_writes="0", background_errors="0",
                           file_read_calls="3", column_decodes="2", elapsed_us="100",
                           p50_us="1", p95_us="2", p99_us="3")
                row.update(changes)
                rows.append(row)
            stream = io.StringIO()
            writer = csv.DictWriter(stream, fieldnames=rows[0])
            writer.writeheader()
            writer.writerows(rows)
            return stream.getvalue()

        parse = getattr(matrix, "parse_concurrent_measurement", None)
        self.assertIsNotNone(parse, "concurrent results need four-lane validation")
        self.assertTrue(parse(concurrent(), 0, "lookup-only")["success"])
        for changes in ({"errors": "1"}, {"operations": "0"}, {"accepted": "false"},
                        {"facts_compactions": "0"}, {"wal_unsynced_writes": "1"},
                        {"four_lane_facts_compaction_overlap_ns": "0"},
                        {"lane": "point"}, {"column_decodes": "0"}):
            self.assertFalse(parse(concurrent(**changes), 0, "lookup-only")["success"])
        self.assertFalse(parse(concurrent(), 124, "lookup-only")["success"])
        self.assertFalse(parse(concurrent(), 0, "admit")["success"])

    def test_interval_oom_fails_even_when_benchmark_returns_correct_rows(self):
        def counters(kills):
            return {"cgroup": {"memory.events": {"available": True,
                    "text": f"low 0\nhigh 0\nmax 20\noom {kills}\noom_kill {kills}\n"}}}

        def benchmark(command, **kwargs):
            policy = command[command.index("--cache-admission") + 1]
            return subprocess.CompletedProcess(command, 0,
                                               output(cache_admission=policy), "")

        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            dataset = root / "dataset"
            dataset.mkdir()
            (dataset / "cedar-cache-bench-manifest.json").write_text("{}")
            runner = root / "runner"
            runner.write_bytes(b"fixture runner")
            destination = root / "results"
            arguments = ["matrix", "--runner", str(runner), "--db-path", str(dataset),
                         "--output-directory", str(destination), "--mount-kind", "named-volume",
                         "--entities", "10", "--workloads", "narrow", "--repetitions", "1",
                         "--smoke"]
            with mock.patch.object(sys, "argv", arguments), \
                 mock.patch.object(matrix.platform, "platform", return_value="fixture"), \
                 mock.patch.object(matrix, "capture", return_value={"available": False}), \
                 mock.patch.object(matrix, "snapshot", side_effect=[counters(2), counters(3),
                                                                    counters(3), counters(3)]), \
                 mock.patch.object(matrix.subprocess, "run", side_effect=benchmark):
                result = matrix.main()
            self.assertEqual(result, 1, "an interval OOM must invalidate a correct CSV")
            failed = json.loads((destination / "000-narrow-lookup-only.json").read_text())
            self.assertFalse(failed["success"])
            self.assertIn("oom", failed["reason"])
            self.assertEqual(failed["memory_events_delta"]["oom_kill"], 1)
            unchanged = json.loads((destination / "001-narrow-admit.json").read_text())
            self.assertTrue(unchanged["success"], "historical OOM must not fail later intervals")
            summary = json.loads((destination / "summary.json").read_text())
            self.assertEqual(summary["failed_rounds"], [0])

    def test_runner_hash_streams_large_binary(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "runner"
            payload = b"cedar\x00" * 400000
            path.write_bytes(payload)
            self.assertEqual(file_sha256(path), hashlib.sha256(payload).hexdigest())

    def test_decode_time_must_be_measured_and_summarized(self):
        for value in ("", "-1", "nan"):
            self.assertFalse(parse_measurement(output(decode_us=value), 0,
                                              "narrow", "admit")["success"])
        measured = parse_measurement(output(), 0, "narrow", "admit")
        summary = summarize([dict(round=0, workload="narrow", admission="admit", **measured)])
        self.assertEqual(summary["groups"][0]["decode_us"], {"median": 7, "min": 7, "max": 7})

    def test_five_round_pairs_alternate_without_losing_workloads(self):
        runs = plan_runs(5, ["point", "temporal"])
        self.assertEqual(len(runs), 20)
        self.assertEqual([r["admission"] for r in runs[:4]],
                         ["lookup-only", "admit", "lookup-only", "admit"])
        self.assertEqual([r["admission"] for r in runs[4:8]],
                         ["admit", "lookup-only", "admit", "lookup-only"])
        for workload in ("point", "temporal"):
            for admission in ("lookup-only", "admit"):
                self.assertEqual(sum(r["workload"] == workload and
                                     r["admission"] == admission for r in runs), 5)

    def test_bad_exit_or_incomplete_measurement_is_never_success(self):
        for stdout, returncode in ((output(), 1), (output(error="1"), 0),
                                   (output(operations="0"), 0),
                                   (output(rows="0"), 0), (output() + output(), 0),
                                   (output(p99_us="nan"), 0), ("", 0)):
            result = parse_measurement(stdout, returncode, "narrow", "admit")
            self.assertFalse(result["success"], result)
        result = parse_measurement(output(), 0, "narrow", "admit")
        self.assertTrue(result["success"])
        self.assertEqual(result["row"]["device_read_bytes"], "")
        self.assertFalse(parse_measurement(output(), 0, "point", "admit")["success"])
        self.assertFalse(parse_measurement(output(), 0, "narrow", "lookup-only")["success"])

    def test_failures_stay_visible_in_summary_with_raw_round_indices(self):
        runs = []
        for index, latency in enumerate((10, 30, 20, 40, 50)):
            measured = parse_measurement(output(p99_us=str(latency)), 0, "narrow", "admit")
            runs.append(dict(round=index, workload="narrow", admission="admit", **measured))
        bad = parse_measurement(output(error="1"), 1, "narrow", "admit")
        runs.append(dict(round=5, workload="narrow", admission="admit", **bad))
        summary = summarize(runs)
        group = summary["groups"][0]
        self.assertEqual(group["attempts"], 6)
        self.assertEqual(group["successes"], 5)
        self.assertEqual(group["failures"], 1)
        self.assertEqual(group["p99_us"], {"median": 30, "min": 10, "max": 50})
        self.assertEqual(summary["failed_rounds"], [5])

    def test_warm_interval_requires_prior_decode_evidence(self):
        row = dict(measurement_phase="after_warmup", warmup_operations="1",
                   warmup_file_read_calls="3", warmup_column_decodes="2",
                   file_read_calls="0", column_decodes="0", cache_hits="50")
        self.assertTrue(parse_measurement(output(**row), 0, "narrow", "admit")["success"])
        for key in ("warmup_operations", "warmup_file_read_calls", "warmup_column_decodes"):
            self.assertFalse(parse_measurement(output(**dict(row, **{key: "0"})),
                                              0, "narrow", "admit")["success"])
        self.assertFalse(parse_measurement(output(**dict(row, cache_hits="0")),
                                          0, "narrow", "admit")["success"])


if __name__ == "__main__":
    unittest.main()
