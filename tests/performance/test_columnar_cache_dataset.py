"""End-to-end contract for a dedicated, persisted cache benchmark dataset."""

import csv
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


# One benchmark invocation (setup or read) took ~95 s under ASan, which made the
# hard-coded 90 s subprocess budget fail deterministically. Keep the historical
# default unchanged, but scale it when a sanitizer runtime is active.
BENCH_TIMEOUT_SECONDS = 90
SANITIZER_TIMEOUT_SCALE = 3


def bench_timeout_seconds():
    """Subprocess timeout for a single benchmark invocation."""
    scale = float(os.environ.get("CEDAR_TEST_TIMEOUT_SCALE") or "1")
    if scale == 1 and (os.environ.get("ASAN_OPTIONS") or os.environ.get("UBSAN_OPTIONS")):
        scale = SANITIZER_TIMEOUT_SCALE
    return BENCH_TIMEOUT_SECONDS * scale


class ColumnarCacheDatasetTest(unittest.TestCase):
    def test_setup_accepts_query_budget_above_default_pool(self):
        with tempfile.TemporaryDirectory(prefix="cedar-cache-large-budget-") as root:
            result = self.run_bench("--db-path", str(Path(root) / "dataset"),
                                    "--entities", "12", "--setup-only",
                                    "--query-memory-bytes", "536870912")
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["canonical_rows"], 36)
            self.assertEqual(report["state_rows"], 12)

    def test_dataset_spans_bounded_commits(self):
        with tempfile.TemporaryDirectory(prefix="cedar-cache-batched-") as root:
            common = ["--db-path", str(Path(root) / "dataset"), "--entities", "20001",
                      "--seed", "42", "--part-id", "7"]
            setup = self.run_bench(*common, "--setup-only",
                                   "--query-memory-bytes", "268435456")
            self.assertEqual(setup.returncode, 0, setup.stderr)
            report = json.loads(setup.stdout)
            self.assertEqual(report["canonical_rows"], 60003)
            # Two commits per version: first 20000 entities have seq 1/3/5,
            # last entity has seq 2/4/6. Independent arithmetic oracle.
            expected = (20001 * 20002 // 2) * 393 + 20001 * (186 * 17 + 21) + 7 * (20000 * 9 + 12)
            self.assertEqual(report["checksum"], expected)
            for workload in ("point", "narrow", "temporal"):
                read = self.run_bench(*common, "--read-existing", "--workload", workload)
                self.assertEqual(read.returncode, 0, read.stderr)

    def test_setup_validation_uses_explicit_query_budget(self):
        with tempfile.TemporaryDirectory(prefix="cedar-cache-setup-budget-") as root:
            result = self.run_bench("--db-path", str(Path(root) / "dataset"),
                                    "--entities", "12", "--setup-only",
                                    "--query-memory-bytes", "1")
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("ResourceExhausted", result.stderr)
            self.assertFalse((Path(root) / "dataset" /
                              "cedar-cache-bench-manifest.json").exists())

    def run_bench(self, *arguments):
        return subprocess.run([os.environ["CEDAR_CACHE_BENCH"], *arguments],
                              text=True, capture_output=True,
                              timeout=bench_timeout_seconds())

    def test_benchmark_must_observe_parquet_reads(self):
        with tempfile.TemporaryDirectory(prefix="cedar-cache-bench-contract-") as root:
            dataset = Path(root) / "dataset"
            common = ["--db-path", str(dataset), "--entities", "12", "--seed", "42",
                      "--part-id", "7"]
            setup = self.run_bench(*common, "--setup-only")
            self.assertEqual(setup.returncode, 0, setup.stderr)
            report = json.loads(setup.stdout)
            self.assertEqual(report["canonical_rows"], 36)
            self.assertEqual(report["state_rows"], 12)
            self.assertGreater(report["parquet_files"], 0)
            self.assertGreater(report["row_groups"], 0)
            self.assertGreater(report["column_pages"], 0)
            self.assertTrue(report["recovery_warms_system_cache"])
            before = {p.name: (p.stat().st_size, p.stat().st_mtime_ns)
                      for p in dataset.iterdir() if p.is_file()}
            read = self.run_bench(*common, "--read-existing", "--workload", "wide",
                                  "--cache-admission", "admit")
            self.assertEqual(read.returncode, 0, read.stderr)
            rows = list(csv.DictReader(io.StringIO(read.stdout)))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(int(row["error"]), 0)
            self.assertGreater(int(row["operations"]), 0)
            self.assertEqual(int(row["rows"]), int(row["operations"]) * 36)
            self.assertGreater(int(row["file_read_calls"]), 0)
            self.assertGreater(int(row["column_decodes"]), 0)
            self.assertGreater(int(row["decode_us"]), 0)
            self.assertGreater(int(row["cache_hits"]), 0)
            self.assertEqual(row["cache_state"], "cold_unverified")
            for workload, expected_per_operation in (("point", 3), ("narrow", 36),
                                                       ("temporal", 12), ("mixed", None)):
                result = self.run_bench(*common, "--read-existing", "--workload", workload,
                                        "--cache-admission", "lookup-only", "--sessions", "2")
                self.assertEqual(result.returncode, 0, result.stderr)
                measured = list(csv.DictReader(io.StringIO(result.stdout)))[0]
                self.assertEqual(int(measured["error"]), 0)
                self.assertGreater(int(measured["operations"]), 0)
                self.assertGreater(int(measured["file_read_calls"]), 0)
                self.assertEqual(int(measured["resident_bytes"]), 0)
                if expected_per_operation is not None:
                    self.assertEqual(int(measured["rows"]),
                                     int(measured["operations"]) * expected_per_operation)
            after = {p.name: (p.stat().st_size, p.stat().st_mtime_ns)
                     for p in dataset.iterdir() if p.is_file()}
            self.assertEqual(before, after, "read-existing must not alter dataset files")
            retry = self.run_bench(*common, "--setup-only")
            self.assertNotEqual(retry.returncode, 0)
            mismatch = self.run_bench(*common, "--read-existing", "--seed", "43")
            self.assertNotEqual(mismatch.returncode, 0)
            for capacity in ("--storage-read-working-bytes", "--query-memory-bytes"):
                exhausted = self.run_bench(*common, "--read-existing", capacity, "1")
                self.assertNotEqual(exhausted.returncode, 0, exhausted.stdout)
                failure = list(csv.DictReader(io.StringIO(exhausted.stdout)))[0]
                self.assertGreater(int(failure["error"]), 0)
            warmed = self.run_bench(*common, "--read-existing", "--warmup-operations", "1",
                                    "--cache-admission", "admit", "--workload", "narrow")
            self.assertEqual(warmed.returncode, 0, warmed.stderr)
            warm_row = list(csv.DictReader(io.StringIO(warmed.stdout)))[0]
            self.assertEqual(warm_row["measurement_phase"], "after_warmup")
            self.assertGreater(int(warm_row["warmup_file_read_calls"]), 0)
            self.assertGreater(int(warm_row["warmup_column_decodes"]), 0)
            self.assertEqual(int(warm_row["file_read_calls"]), 0)
            self.assertEqual(int(warm_row["column_decodes"]), 0)
            self.assertEqual(int(warm_row["decode_us"]), 0)
            self.assertGreater(int(warm_row["cache_hits"]), 0)
            disabled = self.run_bench(*common, "--read-existing", "--cache-bytes", "0",
                                     "--cache-admission", "admit", "--workload", "narrow")
            self.assertEqual(disabled.returncode, 0, disabled.stderr)
            self.assertEqual(int(list(csv.DictReader(io.StringIO(disabled.stdout)))[0]["resident_bytes"]), 0)
            self.assertEqual(after, {p.name: (p.stat().st_size, p.stat().st_mtime_ns)
                                    for p in dataset.iterdir() if p.is_file()})

    def test_explicit_reader_metadata_reaches_both_measurement_paths(self):
        with tempfile.TemporaryDirectory(prefix="cedar-cache-metadata-pool-") as root:
            root = Path(root)
            dataset = root / "dataset"
            common = ["--db-path", str(dataset), "--entities", "12", "--seed", "42",
                      "--part-id", "7"]
            setup = self.run_bench(*common, "--setup-only")
            self.assertEqual(setup.returncode, 0, setup.stderr)
            tiny = self.run_bench(*common, "--read-existing", "--workload", "wide",
                                  "--storage-reader-metadata-bytes", "1")
            self.assertNotEqual(tiny.returncode, 0,
                                "an explicit reader-metadata pool must reach the read-only path")
            self.assertIn("metadata budget exhausted", tiny.stderr)
            for explicit in ((), ("--storage-reader-metadata-bytes", "16777216")):
                read = self.run_bench(*common, "--read-existing", "--workload", "wide",
                                      *explicit)
                self.assertEqual(read.returncode, 0, read.stderr)

    def test_existing_missing_path_never_creates_database(self):
        with tempfile.TemporaryDirectory(prefix="cedar-cache-bench-missing-") as root:
            dataset = Path(root) / "missing"
            result = self.run_bench("--db-path", str(dataset), "--read-existing")
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(dataset.exists())


if __name__ == "__main__":
    unittest.main()
